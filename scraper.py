import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
import argparse 
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import openai 

# --- 1. SETUP & GEHEIMNISSE ---
# Der Key wird automatisch aus GitHub Secrets oder der lokalen Umgebung geladen
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if OPENAI_API_KEY:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
else:
    client = None
    print("⚠️ WARNUNG: Kein OPENAI_API_KEY gefunden. Vision-Feature ist inaktiv.")

# --- 2. FESTE KONFIGURATION ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

# Tags, die direkt aus dem Titel übernommen werden
TITLE_TAG_WHITELIST = [
    "Shopping-Event", "Kultur- und Musiktage", "Kabarett-Picknick", "Werftbühne",
    "Ausstellung", "Sonderausstellung", "Vernissage", "Lesung", "Konzert", 
    "Flohmarkt", "Kindermaskenball"
]

# Text, der aus der Untertitel-Zeile entfernt wird
SUBTITLE_REMOVE_LIST = [
    "Veranstaltungen - Rathaus", "Veranstaltungen - Stadt", "Veranstaltungen -"
]

# Wir täuschen vor, von diesen Seiten zu kommen (gegen Bot-Schutz)
REFERER_LIST = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://www.wix.com/",
    "https://duckduckgo.com/",
    "https://www.facebook.com/"
]

ua = UserAgent()

def get_random_header():
    """Erstellt einen Header, der wie ein echter Browser-Nutzer aussieht."""
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': random.choice(REFERER_LIST),
        'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            url TEXT PRIMARY KEY,
            title TEXT,
            tags TEXT, 
            date_str TEXT,
            location TEXT,
            description TEXT,
            image_urls TEXT,
            content_hash TEXT,
            last_scraped TIMESTAMP
        )
    ''')
    conn.commit()
    return conn

def make_hash(data_string):
    return hashlib.md5(data_string.encode('utf-8')).hexdigest()

def clean_tag_line(raw_text):
    if not raw_text: return set()
    text = raw_text
    for remove_phrase in SUBTITLE_REMOVE_LIST:
        text = text.replace(remove_phrase, "")
    parts = text.split(",")
    cleaned = set()
    for p in parts:
        tag = p.strip()
        if len(tag) > 2: cleaned.add(tag)
    return cleaned

def get_tags_from_title(title):
    found = set()
    if ":" in title:
        prefix = title.split(":")[0].strip()
        for allowed_tag in TITLE_TAG_WHITELIST:
            if allowed_tag.lower() == prefix.lower():
                found.add(allowed_tag)
                break
    return found

def analyze_image_content(image_url):
    """Sendet das Bild an OpenAI GPT-4o-mini zur Analyse."""
    if not client: return ""
    try:
        print(f"    --> 🤖 Start AI Vision Analyse: {image_url[-30:]}...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Analysiere dieses Veranstaltungsplakat. Extrahiere alle harten Fakten, die im Fließtext fehlen könnten: Genaue Uhrzeiten (Einlass vs Beginn), Preise/Eintrittskosten, Zusatzinfos (Essen, Tombola), Kontaktnummern, Veranstalter. Fasse dich kurz und listenartig."},
                        {"type": "image_url", "image_url": {"url": image_url, "detail": "low"}},
                    ],
                }
            ],
            max_tokens=300,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"    ⚠️ [Vision Error] {e}")
        return ""

def fix_korneuburg_url(url):
    """Repariert Korneuburg GetImage.ashx URLs durch Erzwingen von Parametern."""
    if "GetImage.ashx" not in url:
        return url
    
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    # Standard-Parameter, die oft fehlen, aber nötig sind
    defaults = {
        'mode': 'T',
        'height': '600',
        'width': '800',
        'cropping': 'NONE'
    }
    
    changed = False
    for key, val in defaults.items():
        if key not in query_params:
            query_params[key] = [val]
            changed = True
            
    if changed:
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
        # print(f"      🔧 URL repariert: {new_url[-30:]}")
        return new_url
        
    return url

def get_best_image_url(container, base_url, context=""):
    """Hilfsfunktion: Holt URL aus img src, data-src oder source srcset."""
    raw_url = None
    
    # 1. IMG Tag Check
    img = container if container.name == 'img' else container.find('img')
    if img:
        if img.get('data-src'):
            raw_url = img.get('data-src')
        elif img.get('src') and "data:image" not in img.get('src'):
            raw_url = img.get('src')

    # 2. Picture Tag Check (Source Set)
    if not raw_url and (container.name == 'picture' or container.find('picture')):
        pic = container if container.name == 'picture' else container.find('picture')
        source = pic.find('source')
        if source and source.get('srcset'):
            # Nimmt das erste Bild aus dem Set
            raw_url = source.get('srcset').split(',')[0].split(' ')[0]

    if raw_url:
        # Dummy-Bilder filtern
        if "dummy" in raw_url or "pixel" in raw_url:
            return None
            
        full_url = urljoin(base_url, raw_url)
        return fix_korneuburg_url(full_url)
            
    return None

def scrape_details(url, title):
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.body
        
        # Tags sammeln
        all_tags = get_tags_from_title(title)
        tag_elem = soup.select_one('small.d-block.text-muted')
        if tag_elem:
            all_tags.update(clean_tag_line(tag_elem.get_text(strip=True)))
        final_tags_str = ", ".join(sorted(list(all_tags)))
        
        images = []
        target_image_url = None
        
        # --- BILDER FINDEN ---
        
        # STRATEGIE 1: Open Graph Meta Tag (Gold Standard)
        og_img = soup.select_one('meta[property="og:image"]')
        if og_img and og_img.get('content'):
            raw_og = og_img.get('content')
            if "dummy" not in raw_og:
                full_og = urljoin(BASE_URL, raw_og)
                target_image_url = fix_korneuburg_url(full_og)
                images.append(target_image_url)
                print(f"    🏆 OG-Image gefunden: {target_image_url[-30:]}")

        # STRATEGIE 2: Container Suche (Fallback)
        if not target_image_url:
            container = content_div.select_one('.bemTextImageContainer')
            if container:
                target_image_url = get_best_image_url(container, BASE_URL, "Container")
                if target_image_url:
                    images.append(target_image_url)
                    print(f"    👀 Container-Bild gefunden.")

        # STRATEGIE 3: Sammeln aller Bilder für Galerie (Fallback Scan)
        found_imgs = content_div.find_all('img')
        for img in found_imgs:
            cand = get_best_image_url(img, BASE_URL, "Scan")
            if cand:
                images.append(cand)
                # Letzter Notnagel: Das erste gefundene Bild nehmen
                if not target_image_url: 
                    target_image_url = cand
        
        # Duplikate entfernen
        images = list(set(images))

        # --- VISION API ---
        vision_text = ""
        if target_image_url and client:
            # Check auf gültige Formate/Handler
            if any(x in target_image_url for x in [".jpg", ".png", "GetImage.ashx", "files"]):
                vision_info = analyze_image_content(target_image_url)
                if vision_info:
                    vision_text = f"\n\n--- ZUSATZINFO AUS PLAKAT ---\n{vision_info}"
            else:
                print(f"    🚫 Bildformat ungültig für AI: {target_image_url}")
        elif not target_image_url:
            print("    ❌ Kein geeignetes Bild für AI Analyse gefunden.")

        full_text = content_div.get_text(separator="\n", strip=True) if content_div else ""
        final_description = full_text + vision_text
        
        return final_description, final_tags_str, images
        
    except Exception as e:
        print(f"Fehler Details bei {url}: {e}")
        return "", "", []

def main():
    parser = argparse.ArgumentParser(description="EVKO Scraper")
    parser.add_argument("-test", action="store_true", help="Nur Seite 1 scrapen für Tests")
    args = parser.parse_args()

    mode_text = "TEST MODUS (Nur Seite 1)" if args.test else "VOLL MODUS (Alle Seiten)"
    print(f"--- START EVKO SCRAPER [{mode_text}] ---")

    conn = init_db()
    c = conn.cursor()
    
    current_url = START_URL
    page_count = 1
    max_pages = 1 if args.test else 20
    
    while current_url and page_count <= max_pages:
        print(f"\nScrape Seite {page_count}...")
        try:
            response = requests.get(current_url, headers=get_random_header())
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.select_one('table.vazusatzinfo_tabelle')
            
            if not table: break
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 3: continue 
                
                raw_date = cells[0].get_text(strip=True)
                link_tag = cells[1].find('a')
                if not link_tag: continue
                
                title = link_tag.get_text(strip=True)
                full_url = urljoin(BASE_URL, link_tag['href'])
                location = cells[2].get_text(strip=True)
                
                fingerprint = f"{title}{raw_date}{location}"
                new_hash = make_hash(fingerprint)
                
                # --- HASH CHECK (AKTUELL DEAKTIVIERT FÜR UPDATES) ---
                # c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
                # db_row = c.fetchone()
                # if db_row and db_row[0] == new_hash: continue
                
                print(f"  [UPDATE] {title}")
                desc, tags, imgs = scrape_details(full_url, title)
                
                c.execute('''
                    INSERT INTO events (url, title, tags, date_str, location, description, image_urls, content_hash, last_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, location=excluded.location,
                        description=excluded.description, image_urls=excluded.image_urls,
                        content_hash=excluded.content_hash, last_scraped=excluded.last_scraped
                ''', (full_url, title, tags, raw_date, location, desc, ",".join(imgs), new_hash, datetime.now()))
                conn.commit()

            next_link = soup.select_one('a[rel="Next"]')
            if next_link:
                current_url = urljoin(BASE_URL, next_link['href'])
                page_count += 1
            else:
                current_url = None

        except Exception as e:
            print(f"Fehler: {e}")
            break

    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__":
    main()