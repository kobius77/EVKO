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

TITLE_TAG_WHITELIST = [
    "Shopping-Event", "Kultur- und Musiktage", "Kabarett-Picknick", "Werftbühne",
    "Ausstellung", "Sonderausstellung", "Vernissage", "Lesung", "Konzert", 
    "Flohmarkt", "Kindermaskenball"
]

SUBTITLE_REMOVE_LIST = [
    "Veranstaltungen - Rathaus", "Veranstaltungen - Stadt", "Veranstaltungen -"
]

REFERER_LIST = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://www.wix.com/",
    "https://duckduckgo.com/",
    "https://www.facebook.com/"
]

ua = UserAgent()

def get_random_header():
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
    """Vision API Analyse."""
    if not client: return ""
    try:
        print(f"    --> 🤖 Start AI Vision Analyse: {image_url[-40:]}...")
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
    """
    Repariert und optimiert Korneuburg URLs.
    ERZWINGT Parameter für vollständige Bilder (kein Crop).
    """
    if "GetImage.ashx" not in url:
        return url
    
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    # Diese Parameter werden HART überschrieben, egal was vorher da stand
    force_params = {
        'mode': 'T',
        'height': '600',
        'width': '800',
        'cropping': 'NONE' # Das ist der Schlüssel gegen abgeschnittene Köpfe/Füße
    }
    
    # Wir iterieren und setzen die Werte gnadenlos neu
    for key, val in force_params.items():
        query_params[key] = [val]
            
    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    
    # Debug: Damit wir sehen, dass es klappt
    # if "cropping=NONE" in new_url: print(f"      🔧 Crop entfernt: {new_url[-30:]}")
    
    return new_url

def get_best_image_url(container, base_url, context=""):
    raw_url = None
    
    # IMG Tag Check
    img = container if container.name == 'img' else container.find('img')
    if img:
        if img.get('data-src'):
            raw_url = img.get('data-src')
        elif img.get('src') and "data:image" not in img.get('src'):
            raw_url = img.get('src')

    # Picture Tag Check
    if not raw_url and (container.name == 'picture' or container.find('picture')):
        pic = container if container.name == 'picture' else container.find('picture')
        source = pic.find('source')
        if source and source.get('srcset'):
            raw_url = source.get('srcset').split(',')[0].split(' ')[0]

    if raw_url:
        if "dummy" in raw_url or "pixel" in raw_url: return None
        full_url = urljoin(base_url, raw_url)
        # Auch hier: URL fixen!
        return fix_korneuburg_url(full_url)
            
    return None

def scrape_details(url, title):
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.body
        
        all_tags = get_tags_from_title(title)
        tag_elem = soup.select_one('small.d-block.text-muted')
        if tag_elem:
            all_tags.update(clean_tag_line(tag_elem.get_text(strip=True)))
        final_tags_str = ", ".join(sorted(list(all_tags)))
        
        images = []
        target_image_url = None
        
        # --- BILD STRATEGIEN ---
        
        # 1. OG Image (Priorität 1)
        og_img = soup.select_one('meta[property="og:image"]')
        if og_img and og_img.get('content'):
            raw_og = og_img.get('content')
            if "dummy" not in raw_og:
                full_og = urljoin(BASE_URL, raw_og)
                # Hier greift der Fix: Aus cropping=CENTER wird cropping=NONE
                target_image_url = fix_korneuburg_url(full_og)
                images.append(target_image_url)
                print(f"    🏆 OG-Image gefunden (Optimiert): {target_image_url[-30:]}")

        # 2. Container (Fallback)
        if not target_image_url:
            container = content_div.select_one('.bemTextImageContainer')
            if container:
                target_image_url = get_best_image_url(container, BASE_URL, "Container")
                if target_image_url:
                    images.append(target_image_url)

        # 3. Alle Bilder scannen (für Galerie)
        found_imgs = content_div.find_all('img')
        for img in found_imgs:
            cand = get_best_image_url(img, BASE_URL, "Scan")
            if cand:
                images.append(cand)
                if not target_image_url: target_image_url = cand
        
        images = list(set(images))

        # --- VISION ---
        vision_text = ""
        if target_image_url and client:
            if any(x in target_image_url for x in [".jpg", ".png", "GetImage.ashx", "files"]):
                vision_info = analyze_image_content(target_image_url)
                if vision_info:
                    vision_text = f"\n\n--- ZUSATZINFO AUS PLAKAT ---\n{vision_info}"
            else:
                print(f"    🚫 Bildformat ungültig: {target_image_url}")
        elif not target_image_url:
            print("    ❌ Kein Bild gefunden.")

        full_text = content_div.get_text(separator="\n", strip=True) if content_div else ""
        return full_text + vision_text, final_tags_str, images
        
    except Exception as e:
        print(f"Fehler bei {url}: {e}")
        return "", "", []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-test", action="store_true", help="Nur Seite 1 scrapen")
    args = parser.parse_args()

    print(f"--- START EVKO SCRAPER [{'TEST' if args.test else 'FULL'}] ---")

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
                
                # Hash Check disabled for Force-Update
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