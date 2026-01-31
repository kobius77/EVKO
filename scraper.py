import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
import argparse 
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import openai 

# --- 1. SETUP ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if OPENAI_API_KEY:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
else:
    client = None

# --- 2. CONFIG ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

TITLE_TAG_WHITELIST = ["Shopping-Event", "Kultur- und Musiktage", "Kabarett-Picknick", "Werftbühne", "Ausstellung", "Sonderausstellung", "Vernissage", "Lesung", "Konzert", "Flohmarkt", "Kindermaskenball"]
SUBTITLE_REMOVE_LIST = ["Veranstaltungen - Rathaus", "Veranstaltungen - Stadt", "Veranstaltungen -"]
REFERER_LIST = ["https://www.google.com/", "https://www.bing.com/", "https://www.wix.com/", "https://duckduckgo.com/"]
ua = UserAgent()

def get_random_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': random.choice(REFERER_LIST),
        'Accept-Language': 'de-DE,de;q=0.9'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # NEU: Spalte 'time_str' hinzugefügt
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            url TEXT PRIMARY KEY,
            title TEXT,
            tags TEXT, 
            date_str TEXT,
            start_iso TEXT,
            time_str TEXT, 
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

def parse_german_date(date_text):
    try:
        clean_date = re.search(r'\d{2}\.\d{2}\.\d{4}', date_text)
        if clean_date:
            dt = datetime.strptime(clean_date.group(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d") 
    except Exception:
        return None 
    return None

def clean_tag_line(raw_text):
    if not raw_text: return set()
    text = raw_text
    for remove_phrase in SUBTITLE_REMOVE_LIST: text = text.replace(remove_phrase, "")
    parts = text.split(",")
    return {p.strip() for p in parts if len(p.strip()) > 2}

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
    if not client: return ""
    
    # Blockliste für typische "Versager"-Sätze der KI
    REFUSAL_PHRASES = [
        "tut mir leid", "kann das veranstaltungsplakat nicht", 
        "kann das bild nicht", "keine veranstaltungsdetails", 
        "keine informationen", "entschuldigung", "kann text nicht",
        "keine lesbaren", "bild enthält keine"
    ]

    try:
        print(f"    --> 🤖 AI Vision: {image_url[-35:]}...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user", 
                    "content": [
                        {"type": "text", "text": "Extrahiere Fakten vom Plakat (Datum, Zeit, Preis, Ort). Wenn das Bild KEIN Plakat ist oder KEINEN Text enthält, antworte NUR mit dem Wort 'SKIP'. Sei sonst präzise und kurz."}, 
                        {"type": "image_url", "image_url": {"url": image_url, "detail": "low"}}
                    ]
                }
            ],
            max_tokens=300,
        )
        content = response.choices[0].message.content.strip()
        
        # 1. Check auf unser Codewort "SKIP"
        if "SKIP" in content:
            print("    🚫 AI sagt: Kein Plakat/Text erkannt.")
            return ""

        # 2. Check auf höfliche Absagen (Falls die AI 'SKIP' ignoriert hat)
        content_lower = content.lower()
        if any(phrase in content_lower for phrase in REFUSAL_PHRASES):
            print("    🚫 AI sagt: Kann nicht lesen (Verworfen).")
            return ""

        return content

    except Exception as e:
        print(f"    ⚠️ AI Error: {e}")
        return ""

def fix_korneuburg_url(url):
    if "GetImage.ashx" not in url: return url
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    defaults = {'mode': 'T', 'height': '600', 'width': '800', 'cropping': 'NONE'}
    changed = False
    for k, v in defaults.items():
        q[k] = [v]; changed = True
    if changed:
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(q, doseq=True), parsed.fragment))
    return url

def get_best_image_url(container, base_url):
    raw = None
    img = container if container.name == 'img' else container.find('img')
    if img:
        raw = img.get('data-src') or (img.get('src') if "data:" not in img.get('src', '') else None)
    
    if not raw and (container.name == 'picture' or container.find('picture')):
        pic = container if container.name == 'picture' else container.find('picture')
        src = pic.find('source')
        if src and src.get('srcset'): raw = src.get('srcset').split(',')[0].split(' ')[0]

    if raw and "dummy" not in raw and "pixel" not in raw:
        return fix_korneuburg_url(urljoin(base_url, raw))
    return None

def scrape_details(url, title):
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.body
        
        tags = get_tags_from_title(title)
        t_elem = soup.select_one('small.d-block.text-muted')
        if t_elem: tags.update(clean_tag_line(t_elem.get_text(strip=True)))
        
        # --- UHRZEIT EXTRAHIEREN (NEU) ---
        time_str = ""
        # Wir suchen den Container aus Ihrem Screenshot
        time_container = content_div.select_one('.bemContainer--appointmentInfo .bemContainer--time')
        
        if time_container:
            # WICHTIG: Wir entfernen versteckte Screenreader-Texte ("Uhrzeit der Veranstaltung")
            # Wir arbeiten an einer Kopie, um das Original-HTML für andere Scrapes nicht zu zerstören
            import copy
            container_copy = copy.copy(time_container)
            
            # Lösche alle Elemente mit Klasse 'sr-only' aus der Kopie
            for sr in container_copy.select('.sr-only'):
                sr.decompose()
            
            # Hole den sauberen Text
            time_str = container_copy.get_text(separator=" ", strip=True)
            # print(f"    🕒 Uhrzeit gefunden: {time_str}")

        # --- BILDER ---
        images = []
        target_img = None
        
        og_img = soup.select_one('meta[property="og:image"]')
        if og_img and og_img.get('content') and "dummy" not in og_img.get('content'):
            target_img = fix_korneuburg_url(urljoin(BASE_URL, og_img.get('content')))
            images.append(target_img)

        if not target_img:
            cont = content_div.select_one('.bemTextImageContainer')
            if cont:
                target_img = get_best_image_url(cont, BASE_URL)
                if target_img: images.append(target_img)

        for img in content_div.find_all('img'):
            cand = get_best_image_url(img, BASE_URL)
            if cand:
                images.append(cand)
                if not target_img: target_img = cand
        
        images = list(set(images))

        # Vision
        vision_text = ""
        if target_img and client:
            if any(x in target_img for x in [".jpg", ".png", "GetImage.ashx"]):
                info = analyze_image_content(target_img)
                if info: vision_text = f"\n\n--- ZUSATZINFO AUS PLAKAT ---\n{info}"
        
        full_text = content_div.get_text(separator="\n", strip=True) if content_div else ""
        
        # Rückgabe erweitert um time_str
        return full_text + vision_text, ", ".join(sorted(list(tags))), images, time_str
        
    except Exception as e:
        print(f"Error {url}: {e}"); return "", "", [], ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-test", action="store_true", help="Nur Seite 1 scrapen")
    args = parser.parse_args()

    print(f"--- EVKO SCRAPER [{'TEST' if args.test else 'FULL'}] ---")
    conn = init_db()
    c = conn.cursor()
    
    curr = START_URL
    p_cnt = 1
    max_p = 1 if args.test else 20
    
    while curr and p_cnt <= max_p:
        print(f"\nSeite {p_cnt}...")
        try:
            r = requests.get(curr, headers=get_random_header())
            soup = BeautifulSoup(r.content, 'html.parser')
            tbl = soup.select_one('table.vazusatzinfo_tabelle')
            if not tbl: break
            
            for row in tbl.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 3: continue 
                
                raw_date = cells[0].get_text(strip=True) 
                iso_date = parse_german_date(raw_date)   
                
                link = cells[1].find('a')
                if not link: continue
                title = link.get_text(strip=True)
                url = urljoin(BASE_URL, link['href'])
                loc = cells[2].get_text(strip=True)
                
                h = make_hash(f"{title}{raw_date}{loc}")
                
                print(f"  [UPDATE] {title}")
                # Entpacken inkl. time_str
                desc, t_str, imgs, time_val = scrape_details(url, title)
                
                c.execute('''
                    INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso,
                        time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                        image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped
                ''', (url, title, t_str, raw_date, iso_date, time_val, loc, desc, ",".join(imgs), h, datetime.now()))
                conn.commit()

            nxt = soup.select_one('a[rel="Next"]')
            curr = urljoin(BASE_URL, nxt['href']) if nxt else None
            p_cnt += 1
        except Exception as e:
            print(e); break
    
    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__":
    main()
