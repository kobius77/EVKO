import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
from datetime import datetime
from urllib.parse import urljoin
import openai

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if OPENAI_API_KEY:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
else:
    client = None
    # print("INFO: Kein API-Key gefunden. Vision-Features sind inaktiv.")

# --- 2. FESTE KONFIGURATION (Hardcoded) ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

# Whitelist: Begriffs-Präfixe für den Titel
TITLE_TAG_WHITELIST = [
    "Shopping-Event",
    "Kultur- und Musiktage",
    "Kabarett-Picknick",
    "Werftbühne",
    "Ausstellung",
    "Sonderausstellung",
    "Vernissage",
    "Lesung",
    "Konzert",
    "Flohmarkt",
    "Kindermaskenball"
]

# Blacklist/Cleanup: Diese Phrasen werden aus der Untertitel-Zeile gelöscht
SUBTITLE_REMOVE_LIST = [
    "Veranstaltungen - Rathaus",
    "Veranstaltungen - Stadt",
    "Veranstaltungen -"
]

ua = UserAgent()

def get_random_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
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
    """Reinigt die Tag-Zeile basierend auf der Config-Liste."""
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
    """Vision API Call"""
    if not client: return ""
    
    try:
        print(f"    --> Analysiere Bild (Vision API): {image_url[-25:]}...")
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
        print(f"    [Vision Error] {e}")
        return ""

def scrape_details(url, title):
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.body
        
        # 1. Tags sammeln
        all_tags = get_tags_from_title(title)
        tag_elem = soup.select_one('small.d-block.text-muted')
        if tag_elem:
            all_tags.update(clean_tag_line(tag_elem.get_text(strip=True)))
            
        final_tags_str = ", ".join(sorted(list(all_tags)))
        
        # 2. Bilder & Vision
        images = []
        vision_text = ""
        found_imgs = content_div.find_all('img')
        
        for i, img in enumerate(found_imgs):
            src = img.get('src')
            if src and "data:image" not in src and "dummy.gif" not in src:
                full_url = urljoin(BASE_URL, src)
                images.append(full_url)
                
                # Vision nur beim ersten Bild, wenn Key da ist
                if i == 0 and client:
                    vision_info = analyze_image_content(full_url)
                    if vision_info:
                        vision_text = f"\n\n--- ZUSATZINFO AUS PLAKAT ---\n{vision_info}"

        full_text = content_div.get_text(separator="\n", strip=True)
        final_description = full_text + vision_text
                
        return final_description, final_tags_str, list(set(images))
        
    except Exception as e:
        print(f"Fehler Details: {e}")
        return "", "", []

def main():
    print(f"--- START EVKO SCRAPER ---")
    conn = init_db()
    c = conn.cursor()
    
    current_url = START_URL
    page_count = 1
    max_pages = 20
    
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
                
                # Hash Check (Optional: Auskommentieren für Force-Update)
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
