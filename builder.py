import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
import re
from datetime import datetime
from urllib.parse import urljoin

# --- KONFIGURATION ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

ua = UserAgent()

# Bekannte Kategorien aus dem Quelltext der Stadt (für Abgleich)
KNOWN_CATEGORIES = [
    "Ausstellung", "Ball", "Fest", "Film", "Gesundheit", "Jugend", 
    "Kirche", "Kulinarisches", "Kurs", "Markt", "Musik", "Sport", 
    "Theater", "Vortrag", "Lesung", "Kinder", "Schule"
]

def get_random_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Neue Spalte: category
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            url TEXT PRIMARY KEY,
            title TEXT,
            category TEXT, 
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

def extract_category_from_text(text):
    """Sucht nach 'Kategorie: ...' im Detailtext."""
    match = re.search(r'Kategorie:\s*([^\n\r]+)', text)
    if match:
        return match.group(1).strip()
    return None

def guess_category_from_title(title):
    """Prüft, ob der Titel wie 'Ausstellung: ...' beginnt."""
    if ":" in title:
        prefix = title.split(":")[0].strip()
        # Prüfen, ob das Prefix sinnvol macht (Teil der bekannten Liste ist)
        # Wir prüfen unscharf (z.B. "Ausstellung" in "Sonderausstellung")
        for cat in KNOWN_CATEGORIES:
            if cat.lower() in prefix.lower():
                return prefix
    return "Event" # Fallback

def scrape_details(url):
    # ... (Code wie vorher, nur Rückgabewert erweitert)
    time.sleep(random.uniform(1, 3))
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "Fehler", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.select_one('main') or soup.body
        full_text = content_div.get_text(separator="\n", strip=True)
        
        # Versuch 1: Kategorie aus Detailtext holen
        extracted_cat = extract_category_from_text(full_text)
        
        images = []
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src and "data:image" not in src and "dummy.gif" not in src:
                images.append(urljoin(BASE_URL, src))
                
        return full_text, extracted_cat, list(set(images))
    except Exception as e:
        return "", None, []

def main():
    print(f"--- START EVKO SCRAPER MIT KATEGORIEN ---")
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
                relative_url = link_tag['href']
                full_url = urljoin(BASE_URL, relative_url)
                location = cells[2].get_text(strip=True)
                
                # Hash Logik...
                fingerprint = f"{title}{raw_date}{location}"
                new_hash = make_hash(fingerprint)
                
                c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
                db_row = c.fetchone()
                
                if db_row and db_row[0] == new_hash: continue
                
                print(f"  [NEU] {title}")
                
                # Details laden + Kategorie suchen
                desc, detail_cat, imgs = scrape_details(full_url)
                
                # Entscheidung: Detail-Kategorie > Titel-Kategorie > Fallback
                final_category = detail_cat if detail_cat else guess_category_from_title(title)
                
                c.execute('''
                    INSERT INTO events (url, title, category, date_str, location, description, image_urls, content_hash, last_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, category=excluded.category, date_str=excluded.date_str, location=excluded.location,
                        description=excluded.description, image_urls=excluded.image_urls,
                        content_hash=excluded.content_hash, last_scraped=excluded.last_scraped
                ''', (full_url, title, final_category, raw_date, location, desc, ",".join(imgs), new_hash, datetime.now()))
                conn.commit()

            next_link = soup.select_one('a[rel="Next"]')
            if next_link:
                current_url = urljoin(BASE_URL, next_link['href'])
                page_count += 1
                time.sleep(2)
            else:
                current_url = None

        except Exception as e:
            print(f"Fehler: {e}")
            break

    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__":
    main()
