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

# --- KONFIGURATION ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

# Initialisiere Fake UserAgent
ua = UserAgent()

def get_random_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Referer': 'https://www.google.com/'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            url TEXT PRIMARY KEY,
            title TEXT,
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

def scrape_details(url):
    print(f"    └── Lade Details...")
    time.sleep(random.uniform(1, 3)) # Kurze Pause
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "Fehler", ""
        soup = BeautifulSoup(response.content, 'html.parser')
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.select_one('main') or soup.body
        full_text = content_div.get_text(separator="\n", strip=True)
        images = []
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src and "data:image" not in src and "dummy.gif" not in src:
                images.append(urljoin(BASE_URL, src))
        return full_text, list(set(images))
    except Exception as e:
        print(f"    Warnung Details: {e}")
        return "", []

def main():
    print(f"--- START EVKO SCRAPER: {datetime.now()} ---")
    conn = init_db()
    c = conn.cursor()
    
    current_url = START_URL
    page_count = 1
    max_pages = 20 # Sicherheits-Limit

    while current_url and page_count <= max_pages:
        print(f"\nScrape Seite {page_count}: {current_url}")
        
        try:
            response = requests.get(current_url, headers=get_random_header())
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.select_one('table.vazusatzinfo_tabelle')
            
            if not table:
                print("  Keine Tabelle auf dieser Seite gefunden.")
                break

            rows = table.find_all('tr')
            events_on_page = 0
            
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
                
                # Hash Check
                fingerprint = f"{title}{raw_date}{location}"
                new_hash = make_hash(fingerprint)
                
                c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
                db_row = c.fetchone()
                
                if db_row and db_row[0] == new_hash:
                    # Event existiert und ist gleich -> Skip Detail Scraping
                    # Wir loggen das nicht mehr, um die Konsole sauber zu halten
                    continue
                
                print(f"  [NEU/UPDATE] {title} ({raw_date})")
                
                # Nur wenn neu: Details laden
                desc, imgs = scrape_details(full_url)
                
                c.execute('''
                    INSERT INTO events (url, title, date_str, location, description, image_urls, content_hash, last_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, date_str=excluded.date_str, location=excluded.location,
                        description=excluded.description, image_urls=excluded.image_urls,
                        content_hash=excluded.content_hash, last_scraped=excluded.last_scraped
                ''', (full_url, title, raw_date, location, desc, ",".join(imgs), new_hash, datetime.now()))
                conn.commit()
                events_on_page += 1
            
            print(f"  -> {events_on_page} Events aktualisiert/hinzugefügt.")

            # --- PAGINATION LOGIK ---
            # Suche nach dem "Weiter" Button (meist '>' oder 'Next')
            # Im HTML Code: <a rel="Next" ...>
            next_link = soup.select_one('a[rel="Next"]')
            
            if next_link:
                next_href = next_link['href']
                current_url = urljoin(BASE_URL, next_href)
                page_count += 1
                # Kurze Pause vor der nächsten Seite (Server schonen)
                time.sleep(random.uniform(2, 4))
            else:
                print("Keine weitere Seite gefunden. Fertig.")
                current_url = None

        except Exception as e:
            print(f"Fehler auf Seite {page_count}: {e}")
            break

    conn.close()
    print("--- SCRAPER ENDE ---")

if __name__ == "__main__":
    main()
