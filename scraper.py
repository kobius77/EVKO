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
    """Erzeugt einen zufälligen Browser-Header."""
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Referer': 'https://www.google.com/'
    }

def init_db():
    """Erstellt die SQLite Datenbank."""
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
    """Besucht die Detailseite."""
    print(f"  └── Lade Details: {url}")
    time.sleep(random.uniform(2, 4)) # "Slow Mode"
    
    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200:
            return "Fehler beim Laden", ""
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # RIS CMS speichert Inhalte meist in einem Container, der oft id="content" hat
        # oder in .main-content. Wir suchen generisch:
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.select_one('main')
        
        if not content_div:
            # Fallback: Body nehmen, aber Navigation entfernen (grob)
            content_div = soup.body
        
        # Text holen
        full_text = content_div.get_text(separator="\n", strip=True)
        
        # Bilder holen
        images = []
        # Wir filtern kleine Icons raus (RIS CMS hat viele kleine Icons)
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src:
                full_img_url = urljoin(BASE_URL, src)
                # Filter: Keine Base64 Bilder und keine winzigen Icons
                if "data:image" not in full_img_url and "dummy.gif" not in full_img_url:
                    images.append(full_img_url)
        
        return full_text, list(set(images)) # Duplikate entfernen
        
    except Exception as e:
        print(f"  Warnung: Details konnten nicht geladen werden ({e})")
        return "", []

def main():
    print(f"--- START EVKO SCRAPER: {datetime.now()} ---")
    conn = init_db()
    c = conn.cursor()
    
    print("Lade Listenansicht...")
    try:
        response = requests.get(START_URL, headers=get_random_header())
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # --- SELEKTOR LOGIK BASIEREND AUF DEM HTML ---
        # Die Tabelle hat die Klasse "vazusatzinfo_tabelle"
        table = soup.select_one('table.vazusatzinfo_tabelle')
        
        if not table:
            print("FEHLER: Tabelle 'vazusatzinfo_tabelle' nicht gefunden. Layout geändert?")
            return

        # Alle Zeilen (tr) holen. Die erste Zeile ist oft Header, aber wir prüfen auf 'td'
        rows = table.find_all('tr')
        print(f"Zeilen gefunden: {len(rows)}")

        for row in rows:
            cells = row.find_all('td')
            
            # Wir brauchen genau 3 Zellen: Datum, Veranstaltung(Link), Ort
            if len(cells) < 3:
                continue 
            
            # 1. Datum (Zelle 1)
            raw_date = cells[0].get_text(strip=True)
            
            # 2. Titel und Link (Zelle 2)
            link_tag = cells[1].find('a')
            if not link_tag:
                continue
            
            title = link_tag.get_text(strip=True)
            relative_link = link_tag['href']
            full_url = urljoin(BASE_URL, relative_link)
            
            # 3. Ort (Zelle 3)
            location = cells[2].get_text(strip=True)

            # --- VERARBEITUNG ---
            
            # Hash für Änderungsprüfung (Titel + Datum + Ort)
            fingerprint = f"{title}{raw_date}{location}"
            new_hash = make_hash(fingerprint)
            
            # Prüfen ob Eintrag existiert
            c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
            db_row = c.fetchone()
            
            if db_row and db_row[0] == new_hash:
                # print(f"Skipping: {title}") # Um Log sauber zu halten, auskommentiert
                continue
            
            print(f"Verarbeite: {title} ({raw_date})")
            
            # Details laden
            desc, imgs = scrape_details(full_url)
            img_str = ",".join(imgs)
            
            # Speichern (Upsert)
            c.execute('''
                INSERT INTO events (url, title, date_str, location, description, image_urls, content_hash, last_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title=excluded.title,
                    date_str=excluded.date_str,
                    location=excluded.location,
                    description=excluded.description,
                    image_urls=excluded.image_urls,
                    content_hash=excluded.content_hash,
                    last_scraped=excluded.last_scraped
            ''', (full_url, title, raw_date, location, desc, img_str, new_hash, datetime.now()))
            
            conn.commit()

    except Exception as e:
        print(f"Kritischer Fehler: {e}")
    finally:
        conn.close()
        print("--- ENDE ---")

if __name__ == "__main__":
    main()
