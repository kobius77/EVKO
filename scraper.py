import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
from datetime import datetime

# --- KONFIGURATION ---
DB_FILE = "evko.db"
BASE_URL = "https://www.korneuburg.gv.at"
START_URL = "https://www.korneuburg.gv.at/Stadt/Kultur/Veranstaltungskalender"

# Initialisiere Fake UserAgent
ua = UserAgent()

def get_random_header():
    """Erzeugt einen zufälligen Browser-Header, um Blocking zu vermeiden."""
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Referer': 'https://www.google.com/'
    }

def init_db():
    """Erstellt die SQLite Datenbank, falls nicht vorhanden."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Wir speichern Hash, um Änderungen zu erkennen
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
    """Erstellt einen MD5 Hash aus einem String."""
    return hashlib.md5(data_string.encode('utf-8')).hexdigest()

def scrape_details(url):
    """Besucht die Detailseite und holt Text + Bild-Links."""
    print(f"  └── Scrape Details: {url}")
    
    # SLOW MODE: Zufällige Pause zwischen 2 und 5 Sekunden
    time.sleep(random.uniform(2, 5))
    
    try:
        response = requests.get(url, headers=get_random_header(), timeout=10)
        if response.status_code != 200:
            return None, []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # TODO: CSS SELEKTOREN FÜR DETAILSEITE ANPASSEN
        # Versuche, den Hauptinhalt zu finden. Oft #content, .main-content oder article
        content_div = soup.select_one('#content') or soup.select_one('main') or soup.body
        
        # Text extrahieren
        full_text = content_div.get_text(separator="\n", strip=True) if content_div else ""
        
        # Bild-Links extrahieren (nur Links, keine Downloads)
        images = []
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src:
                # Relative URLs zu absoluten machen
                if src.startswith('/'):
                    src = BASE_URL + src
                images.append(src)
        
        return full_text, images
        
    except Exception as e:
        print(f"  Error scraping details: {e}")
        return None, []

def main():
    print(f"--- START EVKO SCRAPER: {datetime.now()} ---")
    conn = init_db()
    c = conn.cursor()
    
    # 1. Hauptseite abrufen
    print("Lade Listenansicht...")
    response = requests.get(START_URL, headers=get_random_header())
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # TODO: CSS SELEKTOR FÜR DIE LISTE ANPASSEN
    # Suchen Sie nach dem Container, der ein einzelnes Event umschließt
    event_list = soup.select('.event_preview') # Beispiel-Klasse, bitte prüfen!
    
    print(f"Gefundene Events in Liste: {len(event_list)}")

    for item in event_list:
        try:
            # Daten aus der Liste extrahieren
            # TODO: CSS SELEKTOREN INNERHALB DES ITEMS ANPASSEN
            link_tag = item.select_one('a')
            if not link_tag: continue
            
            relative_url = link_tag['href']
            full_url = BASE_URL + relative_url if relative_url.startswith('/') else relative_url
            
            title = item.select_one('h2').text.strip() if item.select_one('h2') else "Kein Titel"
            date_str = item.select_one('.date').text.strip() if item.select_one('.date') else ""
            
            # Hash erstellen um Änderungen zu prüfen (Title + Date + URL)
            current_fingerprint = f"{title}{date_str}{full_url}"
            current_hash = make_hash(current_fingerprint)
            
            # Prüfen ob Event schon in DB und ob es sich geändert hat
            c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
            row = c.fetchone()
            
            if row and row[0] == current_hash:
                print(f"Skipping (Unverändert): {title}")
                continue
            
            print(f"Processing (Neu/Update): {title}")
            
            # Detailseite scrapen
            description, image_urls = scrape_details(full_url)
            
            # DB Update / Insert
            img_str = ",".join(image_urls) # Simple CSV Speicherung für Bild-Links
            
            c.execute('''
                INSERT INTO events (url, title, date_str, description, image_urls, content_hash, last_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title=excluded.title,
                    date_str=excluded.date_str,
                    description=excluded.description,
                    image_urls=excluded.image_urls,
                    content_hash=excluded.content_hash,
                    last_scraped=excluded.last_scraped
            ''', (full_url, title, date_str, description, img_str, current_hash, datetime.now()))
            
            conn.commit()
            
        except Exception as e:
            print(f"Fehler bei Item: {e}")
            continue

    conn.close()
    print("--- ENDE EVKO SCRAPER ---")

if __name__ == "__main__":
    main()
