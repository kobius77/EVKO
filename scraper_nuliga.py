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
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# --- KONFIGURATION ---
DB_FILE = "evko.db"
BASE_URL = "https://oehb-handball.liga.nu"
# Das ist die "Basis-URL", auf die der User geleitet wird
START_URL = "https://oehb-handball.liga.nu/cgi-bin/WebObjects/nuLigaHBAT.woa/wa/courtInfo?federation=%C3%96HB&location=18471"

AK_WHITELIST = ["WHA1", "WHA2", "WHA1U18", "HLA-HLA2-RL"]
FIXED_LOCATION = "Franz Guggenberger Sporthalle"
FIXED_TAGS = "Sport, Handball"

ua = UserAgent()

def get_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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

def scrape_month_page(url, conn):
    print(f"Scrape URL: {url} ...")
    try:
        response = requests.get(url, headers=get_header(), timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        location_raw = FIXED_LOCATION

        table = soup.select_one('table.result-set')
        if not table: return []

        rows = table.find_all('tr')
        current_date_str = None
        new_links = [] 
        c = conn.cursor()

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 8: continue

            # Datum merken
            date_cell_text = cells[1].get_text(strip=True)
            if date_cell_text: current_date_str = date_cell_text
            if not current_date_str: continue

            # Whitelist Check
            ak_text = cells[4].get_text(strip=True)
            is_relevant = False
            for allowed in AK_WHITELIST:
                if allowed in ak_text:
                    is_relevant = True
                    break
            
            if not is_relevant: continue

            time_raw = cells[2].get_text(strip=True).replace('v', '').strip()
            game_id = cells[3].get_text(strip=True)
            if not game_id: continue
            
            # --- URL FIX ---
            # Wir nehmen die echte Hallenplan-URL und hängen die ID als Anker an.
            # Ergebnis: Ein funktionierender Link, der trotzdem eindeutig in der DB ist.
            valid_url = f"{START_URL}#match-{game_id}"
            
            home_team = cells[6].get_text(strip=True)
            guest_team = cells[7].get_text(strip=True)
            
            title = f"Handball: {home_team} vs. {guest_team}"
            iso_date = parse_german_date(current_date_str)
            description = f"Liga/AK: {ak_text}\nHeim: {home_team}\nGast: {guest_team}\nSpiel-Nr: {game_id}"
            
            tags = FIXED_TAGS
            
            fingerprint = f"{game_id}{iso_date}{time_raw}{home_team}"
            content_hash = make_hash(fingerprint)
            
            print(f"  [HANDBALL] {iso_date} | {title}")

            c.execute('''
                INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title=excluded.title, 
                    tags=excluded.tags,
                    date_str=excluded.date_str, 
                    start_iso=excluded.start_iso,
                    time_str=excluded.time_str,
                    location=excluded.location, 
                    description=excluded.description,
                    content_hash=excluded.content_hash, 
                    last_scraped=excluded.last_scraped
            ''', (valid_url, title, tags, current_date_str, iso_date, time_raw, location_raw, description, "", content_hash, datetime.now()))
            conn.commit()

        # Navigation
        nav = soup.select('#sub-navigation li a')
        for a in nav:
            href = a.get('href')
            if href:
                new_links.append(urljoin(BASE_URL, href))
                
        return new_links

    except Exception as e:
        print(f"Fehler bei {url}: {e}")
        return []

def main():
    print(f"--- START NULIGA SCRAPER ---")
    conn = init_db()
    
    visited = set()
    queue = [START_URL]
    max_pages = 12 
    count = 0

    while queue and count < max_pages:
        current_url = queue.pop(0)
        if current_url in visited: continue
            
        visited.add(current_url)
        count += 1
        
        found_links = scrape_month_page(current_url, conn)
        
        for link in found_links:
            if link not in visited:
                queue.append(link)
                
        time.sleep(1)

    conn.close()
    print("--- ENDE NULIGA SCRAPER ---")

if __name__ == "__main__":
    main()
