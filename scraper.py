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

def get_random_header():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Spalte 'tags' statt 'category'
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

def clean_tags(tag_set):
    """Wendet die Geschäftslogik auf die gefundenen Tags an."""
    cleaned = set()
    for tag in tag_set:
        t = tag.strip()
        if len(t) < 2: continue # Zu kurz
        
        # Mapping Regeln
        if "Veranstaltungen - Stadt" in t:
            continue # Ignorieren
        if "Veranstaltungen - Rathaus" in t:
            cleaned.add("Rathaus") # Umbenennen
            continue
            
        # Standard Clean
        cleaned.add(t)
    
    # Sortierte, komma-separierte Liste zurückgeben
    return ", ".join(sorted(list(cleaned)))

def scrape_details(url, title):
    """Holt Details UND sucht aggressiv nach Tags."""
    time.sleep(random.uniform(1, 3))
    
    found_tags = set()
    
    # 1. Tag aus Titel-Prefix (z.B. "Shopping-Event: ...")
    if ":" in title:
        prefix = title.split(":")[0].strip()
        # Nur nehmen wenn nicht zu lang (kein ganzer Satz)
        if len(prefix) < 30:
            found_tags.add(prefix)

    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "Fehler", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 2. Meta Keywords auslesen (Oft versteckt das CMS hier Daten)
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        if meta_keywords and meta_keywords.get("content"):
            keywords = meta_keywords["content"].split(",")
            for k in keywords:
                # Korneuburg selbst ist kein sinnvoller Tag hier
                if "Korneuburg" not in k and "Gemeinde" not in k:
                    found_tags.add(k.strip())

        # Content suchen
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.select_one('main') or soup.body
        full_text = content_div.get_text(separator="\n", strip=True)
        
        # 3. Regex Suche im Text ("Kategorie: X, Y")
        # Sucht nach Kategorie, Bereich, Art gefolgt von Text bis zum Zeilenende
        match = re.search(r'(?:Kategorie|Bereich|Art|Thema):\s*([^\n\r]+)', full_text, re.IGNORECASE)
        if match:
            raw_cats = match.group(1)
            # Split bei Komma oder Slash
            for c in re.split(r'[,/]', raw_cats):
                found_tags.add(c.strip())

        # Bilder
        images = []
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src and "data:image" not in src and "dummy.gif" not in src:
                images.append(urljoin(BASE_URL, src))
                
        # Tags bereinigen
        final_tags_str = clean_tags(found_tags)
                
        return full_text, final_tags_str, list(set(images))
        
    except Exception as e:
        print(f"Fehler Details: {e}")
        # Auch bei Fehler geben wir zumindest die Titel-Tags zurück
        return "", clean_tags(found_tags), []

def main():
    print(f"--- START EVKO SCRAPER (TAG MODE) ---")
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
                
                # Check ob Update nötig
                c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
                db_row = c.fetchone()
                if db_row and db_row[0] == new_hash: continue
                
                print(f"  [NEU] {title}")
                
                # Hier übergeben wir den Title zum Tag-Raten
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
