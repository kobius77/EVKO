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

# 1. TITEL-WHITELIST (Falls im Titel was steht wie "Shopping-Event:")
TITLE_TAG_WHITELIST = [
    "Shopping-Event", "Kultur- und Musiktage", "Kabarett-Picknick", 
    "Werftbühne", "Ausstellung", "Sonderausstellung", "Vernissage", 
    "Lesung", "Konzert", "Flohmarkt"
]

# 2. BLACKLIST (Wörter, die wir rausfiltern)
TAG_BLACKLIST = [
    "stadt", "gemeinde", "korneuburg", "event", "events", 
    "kategorie", "bereich", "art", "thema"
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
    """
    Macht aus 'Theater, Kabarett, Veranstaltungen - Rathaus' -> 'Theater, Kabarett, Rathaus'
    """
    cleaned = set()
    raw_list = []

    for raw_t in tag_set:
        # Schritt 1: Spezifische Ersetzungen VOR dem Split
        # Wir entfernen "Veranstaltungen - " damit das Wort danach übrig bleibt
        t_processed = raw_t.replace("Veranstaltungen - Stadt", "") # Komplett weg
        t_processed = t_processed.replace("Veranstaltungen - ", "") # "Rathaus" bleibt stehen
        
        # Schritt 2: Splitten
        # Wir splitten hart am Komma. Bindestriche sind jetzt meist bereinigt oder Teil eines Wortes
        parts = t_processed.split(",")
        raw_list.extend(parts)

    for tag in raw_list:
        t = tag.strip()
        
        # Filter: Zu kurz, leer oder Blacklist
        if len(t) < 3: continue
        is_blocked = False
        for blocked in TAG_BLACKLIST:
            if blocked == t.lower(): # Exakter Match auf Blacklist (z.B. "stadt")
                is_blocked = True
        
        if not is_blocked:
            cleaned.add(t)
    
    return ", ".join(sorted(list(cleaned)))

def scrape_details(url, title):
    time.sleep(random.uniform(1, 3))
    found_tags = set()
    
    # A) Titel-Check
    if ":" in title:
        prefix = title.split(":")[0].strip()
        for allowed in TITLE_TAG_WHITELIST:
            if allowed.lower() in prefix.lower():
                found_tags.add(prefix)
                break

    try:
        response = requests.get(url, headers=get_random_header(), timeout=15)
        if response.status_code != 200: return "Fehler", "", []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Content Area finden
        content_div = soup.select_one('#content') or soup.select_one('.main-content') or soup.select_one('main') or soup.body
        
        # B) "Subtitle" Scan - Das ist die neue Logik!
        # Wir suchen in den ersten 5 Elementen nach dem Tag-String
        # Der String enthält oft Kommas oder "Veranstaltungen -"
        candidates = content_div.find_all(['p', 'div', 'span', 'h3', 'h4'], limit=6)
        
        subtitle_found = False
        for el in candidates:
            text = el.get_text(strip=True)
            # Heuristik: Enthält "Veranstaltungen -" ODER hat mehrere Kommas
            if "Veranstaltungen -" in text or text.count(',') >= 1:
                # Das ist höchstwahrscheinlich unsere Tag-Zeile!
                # print(f"    Tags gefunden in: '{text[:50]}...'")
                found_tags.add(text)
                subtitle_found = True
                
                # Optional: Wir könnten dieses Element aus dem Description-Text entfernen,
                # damit es nicht doppelt auftaucht. (Hier lassen wir es erstmal drin)
                break # Wir nehmen nur die erste passende Zeile als Tags

        # Wenn wir nichts im Subtitle gefunden haben, fallback auf Regex im ganzen Text
        full_text = content_div.get_text(separator="\n", strip=True)
        if not subtitle_found:
            match = re.search(r'(?:Kategorie|Bereich|Art):\s*([^\n\r]+)', full_text, re.IGNORECASE)
            if match:
                found_tags.add(match.group(1))

        # Bilder
        images = []
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src and "data:image" not in src and "dummy.gif" not in src:
                images.append(urljoin(BASE_URL, src))
                
        final_tags_str = clean_tags(found_tags)
                
        return full_text, final_tags_str, list(set(images))
        
    except Exception as e:
        print(f"Fehler Details: {e}")
        return "", clean_tags(found_tags), []

def main():
    print(f"--- START EVKO SCRAPER (SUBTITLE TARGETING) ---")
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
                
                c.execute("SELECT content_hash FROM events WHERE url=?", (full_url,))
                db_row = c.fetchone()
                if db_row and db_row[0] == new_hash: continue
                
                print(f"  [NEU] {title}")
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
