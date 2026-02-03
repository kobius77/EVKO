import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import random
import os
import re
import base64
from datetime import datetime
from urllib.parse import urljoin

# --- KONFIGURATION ---
DB_FILE = "evko.db"

# URLs Base64 kodiert (Sichtschutz)
# Target: Handball Verband Base
_SOURCE_BASE_B64 = "aHR0cHM6Ly9vZWhiLWhhbmRiYWxsLmxpZ2EubnU="

# Target: Court Info Korneuburg (ID 18471)
_SOURCE_START_B64 = "aHR0cHM6Ly9vZWhiLWhhbmRiYWxsLmxpZ2EubHUvY2dpLWJpbi9XZWJPYmplY3RzL251TGlnYUhCQVQud29hL3dhL2NvdXJ0SW5mbz9mZWRlcmF0aW9uPSVDMyU5NkhCJmxvY2F0aW9uPTE4NDcx"

# Ligen-Filter (WHA, Jugend, HLA etc.)
AK_WHITELIST = ["WHA1", "WHA2", "WHA1U18", "HLA-HLA2-RL"]

FIXED_LOCATION = "Franz Guggenberger Sporthalle"
FIXED_TAGS = "Sport, Handball"

ua = UserAgent()

def decode_url(b64_string):
    """Entschlüsselt die Base64 URLs zur Laufzeit"""
    return base64.b64decode(b64_string).decode('utf-8')

def get_header(): 
    return {'User-Agent': ua.random}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS events (
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
    )''')
    conn.commit()
    return conn

def make_hash(s): return hashlib.md5(s.encode('utf-8')).hexdigest()

def parse_german_date(t):
    try:
        clean = re.search(r'\d{2}\.\d{2}\.\d{4}', t)
        if clean: return datetime.strptime(clean.group(), "%d.%m.%Y").strftime("%Y-%m-%d")
    except: return None
    return None

def scrape_month_page(url, conn):
    print(f"Scrape: {url[-20:]}...") # Nur das Ende der URL loggen
    try:
        r = requests.get(url, headers=get_header(), timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.select_one('table.result-set')
        
        # Navigation für weitere Monate finden
        new_links = []
        base_url = decode_url(_SOURCE_BASE_B64)
        for a in soup.select('#sub-navigation li a'):
            if a.get('href'): 
                new_links.append(urljoin(base_url, a.get('href')))

        if not table: return new_links
        
        c = conn.cursor()
        curr_date = None
        
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            # NuLiga Struktur: Datum oft nur in der ersten Zeile des Tages
            if len(cells) < 8: continue
            
            d_txt = cells[1].get_text(strip=True)
            if d_txt: curr_date = d_txt
            if not curr_date: continue
            
            # Altersklasse prüfen
            ak = cells[4].get_text(strip=True)
            if not any(w in ak for w in AK_WHITELIST): continue
            
            time_raw = cells[2].get_text(strip=True).replace('v', '').strip()
            gid = cells[3].get_text(strip=True)
            
            # Spiel muss eine ID haben
            if not gid: continue
            
            # URL bauen
            # Wir nutzen den Start-Link + Anker, da NuLiga Session-IDs hat
            start_url = decode_url(_SOURCE_START_B64)
            valid_url = f"{start_url}#match-{gid}"
            
            home = cells[6].get_text(strip=True)
            guest = cells[7].get_text(strip=True)
            
            title = f"{home} - {guest}"
            iso = parse_german_date(curr_date)
            desc = f"Liga: {ak}\nHeim: {home}\nGast: {guest}"
            h = make_hash(f"{gid}{iso}{home}")
            
            print(f"  [HANDBALL] {iso} | {title}")
            
            # FIX: datetime.now().isoformat()
            c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                         ON CONFLICT(url) DO UPDATE SET 
                         title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                         time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                         content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                         (valid_url, title, FIXED_TAGS, curr_date, iso, time_raw, FIXED_LOCATION, desc, "", h, datetime.now().isoformat()))
            conn.commit()

        return new_links

    except Exception as e: 
        print(f"Fehler: {e}")
        return []

def main():
    print("--- START HANDBALL SCRAPER ---")
    conn = init_db()
    
    start_url = decode_url(_SOURCE_START_B64)
    visited = set()
    queue = [start_url]
    count = 0
    
    # Breadth-First Search durch die Monate
    while queue and count < 12: # Max 12 Monate/Seiten crawlen
        curr = queue.pop(0)
        
        if curr in visited: continue
        visited.add(curr)
        count += 1
        
        found_links = scrape_month_page(curr, conn)
        
        for l in found_links:
            if l not in visited and l not in queue:
                queue.append(l)
        
        time.sleep(1) # Höflich bleiben
        
    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__": main()
