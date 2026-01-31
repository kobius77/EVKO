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
BASE_URL = "https://oehb-handball.liga.nu"
START_URL = "https://oehb-handball.liga.nu/cgi-bin/WebObjects/nuLigaHBAT.woa/wa/courtInfo?federation=%C3%96HB&location=18471"

AK_WHITELIST = ["WHA1", "WHA2", "WHA1U18", "HLA-HLA2-RL"]
FIXED_LOCATION = "Franz Guggenberger Sporthalle"
FIXED_TAGS = "Sport, Handball"

ua = UserAgent()

def get_header(): return {'User-Agent': ua.random}
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS events (url TEXT PRIMARY KEY, title TEXT, tags TEXT, date_str TEXT, start_iso TEXT, time_str TEXT, location TEXT, description TEXT, image_urls TEXT, content_hash TEXT, last_scraped TIMESTAMP)''')
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
    print(f"Scrape: {url}")
    try:
        r = requests.get(url, headers=get_header(), timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.select_one('table.result-set')
        if not table: return []
        
        c = conn.cursor()
        curr_date = None
        new_links = []
        
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 8: continue
            
            d_txt = cells[1].get_text(strip=True)
            if d_txt: curr_date = d_txt
            if not curr_date: continue
            
            ak = cells[4].get_text(strip=True)
            if not any(w in ak for w in AK_WHITELIST): continue
            
            time_raw = cells[2].get_text(strip=True).replace('v', '').strip()
            gid = cells[3].get_text(strip=True)
            if not gid: continue
            
            valid_url = f"{START_URL}#match-{gid}"
            home = cells[6].get_text(strip=True)
            guest = cells[7].get_text(strip=True)
            
            # NEU: Titel ohne "Handball:" Prefix
            title = f"{home} - {guest}"
            
            iso = parse_german_date(curr_date)
            desc = f"Liga: {ak}\nHeim: {home}\nGast: {guest}"
            h = make_hash(f"{gid}{iso}{home}")
            
            print(f"  [HANDBALL] {iso} | {title}")
            
            c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(url) DO UPDATE SET title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, time_str=excluded.time_str, location=excluded.location, description=excluded.description, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', (valid_url, title, FIXED_TAGS, curr_date, iso, time_raw, FIXED_LOCATION, desc, "", h, datetime.now()))
            conn.commit()

        for a in soup.select('#sub-navigation li a'):
            if a.get('href'): new_links.append(urljoin(BASE_URL, a.get('href')))
        return new_links
    except Exception as e: print(e); return []

def main():
    conn = init_db()
    visited = set(); queue = [START_URL]; count = 0
    while queue and count < 12:
        curr = queue.pop(0)
        if curr in visited: continue
        visited.add(curr); count += 1
        for l in scrape_month_page(curr, conn):
            if l not in visited: queue.append(l)
        time.sleep(1)
    conn.close()

if __name__ == "__main__": main()
