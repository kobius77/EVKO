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

# --- KONFIGURATION ---
DB_FILE = "evko.db"
START_URL = "https://ticker.ligaportal.at/mannschaft/1295/sk-korneuburg/spielplan"
BASE_URL = "https://ticker.ligaportal.at"

HOME_TEAM_FILTER = ["Korneuburg", "Korneuburg/Stetten"]
LOCATION_NAME = "Ratgeber-Stadion Korneuburg"
FIXED_TAGS = "Sport, Fussball, 1. Landesliga"
DEFAULT_IMAGE = "https://static.ligaportal.at/images/club/club-1179-large.png"

ua = UserAgent()

def get_header():
    return {'User-Agent': ua.random, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

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

def main():
    print("--- START KICKS SCRAPER ---")
    conn = init_db()
    c = conn.cursor()

    try:
        r = requests.get(START_URL, headers=get_header(), timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.select_one('table.teamSchedule')
        
        if table:
            current_date_str = None
            for row in table.find_all('tr'):
                # Datum suchen
                strong = row.find('strong')
                if strong and (row.find('td', colspan=True) or "colspan" in str(row)):
                     text = row.get_text(strip=True)
                     match = re.search(r'\d{2}\.\d{2}\.\d{4}', text)
                     if match: current_date_str = match.group()
                     continue

                # Spielzeile
                if "game-row" in row.get('class', []):
                    if not current_date_str: continue
                    
                    home_td = row.select_one('td.team.text-right')
                    guest_td = row.select_one('td.team.text-left')
                    score_td = row.select_one('td.score')
                    btn_td = row.select_one('td.button-holder a')

                    if not home_td or not guest_td: continue
                    
                    home = home_td.get_text(strip=True)
                    guest = guest_td.get_text(strip=True)
                    
                    # Filter
                    is_home = False
                    for f in HOME_TEAM_FILTER:
                        if f in home: is_home = True; break
                    if not is_home: continue

                    # Zeit
                    raw_score = score_td.get_text(strip=True)
                    time_str = raw_score if re.match(r'^\d{1,2}:\d{2}$', raw_score) else ""
                    
                    # URL
                    if btn_td and btn_td.get('href'):
                        full_url = BASE_URL + btn_td.get('href') if btn_td.get('href').startswith('/') else btn_td.get('href')
                    else:
                        full_url = f"https://ligaportal.at/match/{make_hash(current_date_str+home)}"

                    iso_date = parse_german_date(current_date_str)
                    
                    # NEU: Titel ohne "Fussball:" Prefix
                    title = f"{home} - {guest}"
                    
                    desc = f"Liga: 1. Landesliga\nHeim: {home}\nGast: {guest}"
                    h = make_hash(f"{iso_date}{home}{guest}")

                    print(f"  [FUSSBALL] {iso_date} | {title}")

                    c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(url) DO UPDATE SET title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, time_str=excluded.time_str, location=excluded.location, description=excluded.description, image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', (full_url, title, FIXED_TAGS, current_date_str, iso_date, time_str, LOCATION_NAME, desc, DEFAULT_IMAGE, h, datetime.now()))
                    conn.commit()

    except Exception as e: print(e)
    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__": main()
