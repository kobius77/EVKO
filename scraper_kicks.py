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

# Filter: Nur importieren, wenn Heimteam diesen String enthält
HOME_TEAM_FILTER = ["Korneuburg", "Korneuburg/Stetten"]

# Fester Ort & Tags
LOCATION_NAME = "Ratgeber-Stadion Korneuburg"
FIXED_TAGS = "Sport, Fussball, 1. Landesliga"
DEFAULT_IMAGE = "https://static.ligaportal.at/images/club/club-1179-large.png"

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
    # Format "Fr., 06.03.2026" -> "2026-03-06"
    try:
        clean_date = re.search(r'\d{2}\.\d{2}\.\d{4}', date_text)
        if clean_date:
            dt = datetime.strptime(clean_date.group(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        return None
    return None

def main():
    print("--- START KICKS SCRAPER (Ligaportal) ---")
    conn = init_db()
    c = conn.cursor()

    try:
        response = requests.get(START_URL, headers=get_header(), timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.select_one('table.teamSchedule')
        if not table:
            print("Keine Spielplan-Tabelle gefunden.")
            return

        rows = table.find_all('tr')
        current_date_str = None

        for row in rows:
            # 1. Datum suchen (Zeile mit colspan oder strong)
            strong = row.find('strong')
            if strong and (row.find('td', colspan=True) or "colspan" in str(row)):
                 text = row.get_text(strip=True)
                 match = re.search(r'\d{2}\.\d{2}\.\d{4}', text)
                 if match:
                     current_date_str = match.group()
                     continue

            # 2. Spiel-Zeile prüfen
            if "game-row" in row.get('class', []):
                if not current_date_str: continue

                home_td = row.select_one('td.team.text-right')
                guest_td = row.select_one('td.team.text-left')
                score_td = row.select_one('td.score')
                btn_td = row.select_one('td.button-holder a')

                if not home_td or not guest_td: continue

                home_team = home_td.get_text(strip=True)
                guest_team = guest_td.get_text(strip=True)

                # --- HEIMSPIEL CHECK ---
                is_home = False
                for f in HOME_TEAM_FILTER:
                    if f in home_team:
                        is_home = True
                        break
                
                if not is_home: continue
                # -----------------------

                # Uhrzeit
                raw_score = score_td.get_text(strip=True)
                time_str = ""
                # Wenn es wie "19:30" aussieht, ist es die Zeit. Wenn "3:1", ist es vorbei.
                if re.match(r'^\d{1,2}:\d{2}$', raw_score):
                    time_str = raw_score
                
                # URL (ID)
                if btn_td and btn_td.get('href'):
                    full_url = BASE_URL + btn_td.get('href') if btn_td.get('href').startswith('/') else btn_td.get('href')
                else:
                    # Fallback ID
                    full_url = f"https://ligaportal.at/match/{make_hash(current_date_str+home_team)}"

                iso_date = parse_german_date(current_date_str)
                title = f"Fussball: {home_team} vs. {guest_team}"
                description = f"Heim: {home_team}\nGast: {guest_team}\nLiga: 1. Landesliga"
                tags = FIXED_TAGS
                
                fingerprint = f"{iso_date}{home_team}{guest_team}"
                content_hash = make_hash(fingerprint)

                print(f"  [FUSSBALL] {iso_date} | {title}")

                c.execute('''
                    INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, 
                        start_iso=excluded.start_iso, time_str=excluded.time_str, location=excluded.location, 
                        description=excluded.description, image_urls=excluded.image_urls,
                        content_hash=excluded.content_hash, last_scraped=excluded.last_scraped
                ''', (full_url, title, tags, current_date_str, iso_date, time_str, LOCATION_NAME, description, DEFAULT_IMAGE, content_hash, datetime.now()))
                conn.commit()

    except Exception as e:
        print(f"Fehler: {e}")

    conn.close()
    print("--- ENDE KICKS SCRAPER ---")

if __name__ == "__main__":
    main()
