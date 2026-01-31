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

# Wir importieren nur Spiele, bei denen die HEIMMANNSCHAFT diesen Namen trägt
HOME_TEAM_FILTER = ["Korneuburg", "Korneuburg/Stetten"]

# Ort für Heimspiele
LOCATION_NAME = "Ratgeber-Stadion Korneuburg"

# Bild für alle Fussball-Einträge (Logo SK Korneuburg)
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
    # Format: "Fr., 06.03.2026" -> "2026-03-06"
    try:
        clean_date = re.search(r'\d{2}\.\d{2}\.\d{4}', date_text)
        if clean_date:
            dt = datetime.strptime(clean_date.group(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        return None
    return None

def main():
    print("--- START LIGAPORTAL SCRAPER ---")
    conn = init_db()
    c = conn.cursor()

    try:
        response = requests.get(START_URL, headers=get_header(), timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Die Tabelle mit den Spielen
        table = soup.select_one('table.teamSchedule')
        if not table:
            print("Keine Spielplan-Tabelle gefunden.")
            return

        rows = table.find_all('tr')
        current_date_str = None

        for row in rows:
            # 1. Ist es eine Datums-Zeile? (Erkennbar am colspan oder strong)
            # Bsp: <strong>Fr., 08.08.2025</strong>
            strong_tag = row.find('strong')
            if strong_tag and "colspan" in row.attrs.get('td', {}) or row.find('td', colspan=True):
                 # Manchmal ist das Datum direkt im Text des td
                 text = row.get_text(strip=True)
                 # Suche nach Datumsmuster DD.MM.YYYY
                 match = re.search(r'\d{2}\.\d{2}\.\d{4}', text)
                 if match:
                     current_date_str = match.group()
                     continue

            # 2. Ist es eine Spiel-Zeile? (Klasse 'game-row')
            if "game-row" in row.get('class', []):
                if not current_date_str: continue

                # Spalten extrahieren
                # td.team.text-right = Heimmannschaft
                # td.score = Ergebnis oder Uhrzeit
                # td.team.text-left = Gastmannschaft
                # td.button-holder = Link zum Ticker

                home_td = row.select_one('td.team.text-right')
                guest_td = row.select_one('td.team.text-left')
                score_td = row.select_one('td.score')
                btn_td = row.select_one('td.button-holder a')

                if not home_td or not guest_td: continue

                home_team = home_td.get_text(strip=True)
                guest_team = guest_td.get_text(strip=True)

                # --- FILTER: IST ES EIN HEIMSPIEL? ---
                is_home_game = False
                for f in HOME_TEAM_FILTER:
                    if f in home_team:
                        is_home_game = True
                        break
                
                if not is_home_game:
                    continue
                # -------------------------------------

                # Uhrzeit / Ergebnis parsen
                raw_score_time = score_td.get_text(strip=True)
                time_str = ""
                
                # Wenn es wie eine Uhrzeit aussieht (19:30), nehmen wir es.
                # Wenn es wie ein Ergebnis aussieht (3:1), ist das Spiel vorbei -> keine Uhrzeit (oder 00:00)
                if re.match(r'^\d{1,2}:\d{2}$', raw_score_time):
                    time_str = raw_score_time
                else:
                    # Spiel vorbei oder läuft gerade
                    time_str = "" # Leer lassen oder "00:00"

                # Link zur Detailseite (als Unique ID)
                full_url = ""
                if btn_td and btn_td.get('href'):
                    full_url = BASE_URL + btn_td.get('href') if btn_td.get('href').startswith('/') else btn_td.get('href')
                else:
                    # Fallback ID bauen, falls kein Link da ist
                    full_url = f"https://ligaportal.at/match/{make_hash(current_date_str+home_team+guest_team)}"

                # Daten aufbereiten
                title = f"Fussball: {home_team} vs. {guest_team}"
                iso_date = parse_german_date(current_date_str)
                
                description = f"Heim: {home_team}\nGast: {guest_team}\nLiga: 1. Landesliga"
                tags = "Sport, Fussball, 1. Landesliga"

                fingerprint = f"{iso_date}{home_team}{guest_team}"
                content_hash = make_hash(fingerprint)

                print(f"  [FUSSBALL] {iso_date} | {title}")

                # Speichern
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
                        image_urls=excluded.image_urls,
                        content_hash=excluded.content_hash, 
                        last_scraped=excluded.last_scraped
                ''', (full_url, title, tags, current_date_str, iso_date, time_str, LOCATION_NAME, description, DEFAULT_IMAGE, content_hash, datetime.now()))
                conn.commit()

    except Exception as e:
        print(f"Fehler: {e}")

    conn.close()
    print("--- ENDE LIGAPORTAL SCRAPER ---")

if __name__ == "__main__":
    main()
