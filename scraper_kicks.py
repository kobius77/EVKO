import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import re
from datetime import datetime

# --- KONFIGURATION ---
DB_FILE = "evko.db"

# Ligaportal (Fallback)
LIGAPORTAL_URL = "https://ticker.ligaportal.at/mannschaft/1295/sk-korneuburg/spielplan"
BASE_URL_LIGA = "https://ticker.ligaportal.at"

# OEFB (Primär) - Basis URL ohne Saison
# Wir suchen nach SK Sparkasse Korneuburg
OEFB_BASE_ID_URL = "https://vereine.oefb.at/SKSparkasseKorneuburg/Mannschaften" 

LOCATION_NAME = "Rattenfängerstadion Korneuburg"
DEFAULT_IMAGE = "https://static.ligaportal.at/images/club/club-1179-large.png"
HOME_TEAM_FILTER = ["Korneuburg", "Korneuburg/Stetten", "SK Sparkasse Korneuburg"]

ua = UserAgent()

def get_header():
    return {
        'User-Agent': ua.random, 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cache-Control': 'no-cache'
    }

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

def get_oefb_season_url():
    """
    Berechnet die URL für die aktuelle Saison dynamisch.
    Ab Juli (Monat 7) beginnt die neue Saison (z.B. 2025-26).
    Davor ist es die alte (z.B. 2024-25).
    """
    now = datetime.now()
    year = now.year
    # Wenn wir vor Juli sind, gehört die Saison zum Vorjahr/Aktuelles Jahr (z.B. Frühling 2025 gehört zu 24/25)
    if now.month < 7:
        start_year = year - 1
        end_year = year
    else:
        start_year = year
        end_year = year + 1
    
    season_str = f"Saison-{start_year}-{str(end_year)[-2:]}" # Resultat z.B. "Saison-2025-26"
    
    # KM steht für Kampfmannschaft
    return f"{OEFB_BASE_ID_URL}/{season_str}/KM/Spiele"

def determine_category(competition_text):
    """Mappt OEFB Wettbewerbe auf unsere Tags"""
    comp = competition_text.lower()
    base_tags = "Sport, Fussball"
    
    if "test" in comp or "aufbau" in comp or "freundschaft" in comp:
        return f"{base_tags}, Vorbereitung"
    elif "cup" in comp or "pokal" in comp:
        return f"{base_tags}, Cup"
    elif "liga" in comp or "klasse" in comp or "meisterschaft" in comp:
        return f"{base_tags}, Meisterschaft"
    else:
        return base_tags

# --- SCRAPER 1: OEFB (PRIMARY) ---
def scrape_oefb(conn):
    url = get_oefb_season_url()
    print(f"Versuche OEFB Scrape: {url}")
    c = conn.cursor()
    count = 0
    
    try:
        r = requests.get(url, headers=get_header(), timeout=15)
        if r.status_code != 200:
            print(f"OEFB Status Code Fehler: {r.status_code}")
            return 0
            
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # OEFB Tabellen sind oft responsive verpackt, wir suchen die Haupttabelle
        # Die Klasse variiert oft, aber meistens gibt es 'table'
        tables = soup.find_all('table')
        if not tables:
            print("Keine Tabelle auf OEFB gefunden.")
            return 0

        # Wir nehmen an, die Spiele sind in der Tabelle mit den meisten Zeilen
        target_table = max(tables, key=lambda t: len(t.find_all('tr')))
        
        for row in target_table.find_all('tr'):
            cells = row.find_all('td')
            # OEFB Struktur (typisch): Datum | Zeit | Heim | Ergebnis | Gast | ... | Info
            if len(cells) < 5: continue
            
            full_text = row.get_text(" ", strip=True)
            
            # Datum extrahieren
            date_match = re.search(r'\d{2}\.\d{2}\.\d{4}', full_text)
            if not date_match: continue
            date_str = date_match.group()
            iso_date = parse_german_date(date_str)
            
            # Zeit extrahieren
            time_match = re.search(r'\d{2}:\d{2}', full_text)
            time_str = time_match.group() if time_match else "00:00"
            
            # Teams finden (oft in Bildern oder spans versteckt, Text ist sicherer)
            # Wir müssen schauen, wo unser Heim-Filter greift
            # Zellenlogik ist oft wackelig bei OEFB, wir analysieren den Text der Zellen
            
            # Normalerweise Spalte 3 (Heim) und 5 (Gast) oder ähnlich.
            # Wir holen uns alle Texte der Zellen
            row_texts = [td.get_text(strip=True) for td in cells]
            
            # OEFB Zeilen haben oft Wettbewerb in der letzten oder vorletzten Spalte oder ganz vorne
            # Wir suchen nach Keywords
            comp_tag = determine_category(full_text)
            
            # Teams extrahieren (Heuristik: Text vor und nach dem "vs" oder Ergebnis)
            # Hier machen wir es einfach: Wir suchen nach Korneuburg in den Zellen
            
            home_candidate = ""
            guest_candidate = ""
            
            # Versuche Struktur zu lesen (OEFB Mobile view ist oft anders als Desktop)
            # Desktop: Datum | Zeit | Heim | : | Gast
            # Suche nach dem "Doppelpunkt" oder Ergebnis Trenner
            
            # Wir iterieren durch Zellen um "Heim" zu finden
            # Indexe raten (Datum=0, Zeit=1, Heim=2, Score=3, Gast=4 - variiert)
            # Wir nutzen die HOME_TEAM_FILTER Logik
            
            is_home_match = False
            
            # Wir prüfen, ob eine der ersten Text-Zellen unser Team ist
            # Aber wir müssen sicherstellen, dass es die HEIM Spalte ist.
            # OEFB Tabellen haben meistens Klassennamen oder fixe Struktur
            
            # Robuster Ansatz für OEFB:
            # Heimteam steht meistens links vom Ergebnis/Doppelpunkt
            sep_index = -1
            for i, txt in enumerate(row_texts):
                if ":" in txt and len(txt) < 8: # Ergebnis oder Zeit, aber Zeit ist meistens früher
                   # Wenn es nicht die Uhrzeit ist (die ist meistens Spalte 1)
                   if i > 1: 
                       sep_index = i
                       break
            
            if sep_index > 0:
                home_candidate = row_texts[sep_index - 1]
                guest_candidate = row_texts[sep_index + 1]
                
                # Check ob Korneuburg Heim ist
                for f in HOME_TEAM_FILTER:
                    if f.lower() in home_candidate.lower():
                        is_home_match = True
                        break
            
            if not is_home_match: continue
            
            title = f"{home_candidate} - {guest_candidate}"
            full_url = url # Wir verlinken auf den Spielplan, da Einzelspiel-URLs komplex sind
            
            # Hash
            h = make_hash(f"{iso_date}{home_candidate}{guest_candidate}{time_str}")
            
            desc = f"Wettbewerb: {comp_tag.replace('Sport, Fussball, ', '')}\nHeim: {home_candidate}\nGast: {guest_candidate}"
            
            print(f"  [OEFB] {iso_date} | {title} ({comp_tag})")
            
            c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                         ON CONFLICT(url) DO UPDATE SET 
                         title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                         time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                         image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                         (f"oefb_{h}", title, comp_tag, date_str, iso_date, time_str, LOCATION_NAME, desc, DEFAULT_IMAGE, h, datetime.now()))
            conn.commit()
            count += 1
            
    except Exception as e:
        print(f"Fehler bei OEFB Scrape: {e}")
        return 0
        
    return count

# --- SCRAPER 2: LIGAPORTAL (FALLBACK) ---
def scrape_ligaportal(conn):
    print("Starte Ligaportal Fallback...")
    c = conn.cursor()
    count = 0
    
    try:
        r = requests.get(LIGAPORTAL_URL, headers=get_header(), timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.select_one('table.teamSchedule')
        
        if table:
            current_date_str = None
            for row in table.find_all('tr'):
                strong = row.find('strong')
                if strong and (row.find('td', colspan=True) or "colspan" in str(row)):
                      text = row.get_text(strip=True)
                      match = re.search(r'\d{2}\.\d{2}\.\d{4}', text)
                      if match: current_date_str = match.group()
                      continue

                if "game-row" in row.get('class', []):
                    if not current_date_str: continue
                    
                    home_td = row.select_one('td.team.text-right')
                    guest_td = row.select_one('td.team.text-left')
                    score_td = row.select_one('td.score')
                    btn_td = row.select_one('td.button-holder a')

                    if not home_td or not guest_td: continue
                    
                    home = home_td.get_text(strip=True)
                    guest = guest_td.get_text(strip=True)
                    
                    is_home = False
                    for f in HOME_TEAM_FILTER:
                        if f in home: is_home = True; break
                    if not is_home: continue

                    raw_score = score_td.get_text(strip=True)
                    time_str = raw_score if re.match(r'^\d{1,2}:\d{2}$', raw_score) else ""
                    
                    if btn_td and btn_td.get('href'):
                        full_url = BASE_URL_LIGA + btn_td.get('href') if btn_td.get('href').startswith('/') else btn_td.get('href')
                    else:
                        full_url = f"https://ligaportal.at/match/{make_hash(current_date_str+home)}"

                    iso_date = parse_german_date(current_date_str)
                    title = f"{home} - {guest}"
                    desc = f"Liga: Meisterschaft (Fallback)\nHeim: {home}\nGast: {guest}"
                    h = make_hash(f"{iso_date}{home}{guest}")

                    print(f"  [LIGA-FALLBACK] {iso_date} | {title}")

                    c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                                 VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                                 ON CONFLICT(url) DO UPDATE SET 
                                 title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                                 time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                                 image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                                 (full_url, title, "Sport, Fussball, Meisterschaft", current_date_str, iso_date, time_str, LOCATION_NAME, desc, DEFAULT_IMAGE, h, datetime.now()))
                    conn.commit()
                    count += 1
                    
    except Exception as e: print(e)
    return count

def main():
    print("--- START KICKS SCRAPER (OEFB Primary) ---")
    conn = init_db()
    
    # 1. Versuch: OEFB
    matches_found = scrape_oefb(conn)
    
    # 2. Versuch: Fallback wenn OEFB leer war oder Fehler hatte
    if matches_found == 0:
        print("OEFB lieferte keine Ergebnisse. Wechsel zu Ligaportal...")
        scrape_ligaportal(conn)
    else:
        print(f"Erfolgreich {matches_found} Spiele von OEFB geladen.")
        
    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__": main()
