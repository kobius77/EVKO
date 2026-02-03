import sqlite3
import requests
import json
import re
from fake_useragent import UserAgent
import hashlib
import time
import base64
from datetime import datetime
import argparse  # <--- NEU

# --- KONFIGURATION ---
DB_FILE = "evko.db"

# URLs (Base64 kodiert)
_SOURCE_A_B64 = "aHR0cHM6Ly92ZXJlaW5lLm9lZmIuYXQvU0tTcGFya2Fzc2VLb3JuZXVidXJnL01hbm5zY2hhZnRlbg=="
_SOURCE_B_START_B64 = "aHR0cHM6Ly90aWNrZXIubGlnYXBvcnRhbC5hdC9tYW5uc2NoYWZ0LzEyOTUvc2sta29ybmV1YnVyZy9zcGllbHBsYW4="
_SOURCE_B_BASE_B64 = "aHR0cHM6Ly90aWNrZXIubGlnYXBvcnRhbC5hdA=="
_IMG_DEFAULT_B64 = "aHR0cHM6Ly9zdGF0aWMubGlnYXBvcnRhbC5hdC9pbWFnZXMvY2x1Yi9jbHViLTExNzktbGFyZ2UucG5n"

LOCATION_NAME = "Rattenfängerstadion Korneuburg"

# Filter
HOME_TEAM_FILTER = ["Korneuburg", "Korneuburg/Stetten", "SK Sparkasse Korneuburg", "SG Korneuburg"]

ua = UserAgent()

def decode_url(b64_string):
    return base64.b64decode(b64_string).decode('utf-8')

def get_header():
    return {
        'User-Agent': ua.random, 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS events (
        url TEXT PRIMARY KEY, title TEXT, tags TEXT, date_str TEXT, start_iso TEXT, 
        time_str TEXT, location TEXT, description TEXT, image_urls TEXT, 
        content_hash TEXT, last_scraped TIMESTAMP
    )''')
    conn.commit()
    return conn

def make_hash(s): return hashlib.md5(s.encode('utf-8')).hexdigest()

def get_primary_season_url():
    now = datetime.now()
    year = now.year
    if now.month < 7:
        start_year = year - 1
        end_year = year
    else:
        start_year = year
        end_year = year + 1
    
    season_str = f"Saison-{start_year}-{str(end_year)[-2:]}"
    base_url = decode_url(_SOURCE_A_B64)
    return f"{base_url}/{season_str}/KM/Spiele"

def map_competition_to_tags(bewerb_name):
    if not bewerb_name: return "Sport, Fussball"
    name_lower = bewerb_name.lower()
    tags = ["Sport", "Fussball"]
    
    if "freundschaft" in name_lower or "test" in name_lower or "aufbau" in name_lower:
        tags.append("Testspiel")
    elif "cup" in name_lower or "pokal" in name_lower:
        tags.append("Cup")
    elif "liga" in name_lower or "klasse" in name_lower or "meisterschaft" in name_lower:
        tags.append("Meisterschaft")
        clean_name = bewerb_name.replace("11teamsports", "").replace("Admiral", "").strip()
        clean_name = " ".join(clean_name.split())
        if clean_name: tags.append(clean_name)
    else:
        tags.append("Meisterschaft")

    return ", ".join(tags)

def find_games_list_recursive(data):
    if isinstance(data, dict):
        if "spiele" in data and isinstance(data["spiele"], list) and len(data["spiele"]) > 0:
            if "datum" in data["spiele"][0]:
                return data["spiele"]
        for key, value in data.items():
            result = find_games_list_recursive(value)
            if result: return result
    elif isinstance(data, list):
        for item in data:
            result = find_games_list_recursive(item)
            if result: return result
    return None

def scrape_primary(conn):
    url = get_primary_season_url()
    print(f"Versuche PRIMARY Scrape (Obfuscated): {url}")
    c = conn.cursor()
    count = 0
    
    try:
        r = requests.get(url, headers=get_header(), timeout=15)
        html_content = r.text
        
        pattern = r"SG\.container\.appPreloads\['[^']+'\]\s*=\s*(\[.*?\]);"
        matches = list(re.finditer(pattern, html_content, re.DOTALL))
        
        if not matches:
            print("  ⚠️ Kein JSON-Datenblock gefunden.")
            return 0
            
        print(f"  -> {len(matches)} JSON-Blöcke gefunden. Analysiere...")

        games_list = []
        for i, match in enumerate(matches):
            try:
                data = json.loads(match.group(1))
                found = find_games_list_recursive(data)
                if found:
                    print(f"  ✅ Spiele in Block {i+1} gefunden!")
                    games_list = found
                    break 
            except: continue

        if not games_list:
            print("  ⚠️ Keine Spiele gefunden.")
            return 0
        
        print(f"  -> Verarbeite {len(games_list)} Spiele...")
        default_img = decode_url(_IMG_DEFAULT_B64)

        for game in games_list:
            timestamp_ms = game.get("datum")
            if not timestamp_ms: continue
            
            dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")
            
            heim = game.get("heimName", "Unbekannt")
            gast = game.get("gastName", "Unbekannt")
            
            is_home = False
            for f in HOME_TEAM_FILTER:
                if f.lower() in heim.lower(): is_home = True; break
            if not is_home: continue
            
            bewerb = game.get("bewerbBezeichnung", "")
            tags = map_competition_to_tags(bewerb)
            
            # --- ORT BEREINIGEN (NEU) ---
            ort_raw = game.get("spielort", "")
            ort_clean = ort_raw.replace("in ", "").replace("In ", "").strip()
            
            # Wenn "U/GROUND" oder "Rattenfänger" vorkommt -> Standardisieren
            if "U/GROUND" in ort_clean or "Rattenfänger" in ort_clean:
                ort_clean = LOCATION_NAME
            
            if not ort_clean or len(ort_clean) < 3: ort_clean = LOCATION_NAME
            # ---------------------------
            
            title = f"{heim} - {gast}"
            
            desc = f"Wettbewerb: {bewerb}\nHeim: {heim}\nGast: {gast}"
            
            if game.get("abgeschlossen"):
                h_tore = game.get("heimTore", "0")
                g_tore = game.get("gastTore", "0")
                desc += f"\nEndstand: {h_tore}:{g_tore}"
                if "ergebnis" in game and game["ergebnis"]:
                    desc += f" {game['ergebnis']}"
                
            h = make_hash(f"{date_str}{heim}{gast}{time_str}")
            full_url = f"verband_{h}" 
            
            print(f"  [VERBAND] {date_str} | {title} | {tags}")
            
            c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                         ON CONFLICT(url) DO UPDATE SET 
                         title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                         time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                         image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                         (full_url, title, tags, date_str, date_str, time_str, ort_clean, desc, default_img, h, datetime.now().isoformat()))
            conn.commit()
            count += 1
            
    except Exception as e:
        print(f"Fehler: {e}")
        return 0
    return count

def scrape_secondary(conn):
    print("\n--- Fallback Scraper ---")
    c = conn.cursor()
    count = 0
    try:
        url = decode_url(_SOURCE_B_START_B64)
        base_url = decode_url(_SOURCE_B_BASE_B64)
        default_img = decode_url(_IMG_DEFAULT_B64)
        
        r = requests.get(url, headers=get_header(), timeout=15)
        from bs4 import BeautifulSoup
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
                    if not home_td or not guest_td: continue
                    home = home_td.get_text(strip=True)
                    guest = guest_td.get_text(strip=True)
                    is_home = False
                    for f in HOME_TEAM_FILTER:
                        if f in home: is_home = True; break
                    if not is_home: continue

                    raw_score = score_td.get_text(strip=True)
                    time_str = raw_score if re.match(r'^\d{1,2}:\d{2}$', raw_score) else ""
                    try: iso_date = datetime.strptime(current_date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
                    except: iso_date = None
                    title = f"{home} - {guest}"
                    desc = f"Liga: Meisterschaft (Fallback)\nHeim: {home}\nGast: {guest}"
                    h = make_hash(f"{iso_date}{home}{guest}")
                    full_url = f"liga_{h}"
                    print(f"  [FALLBACK] {iso_date} | {title}")
                    
                    c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                                 VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                                 ON CONFLICT(url) DO UPDATE SET 
                                 title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                                 time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                                 image_urls=excluded.image_urls, content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                                 (full_url, title, "Sport, Fussball, Meisterschaft", current_date_str, iso_date, time_str, LOCATION_NAME, desc, default_img, h, datetime.now().isoformat()))
                    conn.commit()
                    count += 1
    except Exception as e: print(e)
    return count

def run_correction(conn):
    """Führt eine direkte DB-Bereinigung durch"""
    print("\n--- 🔧 STARTE KORREKTUR-MODUS (-korr) ---")
    c = conn.cursor()
    
    # 1. Wir suchen alles, was "U/GROUND" oder "Arena" im Ort hat UND Fussball ist
    search_pattern = "%U/GROUND%"
    
    c.execute("SELECT count(*) FROM events WHERE location LIKE ? AND tags LIKE '%Fussball%'", (search_pattern,))
    count = c.fetchone()[0]
    
    if count > 0:
        print(f"  -> {count} Einträge mit altem Stadionnamen gefunden.")
        print(f"  -> Ersetze durch: {LOCATION_NAME}")
        
        c.execute("UPDATE events SET location = ? WHERE location LIKE ? AND tags LIKE '%Fussball%'", 
                  (LOCATION_NAME, search_pattern))
        conn.commit()
        print("  ✅ Bereinigung abgeschlossen.")
    else:
        print("  ✅ Keine Korrekturen notwendig (Alles sauber).")

def main():
    # Argumente parsen
    parser = argparse.ArgumentParser()
    parser.add_argument("-korr", action="store_true", help="Führt nur eine Korrektur der Stadionnamen in der DB durch")
    args = parser.parse_args()

    conn = init_db()

    if args.korr:
        # Nur Korrektur laufen lassen, kein Scraping
        run_correction(conn)
    else:
        # Normaler Modus
        print("--- KICKS SCRAPER ---")
        matches_found = scrape_primary(conn)
        if matches_found == 0:
            print("Wechsel zu Fallback...")
            scrape_secondary(conn)
        else:
            print(f"Fertig! {matches_found} Spiele geladen.")
            
        # Optional: Nach dem Scrape trotzdem kurz prüfen (schadet nicht)
        # run_correction(conn) 

    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__": main()
