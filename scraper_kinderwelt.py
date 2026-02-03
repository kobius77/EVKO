import sqlite3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import hashlib
import time
import os
import json
import re
from datetime import datetime
from urllib.parse import urljoin
import openai

# --- KONFIGURATION ---
DB_FILE = "evko.db"
STATE_FILE = "kinderwelt.state"
BASE_URL = "https://kinderwelt-korneuburg.at"
START_URL = "https://kinderwelt-korneuburg.at/index.php?option=com_content&view=featured&Itemid=110"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

ua = UserAgent()

def get_header():
    return {'User-Agent': ua.random}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS events (
        url TEXT PRIMARY KEY, title TEXT, tags TEXT, date_str TEXT, start_iso TEXT, 
        time_str TEXT, location TEXT, description TEXT, image_urls TEXT, 
        content_hash TEXT, last_scraped TIMESTAMP
    )''')
    try: c.execute("ALTER TABLE events ADD COLUMN embedding TEXT")
    except: pass
    try: c.execute("ALTER TABLE events ADD COLUMN embedding_hash TEXT")
    except: pass
    conn.commit()
    return conn

def make_hash(s): return hashlib.md5(s.encode('utf-8')).hexdigest()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f: return f.read().strip()
    return ""

def save_state(new_hash):
    with open(STATE_FILE, "w") as f: f.write(new_hash)

# --- AI ANALYSE (Text + normale Bilder) ---
def analyze_content_with_ai(text_content, image_urls):
    if not client: return []

    print(f"    🧠 Frage AI (Textlänge: {len(text_content)}, Bilder: {len(image_urls)})...")
    
    prompt = """
    Analysiere diesen Webseiten-Text. Er kann MEHRERE verschiedene Veranstaltungen enthalten.
    Extrahiere alle zukünftigen Events als JSON-Liste.
    
    Format:
    {
        "events": [
            {
                "title": "Titel des Events",
                "date_iso": "YYYY-MM-DD",
                "time": "HH:MM" (oder null),
                "location": "Ort" (kurz),
                "description": "Zusammenfassung (max 2 Sätze)"
            },
            ...
        ]
    }
    
    Regeln:
    1. Ignoriere Rückblicke.
    2. Wenn KEIN Event gefunden wird: "events": []
    3. Nutze das aktuelle Jahr (oder nächstes), falls im Text nur Tag/Monat steht.
    """

    try:
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "text", "text": f"Webseiten-Inhalt:\n{text_content[:2500]}"} 
                ]
            }
        ]
        
        # Nur echte URLs anhängen, keine Base64 Blobs
        for img_url in image_urls[:2]:
            messages[0]["content"].append(
                {"type": "image_url", "image_url": {"url": img_url, "detail": "low"}}
            )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=600,
            response_format={"type": "json_object"}
        )
        
        data = json.loads(response.choices[0].message.content)
        return data.get("events", [])
        
    except Exception as e:
        print(f"    ⚠️ AI Fehler: {e}")
        return []

def main():
    print("--- START KINDERWELT SCRAPER (Text Only) ---")
    
    try:
        r = requests.get(START_URL, headers=get_header(), timeout=15)
        soup = BeautifulSoup(r.content, 'html.parser')
    except Exception as e:
        print(f"❌ Fehler Startseite: {e}")
        return

    articles = []
    blog_container = soup.select_one('.blog-featured')
    if blog_container:
        items = blog_container.select('div[class*="leading-"]')
        articles.extend(items)
        
    print(f"🔍 Gefunden: {len(articles)} Beiträge.")

    if not articles:
        print("❌ Keine Joomla-Artikel gefunden.")
        return

    # Change Detection (Hash über alle Überschriften)
    state_str = ""
    for art in articles:
        h1 = art.find('h1', class_='item-title')
        if h1: state_str += h1.get_text(strip=True)
    
    current_hash = make_hash(state_str)
    
    if current_hash == load_state():
        print("💤 Startseite unverändert (Hash Match).")
        return
    
    print("✨ Änderungen erkannt! Analysiere Beiträge...")
    conn = init_db()
    c = conn.cursor()

    for i, art in enumerate(articles):
        h1 = art.find('h1', class_='item-title')
        title_raw = h1.get_text(strip=True) if h1 else f"Beitrag {i}"
        print(f"\nPrüfe Post: {title_raw}")
        
        post_link = START_URL
        a_tag = h1.find('a') if h1 else None
        if a_tag and a_tag.get('href'):
            post_link = urljoin(BASE_URL, a_tag.get('href'))

        full_text = art.get_text(separator="\n", strip=True)
        
        # Bilder sammeln (NUR ECHTE URLS)
        clean_images = []
        
        for img in art.find_all('img'):
            src = img.get('src')
            if not src: continue
            
            # Base64 ignorieren
            if "data:image" in src: continue
            
            # URL fixen
            src = urljoin(BASE_URL, src)
            
            # Icons ignorieren
            if "spacer.gif" in src or "printButton" in src or "logo" in src.lower(): continue
            
            clean_images.append(src)

        # AI Analyse
        extracted_events = analyze_content_with_ai(full_text, clean_images)
        
        if not extracted_events:
            print("  -> Keine Events gefunden.")
            continue
            
        for evt in extracted_events:
            evt_title = evt.get('title', 'Unbekannt')
            evt_date = evt.get('date_iso')
            
            if not evt_date: continue
            
            print(f"  ✅ GEFUNDEN: {evt_date} | {evt_title}")
            
            # Unique ID bauen
            unique_part = make_hash(f"{evt_date}{evt_title}")
            unique_url = f"{post_link}#{unique_part}"
            
            # Das erste saubere Bild für die DB verwenden
            main_img = clean_images[0] if clean_images else ""
            
            h_content = make_hash(json.dumps(evt, sort_keys=True))
            
            c.execute('''INSERT INTO events (url, title, tags, date_str, start_iso, time_str, location, description, image_urls, content_hash, last_scraped) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?) 
                         ON CONFLICT(url) DO UPDATE SET 
                         title=excluded.title, tags=excluded.tags, date_str=excluded.date_str, start_iso=excluded.start_iso, 
                         time_str=excluded.time_str, location=excluded.location, description=excluded.description, 
                         content_hash=excluded.content_hash, last_scraped=excluded.last_scraped''', 
                         (
                             unique_url, 
                             evt_title, 
                             "Kinder, Familie, Freizeit", 
                             evt_date, 
                             evt_date, 
                             evt.get('time', ''), 
                             evt.get('location', 'Korneuburg'), 
                             evt.get('description', ''), 
                             main_img, 
                             h_content, 
                             datetime.now().isoformat()
                         ))
            conn.commit()
            
        time.sleep(1)

    save_state(current_hash)
    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__":
    main()