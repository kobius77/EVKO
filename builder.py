import sqlite3
from datetime import datetime
import os
import json

# --- KONFIGURATION ---
DB_FILE = "evko.db"
HTML_FILE = "index.html"
JSON_FILE = "events.json"
AI_MARKER = "--- ZUSATZINFO AUS PLAKAT ---"

def get_subtle_color(text):
    """Generiert eine konsistente, sehr helle Pastellfarbe basierend auf dem Text."""
    if not text: return "#f0f0f0"
    hash_val = sum(ord(c) for c in text)
    hue = (hash_val * 37) % 360
    # 60% Sättigung, 96% Helligkeit -> Sehr dezent
    return f"hsl({hue}, 60%, 96%)"

def format_date_german(iso_date):
    """Wandelt YYYY-MM-DD in DD.MM.YYYY um"""
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d")
        return dt.strftime("%d.%m.%Y")
    except:
        return iso_date

def main():
    print("--- START BUILDER (No-Chat Edition) ---")
    if not os.path.exists(DB_FILE):
        print(f"Datenbank {DB_FILE} nicht gefunden.")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Zugriff über Spaltennamen ermöglichen
    c = conn.cursor()
    
    today_iso = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Daten abfragen
    try:
        c.execute("""
            SELECT date_str, title, tags, location, url, description, time_str, embedding 
            FROM events 
            WHERE start_iso >= ? 
            ORDER BY start_iso ASC, time_str ASC
        """, (today_iso,))
    except sqlite3.OperationalError:
        print("WARNUNG: Spalte 'embedding' fehlt in der DB. (embedder.py ausführen!)")
        print("Erstelle JSON ohne Vektoren...")
        c.execute("""
            SELECT date_str, title, tags, location, url, description, time_str, NULL as embedding
            FROM events 
            WHERE start_iso >= ? 
            ORDER BY start_iso ASC, time_str ASC
        """, (today_iso,))
    
    rows = c.fetchall()
    conn.close()

    print(f"Verarbeite {len(rows)} Events...")

    # --- HTML KOPF ---
    html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Korneuburg Events</title>
    <style>
        body {{ background-color: #fff; color: #111; font-family: "Courier New", monospace; padding: 20px; margin: 0; }}
        .container {{ max-width: 950px; margin: 0 auto; }}
        
        /* Tabelle */
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 50px; }}
        th {{ text-align: left; border-bottom: 2px solid #000; padding: 10px; text-transform: uppercase; font-size: 0.9em; }}
        td {{ border-bottom: 1px solid #ccc; padding: 12px 10px; vertical-align: top; }}
        tr:hover {{ background-color: #f9f9f9; }}
        
        /* Spaltenbreiten */
        .col-date {{ width: 150px; font-weight: bold; white-space: nowrap; }}
        .col-loc {{ width: 200px; font-size: 0.85em; color: #444; }}
        
        /* Tags */
        .tags-container {{ margin-top: 6px; display: flex; flex-wrap: wrap; gap: 6px; }}
        .tag {{ 
            font-size: 0.7em; 
            font-weight: 600; 
            text-transform: uppercase; 
            border: 1px solid rgba(0,0,0,0.05); 
            color: #444; 
            padding: 2px 6px; 
            border-radius: 4px; 
            white-space: nowrap; 
        }}
        
        /* Links & Text */
        .title a {{ font-size: 1.1em; font-weight: bold; color: #000; text-decoration: none; }}
        .title a:hover {{ text-decoration: underline; }}
        .ai-hint {{ cursor: help; font-size: 14px; text-decoration: none; margin-left: 5px; opacity: 0.6; }}
        
        footer {{ margin-top: 40px; padding-top: 10px; border-top: 2px solid #000; text-align: right; font-size: 0.75em; color: #555; }}
    </style>
</head>
<body>
    <div class="container">
        <table>
            <thead>
                <tr>
                    <th>Wann</th>
                    <th>Was</th>
                    <th>Wo</th>
                </tr>
            </thead>
            <tbody>
    """

    json_data = []

    for row in rows:
        # Daten aus Row extrahieren
        date_iso = row['date_str'] 
        title = row['title']
        tags_str = row['tags']
        location = row['location']
        url = row['url']
        desc = row['description']
        time_str = row['time_str']
        emb_json = row['embedding']

        # 1. Beschreibung bereinigen & AI Tooltip erstellen
        clean_desc = desc or ""
        ai_tooltip = ""
        
        if AI_MARKER in clean_desc:
            parts = clean_desc.split(AI_MARKER)
            clean_desc = parts[0].strip()
            
            # Prüfen ob AI-Text vorhanden und sinnvoll ist
            if len(parts) > 1 and len(parts[1].strip()) > 10:
                ai_text = parts[1].strip()
                if "tut mir leid" not in ai_text.lower():
                    # HTML-Safe machen für title-Attribut
                    safe_ai = ai_text.replace('"', '&quot;').replace('\n', ' &#10; ')
                    ai_tooltip = f'<span class="ai-hint" title="KI-Infos vom Plakat:&#10;{safe_ai}">ℹ️</span>'

        # 2. Vektor parsen (für JSON)
        vector = []
        if emb_json:
            try:
                vector = json.loads(emb_json)
            except:
                vector = []

        # 3. HTML Datum formatieren (Schön machen!)
        nice_date = format_date_german(date_iso)
        
        display_date = nice_date
        if time_str and time_str != "00:00":
            display_date += f"<br><span style='font-weight:normal; font-size:0.85em; color:#666;'>{time_str} Uhr</span>"

        # 4. Tags HTML bauen (mit Pastellfarben)
        tags_html = ""
        tag_list = []
        if tags_str:
            tag_list = [t.strip() for t in tags_str.split(",") if t.strip()]
            for tag in tag_list:
                bg_color = get_subtle_color(tag)
                tags_html += f'<span class="tag" style="background-color: {bg_color};">{tag}</span>'
            
            if tags_html:
                tags_html = f'<div class="tags-container">{tags_html}</div>'

        # 5. Tabellenzeile hinzufügen
        html_content += f"""
                <tr>
                    <td class="col-date">{display_date}</td>
                    <td>
                        <div class="title"><a href="{url}" target="_blank">{title}</a> {ai_tooltip}</div>
                        {tags_html}
                    </td>
                    <td class="col-loc">{location}</td>
                </tr>
        """
        
        # 6. JSON Datensatz erstellen (inkl. Embedding für n8n)
        json_data.append({
            "date": date_iso, 
            "nice_date": nice_date, 
            "time": time_str,
            "title": title,
            "location": location,
            "tags": tag_list,
            "url": url,
            "description": clean_desc,
            "embedding": vector
        })

    html_content += f"""
            </tbody>
        </table>
        <footer>
            Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {len(rows)} Events
        </footer>
    </div>
</body>
</html>
    """

    # HTML Speichern
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)

    # JSON Speichern
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Builder fertig.")
    print(f"   - HTML: {HTML_FILE}")
    print(f"   - JSON: {JSON_FILE} (Größe: {os.path.getsize(JSON_FILE)/1024:.1f} KB)")

if __name__ == "__main__":
    main()
