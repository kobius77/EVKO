import sqlite3
from datetime import datetime
import os
import json

# --- KONFIGURATION ---
DB_FILE = "evko.db"
HTML_FILE = "index.html"
JSON_FILE = "events.json"
AI_MARKER = "--- ZUSATZINFO AUS PLAKAT ---"

# Placeholder für Ihre n8n URL (bitte später anpassen!)
N8N_WEBHOOK_URL = "https://n8.oida.top/webhook/2c8fbf33-31cc-44eb-8715-cead932853f7" 

def get_subtle_color(text):
    """Generiert eine konsistente, sehr helle Pastellfarbe basierend auf dem Text."""
    if not text: return "#f0f0f0"
    hash_val = sum(ord(c) for c in text)
    hue = (hash_val * 37) % 360
    # 60% Sättigung, 96% Helligkeit -> Sehr dezent
    return f"hsl({hue}, 60%, 96%)"

def main():
    print("--- START BUILDER (RAG Edition) ---")
    if not os.path.exists(DB_FILE):
        print(f"Datenbank {DB_FILE} nicht gefunden.")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Zugriff über Spaltennamen ermöglichen
    c = conn.cursor()
    
    today_iso = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Daten abfragen
    # Wir versuchen, die 'embedding' Spalte mitzuladen.
    # Falls embedder.py noch nie lief, fangen wir den Fehler ab.
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
        # Fallback-Query ohne Embedding
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
        
        /* Chat Widget Styles */
        #chat-widget {{ position: fixed; bottom: 20px; right: 20px; width: 320px; z-index: 1000; font-family: sans-serif; }}
        #chat-toggle {{ background: #222; color: #fff; border: none; padding: 12px 20px; cursor: pointer; border-radius: 8px; font-weight: bold; width: 100%; text-align: left; box-shadow: 0 4px 12px rgba(0,0,0,0.15); display: flex; justify-content: space-between; align-items: center; }}
        #chat-window {{ display: none; background: #fff; border: 1px solid #ddd; height: 450px; flex-direction: column; margin-bottom: 10px; box-shadow: 0 5px 25px rgba(0,0,0,0.2); border-radius: 8px; overflow: hidden; }}
        #chat-messages {{ flex: 1; padding: 15px; overflow-y: auto; font-size: 14px; background: #f9f9f9; display: flex; flex-direction: column; gap: 10px; }}
        #chat-input-area {{ display: flex; border-top: 1px solid #eee; background: #fff; padding: 5px; }}
        #chat-input {{ flex: 1; border: none; padding: 12px; outline: none; font-size: 14px; }}
        #chat-send {{ background: #fff; color: #222; border: none; padding: 0 15px; cursor: pointer; font-size: 18px; }}
        #chat-send:hover {{ color: #007bff; }}
        .msg {{ padding: 10px 14px; border-radius: 12px; max-width: 85%; line-height: 1.4; word-wrap: break-word; }}
        .msg.user {{ background: #222; color: #fff; align-self: flex-end; border-bottom-right-radius: 2px; }}
        .msg.bot {{ background: #e9ecef; color: #333; align-self: flex-start; border-bottom-left-radius: 2px; }}
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
        date_str = row['date_str']
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

        # 3. HTML Datum formatieren
        display_date = date_str
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
            "date": date_str,
            "time": time_str,
            "title": title,
            "location": location,
            "tags": tag_list,
            "url": url,
            "description": clean_desc, # Nur der saubere Text für den Chatbot-Kontext
            "embedding": vector        # Der Vektor für die Ähnlichkeitssuche
        })

    # Chat Widget JavaScript hinzufügen
    chat_script = f"""
    <div id="chat-widget">
        <div id="chat-window">
            <div id="chat-messages">
                <div class="msg bot">Hallo! Ich bin dein Event-Assistent. Suche z.B. nach "Sport am Wochenende" oder "Konzerte".</div>
            </div>
            <div id="chat-input-area">
                <input type="text" id="chat-input" placeholder="Frage stellen..." onkeypress="handleKey(event)">
                <button id="chat-send" onclick="sendMessage()">➤</button>
            </div>
        </div>
        <button id="chat-toggle" onclick="toggleChat()"><span>💬 Frage stellen</span> <span>▲</span></button>
    </div>

    <script>
        const WEBHOOK_URL = '{N8N_WEBHOOK_URL}';

        function toggleChat() {{
            const win = document.getElementById('chat-window');
            const btn = document.getElementById('chat-toggle').lastElementChild;
            if (win.style.display === 'flex') {{
                win.style.display = 'none';
                btn.innerText = '▲';
            }} else {{
                win.style.display = 'flex';
                btn.innerText = '▼';
                setTimeout(() => document.getElementById('chat-input').focus(), 100);
            }}
        }}

        function handleKey(e) {{
            if(e.key === 'Enter') sendMessage();
        }}

        async function sendMessage() {{
            const input = document.getElementById('chat-input');
            const text = input.value.trim();
            if (!text) return;

            addMessage(text, 'user');
            input.value = '';
            
            // Lade-Indikator
            const loadingId = addMessage('...', 'bot');

            try {{
                const response = await fetch(WEBHOOK_URL, {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{ question: text }})
                }});
                
                const data = await response.json();
                
                // Lade-Indikator mit Antwort ersetzen
                document.getElementById(loadingId).innerText = data.answer || "Keine Antwort erhalten.";
            
            }} catch (error) {{
                console.error(error);
                document.getElementById(loadingId).innerText = "Fehler: Konnte n8n nicht erreichen.";
            }}
        }}

        function addMessage(text, sender) {{
            const div = document.createElement('div');
            div.className = 'msg ' + sender;
            div.innerText = text;
            div.id = 'msg-' + Date.now();
            
            const container = document.getElementById('chat-messages');
            container.appendChild(div);
            container.scrollTop = container.scrollHeight;
            return div.id;
        }}
    </script>
    """

    html_content += f"""
            </tbody>
        </table>
        <footer>
            Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {len(rows)} Events
        </footer>
    </div>
    {chat_script}
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
