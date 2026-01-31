import sqlite3
import os
from datetime import datetime

# --- KONFIGURATION ---
DB_FILE = "evko.db"
OUTPUT_FILE = "index.html"
AI_MARKER = "--- ZUSATZINFO AUS PLAKAT ---"

def build_site():
    if not os.path.exists(DB_FILE):
        print(f"Fehler: {DB_FILE} nicht gefunden.")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Nur Zukunft & Heute
    today_iso = datetime.now().strftime("%Y-%m-%d")
    print(f"Erstelle Tabelle ab: {today_iso}")

    c.execute("""
        SELECT * FROM events 
        WHERE start_iso >= ?
        ORDER BY start_iso ASC, time_str ASC
    """, (today_iso,))
    
    events = c.fetchall()

    html = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <title>Events Korneuburg</title>
        <style>
            body { 
                font-family: sans-serif; 
                background-color: #ffffff; 
                color: #000000; 
                padding: 20px;
            }
            table { 
                width: 100%; 
                border-collapse: collapse; 
                font-size: 14px;
            }
            th, td { 
                border: 1px solid #000; 
                padding: 8px; 
                text-align: left; 
                vertical-align: middle;
            }
            th { background-color: #eee; font-weight: bold; }
            a { color: #000; text-decoration: underline; }
            .ai-hint { cursor: help; font-size: 12px; margin-left: 5px; text-decoration: none; }
        </style>
    </head>
    <body>
        <h1>Veranstaltungskalender</h1>
        <table>
            <thead>
                <tr>
                    <th style="width: 100px;">Datum</th>
                    <th style="width: 60px;">Zeit</th>
                    <th>Titel</th>
                    <th>Ort</th>
                    <th>Tags</th>
                </tr>
            </thead>
            <tbody>
    """

    for e in events:
        # Datum formatieren
        date_obj = datetime.strptime(e['start_iso'], "%Y-%m-%d")
        nice_date = date_obj.strftime("%d.%m.%Y")
        
        # Wochentag kurz
        days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
        weekday = days[date_obj.weekday()]
        date_str = f"{weekday}, {nice_date}"

        # Zeit
        time_str = e['time_str'] if e['time_str'] else ""

        # AI Tooltip Vorbereitung (falls Zusatzinfos da sind, nur als kleines Icon)
        ai_tooltip = ""
        full_text = e['description'] or ""
        if AI_MARKER in full_text:
            parts = full_text.split(AI_MARKER)
            if len(parts) > 1 and len(parts[1].strip()) > 10:
                ai_content = parts[1].strip()
                if "tut mir leid" not in ai_content.lower():
                    # Bereinigen für HTML-Attribut
                    clean_ai = ai_content.replace('"', '&quot;').replace('\n', ' &#10; ')
                    ai_tooltip = f'<span class="ai-hint" title="{clean_ai}">ℹ️</span>'

        # Tags aufräumen
        tags = e['tags'] if e['tags'] else ""

        html += f"""
                <tr>
                    <td>{date_str}</td>
                    <td>{time_str}</td>
                    <td><a href="{e['url']}" target="_blank">{e['title']}</a> {ai_tooltip}</td>
                    <td>{e['location']}</td>
                    <td>{tags}</td>
                </tr>
        """

    html += """
            </tbody>
        </table>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ {OUTPUT_FILE} (Tabelle pur) erstellt.")

if __name__ == "__main__":
    build_site()
