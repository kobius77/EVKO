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

    # --- FILTER: NUR ZUKUNFT & HEUTE ---
    today_iso = datetime.now().strftime("%Y-%m-%d")
    print(f"Erstelle Website für Events ab: {today_iso}")

    c.execute("""
        SELECT * FROM events 
        WHERE start_iso >= ?
        ORDER BY start_iso ASC, time_str ASC
    """, (today_iso,))
    
    events = c.fetchall()

    # --- HTML HEADER (CSS für reduziertes Design) ---
    html_head = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Korneuburg Events</title>
        <style>
            :root { --primary: #2c3e50; --accent: #3498db; --bg: #f8f9fa; --card-bg: #ffffff; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
                background: var(--bg); color: #333; 
                max-width: 700px; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; 
            }
            h1 { text-align: center; color: var(--primary); font-weight: 300; margin-bottom: 2rem; }
            
            /* Karten-Design */
            .event-card { 
                background: var(--card-bg); border-radius: 8px; 
                box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 1.5rem; 
                padding: 1.25rem; transition: transform 0.2s; border: 1px solid #eee;
            }
            .event-card:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(0,0,0,0.08); }

            /* Kopfzeile: Datum & Titel */
            .event-header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 0.5rem; }
            .event-date { font-weight: 700; color: var(--accent); font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.5px; }
            .event-title { margin: 0; font-size: 1.2rem; font-weight: 600; color: var(--primary); }
            .event-title a { text-decoration: none; color: inherit; }
            .event-title a:hover { text-decoration: underline; }

            /* Meta-Infos (Zeit, Ort) */
            .event-meta { font-size: 0.85rem; color: #7f8c8d; margin-bottom: 0.75rem; display: flex; gap: 1rem; }
            .event-meta span { display: flex; align-items: center; gap: 4px; }

            /* Beschreibung */
            .event-desc { font-size: 0.95rem; color: #444; white-space: pre-wrap; margin-bottom: 1rem; }

            /* Footer: Tags & Bilder */
            .event-footer { display: flex; justify-content: space-between; align-items: center; border-top: 1px solid #f1f1f1; padding-top: 0.75rem; }
            
            /* Tags */
            .tags { display: flex; flex-wrap: wrap; gap: 6px; }
            .tag { background: #f1f3f5; color: #555; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; }
            
            /* AI Icon Tooltip */
            .ai-icon { cursor: help; font-size: 1.1rem; margin-left: 5px; opacity: 0.8; }
            .ai-icon:hover { opacity: 1; transform: scale(1.1); }

            /* Mini-Galerie (nur wenn Bilder da sind) */
            .mini-gallery img { height: 40px; width: 40px; object-fit: cover; border-radius: 4px; border: 1px solid #ddd; margin-left: 8px; }

            /* Leere Nachricht */
            .empty-msg { text-align: center; color: #999; margin-top: 3rem; }
            .footer-info { text-align: center; margin-top: 3rem; font-size: 0.8rem; color: #aaa; }
        </style>
    </head>
    <body>
        <h1>📅 Aktuelles in Korneuburg</h1>
    """

    html_body = ""

    if not events:
        html_body = "<div class='empty-msg'>Keine anstehenden Veranstaltungen gefunden.</div>"

    for e in events:
        # --- LOGIK: TEXT BEREINIGEN ---
        full_text = e['description'] or ""
        human_text = full_text
        ai_tooltip = ""
        has_ai = False

        if AI_MARKER in full_text:
            parts = full_text.split(AI_MARKER)
            human_text = parts[0].strip()
            
            # AI Text für Tooltip extrahieren
            if len(parts) > 1 and len(parts[1].strip()) > 10:
                ai_content = parts[1].strip()
                if "tut mir leid" not in ai_content.lower():
                    has_ai = True
                    # Tooltip-Safe machen
                    ai_tooltip = ai_content.replace('"', '&quot;').replace('\n', ' &#10; ')

        # --- DATUM & ZEIT FORMATIEREN ---
        # Wir machen das Datum schön: "2026-03-06" -> "06.03.2026"
        date_obj = datetime.strptime(e['start_iso'], "%Y-%m-%d")
        nice_date = date_obj.strftime("%d.%m.%Y")
        
        # Wochentag dazu (optional, aber hübsch)
        days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
        weekday = days[date_obj.weekday()]
        
        date_display = f"{weekday}, {nice_date}"

        time_display = e['time_str'] if e['time_str'] else ""

        # --- BILDER ---
        # Nur anzeigen, wenn URL existiert (Stadt). Sport hat "" -> wird ignoriert.
        imgs_html = ""
        if e['image_urls']:
            # Wir nehmen nur das erste Bild für die Mini-Ansicht
            first_img = e['image_urls'].split(',')[0]
            if first_img.strip():
                imgs_html = f'<a href="{first_img}" target="_blank" class="mini-gallery"><img src="{first_img}" loading="lazy"></a>'

        # --- TAGS & EMOJI ---
        tags_html = ""
        if e['tags']:
            # Nur die ersten 3 Tags anzeigen, damit es clean bleibt
            tag_list = [t.strip() for t in e['tags'].split(',') if t]
            tags_html = "".join([f'<span class="tag">{t}</span>' for t in tag_list])

        ai_html = f'<span class="ai-icon" title="🔍 KI-Infos aus Plakat:\n{ai_tooltip}">🖼️</span>' if has_ai else ''

        # --- HTML KARTE ---
        html_body += f"""
        <div class="event-card">
            <div class="event-header">
                <span class="event-date">{date_display}</span>
            </div>
            <h2 class="event-title"><a href="{e['url']}" target="_blank">{e['title']}</a></h2>
            
            <div class="event-meta">
                {f'<span>🕒 {time_display}</span>' if time_display else ''}
                <span>📍 {e['location']}</span>
            </div>

            <div class="event-desc">{human_text}</div>

            <div class="event-footer">
                <div class="tags">
                    {tags_html} {ai_html}
                </div>
                {imgs_html}
            </div>
        </div>
        """

    html_footer = f"""
        <div class="footer-info">
            Zuletzt aktualisiert: {datetime.now().strftime('%d.%m.%Y %H:%M')}
        </div>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html_head + html_body + html_footer)

    print(f"✅ {OUTPUT_FILE} (Clean Design) erfolgreich erstellt.")

if __name__ == "__main__":
    build_site()
