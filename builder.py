import sqlite3
import os
from datetime import datetime

# Konfiguration
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

    # Nur künftige Events oder alle? Hier: Alle, sortiert nach Datum
    # Wir nehmen start_iso für die Sortierung, falls vorhanden
    c.execute("""
        SELECT * FROM events 
        ORDER BY CASE WHEN start_iso IS NULL THEN 1 ELSE 0 END, start_iso ASC
    """)
    events = c.fetchall()

    html_head = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Veranstaltungen Korneuburg</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #f8f9fa; color: #333; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
            h1 { text-align: center; margin-bottom: 2rem; color: #2c3e50; }
            .card { background: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 20px; overflow: hidden; display: flex; flex-direction: column; }
            .card-header { padding: 15px 20px; background: #fff; border-bottom: 1px solid #eee; }
            .card-header h2 { margin: 0; font-size: 1.25rem; color: #007bff; }
            .meta { font-size: 0.9rem; color: #666; margin-top: 5px; display: flex; flex-wrap: wrap; gap: 15px; }
            .meta span { display: inline-flex; align-items: center; }
            .card-body { padding: 20px; }
            .desc { line-height: 1.6; white-space: pre-wrap; }
            .tags { margin-top: 15px; }
            .tag { background: #e9ecef; color: #495057; padding: 2px 8px; border-radius: 12px; font-size: 0.8rem; margin-right: 5px; display: inline-block; }
            .ai-icon { cursor: help; font-size: 1.1rem; vertical-align: middle; margin-left: 5px; }
            .gallery { display: flex; gap: 10px; overflow-x: auto; padding-top: 15px; }
            .gallery img { height: 80px; border-radius: 4px; border: 1px solid #ddd; }
            .footer { text-align: center; margin-top: 40px; color: #aaa; font-size: 0.8rem; }
            a { color: inherit; text-decoration: none; }
            a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <h1>📅 Events in Korneuburg</h1>
    """

    html_body = ""

    for e in events:
        # --- LOGIK: AI TEXT TRENNEN ---
        full_text = e['description'] or ""
        human_text = full_text
        ai_tooltip = ""
        has_ai = False

        if AI_MARKER in full_text:
            parts = full_text.split(AI_MARKER)
            human_text = parts[0].strip()
            # Der Teil nach dem Marker ist der AI Text
            if len(parts) > 1 and len(parts[1].strip()) > 10:
                ai_content = parts[1].strip()
                # Einfacher Check: Ist das wirklich Info oder nur Müll?
                if "tut mir leid" not in ai_content.lower():
                    has_ai = True
                    # Für den Tooltip bereinigen wir Anführungszeichen
                    ai_tooltip = ai_content.replace('"', '&quot;').replace('\n', ' &#10; ')

        # --- HTML ZUSAMMENBAUEN ---
        
        # Zeit formatieren
        date_display = e['date_str']
        if e['time_str']:
            date_display += f", {e['time_str']}"

        # Tags & Emoji
        tags_html = "".join([f'<span class="tag">{t.strip()}</span>' for t in e['tags'].split(',') if t])
        
        # Das Emoji wird nur angezeigt, wenn has_ai True ist
        # title="..." erzeugt den Tooltip beim Hover
        emoji_html = f'<span class="ai-icon" title="🔍 Infos aus Plakat:\n{ai_tooltip}">🖼️</span>' if has_ai else ''

        # Bilder
        imgs_html = ""
        if e['image_urls']:
            for url in e['image_urls'].split(','):
                if url.strip():
                    imgs_html += f'<a href="{url}" target="_blank"><img src="{url}" loading="lazy"></a>'
        if imgs_html:
            imgs_html = f'<div class="gallery">{imgs_html}</div>'

        html_body += f"""
        <div class="card">
            <div class="card-header">
                <h2><a href="{e['url']}" target="_blank">{e['title']}</a></h2>
                <div class="meta">
                    <span>📅 {date_display}</span>
                    <span>📍 {e['location']}</span>
                </div>
            </div>
            <div class="card-body">
                <div class="desc">{human_text}</div>
                {imgs_html}
                <div class="tags">
                    {tags_html} {emoji_html}
                </div>
            </div>
        </div>
        """

    html_footer = f"""
        <div class="footer">
            Zuletzt aktualisiert: {datetime.now().strftime('%d.%m.%Y %H:%M')}
        </div>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html_head + html_body + html_footer)

    print(f"✅ {OUTPUT_FILE} wurde erfolgreich erstellt ({len(events)} Events).")

if __name__ == "__main__":
    build_site()
