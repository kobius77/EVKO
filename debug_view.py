import sqlite3
import os

DB_FILE = "evko.db"

def generate_debug_html():
    if not os.path.exists(DB_FILE):
        print("Fehler: Keine Datenbank gefunden.")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Erlaubt Zugriff via Spaltennamen
    c = conn.cursor()

    # Wir suchen exakt nach dem Kindermaskenball
    print("Suche nach Event...")
    c.execute("SELECT * FROM events WHERE title LIKE '%Kindermaskenball%'")
    event = c.fetchone()

    if not event:
        print("KEIN EVENT GEFUNDEN! Haben Sie den Scraper laufen lassen?")
        return

    # HTML zusammenbauen
    html_content = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Debug: {event['title']}</title>
        <style>
            body {{ font-family: sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; line-height: 1.6; }}
            .card {{ border: 1px solid #ddd; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; }}
            .label {{ font-weight: bold; color: #555; display: inline-block; width: 120px; }}
            .tag {{ background: #007bff; color: white; padding: 3px 8px; border-radius: 12px; font-size: 0.85em; margin-right: 5px; }}
            .desc-box {{ background: #f8f9fa; border-left: 5px solid #28a745; padding: 15px; margin-top: 10px; white-space: pre-wrap; }}
            .ai-highlight {{ background-color: #e8f5e9; font-weight: bold; color: #1b5e20; }}
            img {{ max-width: 100%; height: auto; margin-top: 15px; border: 1px solid #ccc; }}
        </style>
    </head>
    <body>
        <h1>🔍 Debug Ansicht</h1>
        <div class="card">
            <h2>{event['title']}</h2>
            
            <p><span class="label">📅 Datum:</span> {event['date_str']}</p>
            <p><span class="label">📍 Ort:</span> {event['location']}</p>
            <p><span class="label">🏷️ Tags:</span> 
                {''.join([f'<span class="tag">{t.strip()}</span>' for t in event['tags'].split(',') if t])}
            </p>
            <p><span class="label">🔗 Original:</span> <a href="{event['url']}" target="_blank">Link zur Stadt-Webseite</a></p>
            
            <hr>
            
            <h3>📝 Beschreibung (Datenbank Inhalt)</h3>
            <p><em>Achten Sie unten auf den Abschnitt "ZUSATZINFO AUS PLAKAT" – das kommt von der AI!</em></p>
            <div class="desc-box">{event['description'].replace("--- ZUSATZINFO AUS PLAKAT ---", "<span class='ai-highlight'>--- ZUSATZINFO AUS PLAKAT ---</span>")}</div>
            
            <hr>
            
            <h3>🖼️ Gespeicherte Bilder</h3>
            {''.join([f'<img src="{url}" /><br>' for url in event['image_urls'].split(',') if url])}
        </div>
    </body>
    </html>
    """

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print("✅ index.html wurde erfolgreich erstellt! Öffnen Sie die Datei im Browser.")

if __name__ == "__main__":
    generate_debug_html()
