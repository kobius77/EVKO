import sqlite3
from datetime import datetime
import os

# Konfiguration
DB_FILE = "evko.db"
HTML_FILE = "index.html"

def parse_date_for_sort(date_str):
    """Hilfsfunktion: Wandelt '01.02.2026' in ein echtes Datum um für Sortierung."""
    try:
        clean_date = date_str.split("-")[0].strip()
        return datetime.strptime(clean_date, "%d.%m.%Y")
    except:
        return datetime.max

def main():
    print("--- START BUILDER ---")
    
    if not os.path.exists(DB_FILE):
        print(f"Fehler: Datenbank '{DB_FILE}' nicht gefunden.")
        return

    # 1. Daten lesen (Read Only)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Wir holen nur, was wir für die Tabelle brauchen
    c.execute("SELECT date_str, title, location, url, description, image_urls FROM events")
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("Keine Events in der Datenbank gefunden.")
        return

    # 2. Sortieren
    rows.sort(key=lambda x: parse_date_for_sort(x[0]))

    # 3. HTML Generieren
    print(f"Generiere HTML aus {len(rows)} Datensätzen...")
    
    html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EVKO - Event Liste</title>
    <style>
        body {{
            background-color: #ffffff;
            color: #1a1a1a;
            font-family: "Courier New", Courier, monospace;
            margin: 0;
            padding: 20px;
            line-height: 1.4;
        }}
        .container {{ max-width: 900px; margin: 0 auto; }}
        header {{ text-align: center; border-bottom: 3px double #000; margin-bottom: 30px; padding-bottom: 20px; }}
        h1 {{ margin: 0; text-transform: uppercase; letter-spacing: 2px; }}
        .stats {{ font-size: 0.8em; color: #555; margin-top: 5px; }}
        
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ text-align: left; border-bottom: 2px solid #000; padding: 10px 5px; text-transform: uppercase; }}
        td {{ border-bottom: 1px solid #ddd; padding: 12px 5px; vertical-align: top; }}
        tr:hover {{ background-color: #f9f9f9; }}
        
        .date {{ white-space: nowrap; font-weight: bold; width: 110px; }}
        .title a {{ color: #000; text-decoration: none; font-weight: bold; border-bottom: 1px dotted #999; }}
        .title a:hover {{ background: #000; color: #fff; }}
        .location {{ font-size: 0.9em; color: #444; }}
        .desc-preview {{ font-size: 0.8em; color: #666; margin-top: 4px; display: block; }}
        
        footer {{ margin-top: 50px; border-top: 1px solid #ccc; padding-top: 10px; text-align: center; font-size: 0.8em; color: #888; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>EVKO Datenbank</h1>
            <div class="stats">
                Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {len(rows)} Einträge
            </div>
        </header>

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

    for date, title, location, url, desc, imgs in rows:
        # Kurzer Teaser aus der Beschreibung (erste 100 Zeichen)
        short_desc = (desc[:90] + '...') if desc and len(desc) > 90 else ""
        
        html_content += f"""
                <tr>
                    <td class="date">{date}</td>
                    <td class="title">
                        <a href="{url}" target="_blank">{title}</a>
                        {f'<span class="desc-preview">{short_desc}</span>' if short_desc else ''}
                    </td>
                    <td class="location">{location}</td>
                </tr>
        """

    html_content += """
            </tbody>
        </table>
        <footer>
            Generiert durch EVKO Page Builder
        </footer>
    </div>
</body>
</html>
    """

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"Fertig: {HTML_FILE} wurde aktualisiert.")

if __name__ == "__main__":
    main()
