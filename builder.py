import sqlite3
from datetime import datetime
import os

DB_FILE = "evko.db"
HTML_FILE = "index.html"

def parse_date_for_sort(date_str):
    try:
        clean_date = date_str.split("-")[0].strip()
        return datetime.strptime(clean_date, "%d.%m.%Y")
    except:
        return datetime.max

def main():
    print("--- START BUILDER (Minimal Layout) ---")
    if not os.path.exists(DB_FILE): return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Daten lesen (inkl. Category)
    c.execute("SELECT date_str, title, category, location, url, description FROM events")
    rows = c.fetchall()
    conn.close()

    # Sortieren
    rows.sort(key=lambda x: parse_date_for_sort(x[0]))

    print(f"Generiere HTML aus {len(rows)} Datensätzen...")

    html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EVKO Events</title>
    <style>
        body {{ 
            background-color: #fff; 
            color: #111; 
            font-family: "Courier New", monospace; 
            padding: 20px; 
            margin: 0;
        }}
        .container {{ 
            max-width: 900px; 
            margin: 0 auto; 
        }}
        
        /* Tabelle Styling */
        table {{ 
            width: 100%; 
            border-collapse: collapse; 
            margin-top: 10px; 
        }}
        th {{ 
            text-align: left; 
            border-bottom: 2px solid #000; 
            padding: 10px; 
            text-transform: uppercase; 
            font-size: 0.9em;
        }}
        td {{ 
            border-bottom: 1px solid #ccc; 
            padding: 15px 10px; 
            vertical-align: top; 
        }}
        tr:hover {{ background-color: #f4f4f4; }}
        
        /* Spaltenbreiten */
        .col-date {{ width: 130px; }}
        .col-loc {{ width: 200px; font-size: 0.85em; color: #444; }}
        
        /* Elemente */
        .cat-label {{
            display: inline-block;
            font-size: 0.7em;
            text-transform: uppercase;
            background: #000;
            color: #fff;
            padding: 2px 6px;
            margin-top: 5px;
            border-radius: 2px;
        }}
        .title a {{ 
            font-size: 1.1em; 
            font-weight: bold; 
            color: #000; 
            text-decoration: none; 
        }}
        .desc {{ 
            display: block; 
            font-size: 0.8em; 
            color: #666; 
            margin-top: 4px; 
        }}
        
        /* Footer Status */
        footer {{
            margin-top: 40px;
            padding-top: 10px;
            border-top: 2px solid #000;
            text-align: right;
            font-size: 0.75em;
            color: #555;
            text-transform: uppercase;
        }}
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

    for date, title, category, location, url, desc in rows:
        short_desc = (desc[:100] + '...') if desc and len(desc) > 100 else ""
        
        cat_html = f'<span class="cat-label">{category}</span>' if category and category != "Event" else ""

        html_content += f"""
                <tr>
                    <td class="col-date">
                        {date}<br>
                        {cat_html}
                    </td>
                    <td>
                        <div class="title"><a href="{url}" target="_blank">{title}</a></div>
                        <span class="desc">{short_desc}</span>
                    </td>
                    <td class="col-loc">{location}</td>
                </tr>
        """

    html_content += f"""
            </tbody>
        </table>
        <footer>
            Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {len(rows)} Einträge
        </footer>
    </div>
</body>
</html>
    """

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("HTML Update fertig.")

if __name__ == "__main__":
    main()
