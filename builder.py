import sqlite3
from datetime import datetime
import os
import json

DB_FILE = "evko.db"
HTML_FILE = "index.html"
JSON_FILE = "events.json"

def parse_date_for_sort(date_str):
    try:
        clean_date = date_str.split("-")[0].strip()
        return datetime.strptime(clean_date, "%d.%m.%Y")
    except:
        return datetime.max

def main():
    print("--- START BUILDER ---")
    if not os.path.exists(DB_FILE): return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Tags lesen
    c.execute("SELECT date_str, title, tags, location, url, description FROM events")
    rows = c.fetchall()
    conn.close()

    rows.sort(key=lambda x: parse_date_for_sort(x[0]))
    print(f"Verarbeite {len(rows)} Einträge...")

    # --- HTML GENERIERUNG ---
    html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EVKO Events</title>
    <style>
        body {{ background-color: #fff; color: #111; font-family: "Courier New", monospace; padding: 20px; margin: 0; }}
        .container {{ max-width: 950px; margin: 0 auto; }}
        
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th {{ text-align: left; border-bottom: 2px solid #000; padding: 10px; text-transform: uppercase; font-size: 0.9em; }}
        td {{ border-bottom: 1px solid #ccc; padding: 15px 10px; vertical-align: top; }}
        tr:hover {{ background-color: #f8f8f8; }}
        
        .col-date {{ width: 140px; }}
        .col-loc {{ width: 200px; font-size: 0.85em; color: #444; }}
        
        /* Tag Styling */
        .tags-container {{ margin-top: 6px; display: flex; flex-wrap: wrap; gap: 4px; }}
        .tag {{
            font-size: 0.65em;
            text-transform: uppercase;
            border: 1px solid #666;
            color: #333;
            padding: 1px 5px;
            border-radius: 3px;
            white-space: nowrap;
        }}
        
        .title a {{ font-size: 1.1em; font-weight: bold; color: #000; text-decoration: none; }}
        .desc {{ display: block; font-size: 0.8em; color: #666; margin-top: 4px; }}
        
        footer {{ margin-top: 40px; padding-top: 10px; border-top: 2px solid #000; text-align: right; font-size: 0.75em; color: #555; }}
        
        /* Chat Button (Platzhalter) */
        #chat-trigger {{ position: fixed; bottom: 20px; right: 20px; z-index: 1000; }}
    </style>
</head>
<body>
    <div class="container">
        <table>
            <thead>
                <tr>
                    <th>Wann</th>
                    <th>Was / Tags</th>
                    <th>Wo</th>
                </tr>
            </thead>
            <tbody>
    """

    json_data = []

    for date, title, tags_str, location, url, desc in rows:
        short_desc = (desc[:100] + '...') if desc and len(desc) > 100 else ""
        
        # Tags HTML bauen
        tags_html = ""
        tag_list = []
        if tags_str:
            tag_list = [t.strip() for t in tags_str.split(",")]
            for tag in tag_list:
                tags_html += f'<span class="tag">{tag}</span>'
            if tags_html:
                tags_html = f'<div class="tags-container">{tags_html}</div>'

        html_content += f"""
                <tr>
                    <td class="col-date">{date}</td>
                    <td>
                        <div class="title"><a href="{url}" target="_blank">{title}</a></div>
                        <span class="desc">{short_desc}</span>
                        {tags_html}
                    </td>
                    <td class="col-loc">{location}</td>
                </tr>
        """
        
        # JSON Data sammeln
        json_data.append({
            "date": date,
            "title": title,
            "tags": tag_list, # Als echte Liste im JSON
            "location": location,
            "url": url,
            "description": desc
        })

    html_content += f"""
            </tbody>
        </table>
        <footer>Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} | {len(rows)} Einträge</footer>
        
        <div id="chat-trigger">
            <button onclick="alert('Hier kommt n8n Hook rein')" style="background: #000; color: #fff; border: none; padding: 10px 15px; border-radius: 4px; font-family: 'Courier New'; cursor: pointer; border: 1px solid #fff;">
                FRAGEN?
            </button>
        </div>
        </div>
</body>
</html>
    """

    # HTML Speichern
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    # JSON Speichern (für n8n)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print("Builder fertig (HTML + JSON).")

if __name__ == "__main__":
    main()
