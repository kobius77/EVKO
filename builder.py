import sqlite3
from datetime import datetime
import os
import json

DB_FILE = "evko.db"
HTML_FILE = "index.html"
JSON_FILE = "events.json"
AI_MARKER = "--- ZUSATZINFO AUS PLAKAT ---"

def main():
    print("--- START BUILDER (Minimal) ---")
    if not os.path.exists(DB_FILE): return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    today_iso = datetime.now().strftime("%Y-%m-%d")
    
    # Holen der Daten
    c.execute("""
        SELECT date_str, title, tags, location, url, description, time_str 
        FROM events 
        WHERE start_iso >= ? 
        ORDER BY start_iso ASC, time_str ASC
    """, (today_iso,))
    
    rows = c.fetchall()
    conn.close()

    # --- HTML HEADER ---
    html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Korneuburg Events</title>
    <style>
        body {{ background-color: #fff; color: #111; font-family: "Courier New", monospace; padding: 20px; margin: 0; }}
        .container {{ max-width: 950px; margin: 0 auto; }}
        
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ text-align: left; border-bottom: 2px solid #000; padding: 10px; text-transform: uppercase; font-size: 0.9em; }}
        td {{ border-bottom: 1px solid #ccc; padding: 12px 10px; vertical-align: top; }}
        tr:hover {{ background-color: #f8f8f8; }}
        
        .col-date {{ width: 160px; font-weight: bold; white-space: nowrap; }}
        .col-loc {{ width: 220px; font-size: 0.85em; color: #444; }}
        
        /* Tags */
        .tags-container {{ margin-top: 4px; display: flex; flex-wrap: wrap; gap: 4px; }}
        .tag {{
            font-size: 0.65em; text-transform: uppercase; border: 1px solid #666;
            color: #333; padding: 1px 4px; border-radius: 3px; white-space: nowrap;
        }}
        
        .title a {{ font-size: 1.1em; font-weight: bold; color: #000; text-decoration: none; }}
        .title a:hover {{ text-decoration: underline; }}
        
        .ai-hint {{ cursor: help; font-size: 14px; text-decoration: none; margin-left: 5px; }}
        
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

    for date_str, title, tags_str, location, url, desc, time_str in rows:
        
        # AI Info prüfen (für Tooltip neben Titel)
        ai_tooltip = ""
        clean_desc = desc or ""
        if AI_MARKER in clean_desc:
            parts = clean_desc.split(AI_MARKER)
            clean_desc = parts[0].strip()
            if len(parts) > 1 and len(parts[1].strip()) > 10:
                ai_text = parts[1].strip()
                if "tut mir leid" not in ai_text.lower():
                    safe_ai = ai_text.replace('"', '&quot;').replace('\n', ' &#10; ')
                    ai_tooltip = f'<span class="ai-hint" title="KI-Infos vom Plakat:&#10;{safe_ai}">ℹ️</span>'

        # Datum + Zeit
        display_date = date_str
        if time_str and time_str != "00:00":
            display_date += f"<br><span style='font-weight:normal; font-size:0.9em'>{time_str} Uhr</span>"

        # Tags
        tags_html = ""
        tag_list = []
        if tags_str:
            tag_list = [t.strip() for t in tags_str.split(",") if t.strip()]
            for tag in tag_list:
                tags_html += f'<span class="tag">{tag}</span>'
            if tags_html:
                tags_html = f'<div class="tags-container">{tags_html}</div>'

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
        
        json_data.append({
            "date": date_str, "time": time_str, "title": title, "tags": tag_list,
            "location": location, "url": url, "description": desc
        })

    html_content += f"""
            </tbody>
        </table>
        <footer>Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')}</footer>
    </div>
</body>
</html>
    """

    with open(HTML_FILE, "w", encoding="utf-8") as f: f.write(html_content)
    with open(JSON_FILE, "w", encoding="utf-8") as f: json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Builder fertig.")

if __name__ == "__main__": main()
