import sqlite3
import argparse
import sys

DB_FILE = "evko.db"

def check_db():
    # --- Argumente Parsen ---
    parser = argparse.ArgumentParser(description="EVKO Datenbank Checker")
    parser.add_argument("-kick", action="store_true", help="Zeige nur Fussball")
    parser.add_argument("-handball", action="store_true", help="Zeige nur Handball")
    parser.add_argument("-stadt", action="store_true", help="Zeige Stadt/Kultur Events (Kein Sport)")
    parser.add_argument("-all", action="store_true", help="Zeige alle Events")
    
    args = parser.parse_args()

    # Wenn gar kein Argument übergeben wurde, Hilfetext anzeigen
    if not any([args.kick, args.handball, args.stadt, args.all]):
        print("ℹ️  Bitte Parameter wählen: -kick, -handball, -stadt oder -all")
        print("   Beispiel: python check_db.py -kick")
        return

    print(f"--- 🔍 DB CHECK: {DB_FILE} ---")

    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        # Basis Query
        sql = "SELECT date_str, time_str, title, tags, location FROM events"
        where_conditions = []
        params = []

        # Filter Logik bauen
        if not args.all:
            # Wir sammeln Bedingungen mit OR verknüpft
            sub_conditions = []
            
            if args.kick:
                sub_conditions.append("tags LIKE '%Fussball%'")
            
            if args.handball:
                sub_conditions.append("tags LIKE '%Handball%'")
                
            if args.stadt:
                # Stadt definieren wir als: NICHT Fussball UND NICHT Handball
                sub_conditions.append("(tags NOT LIKE '%Fussball%' AND tags NOT LIKE '%Handball%')")
            
            if sub_conditions:
                # Verbinde die gewählten Filter mit OR
                sql += " WHERE " + " OR ".join(sub_conditions)

        sql += " ORDER BY date_str ASC"

        c.execute(sql)
        rows = c.fetchall()

        if not rows:
            print("❌ Keine Einträge für diese Filter gefunden.")
        else:
            print(f"✅ {len(rows)} Einträge gefunden:\n")
            
            # Schönes Tabellen-Layout
            # Datum | Zeit | Titel | Tags
            print(f"{'DATUM':<12} | {'ZEIT':<6} | {'TITEL':<40} | {'TAGS'}")
            print("-" * 100)
            
            for r in rows:
                date_s = r[0] if r[0] else ""
                time_s = r[1] if r[1] else ""
                title = r[2] if r[2] else "Unbekannt"
                tags = r[3] if r[3] else ""
                
                # Titel kürzen falls zu lang
                disp_title = (title[:38] + '..') if len(title) > 38 else title
                # Tags kürzen falls zu lang
                disp_tags = (tags[:35] + '..') if len(tags) > 35 else tags
                
                print(f"{date_s:<12} | {time_s:<6} | {disp_title:<40} | {disp_tags}")

        conn.close()

    except Exception as e:
        print(f"⚠️ Datenbank-Fehler: {e}")

if __name__ == "__main__":
    check_db()