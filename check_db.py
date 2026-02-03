import sqlite3

def check_db():
    conn = sqlite3.connect("evko.db")
    c = conn.cursor()
    
    # 1. Wie viele Events sind insgesamt drin?
    c.execute("SELECT COUNT(*) FROM events")
    total = c.fetchone()[0]
    print(f"Gesamtanzahl Events: {total}")
    
    # 2. Gibt es Fußball-Events?
    print("\n--- Letzte 5 Fußball-Einträge ---")
    c.execute("SELECT title, date_str, tags FROM events WHERE tags LIKE '%Fussball%' ORDER BY date_str DESC LIMIT 5")
    rows = c.fetchall()
    
    if not rows:
        print("WARNUNG: Keine Fußball-Events gefunden!")
    else:
        for r in rows:
            print(f"📅 {r[1]} | ⚽ {r[0]} | 🏷️ {r[2]}")

    conn.close()

if __name__ == "__main__":
    check_db()
