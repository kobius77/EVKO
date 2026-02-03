import sqlite3
import openai
import os
import json
import time

# --- KONFIGURATION ---
DB_FILE = "evko.db"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    print("❌ FEHLER: Kein OPENAI_API_KEY gesetzt!")
    exit(1)

client = openai.OpenAI(api_key=OPENAI_API_KEY)

def init_db_column():
    """Fügt die embedding-Spalte hinzu, falls sie noch nicht existiert"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE events ADD COLUMN embedding TEXT")
        print("✅ Spalte 'embedding' wurde zur Datenbank hinzugefügt.")
    except sqlite3.OperationalError:
        # Fehler ignorieren, wenn Spalte schon da ist
        pass
    conn.commit()
    conn.close()

def get_embedding(text):
    """Holt den Vektor von OpenAI"""
    text = text.replace("\n", " ")
    try:
        response = client.embeddings.create(
            input=[text],
            model="text-embedding-3-small" 
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"⚠️ OpenAI Fehler: {e}")
        return None

def main():
    print("--- START EMBEDDER ---")
    
    # 1. Sicherstellen, dass die Spalte existiert
    init_db_column()
    
    conn = sqlite3.connect(DB_FILE)
    # Damit wir Spaltennamen nutzen können (z.B. row['title'])
    conn.row_factory = sqlite3.Row 
    c = conn.cursor()
    
    # 2. Alle Events holen, die noch KEIN Embedding haben
    # Wir nehmen auch Events, deren Text sich geändert hat (da hash check im scraper das verhindert,
    # ist NULL check hier meist ausreichend. Wenn man ganz sicher gehen will, müsste man Hash prüfen,
    # aber das macht es unnötig komplex für jetzt).
    c.execute("SELECT url, title, description, tags, location FROM events WHERE embedding IS NULL")
    rows = c.fetchall()
    
    total = len(rows)
    print(f"🔍 Finde {total} Events ohne Embedding...")
    
    if total == 0:
        print("✨ Alles aktuell. Nichts zu tun.")
        conn.close()
        return

    count = 0
    for row in rows:
        count += 1
        url = row['url']
        title = row['title'] or ""
        desc = row['description'] or ""
        tags = row['tags'] or ""
        loc = row['location'] or ""
        
        # Den Text bauen, der "verstanden" werden soll
        # Wir kombinieren alle wichtigen Infos
        full_text = f"{title} {tags} {loc} {desc}"
        
        print(f"[{count}/{total}] Embedde: {title[:40]}...")
        
        vector = get_embedding(full_text)
        
        if vector:
            # Als JSON-String speichern
            vector_json = json.dumps(vector)
            
            c.execute("UPDATE events SET embedding = ? WHERE url = ?", (vector_json, url))
            conn.commit()
        else:
            print("   -> Übersprungen wegen API Fehler")
            
        # Kleines Päuschen, um Rate-Limits zu schonen
        time.sleep(0.1)

    conn.close()
    print("--- ENDE ---")

if __name__ == "__main__":
    main()
