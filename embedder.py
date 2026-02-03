import sqlite3
import openai
import os
import json
import time
import hashlib

# --- KONFIGURATION ---
DB_FILE = "evko.db"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    print("❌ FEHLER: Kein OPENAI_API_KEY gesetzt!")
    exit(1)

client = openai.OpenAI(api_key=OPENAI_API_KEY)

def make_hash(text):
    """Erstellt einen MD5 Hash vom Text"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def init_db_columns():
    """Fügt benötigte Spalten hinzu, falls sie fehlen"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # 1. Embedding Vektor Spalte
    try:
        c.execute("ALTER TABLE events ADD COLUMN embedding TEXT")
    except sqlite3.OperationalError: pass
    
    # 2. Embedding Hash Spalte (zum Erkennen von Textänderungen)
    try:
        c.execute("ALTER TABLE events ADD COLUMN embedding_hash TEXT")
        print("✅ Spalte 'embedding_hash' wurde hinzugefügt (Smart Updates aktiviert).")
    except sqlite3.OperationalError: pass
    
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
    print("--- START EMBEDDER (Smart Update) ---")
    
    init_db_columns()
    
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row 
    c = conn.cursor()
    
    # Wir holen ALLE Events, um zu prüfen, ob sich der Text geändert hat
    c.execute("SELECT url, title, description, tags, location, embedding_hash FROM events")
    rows = c.fetchall()
    
    total = len(rows)
    print(f"🔍 Prüfe {total} Events auf Änderungen...")
    
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for row in rows:
        url = row['url']
        title = row['title'] or ""
        desc = row['description'] or ""
        tags = row['tags'] or ""
        loc = row['location'] or ""
        stored_hash = row['embedding_hash']
        
        # Den Text bauen, der "verstanden" werden soll
        full_text = f"{title} {tags} {loc} {desc}"
        
        # Aktuellen Hash berechnen
        current_hash = make_hash(full_text)
        
        # CHECK: Ist der Text neu oder hat er sich geändert?
        if current_hash != stored_hash:
            # Ja -> Wir müssen (neu) embedden
            change_type = "NEU" if not stored_hash else "UPDATE"
            print(f"   📝 [{change_type}] {title[:40]}...")
            
            vector = get_embedding(full_text)
            
            if vector:
                vector_json = json.dumps(vector)
                
                c.execute("""
                    UPDATE events 
                    SET embedding = ?, embedding_hash = ? 
                    WHERE url = ?
                """, (vector_json, current_hash, url))
                conn.commit()
                updated_count += 1
                
                # Kurze Pause für Rate Limits
                time.sleep(0.1)
            else:
                error_count += 1
        else:
            # Nein -> Alles beim Alten, überspringen (Spart Geld!)
            skipped_count += 1

    conn.close()
    print("-" * 40)
    print(f"✅ Fertig.")
    print(f"   - Aktualisiert/Neu: {updated_count}")
    print(f"   - Unverändert (Skip): {skipped_count}")
    print(f"   - Fehler: {error_count}")
    print("--- ENDE ---")

if __name__ == "__main__":
    main()
