import sqlite3
import json
from openai import OpenAI
import os

# Konfiguration
DB_FILE = "evko.db"
client = OpenAI() # Liest OPENAI_API_KEY aus Umgebungsvariablen

def get_embedding(text):
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def main():
    print("--- START EMBEDDER ---")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. Spalte hinzufügen, falls noch nicht existiert
    try:
        c.execute("ALTER TABLE events ADD COLUMN embedding TEXT")
        print("Spalte 'embedding' hinzugefügt.")
    except sqlite3.OperationalError:
        pass # Spalte existiert schon

    # 2. Events ohne Embedding holen
    # Wir nehmen nur Events in der Zukunft, um API-Kosten zu sparen
    c.execute("SELECT url, title, description, tags, location, date_str, time_str FROM events WHERE embedding IS NULL")
    rows = c.fetchall()

    print(f"Generiere Embeddings für {len(rows)} neue Events...")

    for row in rows:
        url = row[0]
        # Wir bauen einen Text, der alle wichtigen Infos enthält
        # Das ist der Text, den die KI später "versteht"
        combined_text = f"Titel: {row[1]}\nDatum: {row[5]} {row[6]}\nOrt: {row[4]}\nTags: {row[3]}\nInhalt: {row[2]}"
        
        try:
            vector = get_embedding(combined_text)
            # Speichern als JSON-String
            c.execute("UPDATE events SET embedding = ? WHERE url = ?", (json.dumps(vector), url))
            print(f"  Embedded: {row[1][:30]}...")
        except Exception as e:
            print(f"  Fehler bei {url}: {e}")

    conn.commit()
    conn.close()
    print("--- EMBEDDER FERTIG ---")

if __name__ == "__main__":
    main()
