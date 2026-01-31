import sqlite3
import json
import numpy as np
from openai import OpenAI
import os
from datetime import datetime

DB_FILE = "evko.db"
client = OpenAI()

def get_embedding(text):
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def cosine_similarity(a, b):
    # Da OpenAI Embeddings normalisiert sind, reicht das Dot-Product
    return np.dot(a, b)

def search_events(query, top_k=5):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Nur zukünftige Events laden
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT * FROM events WHERE embedding IS NOT NULL AND start_iso >= ?", (today,))
    rows = c.fetchall()
    conn.close()

    if not rows: return []

    # Query einbetten
    query_vector = get_embedding(query)

    # Ähnlichkeiten berechnen
    results = []
    for row in rows:
        event_vector = json.loads(row['embedding'])
        score = cosine_similarity(query_vector, event_vector)
        results.append((score, row))

    # Sortieren (höchster Score zuerst) und Top K zurückgeben
    results.sort(key=lambda x: x[0], reverse=True)
    return [r[1] for r in results[:top_k]]

def chat_with_data(user_question):
    print(f"User fragt: {user_question}...\n")
    
    # 1. RAG: Relevante Daten holen
    relevant_events = search_events(user_question, top_k=4)
    
    if not relevant_events:
        print("Keine passenden Events gefunden.")
        return

    # 2. Kontext für LLM bauen
    context_text = ""
    for e in relevant_events:
        context_text += f"""
        --- EVENT ---
        Titel: {e['title']}
        Wann: {e['date_str']} um {e['time_str']}
        Wo: {e['location']}
        Tags: {e['tags']}
        Beschreibung: {e['description'][:200]}...
        Link: {e['url']}
        """

    # 3. Prompt an GPT-4o-mini
    system_prompt = """Du bist ein hilfreicher Event-Assistent für Korneuburg. 
    Nutze NUR die folgenden Veranstaltungsinformationen, um die Frage zu beantworten.
    Wenn du keine passende Veranstaltung findest, sag das ehrlich. 
    Antworte freundlich und kurz. Formatiere Daten schön."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Hier sind die Events:\n{context_text}\n\nFrage des Nutzers: {user_question}"}
    ]

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.7
    )

    print("🤖 ANTWORT:")
    print(response.choices[0].message.content)

if __name__ == "__main__":
    # Testfragen
    chat_with_data("Gibt es diese Woche Sportveranstaltungen?")
    print("\n" + "-"*30 + "\n")
    chat_with_data("Was kann ich mit Kindern machen?")