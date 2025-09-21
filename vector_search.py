import mariadb
import numpy as np
from sentence_transformers import SentenceTransformer
import sys
from import_data import get_connection

def cos_similarities(query_text, top_k=5):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_embedding = model.encode(query_text).astype(np.float32)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT airport_id, name, city, country, embedding FROM airports WHERE embedding IS NOT NULL")
    airports = cursor.fetchall()

    similarities = []
    for airport in airports:
        airport_id, name, city, country, embedding_bytes = airport
        if embedding_bytes is None:
            continue
        embedding_array = np.frombuffer(embedding_bytes, dtype=np.float32)
        similarity = np.dot(query_embedding, embedding_array) / (np.linalg.norm(query_embedding) * np.linalg.norm(embedding_array))
        similarities.append((airport_id, name, city, country, similarity))

    similarities.sort(key=lambda x: x[4], reverse=True)
    top_airports = similarities[:top_k]

    conn.close()
    return top_airports

def euclidean_distance(query_text, top_k=5):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_embedding = model.encode(query_text).astype(np.float32)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT airport_id, name, city, country, embedding FROM airports WHERE embedding IS NOT NULL")
    airports = cursor.fetchall()

    distances = []
    for airport in airports:
        airport_id, name, city, country, embedding_bytes = airport
        if embedding_bytes is None:
            continue
        embedding_array = np.frombuffer(embedding_bytes, dtype=np.float32)
        distance = np.linalg.norm(query_embedding - embedding_array)
        similarity = 1 / (1 + distance)  # Convert distance to similarity
        distances.append((airport_id, name, city, country, distance))

    distances.sort(key=lambda x: x[4])
    top_airports = distances[:top_k]

    conn.close()
    return top_airports



def get_airport_details(airport_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT airport_id, name, city, country, iata_code, icao_code, latitude, longitude, altitude FROM airports WHERE airport_id = ?", (airport_id,))
    airport = cursor.fetchone()

    conn.close()
    if airport:
        return {
            'airport_id': airport[0],
            'name': airport[1],
            'city': airport[2],
            'country': airport[3],
            'iata_code': airport[4],
            'icao_code': airport[5],
            'latitude': airport[6],
            'longitude': airport[7],
            'altitude': airport[8]
        }
    return None

if __name__ == "__main__":
    # Test the vector search
    print("🔍 Testing Vector Similarity Search")
    print("=" * 50)
    
    test_queries = [
        "Find airports in New York",
        "Find airports in China"
    ]
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        print("-" * 30)
        
        print("Choose an option:")
        method = input("1. Cosine Similarity\n2. Euclidean Distance\nSelect (1 or 2): ")
        if method == '1':
            results = cos_similarities(query, top_k=3)
        elif method == '2':
            results = euclidean_distance(query, top_k=3)
        
        for i, (airport_id, name, city, country, distance) in enumerate(results, 1):
            print(f"{i}. {name} ({city}, {country})")
            print(f"   Distance: {distance:.4f}")
            print(f"   IATA: {get_airport_details(airport_id)['iata_code']}")