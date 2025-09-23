import mariadb
import numpy as np
from sentence_transformers import SentenceTransformer
import sys
from import_data import get_connection
import time

model = None
def get_model():
    global model
    if model is None:
        model = SentenceTransformer('all-MiniLM-L6-v2')
    return model

def cos_similarities(query_text, top_k=5):
    model = get_model()
    query_embedding = model.encode(query_text).astype(np.float32)
    query_norm = np.linalg.norm(query_embedding)
        
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT airport_id, name, city, country, embedding FROM airports WHERE embedding IS NOT NULL")
    airports = cursor.fetchall()
    
    embeddings = []
    airport_info = []
    for airport in airports:
        airport_id, name, city, country, embedding_bytes = airport
        if embedding_bytes is None:
            continue
        embedding_array = np.frombuffer(embedding_bytes, dtype=np.float32)
        embedding_norm = np.linalg.norm(embedding_array)
        embeddings.append(embedding_array)
        airport_info.append((airport_id, name, city, country, embedding_norm))

    if not embeddings:
        conn.close()
        return []
    
    embedding_array = np.array(embeddings)
    dot_product_val = np.dot(embedding_array, query_embedding) 

    similarities = []
    for info, dot_product in zip(airport_info, dot_product_val):
        airport_id, name, city, country, embedding_norm = info
        if query_norm == 0 or embedding_norm == 0:
            similarity = 0.0
        else:
            similarity = dot_product / (query_norm * embedding_norm)
        similarities.append((airport_id, name, city, country, similarity))



    similarities.sort(key=lambda x: x[4], reverse=True)
    top_airports = similarities[:top_k]

    conn.close()
    return top_airports

def euclidean_distance(query_text, top_k=5):
    model = get_model()
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
    start_time = time.perf_counter()
    # Test the vector search
    print("🔍 Testing Vector Similarity Search")
    print("=" * 50)
    
    middle_time = time.perf_counter()
    print(f"Setup Time: {middle_time - start_time:.4f} seconds")
    
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
            method_name = "Cosine Similarity"
            results = cos_similarities(query, top_k=3)
        elif method == '2':
            method_name = "Euclidean Distance"
            results = euclidean_distance(query, top_k=3)
        
        for i, (airport_id, name, city, country, distance) in enumerate(results, 1):
            print(f"{i}. {name} ({city}, {country})")
            print(f"   Method: {method_name}")
            print(f"   Distance: {distance:.4f}")
            print(f"   IATA: {get_airport_details(airport_id)['iata_code']}")
            
            
    end_time = time.perf_counter()
    print(f"\nTotal Execution Time: {end_time - start_time:.4f} seconds")