import mariadb
from sentence_transformers import SentenceTransformer
import numpy as np
import sys

# --- CONFIGURATION --- 
DB_CONFIG = {
    'user': 'root',
    'password': 'Familyforever123',
    'host': 'localhost',
    'database': 'openflights'
}

model = SentenceTransformer('all-MiniLM-L6-v2')

def safe_cosine_similarity(vec1, vec2):
    """
    Calculate cosine similarity with proper error handling
    """
    try:
        # Ensure both vectors are the same length
        min_len = min(len(vec1), len(vec2))
        vec1_trunc = vec1[:min_len]
        vec2_trunc = vec2[:min_len]
        
        # Convert to numpy arrays
        a1 = np.array(vec1_trunc)
        a2 = np.array(vec2_trunc)
        
        # Calculate norms
        norm1 = np.linalg.norm(a1)
        norm2 = np.linalg.norm(a2)
        
        # Avoid division by zero
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        # Calculate cosine similarity
        dot_product = np.dot(a1, a2)
        similarity = dot_product / (norm1 * norm2)
        
        # Ensure valid range
        return max(-1.0, min(1.0, similarity))
        
    except Exception as e:
        print(f"Similarity calculation error: {e}")
        return 0.0

def parse_embedding_safely(embedding_data):
    """
    Safely parse embedding from various formats
    """
    try:
        if embedding_data is None:
            return None
            
        # If it's already a list
        if isinstance(embedding_data, list):
            return embedding_data
            
        # If it's bytes (MariaDB binary format)
        if isinstance(embedding_data, (bytes, bytearray)):
            if len(embedding_data) < 6:
                return None
            import struct
            num_floats = (len(embedding_data) - 2) // 4
            floats = struct.unpack('f' * num_floats, embedding_data[2:2 + num_floats * 4])
            return list(floats)
            
        # If it's a string
        if isinstance(embedding_data, str):
            # Remove brackets and split
            clean = embedding_data.strip('[]')
            if clean:
                return [float(x.strip()) for x in clean.split(',')]
                
        return None
    except:
        return None

def search_airports(query, top_k=5):
    """
    Robust airport search
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Generate query embedding
        query_embedding = model.encode(query).tolist()
        print(f"🔍 Searching: '{query}' (dimensions: {len(query_embedding)})")
        
        # Get airports
        cur.execute("SELECT name, city, country, iata_code, embedding FROM airports LIMIT 200")
        airports = cur.fetchall()
        
        results = []
        valid_count = 0
        
        for name, city, country, iata, emb_data in airports:
            stored_embedding = parse_embedding_safely(emb_data)
            
            if stored_embedding and len(stored_embedding) > 0:
                similarity = safe_cosine_similarity(stored_embedding, query_embedding)
                
                # Only include meaningful results
                if similarity > 0.1:  # Filter out very low similarities
                    results.append((name, city, country, iata, similarity))
                    valid_count += 1
        
        print(f"📊 Found {valid_count} relevant airports")
        
        # Sort and return top results
        results.sort(key=lambda x: x[4], reverse=True)
        return results[:top_k]
        
    except Exception as e:
        print(f"❌ Search error: {e}")
        return []
    finally:
        conn.close()

# Rest of your code remains the same...
def ask_flight_question(question):
    print(f"\n🤔 Question: {question}")
    results = search_airports(question)
    
    if not results:
        return "❌ No relevant airports found."
    
    response = "✅ Top results:\n\n"
    for i, (name, city, country, iata, sim) in enumerate(results, 1):
        response += f"{i}. {name} ({iata}) - {city}, {country}\n"
        response += f"   Similarity: {sim:.3f}\n\n"
    
    return response

# Test
if __name__ == "__main__":
    questions = ["Germany", "Tokyo", "London", "New York"]
    
    for q in questions:
        print(ask_flight_question(q))
        print("─" * 50)
