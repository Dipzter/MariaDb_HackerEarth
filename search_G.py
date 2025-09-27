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

# Load the same embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

def parse_binary_embedding(binary_data):
    """
    Parse MariaDB's VECTOR binary format to Python list of floats
    """
    if binary_data is None:
        return None
    
    try:
        # MariaDB VECTOR format: first 2 bytes are header, then 4-byte floats
        # Skip the first 2 bytes (header) and unpack the rest as floats
        num_floats = (len(binary_data) - 2) // 4
        if num_floats <= 0:
            return None
            
        floats = struct.unpack('f' * num_floats, binary_data[2:])
        return list(floats)
    except Exception as e:
        print(f"Error parsing embedding: {e}")
        return None

def search_airports(query, top_k=5):
    """
    Search for airports using vector similarity with numpy
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Convert query to vector
        query_embedding = model.encode(query).tolist()
        query_embedding_np = np.array(query_embedding)
        
        print(f"Query: '{query}'")
        print(f"Query embedding dimensions: {len(query_embedding)}")
        
        # Get all airports with their embeddings
        cur.execute("SELECT id, name, city, country, iata_code, embedding FROM airports")
        all_airports = cur.fetchall()
        
        print(f"Found {len(all_airports)} airports in database")
        
        # Parse embeddings and prepare for numpy calculation
        embeddings = []
        airport_info = []
        valid_count = 0
        
        for (airport_id, name, city, country, iata, embedding_data) in all_airports:
            if embedding_data is None:
                continue
                
            # Try to parse the embedding
            embedding = None
            try:
                # Try binary format first
                if isinstance(embedding_data, (bytes, bytearray)):
                    num_floats = (len(embedding_data) - 2) // 4
                    if num_floats > 0:
                        import struct
                        floats = struct.unpack('f' * num_floats, embedding_data[2:])
                        embedding = list(floats)
                else:
                    # Try string format
                    embedding_str = str(embedding_data).strip('[]')
                    embedding = [float(x.strip()) for x in embedding_str.split(',')]
            except:
                continue
            
            if embedding and len(embedding) == len(query_embedding):
                embeddings.append(embedding)
                airport_info.append((airport_id, name, city, country, iata))
                valid_count += 1
        
        print(f"Successfully parsed {valid_count} valid embeddings")
        
        if not embeddings:
            conn.close()
            return []
        
        # Convert to numpy array for efficient calculation
        embedding_matrix = np.array(embeddings)
        
        # Calculate cosine similarities using numpy (like in your image)
        embedding_norms = np.linalg.norm(embedding_matrix, axis=1)
        query_norm = np.linalg.norm(query_embedding_np)
        
        dot_products = np.dot(embedding_matrix, query_embedding_np)
        cos_similarities = dot_products / (embedding_norms * query_norm + 1e-10)
        
        # Combine results with airport info
        similarities = [(airport_id, name, city, country, iata, similarity) 
                       for (airport_id, name, city, country, iata), similarity 
                       in zip(airport_info, cos_similarities)]
        
        # Sort by similarity (highest first)
        similarities.sort(key=lambda x: x[5], reverse=True)
        top_airports = similarities[:top_k]
        
        return [(name, city, country, iata, sim) for airport_id, name, city, country, iata, sim in top_airports]
        
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def ask_flight_question(question):
    """
    Main RAG function: Takes a natural language question and returns an AI-powered answer
    """
    print(f"\n🤔 Question: {question}")
    
    # Step 1: Vector search to find relevant airports
    print("🔍 Searching for relevant airports...")
    search_results = search_airports(question, top_k=5)
    
    if not search_results:
        return "Sorry, I couldn't find any relevant airports."
    
    # Step 2: Format the context for the AI
    context = "Relevant airports found:\n"
    for i, (name, city, country, iata, similarity) in enumerate(search_results, 1):
        context += f"{i}. {name} ({iata}) - {city}, {country} (Similarity: {similarity:.3f})\n"
    
    print("📊 Context gathered:")
    print(context)
    
    answer = f"I found these airports relevant to your question:\n\n{context}"
    return answer

# --- TEST THE SYSTEM ---
if __name__ == "__main__":
    # Test questions
    test_questions = [
        "Germany",
        "Spain", 
        "United States",
        "London"
    ]
    
    for question in test_questions:
        answer = ask_flight_question(question)
        print(f"💡 Answer: {answer}")
        print("─" * 50)
