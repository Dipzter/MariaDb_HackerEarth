import mariadb, sys, time
import numpy as np
from sentence_transformers import SentenceTransformer
from import_data import get_connection

model = None
def get_model():
    global model
    if model is None:
        model = SentenceTransformer('all-MiniLM-L6-v2')
    return model

def hybrid_cos_search(query_text, location_filter = None, top_k=5):
    query_embedding = get_model().encode(query_text).astype(np.float32)
    conn = get_connection()
    cursor = conn.cursor()
    
    sql = "SELECT airport_id, name, city, country, embedding, latitude, longitude FROM airports WHERE embedding IS NOT NULL"
    params = []

    if location_filter:
        sql += " AND (city = ? OR country = ?)"
        params.extend([location_filter, location_filter])

    cursor.execute(sql, params)
    airports = cursor.fetchall()
    conn.close()

    location_mentions = extract_locations_from_query(query_text)
    
    airport_ids = []
    names = []
    cities = []
    countries = []
    latitudes = []
    longitudes = []
    embedding_list = []

    for airport in airports:
        airport_id, name, city, country, embedding_bytes, lat, lng = airport
        embedding_array = np.frombuffer(embedding_bytes, dtype=np.float32)
        airport_ids.append(airport_id)
        names.append(name)
        cities.append(city)
        countries.append(country)
        latitudes.append(lat)
        longitudes.append(lng)
        embedding_list.append(embedding_array)

    embedding_matrix = np.vstack(embedding_list)
    embedding_norms = np.linalg.norm(embedding_matrix, axis=1)
    query_norm = np.linalg.norm(query_embedding)
    dot_products = np.dot(embedding_matrix, query_embedding)
    semantic_similarities = dot_products / (embedding_norms * query_norm + 1e-10)

    # Compute geographic similarities
    geographic_similarities = np.array([calculate_geographic_similarity(query_text, location_mentions, city, country)
    for city, country in zip(cities, countries)])

    # Weighted combination
    combined_scores = 0.7 * semantic_similarities + 0.3 * geographic_similarities

    # Combine all
    similarities = list(zip(airport_ids, names, cities, countries, combined_scores, latitudes, longitudes))

    # Sort by similarity
    similarities.sort(key=lambda x: x[4], reverse=True)
    return similarities[:top_k]

def extract_locations_from_query(query):
    locations = []
    query_lower = query.lower()
    location_indicators = [" in ", " near ", " around ", " at "]
    for indicator in location_indicators:
        if indicator in query_lower:
            parts = query_lower.split(indicator)
            if len(parts) > 1:
                possible_location = parts[-1].strip().title()
                locations.append(possible_location)
    
    common_locations = ["New York", "Los Angeles", "Chicago", "Houston", "Miami", "London", "Paris", "Berlin", "Tokyo", "Beijing", "Shanghai", "Delhi"]
    for loc in common_locations:
        if loc.lower() in query_lower:
            locations.append(loc)
    
    return locations

def calculate_geographic_similarity(query_text, location_mentions, city, country):
    query_lower = query_text.lower()
    city_lower = city.lower() if city else ""
    country_lower = country.lower() if country else ""

    relevance = 1.0
    
    for loc in location_mentions:
        loc_lower = loc.lower()
        if loc_lower in city_lower or city_lower in loc_lower:
            relevance *= 3.0
        elif loc_lower in country_lower or country_lower in loc_lower:
            relevance *= 2.0
        elif any(term in city_lower for term in loc_lower.split()):
            relevance *= 1.5
        elif any(term in country_lower for term in loc_lower.split()):
            relevance *= 1.3
            
    if "new york" in query_lower:
        if "new york" in city_lower:
            relevance *= 2.5
        elif "usa" in country_lower or "united states" in country_lower:
            relevance *= 1.2
            
    return min(relevance, 5.0)
        

def cos_similarities(query_text, top_k=5):
    model = get_model()
    query_embedding = model.encode(query_text).astype(np.float32)
        
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
    
    embedding_matrix = np.array(embeddings)
    embedding_norms = np.linalg.norm(embedding_matrix, axis=1)
    query_norm = np.linalg.norm(query_embedding)
    
    dot_products = np.dot(embedding_matrix, query_embedding) 
    cos_similarities = dot_products / (embedding_norms * query_norm + 1e-10)

    similarities = [(airport_id, name, city, country, similarity) for 
                    (airport_id, name, city, country, _), similarity in zip(airport_info, cos_similarities)]

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
        method = input("1. Cosine Similarity\n2. Euclidean Distance\n3. Hybrid Search\nSelect (1, 2, or 3): ")
        if method == '1':
            method_name = "Cosine Similarity"
            results = cos_similarities(query, top_k=3)
        elif method == '2':
            method_name = "Euclidean Distance"
            results = euclidean_distance(query, top_k=3)
        elif method == '3':
            method_name = "Hybrid Search"
            results = hybrid_cos_search(query, top_k=3)
        
        for i, result in enumerate(results, 1):
            if method == '3':  # Hybrid search - 7 values
                airport_id, name, city, country, score, lat, lng = result
                metric_value = score
                metric_name = "Score"
            else:  # Cosine/Euclidean - 5 values  
                airport_id, name, city, country, metric_value = result
                metric_name = "Similarity" if method == '1' else "Distance"
            
            details = get_airport_details(airport_id)
            iata_code = details['iata_code'] if details else 'N/A'
            if (not city or city.strip() in ["", "N/A"]) and details and details.get('city'):
                city = details['city']
            
            print(f"{i}. {name} ({city}, {country})")
            print(f"   {metric_name}: {metric_value:.4f}")
            print(f"   IATA: {iata_code}")
            
            # Show coordinates for hybrid search
            if method == '3':
                if lat is not None and lng is not None:
                    print(f"   Coordinates: {lat:.4f}, {lng:.4f}")
                else:
                    print("   Coordinates: N/A")
            
            
    end_time = time.perf_counter()
    print(f"\nTotal Execution Time: {end_time - start_time:.4f} seconds")
    
    
    
""" 
# from openai import OpenAI  # or your preferred LLM

client = OpenAI(api_key="your-api-key")

def generate_llm_response(query, retrieved_airports):
    """
"""
    Generate natural language response using LLM
    """
"""
    # Prepare context from retrieved airports
    context = "Relevant airports:\n"
    for i, (airport_id, name, city, country, score, lat, lng) in enumerate(retrieved_airports):
        details = get_airport_details(airport_id)
        context += f"{i+1}. {name} ({city}, {country}) - IATA: {details['iata_code']}\n"
    
    prompt = f"""
"""
    You are a helpful travel assistant. Based on the following airport information, 
    answer the user's query in a helpful, natural way.
    
    User Query: "{query}"
    
    {context}
    
    Provide a concise, informative response about these airports.
    """
    
"""
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"I found these airports: {context}"

def full_rag_pipeline(query, location_filter=None):
    """
"""
    Complete RAG pipeline: Retrieve → Augment → Generate
    """
"""
    start_time = time.time()
    
    # 1. RETRIEVE: Enhanced semantic search
    print("🔍 Retrieving relevant airports...")
    retrieved_airports = hybrid_cos_search(query, location_filter)
    
    if not retrieved_airports:
        return "No airports found matching your query."
    
    # 2. AUGMENT: Enrich with additional data
    print("📊 Augmenting with airport details...")
    augmented_results = []
    for airport in retrieved_airports:
        airport_id = airport[0]
        details = get_airport_details(airport_id)
        augmented_results.append({**dict(zip([
            'airport_id', 'name', 'city', 'country', 'score', 'lat', 'lng'
        ], airport)), **details})
    
    # 3. GENERATE: LLM response
    print("🤖 Generating natural language response...")
    llm_response = generate_llm_response(query, retrieved_airports)
    
    search_time = time.time() - start_time
    
    return {
        "response": llm_response,
        "airports": augmented_results,
        "search_time": search_time
    }
    
def main():
    print("🏢 Airport RAG Search System")
    print("=" * 40)
    
    while True:
        print("\nEnter your query (or 'quit' to exit):")
        query = input("> ").strip()
        
        if query.lower() in ['quit', 'exit']:
            break
        
        # Extract location filter from query
        location_filter = None
        if "in " in query.lower():
            # Simple location extraction (you can make this more sophisticated)
            parts = query.lower().split(" in ")
            if len(parts) > 1:
                location_filter = parts[-1].strip().title()
        
        # Run full RAG pipeline
        result = full_rag_pipeline(query, location_filter)
        
        print(f"\n🤖 AI Response:")
        print(result["response"])
        
        print(f"\n📊 Matching Airports:")
        for i, airport in enumerate(result["airports"], 1):
            print(f"{i}. {airport['name']} ({airport['city']}, {airport['country']})")
            print(f"   IATA: {airport['iata_code']} | Score: {airport['score']:.3f}")
        
        print(f"\n⏱️  Search completed in {result['search_time']:.2f}s")

if __name__ == "__main__":
    main()
    """