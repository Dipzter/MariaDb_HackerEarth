import mariadb
from sentence_transformers import SentenceTransformer
import sys

# --- CONFIGURATION --- 
DB_CONFIG = {
    'user': 'root',
    'password': 'Familyforever123',
    'host': 'localhost',
    'database': 'openflights'
}

# Load the embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

def regenerate_embeddings():
    """
    Completely regenerate the embeddings correctly
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # First, clear the existing embeddings (they're corrupted)
        print("Clearing existing embeddings...")
        cur.execute("UPDATE airports SET embedding = NULL")
        
        # Get all airports that need embeddings
        cur.execute("SELECT id, name, city, country FROM airports")
        airports = cur.fetchall()
        
        print(f"Generating embeddings for {len(airports)} airports...")
        
        # Generate and store embeddings correctly
        for i, (airport_id, name, city, country) in enumerate(airports):
            # Create text to embed
            text = f"{name} {city} {country}"
            
            # Generate embedding (this will be 384 dimensions)
            embedding = model.encode(text).tolist()
            
            # Verify the dimension
            if len(embedding) != 384:
                print(f"Warning: Airport {name} has {len(embedding)} dimensions")
                continue
            
            # Store in database
            cur.execute("UPDATE airports SET embedding = ? WHERE id = ?", (embedding, airport_id))
            
            # Show progress
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1} airports...")
        
        conn.commit()
        print("✅ Embeddings regenerated successfully!")
        
        # Verify a sample
        cur.execute("SELECT name, embedding, LENGTH(embedding) FROM airports LIMIT 3")
        sample = cur.fetchall()
        print("\nSample verification:")
        for name, emb, length in sample:
            print(f"{name}: {length} bytes")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    regenerate_embeddings()
