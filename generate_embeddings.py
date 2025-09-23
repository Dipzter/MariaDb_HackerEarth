import mariadb
import sys
from sentence_transformers import SentenceTransformer
import numpy as np


def get_connection():
    try:
        conn = mariadb.connect(
            user="root",
            password="$Dipankar917",
            host="localhost",
            port=3306,
            database="openflights_Dip"
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
        
def check_embeddings_exist():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'openflights_DIP' AND table_name = 'airports' AND column_name = 'embedding';")
        column_exists = cur.fetchone()[0] > 0
        
        if not column_exists:
            return 0, 0
        
        cur.execute("SELECT COUNT(*) FROM airports WHERE embedding IS NOT NULL;")
        embedding_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM airports;")
        total_count = cur.fetchone()[0]
        return embedding_count, total_count
    except mariadb.Error as e:
        print(f"Error checking for embedding column: {e}")
        return 0, 0
    finally:
        conn.close()
        
def create_embedding_column():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE airports ADD COLUMN IF NOT EXISTS embedding VECTOR(384);")
        conn.commit()
        print("Ensured embedding column exists in airports table.")
    except mariadb.Error as e:
        print(f"Error creating embedding column: {e}")
    finally:
        conn.close()
        
def clear_embeddings():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE airports SET embedding = NULL;")
        conn.commit()
        print("Cleared existing embeddings from airports table.")
    except mariadb.Error as e:
        print(f"Error clearing embeddings: {e}")
    finally:
        conn.close()

def generate_embedding():
    
    embedding_count, total_count = check_embeddings_exist()
    
    if embedding_count >= total_count and total_count > 0:
        print("Embeddings already exist for all airports. No action needed.")
        return
    
    if embedding_count > 0:
        response = input(f"Embeddings exist for {embedding_count} out of {total_count} airports. Do you want to regenerate all embeddings? (yes/no): ")
        
        if response.lower() not in ["yes", "y"]:
            print("No action taken.")
            return
        clear_embeddings()
     
    print("Creating embedding column if it doesn't exist...")
    create_embedding_column()
          
    print("Loading embedding model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT airport_id, name, city, country FROM airports WHERE embedding is NULL")
    airports = cursor.fetchall()
    
    if not airports:
        print("No airports found without embeddings. Exiting.")
        conn.close()
        return
    
    print("Generating embeddings for airports...")

    for i, airport in enumerate(airports):
        airport_id, name, city, country = airport
        
        description = f"{name} airport in {city}, {country}"
        
        embedding_array = model.encode(description)
        embedding_bytes = embedding_array.astype(np.float32).tobytes()
        
        try:
            cursor.execute(
                "UPDATE airports SET embedding = ? WHERE airport_id = ?",
                (embedding_bytes, airport_id)
            )
            
            if (i+1) % 100 == 0 or (i+1) == len(airports):
                print(f"Processed {i+1}/{len(airports)} airports")
                conn.commit()
            
            print(f"Updated embedding for airport ID {airport_id}")
        except mariadb.Error as e:
            print(f"Error updating airport ID {airport_id}: {e}")
            conn.rollback()
            print(f"Rolling back changes for airport ID {airport_id}")
            
            
    conn.commit() 
    conn.close()


if __name__ == "__main__":
    print("=== Airport Embeddings Generator ===")
    
    # Check current status
    embedding_count, total_count = check_embeddings_exist()
    print(f"Current status: {embedding_count}/{total_count} airports have embeddings")
    
    # Ask user what they want to do
    if embedding_count == 0:
        response = input("No embeddings found. Generate embeddings now? (yes/no): ")
        if response.lower() in ["yes", "y"]:
            generate_embedding()
        else:
            print("No action taken.")
            sys.exit(0)
    else:
        response = input("Options: [1] Check status [2] Generate missing embeddings [3] Regenerate all: ")
        
        if response == "1":
            print(f"Status: {embedding_count}/{total_count} airports have embeddings")
            sys.exit(0)
        elif response == "2":
            generate_embedding()
            print("🎉 Embedding generation completed successfully!")
            final_count, total_count = check_embeddings_exist()
            print(f"Final status: {final_count}/{total_count} airports have embeddings")
        elif response == "3":
            clear_embeddings()
            generate_embedding()
            print("🎉 Embedding generation completed successfully!")
            final_count, total_count = check_embeddings_exist()
            print(f"Final status: {final_count}/{total_count} airports have embeddings")
        else:
            print("Invalid option. No action taken.")
            sys.exit(0)