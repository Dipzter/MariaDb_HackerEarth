import mariadb
import pandas as pd
from sentence_transformers import SentenceTransformer
import sys
import os
import numpy as np

# --- CONFIGURATION --- 
DB_CONFIG = {
    'user': 'root',
    'password': 'Familyforever123',
    'host': 'localhost',
    'port': 3306
}
DATABASE_NAME = 'openflights'
TABLE_NAME = 'airports'

# --- FILE DISCOVERY ---
print("Looking for CSV files in current directory...")
current_dir = os.getcwd()
all_files = os.listdir()
csv_files = [f for f in all_files if f.endswith('.csv') or f.endswith('.dat')]
print(f"Found these data files: {csv_files}")

CSV_FILE_PATH = None
for candidate in ['airports.csv', 'airports.csv.csv', 'airports.dat', 'airport\'s.dat']:
    if candidate in all_files:
        CSV_FILE_PATH = os.path.join(current_dir, candidate)
        print(f"Found data file: {CSV_FILE_PATH}")
        break

if not CSV_FILE_PATH:
    print("Could not automatically find the airports file.")
    CSV_FILE_PATH = input("Please provide the FULL path to your CSV file: ").strip().strip('"')

# --- STEP 1: Connect to MariaDB and Create Database ---
print("1. Connecting to MariaDB and creating database...")
try:
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Check MariaDB version
    cur.execute("SELECT VERSION()")
    version = cur.fetchone()[0]
    print(f"   MariaDB Version: {version}")
    
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    cur.execute(f"USE {DATABASE_NAME}")
    print(f"   Database '{DATABASE_NAME}' is ready.")
    
except mariadb.Error as e:
    print(f"Error connecting to MariaDB: {e}")
    sys.exit(1)

# --- STEP 2: Create the Table with a VECTOR Column ---
print("2. Creating the table with a VECTOR column...")
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    iata_code VARCHAR(3),
    embedding VECTOR(384)
)
"""
try:
    cur.execute(create_table_sql)
    print("   Table created successfully.")
except mariadb.Error as e:
    print(f"Error creating table: {e}")
    sys.exit(1)

# --- STEP 3: Read CSV and Insert Data ---
print("3. Reading CSV file and inserting data...")
try:
    # CLEAR THE TABLE COMPLETELY BEFORE INSERTING
    cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    print("   Table cleared.")
    
    # Try different separators for CSV files
    try:
        df = pd.read_csv(CSV_FILE_PATH)
    except:
        df = pd.read_csv(CSV_FILE_PATH, sep='\t')
    
    print(f"   Found these columns: {df.columns.tolist()}")
    print(f"   CSV has {len(df)} rows.")
    
    # Clean the data
    df = df.where(pd.notnull(df), None)
    
    insert_sql = f"INSERT INTO {TABLE_NAME} (name, city, country, iata_code) VALUES (?, ?, ?, ?)"
    
    data_to_insert = []
    for _, row in df.iterrows():
        name_val = row['name'] if pd.notnull(row['name']) else None
        city_val = row['city'] if pd.notnull(row['city']) else None
        country_val = row['country'] if pd.notnull(row['country']) else None
        iata_val = row['iata'] if 'iata' in df.columns and pd.notnull(row['iata']) else None
        
        data_to_insert.append((name_val, city_val, country_val, iata_val))
    
    cur.executemany(insert_sql, data_to_insert)
    conn.commit()
    print(f"   Inserted {len(data_to_insert)} rows into the table.")
    
except Exception as e:
    print(f"Error reading CSV or inserting data: {e}")
    conn.rollback()

# --- STEP 4: Generate and Store Embeddings (BATCH PROCESSING) ---
print("4. Generating AI embeddings using batch processing...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Now select all rows that need embeddings
cur.execute(f"SELECT id, name, city, country FROM {TABLE_NAME}")
airports = cur.fetchall()

print(f"   Found {len(airports)} airports needing embeddings...")

BATCH_SIZE = 100
total_processed = 0

for i in range(0, len(airports), BATCH_SIZE):
    batch = airports[i:i + BATCH_SIZE]
    
    # Create descriptions for the batch
    descriptions = [f"{name} airport in {city}, {country}" for (_, name, city, country) in batch]
    
    # Generate embeddings for the entire batch
    embeddings = model.encode(descriptions, batch_size=BATCH_SIZE, show_progress_bar=False)
    
    # Prepare updates
    updates = []
    for j, ((airport_id, name, city, country), embedding_array) in enumerate(zip(batch, embeddings)):
        # Convert to binary format that MariaDB expects
        embedding_bytes = embedding_array.astype(np.float32).tobytes()
        updates.append((embedding_bytes, airport_id))
    
    # Batch update
    try:
        update_sql = f"UPDATE {TABLE_NAME} SET embedding = ? WHERE id = ?"
        cur.executemany(update_sql, updates)
        conn.commit()
        total_processed += len(batch)
        print(f"   Processed {total_processed}/{len(airports)} airports")
        
    except mariadb.Error as e:
        print(f"   Batch update failed: {e}")
        conn.rollback()

print("   Embeddings generated and stored in binary format.")

# --- STEP 5: Create Vector Index ---
print("5. Creating vector index for fast search...")
try:
    # Create the vector index
    create_index_sql = f"ALTER TABLE {TABLE_NAME} ADD VECTOR INDEX (embedding)"
    cur.execute(create_index_sql)
    print("   Vector index created successfully!")
    
except mariadb.Error as e:
    print(f"   Error creating index: {e}")

# --- FINAL VALIDATION ---
print("6. Final validation...")
cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE embedding IS NOT NULL")
valid_embeddings = cur.fetchone()[0]
cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
total_rows = cur.fetchone()[0]

print(f"   Database: {DATABASE_NAME}")
print(f"   Table: {TABLE_NAME}")
print(f"   Total rows: {total_rows}")
print(f"   Rows with embeddings: {valid_embeddings}")
print(f"   Success rate: {(valid_embeddings/total_rows)*100:.1f}%")

# --- CLEAN UP ---
cur.close()
conn.close()
print("\n✅ Vector database setup completed!")
print("   You can now run semantic searches on your airport data.")
