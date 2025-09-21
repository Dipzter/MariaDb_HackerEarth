import mariadb
import pandas as pd
from sentence_transformers import SentenceTransformer
import sys
import os

# --- CONFIGURATION --- 
DB_CONFIG = {
    'user': 'root',
    'password': 'Familyforever123',  # CHANGE THIS!
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
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"   Found these columns: {df.columns.tolist()}")
    
    # Clean the data: Replace NaN values with None
    df = df.where(pd.notnull(df), None)
    
    insert_sql = f"INSERT INTO {TABLE_NAME} (name, city, country, iata_code) VALUES (?, ?, ?, ?)"
    
    for _, row in df.iterrows():
        # Get values, using None if value is NaN
        name_val = row['name'] if pd.notnull(row['name']) else None
        city_val = row['city'] if pd.notnull(row['city']) else None
        country_val = row['country'] if pd.notnull(row['country']) else None
        iata_val = row['iata'] if pd.notnull(row['iata']) else None
        
        cur.execute(insert_sql, (name_val, city_val, country_val, iata_val))
    
    conn.commit()
    print(f"   Inserted {len(df)} rows into the table.")
    
except Exception as e:
    print(f"Error reading CSV or inserting data: {e}")
    conn.rollback()

# --- STEP 4: Generate and Store Embeddings ---
print("4. Generating AI embeddings...")
model = SentenceTransformer('all-MiniLM-L6-v2')

cur.execute(f"SELECT id, name, city, country FROM {TABLE_NAME}")
airports = cur.fetchall()

print("   Generating embeddings for each airport...")

for (airport_id, name, city, country) in airports:
    text_to_embed = f"{name} {city} {country}"
    
    # Generate embedding and convert to native Python floats
    embedding = model.encode(text_to_embed).tolist()
    embedding = [float(x) for x in embedding]  # Convert to native Python floats
    
    # Use JSON_ARRAY_PACK function for MariaDB vector format
    vector_str = '[' + ', '.join([str(x) for x in embedding]) + ']'
    update_sql = f"UPDATE {TABLE_NAME} SET embedding = JSON_ARRAY_PACK('{vector_str}') WHERE id = {airport_id}"
    
    try:
        cur.execute(update_sql)
    except mariadb.Error as e:
        print(f"Error updating airport_id {airport_id}: {e}")
        continue

conn.commit()
print("   Embeddings generated and stored.")

# --- STEP 4.5: Check for NULL Embeddings ---
print("4.5: Checking for NULL embeddings...")
cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE embedding IS NULL")
null_count = cur.fetchone()[0]
if null_count > 0:
    print(f"   WARNING: Found {null_count} NULL embeddings. Fixing...")
    cur.execute(f"SELECT id, name, city, country FROM {TABLE_NAME} WHERE embedding IS NULL")
    null_airports = cur.fetchall()
    
    for (airport_id, name, city, country) in null_airports:
        if name is None: name = ""
        if city is None: city = ""
        if country is None: country = ""
        
        text_to_embed = f"{name} {city} {country}".strip()
        if text_to_embed:
            embedding = model.encode(text_to_embed).tolist()
            embedding = [float(x) for x in embedding]
        else:
            embedding = [0.0] * 384
            embedding = [float(x) for x in embedding]
        
        vector_str = '[' + ', '.join([str(x) for x in embedding]) + ']'
        update_sql = f"UPDATE {TABLE_NAME} SET embedding = JSON_ARRAY_PACK('{vector_str}') WHERE id = {airport_id}"
        
        try:
            cur.execute(update_sql)
        except mariadb.Error as e:
            print(f"Error fixing NULL for airport_id {airport_id}: {e}")
            continue
    
    conn.commit()
    print("   NULL embeddings fixed.")

# --- STEP 5: Create the Vector Index ---
print("5. Creating vector index for fast search...")
try:
    # Double-check: Ensure absolutely no NULL values exist
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE embedding IS NULL")
    null_count = cur.fetchone()[0]
    
    if null_count > 0:
        print(f"   Still found {null_count} NULL embeddings. Creating zero vectors...")
        zero_vector = [0.0] * 384
        zero_vector = [float(x) for x in zero_vector]
        zero_str = '[' + ', '.join([str(x) for x in zero_vector]) + ']'
        cur.execute(f"UPDATE {TABLE_NAME} SET embedding = JSON_ARRAY_PACK('{zero_str}') WHERE embedding IS NULL")
        conn.commit()
    
    # Now create the index
    create_index_sql = f"ALTER TABLE {TABLE_NAME} ADD VECTOR INDEX (embedding)"
    cur.execute(create_index_sql)
    print("   Vector index created successfully.")
    
except mariadb.Error as e:
    print(f"   Error creating index: {e}")
    try:
        create_index_sql = f"ALTER TABLE {TABLE_NAME} ADD VECTOR INDEX (embedding) IGNORE"
        cur.execute(create_index_sql)
        print("   Vector index created with IGNORE option.")
    except mariadb.Error as e2:
        print(f"   Failed to create index even with IGNORE: {e2}")

# --- CLEAN UP ---
cur.close()
conn.close()
print("\n✅ All done! Database is ready for semantic search.")
