import mariadb
import sys


# Database configuration
config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'localhost',
    'port': 3307,
    'database': 'hackathon_demo'
}

def create_and_insert_airports_data():
    conn = None
    try:
        conn = mariadb.connect(**config)
        cursor = conn.cursor()

        print("Connection successful!")

        # 1. Drop the table if it exists to ensure a clean slate
        drop_table_sql = "DROP TABLE IF EXISTS airports;"
        cursor.execute(drop_table_sql)
        conn.commit()
        print("Dropped existing airports table (if any).")

        # 2. Create the table with correct UTF8 character set
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS airports (
            airport_id INT,
            name VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            iata VARCHAR(3),
            icao VARCHAR(4),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            altitude INT,
            timezone DECIMAL(4,2),
            dst VARCHAR(1),
            tz VARCHAR(255),
            type VARCHAR(255),
            source VARCHAR(255)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("Airports table created or already exists.")

        # 3. Prepare and insert the data
        cols = [
            'airport_id', 'name', 'city', 'country', 'iata', 'icao',
            'latitude', 'longitude', 'altitude', 'timezone', 'dst', 'tz', 'type', 'source'
        ]
        url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"

        try:
            df = pd.read_csv(url, header=None, names=cols, index_col=False, quotechar ='"', engine='python')
            
            df.replace({'\\N': None}, inplace=True)
            df['city'] = df['city'].astype(str)
            df['name'] = df['name'].astype(str)
            df['country'] = df['country'].astype(str)
            df['tz'] = df['tz'].astype(str)
            df['type'] = df['type'].astype(str)
            df['source'] = df['source'].astype(str)
            df = df.where(pd.notnull(df), None)
            
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)

        # Create the INSERT statement
        insert_sql = """
        INSERT INTO airports (
            airport_id, name, city, country, iata, icao,
            latitude, longitude, altitude, timezone, dst, tz, type, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]
        
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {len(data_to_insert)} rows.")
            
    except mariadb.Error as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

# The rest of the code for vectorization remains the same
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask

def compute_sentence_embedding(sentence):
    inputs = tokenizer(sentence, return_tensors="pt", padding=True, truncation=True)
    with torch.no_grad():
        model_output = model(**inputs)
    token_embeddings = model_output.last_hidden_state
    attention_mask = inputs['attention_mask']
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    sentence_embedding = sum_embeddings / sum_mask
    return sentence_embedding[0].numpy().tolist()

def create_vectors_and_update_table():
    conn = None
    try:
        conn = mariadb.connect(**config)
        cursor = conn.cursor()
        print("Connection successful!")
        
        # Add a check to prevent running this part if the vector column exists
        cursor.execute("SHOW COLUMNS FROM airports LIKE 'vector'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE airports ADD COLUMN vector VECTOR(384) NOT NULL")
            cursor.execute("ALTER TABLE airports ADD VECTOR INDEX airport_vector_index (vector) ENGINE=MariaDB, ALGORITHM=HNSW")
            conn.commit()
            print("Added vector column and index to airports table.")
        else:
            print("Vector column already exists. Skipping.")

        cursor.execute("SELECT airport_id, name, city, country FROM airports")
        airports_data = cursor.fetchall()
        
        update_sql = "UPDATE airports SET vector = ? WHERE airport_id = ?"
        for row in airports_data:
            airport_id, name, city, country = row
            combined_text = f"{name} {city} {country}"
            vector = compute_sentence_embedding(combined_text)
            cursor.execute(update_sql, (vector, airport_id))
            
        conn.commit()
        print("Updated vector embeddings for all airports.")
    except mariadb.Error as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_and_insert_airports_data()
    create_vectors_and_update_table()
