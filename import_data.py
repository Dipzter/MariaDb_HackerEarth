import mariadb, sys, os
from csv import reader

config = {
    'user': 'root',
    'password': '$Dipankar917',
    'host': 'localhost',
    'port': 3306
}
needed_database = 'openflights_Dip'

def get_connection():
    try:
        temp_conn = mariadb.connect(**config)
        temp_cursor = temp_conn.cursor()
        
        create_database = f"CREATE DATABASE IF NOT EXISTS {needed_database};"
        temp_cursor.execute(create_database)
        temp_conn.close()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return mariadb.connect(**{**config, 'database': needed_database})

def check_file_exists(filename):
    if not os.path.isfile(filename):
        print(f"File '{filename}' not found. Please ensure the file is in the current directory.")
        return False   
    return True

def clear_airports_table():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS airports;")
        cursor.execute("""
            CREATE TABLE airports (
                airport_id INT PRIMARY KEY,
                name VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                iata_code VARCHAR(5),
                icao_code VARCHAR(6),
                latitude FLOAT,
                longitude FLOAT,
                altitude INT,
                timezone VARCHAR(10),
                dst VARCHAR(1),
                tz_db_time_zone VARCHAR(50),
                type VARCHAR(20),
                source VARCHAR(20),
                embedding VECTOR(384)
            )
        """)
        conn.commit()
        print("Dropped and recreated airports table.")
    except mariadb.Error as e:
        print(f"Error clearing table: {e}")
    finally:
        conn.close()


def clean_airports_row(row):
    if len(row) < 14:
        print(f"Skipping malformed row: {row}")
        return None
    data = []
    for i, value in enumerate(row):
        if value == '\\N' or value == '':
            data.append(None)
        elif i in [0, 7, 8]:  # airport_id, altitude
            try:
                data.append(int(value))
            except ValueError:
                data.append(None)
        elif i in [6, 9]:  # latitude, longitude
            try:
                data.append(float(value))
            except ValueError:
                data.append(None)
        else:
            data.append(value)
    return data

def update_airports(cur, data):
    try:
        cur.execute("""
            UPDATE airports SET 
            name=?, city=?, country=?, iata_code=?, icao_code=?,
            latitude=?, longitude=?, altitude=?, timezone=?, dst=?,
            tz_db_time_zone=?, type=?, source=?
            WHERE airport_id=?
        """, data[1:14] + [data[0]])
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO airports VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL
                )
            """, data[:14])
    except mariadb.Error as e:
        print(f"Error with airport {data[0]}: {e}")

def import_airports(filename):
    clear_airports_table()
    conn = get_connection()
    cur = conn.cursor()
    with open(filename, 'r', encoding='utf-8') as csvfile:
        csv_reader = reader(csvfile)
        for row in csv_reader:
            data = clean_airports_row(row)
            if data is not None:
                update_airports(cur, data)
    
    conn.commit()
    conn.close()
    print("Finished importing airports")




def clear_airlines_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS airlines;")
        cur.execute("""
            CREATE TABLE airlines (
                airline_id INT PRIMARY KEY,
                name VARCHAR(255),
                alias VARCHAR(255),
                iata_code VARCHAR(5),
                icao_code VARCHAR(6),
                callsign VARCHAR(50),
                country VARCHAR(50),
                active VARCHAR(1)
            )
        """)
        conn.commit()
        print("Dropped and recreated airlines table.")
    except mariadb.Error as e:
        print(f"Error clearing airlines table: {e}")
    finally:
        conn.close()

def clean_airlines_row(row):
    if len(row) < 8:
        print(f"Skipping malformed row: {row}")
        return None
    data = []  
    for i, value in enumerate(row):
        if value == '\\N' or value == '':
            data.append(None)
        elif i == 0:
            try:
                data.append(int(value))
            except ValueError:
                data.append(None)
        else:
            data.append(value)
    return data

def update_airlines(cur, data):
    try:
        cur.execute("""
            UPDATE airlines SET 
            name=?, alias=?, iata_code=?, icao_code=?, callsign=?,
            country=?, active=?
            WHERE airline_id=?
        """, data[1:] + [data[0]])
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO airlines VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, data)
    except mariadb.Error as e:
        print(f"Error with airline {data[0]}: {e}")
        
def import_airlines(filename):
    clear_airlines_table()
    conn = get_connection()
    cur = conn.cursor()
    with open(filename, 'r', encoding='utf-8') as csvfile:
        csv_reader = reader(csvfile)
        for row in csv_reader:
            data = clean_airlines_row(row)
            if data is not None:
                update_airlines(cur, data)
    conn.commit()
    conn.close()
    print("Finished importing airlines")
    
def clear_routes_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS routes;")
        cur.execute("""
            CREATE TABLE routes (
                airline VARCHAR(3),
                airline_id INT,
                source_airport VARCHAR(4),
                source_airport_id INT,
                destination_airport VARCHAR(4),
                destination_airport_id INT,
                codeshare VARCHAR(1),
                stops INT,
                equipment VARCHAR(50)
            )
        """)
        conn.commit()
        print("Dropped and recreated routes table.")
    except mariadb.Error as e:
        print(f"Error clearing routes table: {e}")
    finally:
        conn.close()

def clean_routes_row(row):
    if len(row) < 9:
        print(f"Skipping malformed route row: {row}")
        return None
    data = []
    for i, value in enumerate(row):
        if value == '\\N' or value == '':
            data.append(None)
        elif i in [1, 3, 5, 7]: 
            try:
                data.append(int(value))
            except ValueError:
                data.append(None)
        else:
            data.append(value)
    return data

def update_routes(cur, data):
    try:
        cur.execute("""
            INSERT INTO routes VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, data)
    except mariadb.Error as e:
        print(f"Error with route: {e}")

def import_routes(filename):
    clear_routes_table()
    conn = get_connection()
    cur = conn.cursor()
    with open(filename, 'r', encoding='utf-8') as csvfile:
        csv_reader = reader(csvfile)
        for row in csv_reader:
            data = clean_routes_row(row)
            if data is not None:
                update_routes(cur, data)
    conn.commit()
    conn.close()
    print("Finished importing routes")

if __name__ == "__main__":
    print("=== Starting Data Import ===")
    
    required_files = ["airports.dat", "airlines.dat", "routes.dat"]
    missing_files = [f for f in required_files if not check_file_exists(f)]
    
    if missing_files:
        print("The following required files are missing:")
        for f in missing_files:
            print(f" - {f}")
        print("Please ensure all required files are in the current directory before running the import.")
        sys.exit(1)
    
    print("\n1. Importing airports...")
    import_airports("airports.dat")
    
    print("\n2. Importing airlines...")
    import_airlines("airlines.dat")
    
    print("\n3. Importing routes...")
    import_routes("routes.dat")
    
    print("\n=== All data import completed! ===")