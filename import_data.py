import mariadb, sys
from csv import reader

def get_connection():
    try:
        conn = mariadb.connect(
            user="root",
            password="$Dipankar917",
            host="localhost",
            port=3306,
            database="openflights"
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

def clear_airports_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM airports;")
        conn.commit()
        print("Cleared existing data from airports table.")
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
        """, data[1:] + [data[0]])
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO airports VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, data)
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
        cur.execute("DELETE FROM airlines;")
        conn.commit()
        print("Cleared existing data from airlines table.")
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
        cur.execute("DELETE FROM routes;")
        conn.commit()
        print("Cleared existing data from routes table.")
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
    
    print("\n1. Importing airports...")
    import_airports("airports.dat")
    
    print("\n2. Importing airlines...")
    import_airlines("airlines.dat")
    
    print("\n3. Importing routes...")
    import_routes("routes.dat")
    
    print("\n=== All data import completed! ===")