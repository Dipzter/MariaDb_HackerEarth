import mariadb
import sys

# Database connection configuration - YOU MUST CHANGE THE PASSWORD!
config = {
    'user': 'root',
    'password': 'Familyforever123', # <<< EDIT THIS LINE
    'host': 'localhost',
    'port': 3306,
    'database': 'hackathon_demo'
}

# Function to get a database connection
def get_connection():
    """
    Establishes and returns a connection to the MariaDB database.
    Exits the program if the connection fails.
    """
    try:
        conn = mariadb.connect(**config)
        print("✅ Successfully connected to MariaDB!")
        return conn
    except mariadb.Error as e:
        print(f"❌ Error connecting to MariaDB: {e}")
        sys.exit(1)

# This code only runs if you execute this script directly
if __name__ == "__main__":
    print("Testing connection to MariaDB...")
    connection = get_connection() # This line tries to connect
    connection.close() # This line closes the connection
    print("Connection closed.")