import mariadb
import sys

# Database connection configuration - YOU MUST CHANGE THE PASSWORD!
config = {
    'user': 'root',
    'password': 'Familyforever123',
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

def create_role(role_name):
    """
    Creates a new role in the database.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Using parameterized query to avoid SQL injection
        cur.execute("CREATE ROLE IF NOT EXISTS ?", (role_name,))
        conn.commit()
        print(f"✅ Role '{role_name}' created successfully.")
    except mariadb.Error as e:
        print(f"❌ Error creating role '{role_name}': {e}")
    finally:
        conn.close()

def grant_privilege_to_role(role_name, privilege, table_name):
    """
    Grants a specific privilege (e.g., SELECT) on a specific table to a role.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Example: GRANT SELECT ON company.employees TO hr_reader;
        cur.execute(f"GRANT {privilege} ON {table_name} TO ?", (role_name,))
        conn.commit()
        print(f"✅ Privilege '{privilege}' on table '{table_name}' granted to role '{role_name}'.")
    except mariadb.Error as e:
        print(f"❌ Error granting privilege to role '{role_name}': {e}")
    finally:
        conn.close()

def grant_role_to_user(role_name, username):
    """
    Grants a role to a specific user. This gives the user all the privileges of the role.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("GRANT ? TO ?", (role_name, username))
        conn.commit()
        print(f"✅ Role '{role_name}' granted to user '{username}'.")
    except mariadb.Error as e:
        print(f"❌ Error granting role '{role_name}' to user '{username}': {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Test the connection
    print("Testing connection to MariaDB...")
    connection = get_connection()
    connection.close()

    # Test creating a role
    print("\n1. Testing role creation...")
    create_role('hr_reader')

    # Test granting a privilege to the role
    print("\n2. Testing granting privilege to role...")
    grant_privilege_to_role('hr_reader', 'SELECT', 'employees')

    # Test granting the role to our user 'anna'
    print("\n3. Testing granting role to user...")
    grant_role_to_user('hr_reader', 'anna')
