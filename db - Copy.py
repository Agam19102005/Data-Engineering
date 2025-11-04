import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    """
    Creates and returns a connection to the PostgreSQL database.
    Make sure this info matches your PostgreSQL setup.
    """
    try:
        conn = psycopg2.connect(
            dbname="employee_db",      # Your database name
            user="postgres",           # Your PostgreSQL username
            password="agam1910",          # Your PostgreSQL password
            host="localhost",          # Database host
            port="5432"                # Default PostgreSQL port
        )
        return conn
    except Exception as e:
        print("❌ Database connection failed:", e)
        return None


def create_tables():
    """
    Creates tables if they don't exist.
    Run this once before using the API.
    """
    conn = get_conn()
    if conn is None:
        print("Cannot create tables without DB connection.")
        return

    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(20),
        dept_id INT,
        role_id INT,
        hire_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Employee table ready.")


def fetch_all_employees():
    """
    Returns all employees as a list of dictionaries.
    """
    conn = get_conn()
    if conn is None:
        return []

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM employee ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
