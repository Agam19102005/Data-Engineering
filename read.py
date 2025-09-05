import psycopg2

DB_CONFIG = {
    'dbname': 'weather_db',
    'user': 'postgres',
    'password': 'cse@123',
    'host': 'localhost',
    'port': '5433'
}

def read_weather_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM weather_data;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error reading data:", e)

read_weather_data()
