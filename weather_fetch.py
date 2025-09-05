import requests
import psycopg2
from datetime import datetime

# Configuration
API_KEY = "8adda756db2fa1af4ae49ad2cc00a42a"
DB_CONFIG = {
    'dbname': 'weather_db',
    'user': 'postgres',
    'password': 'cse@123',
    'host': 'localhost',
    'port': '5433'
}

def fetch_weather(city):
    """Fetch current weather data for a specified city."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.status_code, response.text)
        return None

def parse_weather_data(data):
    """Extract relevant weather data from API response."""
    return {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'weather_description': data['weather'][0]['description'],
        'timestamp': datetime.now()
    }

def store_weather_data(db_config, weather_data):
    """Store parsed weather data in PostgreSQL."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO weather_data (city, temperature, humidity, pressure, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            weather_data['city'],
            weather_data['temperature'],
            weather_data['humidity'],
            weather_data['pressure'],
            weather_data['weather_description'],
            weather_data['timestamp']
        ))
        conn.commit()
        print(f"Weather data for {weather_data['city']} saved.")
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error storing data:", e)

def main():
    city = input("Enter city name: ")
    data = fetch_weather(city)
    if data:
        parsed = parse_weather_data(data)
        store_weather_data(DB_CONFIG, parsed)

if __name__ == "__main__":
    main()
