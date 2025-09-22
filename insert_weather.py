import requests
import psycopg2
from datetime import datetime

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="weather_db",
    user="postgres",
    password="agam1910",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Fetch live weather from OpenWeatherMap
API_KEY = "8adda756db2fa1af4ae49ad2cc00a42a"
city = "London"
url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
response = requests.get(url).json()

# Extract data
temperature = response['main']['temp']
humidity = response['main']['humidity']
pressure = response['main']['pressure']
weather = response['weather'][0]['description']
datetime_now = datetime.now()

# Insert into PostgreSQL
cur.execute("""
    INSERT INTO weather_data (city, temperature, humidity, pressure, weather, datetime)
    VALUES (%s, %s, %s, %s, %s, %s);
""", (city, temperature, humidity, pressure, weather, datetime_now))

conn.commit()
cur.close()
conn.close()

print("✅ Weather data inserted successfully!")
