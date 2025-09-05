import psycopg2
import requests
import pandas as pd

# 🔑 Replace with your actual OpenWeatherMap API Key
API_KEY = "8adda756db2fa1af4ae49ad2cc00a42a"  # <-- Replace this
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# 🔗 Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="weather_sales",   # The database you created
        user="postgres",          # Default PostgreSQL username
        password="cse@123", # <-- Replace this with your password
        host="localhost",         # Database server
        port="5433"               # Default PostgreSQL port
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL successfully!")
except Exception as e:
    print("❌ Error connecting to PostgreSQL:", e)
    exit()

# 📝 Create Sales Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    date DATE,
    location TEXT,
    sales_amount REAL
)
""")

# 📝 Create Weather Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    date DATE,
    location TEXT,
    temperature REAL,
    weather_condition TEXT,
    humidity REAL
)
""")
conn.commit()

# 🗑️ Clear old data (for clean runs)
cursor.execute("DELETE FROM sales")
cursor.execute("DELETE FROM weather")
conn.commit()

# ➕ Insert Dummy Sales Data
sample_sales = [
    ("2025-09-01", "London", 500),
    ("2025-09-02", "New York", 650),
    ("2025-09-03", "Tokyo", 800),
]
cursor.executemany(
    "INSERT INTO sales (date, location, sales_amount) VALUES (%s, %s, %s)",
    sample_sales
)
conn.commit()
print("✅ Sales data inserted successfully!")

# 🌦️ Function to fetch weather data
def get_weather(location):
    params = {"q": location, "appid": API_KEY, "units": "metric"}
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if response.status_code == 200:
        temp = data['main']['temp']
        condition = data['weather'][0]['description']
        humidity = data['main']['humidity']
        return temp, condition, humidity
    else:
        print(f"⚠️ Error fetching weather for {location}: {data}")
        return None, None, None

# 🔄 Fetch weather for each sales record
cursor.execute("SELECT date, location FROM sales")
sales_data = cursor.fetchall()

for date, location in sales_data:
    temp, condition, humidity = get_weather(location)
    if temp is not None:
        cursor.execute("""
        INSERT INTO weather (date, location, temperature, weather_condition, humidity)
        VALUES (%s, %s, %s, %s, %s)
        """, (date, location, temp, condition, humidity))
conn.commit()
print("✅ Weather data inserted successfully!")

# 🔍 Join Sales & Weather Data
query = """
SELECT s.date, s.location, s.sales_amount, 
       w.temperature, w.weather_condition, w.humidity
FROM sales s
JOIN weather w ON s.date = w.date AND s.location = w.location
"""
df = pd.read_sql_query(query, conn)

print("\n📊 Sales + Weather Data:")
print(df)

# ✅ Clean up
cursor.close()
conn.close()