import requests
from pymongo import MongoClient
from datetime import datetime, timezone

# Configurations
API_KEY = "8adda756db2fa1af4ae49ad2cc00a42a"  # Replace with your API key
CITY = "Dehradun"  # Specify your city

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "weather_db"
COLLECTION_NAME = "weather_data"

def fetch_weather(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    weather_data = {
        "date": datetime.now(timezone.utc),  # Timezone aware datetime in UTC
        "city": city,
        "temperature": data['main']['temp'],
        "humidity": data['main']['humidity'],
        "description": data['weather'][0]['description'],
    }
    return weather_data

def store_weather_mongodb(weather_data):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    collection.insert_one(weather_data)
    print(f"Weather data for {weather_data['city']} stored successfully.")

def main():
    weather = fetch_weather(API_KEY, CITY)
    store_weather_mongodb(weather)

if __name__ == "__main__":
    main()
