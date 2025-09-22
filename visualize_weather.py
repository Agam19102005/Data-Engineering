import pandas as pd
from sqlalchemy import create_engine

# Replace with your password
PG_PASSWORD = "agam1910"
PG_USER = "postgres"
PG_HOST = "localhost"
PG_PORT = 5432
DB_NAME = "weather_db"

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}')

# Query last 30 days
query = """
SELECT datetime, temperature, humidity
FROM weather_data
WHERE city = 'London'
AND datetime >= NOW() - INTERVAL '30 days'
ORDER BY datetime;
"""

df = pd.read_sql(query, engine)

# Plot
import matplotlib.pyplot as plt
if df.empty:
    print("No data to plot!")
else:
    plt.figure(figsize=(10,5))
    plt.plot(df['datetime'], df['temperature'], marker='o', label='Temperature (°C)')
    plt.plot(df['datetime'], df['humidity'], marker='x', label='Humidity (%)')
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.title('Weather Data (Last 30 Days) - London')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()
