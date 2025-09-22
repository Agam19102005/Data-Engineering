import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine

# ---------------- CONFIG ----------------
PG_USER = "postgres"
PG_PASSWORD = "agam1910"
PG_HOST = "localhost"
PG_PORT = 5432
DB_NAME = "weather_db"
CITY = "London"

# ---------------- CONNECT TO DB ----------------
print("Connecting to PostgreSQL via SQLAlchemy...")
engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}')
print("Connected successfully!")

# ---------------- FETCH LAST 30 DAYS ----------------
query = f"""
SELECT datetime, temperature
FROM weather_data
WHERE city = '{CITY}'
AND datetime >= NOW() - INTERVAL '30 days'
ORDER BY datetime;
"""

df = pd.read_sql(query, engine)
print(f"Fetched {len(df)} rows for city {CITY}.")

if df.empty:
    print("No data available for forecasting. Please insert recent weather data first.")
    exit()

# ---------------- PREPARE DATA FOR ML ----------------
df = df.reset_index(drop=True)
df['day_index'] = range(len(df))  # Day 0,1,2...
X = df[['day_index']].values
y = df['temperature'].values

print("Training Linear Regression model...")
model = LinearRegression()
model.fit(X, y)
print("Model training completed.")

# ---------------- PREDICT NEXT 7 DAYS ----------------
future_days = np.array(range(len(df), len(df)+7)).reshape(-1,1)
predictions = model.predict(future_days)

print("\n📈 Predicted Temperatures for Next 7 Days:")
for i, temp in enumerate(predictions, start=1):
    print(f"Day {i}: {temp:.2f} °C")
