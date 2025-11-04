import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import matplotlib.pyplot as plt
from faker import Faker
import random, json, time
from datetime import datetime, timezone

# --------------------------------------------
# Database connection config
# --------------------------------------------
DB_CONF = dict(
    host="localhost",
    port=5433,
    dbname="retaildb",
    user="retail",
    password="retailpass"
)

# --------------------------------------------
# Create database schema
# --------------------------------------------
def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""CREATE EXTENSION IF NOT EXISTS "uuid-ossp";""")

    schema_sql = """
    CREATE TABLE IF NOT EXISTS staging_transactions (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        raw_payload jsonb NOT NULL,
        received_at timestamptz DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id VARCHAR PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        signup_date DATE
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_id VARCHAR PRIMARY KEY,
        name TEXT,
        category TEXT,
        price NUMERIC(10,2)
    );

    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        transaction_id VARCHAR,
        customer_id VARCHAR,
        product_id VARCHAR,
        qty INT,
        unit_price NUMERIC(10,2),
        total_price NUMERIC(12,2),
        sale_ts timestamptz,
        store_id VARCHAR,
        payment_method TEXT
    );
    """
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    print("✅ Tables created successfully!")

# --------------------------------------------
# Generate fake transactions (Capture stage)
# --------------------------------------------
fake = Faker()
PRODUCTS = [
    {"product_id":"P001","name":"T-Shirt","category":"Apparel","price":299.00},
    {"product_id":"P002","name":"Jeans","category":"Apparel","price":999.00},
    {"product_id":"P003","name":"Coffee Maker","category":"Home","price":2499.00},
    {"product_id":"P004","name":"Headphones","category":"Electronics","price":1499.00},
    {"product_id":"P005","name":"Notebook","category":"Stationery","price":79.00},
]

def random_transaction():
    customer = {
        "customer_id": f"C{random.randint(1000,9999)}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "signup_date": str(fake.date_between(start_date='-2y', end_date='today'))
    }
    items = []
    for _ in range(random.randint(1,4)):
        p = random.choice(PRODUCTS)
        qty = random.randint(1,3)
        items.append({
            "product_id": p["product_id"],
            "name": p["name"],
            "category": p["category"],
            "unit_price": p["price"],
            "qty": qty,
            "line_total": round(p["price"] * qty, 2)
        })
    payload = {
        "transaction_id": f"T{int(time.time()*1000)}{random.randint(100,999)}",
        "store_id": f"S{random.randint(1,5)}",
        "customer": customer,
        "items": items,
        "payment_method": random.choice(["cash","card","upi"]),
        "transaction_ts": datetime.now(timezone.utc).isoformat()
    }
    return payload

def insert_fake_data(conn, n=50):
    cur = conn.cursor()
    data = [(json.dumps(random_transaction()),) for _ in range(n)]
    execute_values(cur, "INSERT INTO staging_transactions (raw_payload) VALUES %s", data)
    conn.commit()
    cur.close()
    print(f"✅ Inserted {n} fake transactions into staging.")

# --------------------------------------------
# ETL (Process + Store)
# --------------------------------------------
def run_etl(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, raw_payload FROM staging_transactions;")
    rows = cur.fetchall()
    for sid, raw in rows:
        payload = raw
        tx_id = payload["transaction_id"]
        cust = payload["customer"]
        cur.execute("""
            INSERT INTO dim_customer (customer_id, first_name, last_name, email, signup_date)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                email=EXCLUDED.email;
        """, (cust["customer_id"], cust["first_name"], cust["last_name"], cust["email"], cust["signup_date"]))
        for item in payload["items"]:
            cur.execute("""
                INSERT INTO dim_product (product_id, name, category, price)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name=EXCLUDED.name,
                    category=EXCLUDED.category,
                    price=EXCLUDED.price;
            """, (item["product_id"], item["name"], item["category"], item["unit_price"]))
            cur.execute("""
                INSERT INTO fact_sales (transaction_id, customer_id, product_id, qty, unit_price, total_price, sale_ts, store_id, payment_method)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                tx_id, cust["customer_id"], item["product_id"],
                item["qty"], item["unit_price"], item["line_total"],
                payload["transaction_ts"], payload["store_id"], payload["payment_method"]
            ))
        cur.execute("DELETE FROM staging_transactions WHERE id = %s;", (sid,))
    conn.commit()
    cur.close()
    print("✅ ETL completed: data moved from staging to fact/dim tables.")

# --------------------------------------------
# Analysis (Analyze stage)
# --------------------------------------------
def analyze(conn):
    print("📊 Running analytics...")
    df_revenue = pd.read_sql("""
        SELECT date_trunc('day', sale_ts) AS day,
               SUM(total_price) AS revenue
        FROM fact_sales
        GROUP BY day
        ORDER BY day;
    """, conn)
    df_products = pd.read_sql("""
        SELECT p.name, SUM(f.total_price) AS revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.name
        ORDER BY revenue DESC;
    """, conn)
    print(df_revenue.head())
    print(df_products.head())
    return df_revenue, df_products

# --------------------------------------------
# Visualization (Visualize stage)
# --------------------------------------------
def visualize(df_revenue, df_products):
    plt.figure(figsize=(8,4))
    plt.plot(df_revenue['day'], df_revenue['revenue'])
    plt.title("Daily Revenue Trend")
    plt.xlabel("Day")
    plt.ylabel("Revenue")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(8,4))
    plt.bar(df_products['name'], df_products['revenue'])
    plt.title("Top Products by Revenue")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# --------------------------------------------
# Main execution
# --------------------------------------------
def main():
    conn = psycopg2.connect(**DB_CONF)
    create_schema(conn)
    insert_fake_data(conn, n=100)
    run_etl(conn)
    df_rev, df_prod = analyze(conn)
    visualize(df_rev, df_prod)
    conn.close()
    print("🏁 All stages (Capture → Store → Process → Analyze → Visualize) completed successfully!")

if __name__ == "__main__":
    main()
