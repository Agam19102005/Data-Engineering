# ingest_and_report.py
import os
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob

DATA_DIR = "data"

# 1) Read sales files
sales_files = sorted(glob(os.path.join(DATA_DIR, "sales_*.csv")))
sales_dfs = []
for f in sales_files:
    df = pd.read_csv(f)
    sales_dfs.append(df)
sales = pd.concat(sales_dfs, ignore_index=True)

# 2) Read inventory files
inv_files = sorted(glob(os.path.join(DATA_DIR, "inventory_*.csv")))
inv_dfs = []
for f in inv_files:
    df = pd.read_csv(f)
    inv_dfs.append(df)
inventory = pd.concat(inv_dfs, ignore_index=True) if inv_dfs else pd.DataFrame()

# 3) Basic cleaning
# Ensure dates are proper
sales['date'] = pd.to_datetime(sales['date'], errors='coerce').dt.date
sales['time'] = pd.to_datetime(sales['time'], errors='coerce').dt.time
sales['quantity'] = pd.to_numeric(sales['quantity'], errors='coerce').fillna(0).astype(int)
sales['unit_price'] = pd.to_numeric(sales['unit_price'], errors='coerce').fillna(0.0)

# Add a revenue column
sales['revenue'] = sales['quantity'] * sales['unit_price']

# 4) Simple aggregations
# Daily totals
daily_totals = sales.groupby('date')['revenue'].sum().reset_index().sort_values('date')

# Top selling items (by quantity)
top_items = sales.groupby(['item_id','item_name'])['quantity'].sum().reset_index().sort_values('quantity', ascending=False)

# 5) Simple "current stock" estimation (starting from historical inventory events + sales)
# Build a basic items list
items = pd.DataFrame(sales[['item_id','item_name']].drop_duplicates())

# compute total restocks per item
if not inventory.empty:
    inv_sum = inventory.groupby(['item_id','item_name'])['change'].sum().reset_index()
else:
    inv_sum = pd.DataFrame(columns=['item_id','item_name','change'])

# compute total sold per item
sold_sum = sales.groupby(['item_id','item_name'])['quantity'].sum().reset_index().rename(columns={'quantity':'sold'})

# merge
stock = items.merge(inv_sum, on=['item_id','item_name'], how='left').merge(sold_sum, on=['item_id','item_name'], how='left')
stock['change'] = stock['change'].fillna(0).astype(int)
stock['sold'] = stock['sold'].fillna(0).astype(int)
stock['est_stock'] = stock['change'] - stock['sold']  # very basic estimate

# 6) Low-stock alerts
LOW_STOCK_THRESHOLD = 5
low_stock = stock[stock['est_stock'] <= LOW_STOCK_THRESHOLD]

# 7) Save aggregated CSVs
os.makedirs("output", exist_ok=True)
daily_totals.to_csv("output/daily_totals.csv", index=False)
top_items.to_csv("output/top_items.csv", index=False)
stock.to_csv("output/stock_estimate.csv", index=False)
low_stock.to_csv("output/low_stock.csv", index=False)

print("Reports saved to output/")

# 8) Simple plots
# Plot 1: daily revenue trend
plt.figure()
plt.plot(daily_totals['date'], daily_totals['revenue'], marker='o')
plt.title('Daily Revenue')
plt.xlabel('Date')
plt.ylabel('Revenue (USD)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/daily_revenue.png")
plt.close()

# Plot 2: top 5 items by quantity
plt.figure()
top5 = top_items.head(5)
plt.bar(top5['item_name'], top5['quantity'])
plt.title('Top 5 Items (by quantity sold)')
plt.xlabel('Item')
plt.ylabel('Quantity Sold')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/top5_items.png")
plt.close()

print("Charts saved as PNG in output/")
