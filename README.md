# Retail Sales Analytics
## Overview
This project sets up and analyzes a sample retail sales database in an Indian context using SQL. It covers typical entities such as customers, products, sales, payment methods, and sales channels, with sample data and core analytics queries on sales, order value, and price deviations.

## Database Schema

### Tables
- **customers**: Indian customer details (ID, name, email, phone, address, city, state, pin code, country)
- **products**: Product catalog with Indian grocery staples (ID, name, description, category, price, SKU, stock quantity)
- **payment_methods**: Reference table with typical Indian payment options (`Cash`, `Card`, `UPI`, `NetBanking`, `Wallet`)
- **sales_channels**: Reference table for sales channels (`Store`, `Online`, `MobileApp`)
- **sales**: Sales transaction records, linking to all above tables, with calculated `total_amount` field

## Sample Data
- **customers**: Arjun Patel (Mumbai), Sneha Sharma (Bengaluru), Rahul Singh (Delhi)
- **products**: Tata Tea 500g, Amul Milk 1L, Britannia Bread 400g
- **sales**: Sales records with quantities, unit prices, payment method, and sales channel. Includes one discounted sale.

## Key Analytics Queries

### Monthly Total Revenue
SELECT DATE_FORMAT(sale_date, '%Y-%m') AS sale_month, SUM(total_amount) AS total_revenue
FROM sales
GROUP BY sale_month;

text
- Summarizes revenue per month from all recorded sales.

### Monthly Average Order Value (AOV)
SELECT DATE_FORMAT(sale_date, '%Y-%m') AS sale_month, AVG(total_amount) AS avg_order_value
FROM sales
GROUP BY sale_month;

text
- Calculates how much, on average, each order is worth monthly.

### Monthly Price Deviations
SELECT s.id AS sale_id, s.sale_date, p.name AS product_name, p.price AS list_price, s.unit_price,
(s.unit_price - p.price) AS price_deviation
FROM sales s
JOIN products p ON s.product_id = p.id
ORDER BY ABS(price_deviation) DESC;

text
- Compares sale unit price to product list price, highlighting discounts or markups.

## Output Examples

| sale_month | total_revenue |
|------------|--------------|
| 2025-09    | 480.00       |

| sale_month | avg_order_value |
|------------|----------------|
| 2025-09    | 160.00         |

| sale_id     | sale_date            | product_name         | list_price | unit_price | price_deviation |
|-------------|--------------------- |---------------------|------------|------------|-----------------|
| SALE-IN-001 | 2025-09-10 10:15:00  | Tata Tea 500g       | 150.00     | 145.00     | -5.00           |
| SALE-IN-002 | 2025-09-15 11:30:00  | Amul Milk 1L        | 50.00      | 50.00      | 0.00            |
| SALE-IN-003 | 2025-09-20 14:55:00  | Britannia Bread 400g| 40.00      | 40.00      | 0.00            |

## Usage

1. Import or run the provided `Experiment-2.sql` in your SQL environment (MySQL, MariaDB, etc.).
2. The script will create and populate all required tables and definitions.
3. Execute the sample analytics queries for revenue, average order value, and price deviations.
4. Review the result grids shown for insights.

## Notes
- Extend sample data, queries, and schema for larger datasets or more advanced analytics as required.