-- Create and use retail database
CREATE DATABASE IF NOT EXISTS retail_india;
USE retail_india;

-- Customers table
CREATE TABLE customers (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(160) UNIQUE,
    phone VARCHAR(32),
    address VARCHAR(255),
    city VARCHAR(120),
    state VARCHAR(120),
    postal_code VARCHAR(32),
    country VARCHAR(120) DEFAULT 'India',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE products (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(120),
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    sku VARCHAR(64) UNIQUE,
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Payment methods reference table
CREATE TABLE payment_methods (
    method VARCHAR(32) NOT NULL PRIMARY KEY
);

-- Insert Indian context payment methods
INSERT INTO payment_methods (method) VALUES
('Cash'), ('Card'), ('UPI'), ('NetBanking'), ('Wallet');

-- Sales channels reference table
CREATE TABLE sales_channels (
    channel VARCHAR(32) NOT NULL PRIMARY KEY
);

-- Insert Indian typical sales channels
INSERT INTO sales_channels (channel) VALUES
('Store'), ('Online'), ('MobileApp');

-- Sales table with integrity constraints and generated total_amount
CREATE TABLE sales (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    sale_date DATETIME NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(12,2) NOT NULL CHECK (unit_price >= 0),
    total_amount DECIMAL(12,2) AS (quantity * unit_price) STORED,
    payment_method VARCHAR(32) NOT NULL,
    sales_channel VARCHAR(32) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (payment_method) REFERENCES payment_methods(method),
    FOREIGN KEY (sales_channel) REFERENCES sales_channels(channel),
    INDEX idx_sale_date (sale_date)
);

-- Sample data for customers (Indian names and cities)
INSERT INTO customers (id, first_name, last_name, email, phone, address, city, state, postal_code, country)
VALUES
('CUST-IN-001','Arjun','Patel','arjun.patel@example.in','+91-9876543210','5 MG Road','Mumbai','Maharashtra','400001','India'),
('CUST-IN-002','Sneha','Sharma','sneha.sharma@example.in','+91-9123456780','12 Brigade Rd','Bengaluru','Karnataka','560001','India'),
('CUST-IN-003','Rahul','Singh','rahul.singh@example.in','+91-9988776655','Ashoka Road','Delhi','Delhi','110001','India');

-- Sample data for products with Indian items
INSERT INTO products (id, name, description, category, price, sku, stock_quantity)
VALUES
('PROD-IN-101','Tata Tea 500g','Strong Assam tea','Groceries',150.00,'SKU-IN-101',500),
('PROD-IN-102','Amul Milk 1L','Fresh toned milk','Dairy',50.00,'SKU-IN-102',1000),
('PROD-IN-103','Britannia Bread 400g','Whole wheat bread','Bakery',40.00,'SKU-IN-103',300);

-- Sample sales data with references to above data
INSERT INTO sales (id, customer_id, product_id, sale_date, quantity, unit_price, payment_method, sales_channel)
VALUES
('SALE-IN-001','CUST-IN-001','PROD-IN-101', '2025-09-10 10:15:00', 2, 145.00, 'UPI', 'Store'),     -- discounted price
('SALE-IN-002','CUST-IN-002','PROD-IN-102', '2025-09-15 11:30:00', 3, 50.00, 'Card', 'Online'),
('SALE-IN-003','CUST-IN-003','PROD-IN-103', '2025-09-20 14:55:00', 1, 40.00, 'Cash', 'Store');

-- Analytics Queries

-- Monthly Total Revenue
SELECT DATE_FORMAT(sale_date, '%Y-%m') AS sale_month, SUM(total_amount) AS total_revenue
FROM sales
GROUP BY sale_month;

-- Monthly Average Order Value (AOV)
SELECT DATE_FORMAT(sale_date, '%Y-%m') AS sale_month, AVG(total_amount) AS avg_order_value
FROM sales
GROUP BY sale_month;

-- Monthly Price Deviations (unit_price vs product price)
SELECT s.id AS sale_id, s.sale_date, p.name AS product_name, p.price AS list_price, s.unit_price,
       (s.unit_price - p.price) AS price_deviation
FROM sales s
JOIN products p ON s.product_id = p.id
ORDER BY ABS(price_deviation) DESC;
