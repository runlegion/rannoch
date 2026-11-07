-- Westwind's eight tables, as the eight smugglr manifests in research document
-- 01a1b37a-e007-75e1-8cbc-8f029c6d397a create them (indexes left out: the toy only reads).
CREATE TABLE categories (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE, description TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE suppliers (id TEXT PRIMARY KEY NOT NULL, company TEXT NOT NULL, contact TEXT, city TEXT, region TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE products (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, supplier_id TEXT NOT NULL, category_id TEXT NOT NULL, unit TEXT, unit_price REAL NOT NULL, in_stock INTEGER NOT NULL DEFAULT 0, discontinued INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL);
CREATE TABLE customers (id TEXT PRIMARY KEY NOT NULL, code TEXT NOT NULL UNIQUE, company TEXT NOT NULL, contact TEXT, title TEXT, city TEXT, region TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE employees (id TEXT PRIMARY KEY NOT NULL, code TEXT NOT NULL UNIQUE, last_name TEXT NOT NULL, first_name TEXT NOT NULL, title TEXT, reports_to TEXT, city TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE shippers (id TEXT PRIMARY KEY NOT NULL, company TEXT NOT NULL, phone TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE orders (id TEXT PRIMARY KEY NOT NULL, customer_id TEXT NOT NULL, employee_id TEXT NOT NULL, shipper_id TEXT NOT NULL, order_date TEXT NOT NULL, required_date TEXT, shipped_date TEXT, freight REAL NOT NULL DEFAULT 0, ship_city TEXT, ship_region TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE order_details (id TEXT PRIMARY KEY NOT NULL, order_id TEXT NOT NULL, product_id TEXT NOT NULL, unit_price REAL NOT NULL, quantity INTEGER NOT NULL, discount REAL NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL);
