-- ============================================
-- Seed Initial Data for E-Commerce Schema
-- ============================================
-- This script populates reference data and sample
-- records for testing CDC replication

ALTER SESSION SET CONTAINER = XEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ecom;

-- ============================================
-- Categories (hierarchical)
-- ============================================
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (1, 'Electronics', NULL);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (2, 'Clothing', NULL);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (3, 'Home & Garden', NULL);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (4, 'Computers', 1);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (5, 'Smartphones', 1);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (6, 'Audio', 1);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (7, 'Men''s Clothing', 2);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (8, 'Women''s Clothing', 2);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (9, 'Furniture', 3);
INSERT INTO ecom.categories (category_id, name, parent_id) VALUES (10, 'Kitchen', 3);

-- ============================================
-- Products
-- ============================================
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (101, 'LAPTOP-001', 'ProBook Laptop 15"', 'High-performance laptop with 16GB RAM', 999.99, 4);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (102, 'LAPTOP-002', 'UltraBook Air 13"', 'Lightweight laptop for professionals', 1299.99, 4);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (103, 'PHONE-001', 'SmartPhone Pro', 'Latest flagship smartphone', 899.99, 5);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (104, 'PHONE-002', 'SmartPhone Lite', 'Budget-friendly smartphone', 399.99, 5);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (105, 'HEADPHONE-001', 'Wireless Headphones Pro', 'Noise-canceling wireless headphones', 299.99, 6);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (106, 'SPEAKER-001', 'Bluetooth Speaker', 'Portable waterproof speaker', 79.99, 6);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (107, 'SHIRT-001', 'Classic Polo Shirt', '100% cotton polo shirt', 49.99, 7);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (108, 'JEANS-001', 'Slim Fit Jeans', 'Modern slim fit denim jeans', 79.99, 7);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (109, 'DRESS-001', 'Summer Dress', 'Lightweight floral summer dress', 89.99, 8);
INSERT INTO ecom.products (product_id, sku, name, description, price, category_id) VALUES
    (110, 'SOFA-001', 'Modern Sectional Sofa', 'L-shaped sectional sofa', 1499.99, 9);

-- ============================================
-- Inventory (multiple warehouses)
-- ============================================
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (1, 101, 'WH01', 50, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (2, 101, 'WH02', 30, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (3, 102, 'WH01', 25, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (4, 103, 'WH01', 100, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (5, 103, 'WH02', 75, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (6, 104, 'WH01', 200, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (7, 105, 'WH01', 40, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (8, 106, 'WH01', 150, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (9, 107, 'WH01', 300, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (10, 108, 'WH01', 250, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (11, 109, 'WH01', 100, 0);
INSERT INTO ecom.inventory (inventory_id, product_id, warehouse_code, quantity, reserved) VALUES
    (12, 110, 'WH01', 15, 0);

-- ============================================
-- Customers
-- ============================================
INSERT INTO ecom.customers (customer_id, email, first_name, last_name, phone) VALUES
    (1, 'john.doe@email.com', 'John', 'Doe', '+1-555-0101');
INSERT INTO ecom.customers (customer_id, email, first_name, last_name, phone) VALUES
    (2, 'jane.smith@email.com', 'Jane', 'Smith', '+1-555-0102');
INSERT INTO ecom.customers (customer_id, email, first_name, last_name, phone) VALUES
    (3, 'bob.wilson@email.com', 'Bob', 'Wilson', '+1-555-0103');
INSERT INTO ecom.customers (customer_id, email, first_name, last_name, phone) VALUES
    (4, 'alice.johnson@email.com', 'Alice', 'Johnson', '+1-555-0104');
INSERT INTO ecom.customers (customer_id, email, first_name, last_name, phone) VALUES
    (5, 'charlie.brown@email.com', 'Charlie', 'Brown', '+1-555-0105');

-- ============================================
-- Update sequences to avoid conflicts
-- ============================================
ALTER SEQUENCE ecom.seq_categories RESTART START WITH 100;
ALTER SEQUENCE ecom.seq_products RESTART START WITH 200;
ALTER SEQUENCE ecom.seq_inventory RESTART START WITH 100;
ALTER SEQUENCE ecom.seq_customers RESTART START WITH 100;
ALTER SEQUENCE ecom.seq_orders RESTART START WITH 1000;
ALTER SEQUENCE ecom.seq_order_items RESTART START WITH 2000;
ALTER SEQUENCE ecom.seq_payments RESTART START WITH 3000;
ALTER SEQUENCE ecom.seq_shipments RESTART START WITH 4000;
ALTER SEQUENCE ecom.seq_shipment_items RESTART START WITH 5000;
ALTER SEQUENCE ecom.seq_returns RESTART START WITH 6000;

COMMIT;

-- ============================================
-- Verify data
-- ============================================
SELECT 'categories' AS table_name, COUNT(*) AS row_count FROM ecom.categories
UNION ALL SELECT 'products', COUNT(*) FROM ecom.products
UNION ALL SELECT 'inventory', COUNT(*) FROM ecom.inventory
UNION ALL SELECT 'customers', COUNT(*) FROM ecom.customers;
