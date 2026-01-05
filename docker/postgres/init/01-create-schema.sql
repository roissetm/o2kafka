-- ============================================
-- PostgreSQL Target Schema for CDC Replication
-- ============================================
-- Mirror of Oracle ECOM schema with PostgreSQL-specific types

-- Create schema
CREATE SCHEMA IF NOT EXISTS ecom;

-- ============================================
-- 1. CATEGORIES
-- ============================================
CREATE TABLE ecom.categories (
    category_id    INTEGER PRIMARY KEY,
    name           VARCHAR(100) NOT NULL,
    parent_id      INTEGER REFERENCES ecom.categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 2. PRODUCTS
-- ============================================
CREATE TABLE ecom.products (
    product_id     INTEGER PRIMARY KEY,
    sku            VARCHAR(50) NOT NULL UNIQUE,
    name           VARCHAR(200) NOT NULL,
    description    TEXT,
    price          DECIMAL(10,2) NOT NULL,
    category_id    INTEGER REFERENCES ecom.categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_category ON ecom.products(category_id);
CREATE INDEX idx_products_sku ON ecom.products(sku);

-- ============================================
-- 3. INVENTORY
-- ============================================
CREATE TABLE ecom.inventory (
    inventory_id   INTEGER PRIMARY KEY,
    product_id     INTEGER REFERENCES ecom.products(product_id),
    warehouse_code VARCHAR(10) NOT NULL,
    quantity       INTEGER NOT NULL,
    reserved       INTEGER DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, warehouse_code)
);

CREATE INDEX idx_inventory_product ON ecom.inventory(product_id);

-- ============================================
-- 4. CUSTOMERS
-- ============================================
CREATE TABLE ecom.customers (
    customer_id    INTEGER PRIMARY KEY,
    email          VARCHAR(255) NOT NULL UNIQUE,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100),
    phone          VARCHAR(20),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_email ON ecom.customers(email);

-- ============================================
-- 5. ORDERS
-- ============================================
CREATE TABLE ecom.orders (
    order_id       INTEGER PRIMARY KEY,
    customer_id    INTEGER REFERENCES ecom.customers(customer_id),
    status         VARCHAR(20) DEFAULT 'PENDING',
    total_amount   DECIMAL(12,2),
    shipping_addr  VARCHAR(500),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_customer ON ecom.orders(customer_id);
CREATE INDEX idx_orders_status ON ecom.orders(status);

-- ============================================
-- 6. ORDER_ITEMS
-- ============================================
CREATE TABLE ecom.order_items (
    item_id        INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    product_id     INTEGER REFERENCES ecom.products(product_id),
    quantity       SMALLINT NOT NULL,
    unit_price     DECIMAL(10,2) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_order_items_order ON ecom.order_items(order_id);
CREATE INDEX idx_order_items_product ON ecom.order_items(product_id);

-- ============================================
-- 7. PAYMENTS
-- ============================================
CREATE TABLE ecom.payments (
    payment_id     INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    amount         DECIMAL(12,2) NOT NULL,
    method         VARCHAR(20) NOT NULL,
    status         VARCHAR(20) DEFAULT 'PENDING',
    transaction_ref VARCHAR(100),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payments_order ON ecom.payments(order_id);

-- ============================================
-- 8. SHIPMENTS
-- ============================================
CREATE TABLE ecom.shipments (
    shipment_id    INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    carrier        VARCHAR(50),
    tracking_num   VARCHAR(100),
    status         VARCHAR(20) DEFAULT 'PREPARING',
    shipped_at     TIMESTAMP,
    delivered_at   TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_shipments_order ON ecom.shipments(order_id);

-- ============================================
-- 9. SHIPMENT_ITEMS
-- ============================================
CREATE TABLE ecom.shipment_items (
    shipment_item_id INTEGER PRIMARY KEY,
    shipment_id      INTEGER REFERENCES ecom.shipments(shipment_id),
    order_item_id    INTEGER REFERENCES ecom.order_items(item_id),
    quantity         SMALLINT NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_shipment_items_shipment ON ecom.shipment_items(shipment_id);

-- ============================================
-- 10. RETURNS
-- ============================================
CREATE TABLE ecom.returns (
    return_id      INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    order_item_id  INTEGER REFERENCES ecom.order_items(item_id),
    quantity       SMALLINT NOT NULL,
    reason         VARCHAR(500),
    status         VARCHAR(20) DEFAULT 'REQUESTED',
    refund_amount  DECIMAL(10,2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_returns_order ON ecom.returns(order_id);

-- ============================================
-- CDC Transaction Tracking (for idempotency)
-- ============================================
CREATE TABLE ecom.cdc_transactions (
    tx_id          VARCHAR(50) PRIMARY KEY,
    event_count    INTEGER NOT NULL,
    applied_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_scn     BIGINT
);

CREATE INDEX idx_cdc_tx_applied ON ecom.cdc_transactions(applied_at);

-- ============================================
-- CDC Replication Metrics
-- ============================================
CREATE TABLE ecom.cdc_metrics (
    id             SERIAL PRIMARY KEY,
    metric_name    VARCHAR(100) NOT NULL,
    metric_value   BIGINT NOT NULL,
    recorded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_metrics_name ON ecom.cdc_metrics(metric_name, recorded_at);

-- ============================================
-- Helper function to check row counts
-- ============================================
CREATE OR REPLACE FUNCTION ecom.get_table_counts()
RETURNS TABLE (table_name TEXT, row_count BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT 'categories'::TEXT, COUNT(*)::BIGINT FROM ecom.categories
    UNION ALL SELECT 'products', COUNT(*) FROM ecom.products
    UNION ALL SELECT 'inventory', COUNT(*) FROM ecom.inventory
    UNION ALL SELECT 'customers', COUNT(*) FROM ecom.customers
    UNION ALL SELECT 'orders', COUNT(*) FROM ecom.orders
    UNION ALL SELECT 'order_items', COUNT(*) FROM ecom.order_items
    UNION ALL SELECT 'payments', COUNT(*) FROM ecom.payments
    UNION ALL SELECT 'shipments', COUNT(*) FROM ecom.shipments
    UNION ALL SELECT 'shipment_items', COUNT(*) FROM ecom.shipment_items
    UNION ALL SELECT 'returns', COUNT(*) FROM ecom.returns;
END;
$$ LANGUAGE plpgsql;

-- Verification query
SELECT 'PostgreSQL schema created successfully' AS status;
