-- ============================================
-- Create E-Commerce Schema (10 Tables)
-- ============================================
-- This schema models a realistic e-commerce system
-- with multi-table transactional relationships

ALTER SESSION SET CONTAINER = XEPDB1;

-- Create ECOM schema user
CREATE USER ecom IDENTIFIED BY ecom_pwd
    DEFAULT TABLESPACE users
    QUOTA UNLIMITED ON users;

GRANT CREATE SESSION TO ecom;
GRANT CREATE TABLE TO ecom;
GRANT CREATE SEQUENCE TO ecom;
GRANT CREATE TRIGGER TO ecom;
GRANT CREATE PROCEDURE TO ecom;

-- Connect as ECOM user context
ALTER SESSION SET CURRENT_SCHEMA = ecom;

-- ============================================
-- 1. CATEGORIES - Product categories (hierarchical)
-- ============================================
CREATE TABLE ecom.categories (
    category_id    NUMBER(10) PRIMARY KEY,
    name           VARCHAR2(100) NOT NULL,
    parent_id      NUMBER(10),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_category_parent FOREIGN KEY (parent_id)
        REFERENCES ecom.categories(category_id)
);

CREATE SEQUENCE ecom.seq_categories START WITH 1 INCREMENT BY 1;

-- ============================================
-- 2. PRODUCTS - Product catalog
-- ============================================
CREATE TABLE ecom.products (
    product_id     NUMBER(10) PRIMARY KEY,
    sku            VARCHAR2(50) NOT NULL UNIQUE,
    name           VARCHAR2(200) NOT NULL,
    description    CLOB,
    price          NUMBER(10,2) NOT NULL,
    category_id    NUMBER(10),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product_category FOREIGN KEY (category_id)
        REFERENCES ecom.categories(category_id)
);

CREATE SEQUENCE ecom.seq_products START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_products_category ON ecom.products(category_id);
CREATE INDEX ecom.idx_products_sku ON ecom.products(sku);

-- ============================================
-- 3. INVENTORY - Stock levels per warehouse
-- ============================================
CREATE TABLE ecom.inventory (
    inventory_id   NUMBER(10) PRIMARY KEY,
    product_id     NUMBER(10) NOT NULL,
    warehouse_code VARCHAR2(10) NOT NULL,
    quantity       NUMBER(10) NOT NULL,
    reserved       NUMBER(10) DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_inventory_product FOREIGN KEY (product_id)
        REFERENCES ecom.products(product_id),
    CONSTRAINT uq_inventory_product_wh UNIQUE (product_id, warehouse_code)
);

CREATE SEQUENCE ecom.seq_inventory START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_inventory_product ON ecom.inventory(product_id);

-- ============================================
-- 4. CUSTOMERS - Customer accounts
-- ============================================
CREATE TABLE ecom.customers (
    customer_id    NUMBER(10) PRIMARY KEY,
    email          VARCHAR2(255) NOT NULL UNIQUE,
    first_name     VARCHAR2(100),
    last_name      VARCHAR2(100),
    phone          VARCHAR2(20),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE ecom.seq_customers START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_customers_email ON ecom.customers(email);

-- ============================================
-- 5. ORDERS - Customer orders
-- ============================================
CREATE TABLE ecom.orders (
    order_id       NUMBER(10) PRIMARY KEY,
    customer_id    NUMBER(10) NOT NULL,
    status         VARCHAR2(20) DEFAULT 'PENDING',
    total_amount   NUMBER(12,2),
    shipping_addr  VARCHAR2(500),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id)
        REFERENCES ecom.customers(customer_id),
    CONSTRAINT chk_order_status CHECK (status IN (
        'PENDING', 'CONFIRMED', 'PROCESSING', 'SHIPPED',
        'DELIVERED', 'CANCELLED', 'REFUNDED'
    ))
);

CREATE SEQUENCE ecom.seq_orders START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_orders_customer ON ecom.orders(customer_id);
CREATE INDEX ecom.idx_orders_status ON ecom.orders(status);

-- ============================================
-- 6. ORDER_ITEMS - Line items in orders
-- ============================================
CREATE TABLE ecom.order_items (
    item_id        NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) NOT NULL,
    product_id     NUMBER(10) NOT NULL,
    quantity       NUMBER(5) NOT NULL,
    unit_price     NUMBER(10,2) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orderitem_order FOREIGN KEY (order_id)
        REFERENCES ecom.orders(order_id),
    CONSTRAINT fk_orderitem_product FOREIGN KEY (product_id)
        REFERENCES ecom.products(product_id)
);

CREATE SEQUENCE ecom.seq_order_items START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_orderitems_order ON ecom.order_items(order_id);
CREATE INDEX ecom.idx_orderitems_product ON ecom.order_items(product_id);

-- ============================================
-- 7. PAYMENTS - Payment transactions
-- ============================================
CREATE TABLE ecom.payments (
    payment_id     NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) NOT NULL,
    amount         NUMBER(12,2) NOT NULL,
    method         VARCHAR2(20) NOT NULL,
    status         VARCHAR2(20) DEFAULT 'PENDING',
    transaction_ref VARCHAR2(100),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_payment_order FOREIGN KEY (order_id)
        REFERENCES ecom.orders(order_id),
    CONSTRAINT chk_payment_method CHECK (method IN (
        'CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'CRYPTO'
    )),
    CONSTRAINT chk_payment_status CHECK (status IN (
        'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'REFUNDED'
    ))
);

CREATE SEQUENCE ecom.seq_payments START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_payments_order ON ecom.payments(order_id);

-- ============================================
-- 8. SHIPMENTS - Shipping information
-- ============================================
CREATE TABLE ecom.shipments (
    shipment_id    NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) NOT NULL,
    carrier        VARCHAR2(50),
    tracking_num   VARCHAR2(100),
    status         VARCHAR2(20) DEFAULT 'PREPARING',
    shipped_at     TIMESTAMP,
    delivered_at   TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_shipment_order FOREIGN KEY (order_id)
        REFERENCES ecom.orders(order_id),
    CONSTRAINT chk_shipment_status CHECK (status IN (
        'PREPARING', 'SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'RETURNED'
    ))
);

CREATE SEQUENCE ecom.seq_shipments START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_shipments_order ON ecom.shipments(order_id);

-- ============================================
-- 9. SHIPMENT_ITEMS - Items in each shipment
-- ============================================
CREATE TABLE ecom.shipment_items (
    shipment_item_id NUMBER(10) PRIMARY KEY,
    shipment_id      NUMBER(10) NOT NULL,
    order_item_id    NUMBER(10) NOT NULL,
    quantity         NUMBER(5) NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_shipitem_shipment FOREIGN KEY (shipment_id)
        REFERENCES ecom.shipments(shipment_id),
    CONSTRAINT fk_shipitem_orderitem FOREIGN KEY (order_item_id)
        REFERENCES ecom.order_items(item_id)
);

CREATE SEQUENCE ecom.seq_shipment_items START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_shipitems_shipment ON ecom.shipment_items(shipment_id);

-- ============================================
-- 10. RETURNS - Product returns
-- ============================================
CREATE TABLE ecom.returns (
    return_id      NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) NOT NULL,
    order_item_id  NUMBER(10) NOT NULL,
    quantity       NUMBER(5) NOT NULL,
    reason         VARCHAR2(500),
    status         VARCHAR2(20) DEFAULT 'REQUESTED',
    refund_amount  NUMBER(10,2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_return_order FOREIGN KEY (order_id)
        REFERENCES ecom.orders(order_id),
    CONSTRAINT fk_return_orderitem FOREIGN KEY (order_item_id)
        REFERENCES ecom.order_items(item_id),
    CONSTRAINT chk_return_status CHECK (status IN (
        'REQUESTED', 'APPROVED', 'RECEIVED', 'REFUNDED', 'REJECTED'
    ))
);

CREATE SEQUENCE ecom.seq_returns START WITH 1 INCREMENT BY 1;
CREATE INDEX ecom.idx_returns_order ON ecom.returns(order_id);

-- ============================================
-- Grant SELECT privileges to Debezium user
-- ============================================
GRANT SELECT ON ecom.categories TO debezium;
GRANT SELECT ON ecom.products TO debezium;
GRANT SELECT ON ecom.inventory TO debezium;
GRANT SELECT ON ecom.customers TO debezium;
GRANT SELECT ON ecom.orders TO debezium;
GRANT SELECT ON ecom.order_items TO debezium;
GRANT SELECT ON ecom.payments TO debezium;
GRANT SELECT ON ecom.shipments TO debezium;
GRANT SELECT ON ecom.shipment_items TO debezium;
GRANT SELECT ON ecom.returns TO debezium;

COMMIT;
