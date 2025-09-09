-- customers
DROP TABLE IF EXISTS silver.olist_customers_dataset;
CREATE TABLE silver.olist_customers_dataset (
  customer_id                VARCHAR(100),
  customer_unique_id         VARCHAR(100),
  customer_zip_code_prefix   INT,              -- keep raw as INT in silver
  customer_city              VARCHAR(100),
  customer_state             VARCHAR(100)
);

-- geolocation
DROP TABLE IF EXISTS silver.olist_geolocation_dataset;
CREATE TABLE silver.olist_geolocation_dataset (
  geolocation_zip_code_prefix INT,
  geolocation_lat             NUMERIC(9,6),
  geolocation_lng             NUMERIC(9,6),
  geolocation_city            VARCHAR(100),
  geolocation_state           VARCHAR(100)
);

-- order items
DROP TABLE IF EXISTS silver.olist_order_items_dataset;
CREATE TABLE silver.olist_order_items_dataset (
  order_id              VARCHAR(100),
  order_item_id         INT,
  product_id            VARCHAR(100),
  seller_id             VARCHAR(100),
  shipping_limit_date   TIMESTAMP,
  price                 NUMERIC(10,2),
  freight_value         NUMERIC(10,2)
);

-- payments
DROP TABLE IF EXISTS silver.olist_order_payments_dataset;
CREATE TABLE silver.olist_order_payments_dataset (
  order_id               VARCHAR(100),
  payment_sequential     INT,
  payment_type           VARCHAR(100),
  payment_installments   INT,
  payment_value          NUMERIC(10,2)
);

-- reviews
DROP TABLE IF EXISTS silver.olist_order_reviews_dataset;
CREATE TABLE silver.olist_order_reviews_dataset (
  review_id                VARCHAR(100),
  order_id                 VARCHAR(100),
  review_score             INT,
  review_comment_title     VARCHAR(255),
  review_comment_message   TEXT,
  review_creation_date     TIMESTAMP,
  review_answer_timestamp  TIMESTAMP
);

-- orders
DROP TABLE IF EXISTS silver.olist_orders_dataset;
CREATE TABLE silver.olist_orders_dataset (
  order_id                       VARCHAR(100),
  customer_id                    VARCHAR(100),
  order_status                   VARCHAR(100),
  order_purchase_timestamp       TIMESTAMP,
  order_approved_at              TIMESTAMP,
  order_delivered_carrier_date   TIMESTAMP,
  order_delivered_customer_date  TIMESTAMP,
  order_estimated_delivery_date  TIMESTAMP
);

-- sellers
DROP TABLE IF EXISTS silver.olist_sellers_dataset;
CREATE TABLE silver.olist_sellers_dataset (
  seller_id               VARCHAR(100),
  seller_zip_code_prefix  INT,
  seller_city             VARCHAR(100),
  seller_state            VARCHAR(100)
);

-- products
DROP TABLE IF EXISTS silver.olist_products_dataset;
CREATE TABLE silver.olist_products_dataset (
  product_id                    VARCHAR(100),
  product_category_name         VARCHAR(100),
  product_name_lenght           INT,
  product_description_lenght    INT,
  product_photos_qty            INT,
  product_weight_g              INT,
  product_length_cm             INT,
  product_height_cm             INT,
  product_width_cm              INT
);

-- category name translation
DROP TABLE IF EXISTS silver.product_category_name_translation;
CREATE TABLE silver.product_category_name_translation (
  product_category_name          VARCHAR(100),
  product_category_name_english  VARCHAR(100)
);

COPY silver.olist_customers_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_customers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 99441

COPY silver.olist_geolocation_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_geolocation_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 1000163

COPY silver.olist_order_items_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_order_items_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 112650

COPY silver.olist_order_payments_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_order_payments_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 103886
 
COPY silver.olist_order_reviews_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_order_reviews_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 99224

COPY silver.olist_orders_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_orders_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 99441

COPY silver.olist_products_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_products_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 32951

COPY silver.product_category_name_translation
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/product_category_name_translation.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 71

COPY silver.olist_sellers_dataset
FROM 'D:\Data_Analytics\Projects\E-Commerce Product Analytics/olist_sellers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', NULL ''); -- 3095

SELECT 'customers' AS tbl, COUNT(*) FROM silver.olist_customers_dataset
UNION ALL SELECT 'geolocation', COUNT(*) FROM silver.olist_geolocation_dataset
UNION ALL SELECT 'order_items', COUNT(*) FROM silver.olist_order_items_dataset
UNION ALL SELECT 'payments', COUNT(*) FROM silver.olist_order_payments_dataset
UNION ALL SELECT 'reviews', COUNT(*) FROM silver.olist_order_reviews_dataset
UNION ALL SELECT 'orders', COUNT(*) FROM silver.olist_orders_dataset
UNION ALL SELECT 'products', COUNT(*) FROM silver.olist_products_dataset
UNION ALL SELECT 'sellers', COUNT(*) FROM silver.olist_sellers_dataset
UNION ALL SELECT 'category_translation', COUNT(*) FROM silver.product_category_name_translation;
