SELECT * FROM silver.olist_orders_dataset;
/*		 order_id                      
  		 customer_id                   
  		 order_status                  
  		 order_purchase_timestamp      
  		 order_approved_at             
  		 order_delivered_carrier_date  
  		 order_delivered_customer_date 
  		 order_estimated_delivery_date		*/
		   
SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp IS NULL ;
SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp > order_approved_at ; -- OK
SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp > order_delivered_carrier_date ; -- We need to set a date
SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp > order_delivered_customer_date ;
SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp > order_estimated_delivery_date ;

-- Hey See here is a dates issues so solve this one by one
-- order_purchase_timestamp
BEGIN;

WITH s1 AS (
  SELECT
    o.order_id,
    GREATEST(
      o.order_purchase_timestamp,
      COALESCE(o.order_approved_at, o.order_purchase_timestamp)
    ) AS approved_fixed
  FROM silver.olist_orders_dataset o
),
s2 AS (
  SELECT
    o.order_id,
    s1.approved_fixed,
    GREATEST(
      s1.approved_fixed,
      COALESCE(o.order_delivered_carrier_date, s1.approved_fixed)
    ) AS carrier_fixed
  FROM silver.olist_orders_dataset o
  JOIN s1 USING (order_id)
),
s3 AS (
  SELECT
    o.order_id,
    s2.approved_fixed,
    s2.carrier_fixed,
    CASE
      WHEN o.order_delivered_customer_date IS NULL
        THEN s2.carrier_fixed
      WHEN o.order_delivered_customer_date < s2.carrier_fixed
        THEN s2.carrier_fixed + INTERVAL '3 days'  -- your rule: add 3 days
      ELSE o.order_delivered_customer_date
    END AS customer_fixed
  FROM silver.olist_orders_dataset o
  JOIN s2 USING (order_id)
), final_fix AS (
 SELECT
    o.order_id,
    s3.approved_fixed,
    s3.carrier_fixed,
    s3.customer_fixed,
    GREATEST(
      s3.customer_fixed,                       -- must be >= customer
      s3.carrier_fixed + INTERVAL '4 days',    -- must be >= carrier + 4 days
      COALESCE(o.order_estimated_delivery_date, s3.customer_fixed)
    ) AS estimated_fixed
  FROM silver.olist_orders_dataset o
  JOIN s3 USING (order_id)
)
UPDATE silver.olist_orders_dataset o
SET
  order_approved_at               = f.approved_fixed,
  order_delivered_carrier_date    = f.carrier_fixed,
  order_delivered_customer_date   = f.customer_fixed,
  order_estimated_delivery_date   = f.estimated_fixed
FROM final_fix f
WHERE o.order_id = f.order_id;

COMMIT;

SELECT * FROM silver.olist_orders_dataset;

SELECT COUNT(*) AS bad_rows
FROM silver.olist_orders_dataset
WHERE NOT (
  order_purchase_timestamp
  <= order_approved_at
  AND order_approved_at
  <= order_delivered_carrier_date
  AND order_delivered_carrier_date
  <= order_delivered_customer_date
  AND order_delivered_customer_date
  <= order_estimated_delivery_date
);

SELECT * FROM silver.olist_orders_dataset WHERE order_purchase_timestamp IS NULL;
SELECT * FROM silver.olist_orders_dataset WHERE order_approved_at IS NULL;  
SELECT * FROM silver.olist_orders_dataset WHERE order_delivered_carrier_date IS NULL; 
SELECT * FROM silver.olist_orders_dataset WHERE order_delivered_customer_date IS NULL;
SELECT * FROM silver.olist_orders_dataset WHERE order_estimated_delivery_date IS NULL;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

-- 1. Add new column
ALTER TABLE silver.olist_orders_dataset ADD COLUMN orders_id INT;

UPDATE silver.olist_orders_dataset s
SET orders_id = nw_order.rn
FROM (
  SELECT order_id, ROW_NUMBER() OVER (ORDER BY order_id) AS rn
  FROM silver.olist_orders_dataset
) nw_order
WHERE s.order_id = nw_order.order_id;

ALTER TABLE silver.olist_orders_dataset
ADD CONSTRAINT orders_id PRIMARY KEY (orders_id);


SELECT * FROM silver.olist_orders_dataset ORDER BY orders_id;

--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

ALTER TABLE silver.olist_orders_dataset ADD COLUMN customers_id INT;

UPDATE silver.olist_orders_dataset s
SET customers_id = nw_cust.customers_id
FROM (
  SELECT customer_id, customers_id
  FROM silver.olist_customers_dataset
) nw_cust
WHERE s.customer_id = nw_cust.customer_id;

SELECT * FROM silver.olist_orders_dataset ORDER BY customers_id;
