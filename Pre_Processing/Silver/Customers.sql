SELECT * FROM bronze.olist_customers_dataset ;

SELECT * FROM bronze.olist_customers_dataset WHERE customer_id NOT IN
(SELECT customer_id FROM bronze.olist_orders_dataset); -- 0


/*	 Ok All customer_id's are present in olist_orders_dataset
 	IN customers dataset there is no nulls present in any column so we just remove duplicate and keep
 	customer_unique_id's	*/

WITH rk AS (
	SELECT customer_id , ROW_NUMBER() OVER (PARTITION BY customer_unique_id ) AS rrkk
FROM bronze.olist_customers_dataset
)
DELETE FROM bronze.olist_customers_dataset WHERE 
customer_id IN (SELECT customer_id FROM rk WHERE rrkk > 1);

-- If we do this then at joining there will 3345 entries get null so dont remove rather than generate new 
-- customer_unique_id's based on customer_id because this are also unique and all present in olist_orders_dataset

SELECT * FROM silver.olist_customers_dataset ;

/*	customer_id                
  	customer_unique_id         
  	customer_zip_code_prefix  
  	customer_city              
  	customer_state	*/

CREATE TABLE silver.olist_customers_dataset AS 
	SELECT 
		TRIM(customer_id) AS customer_id ,
		CONCAT(UPPER(TRIM(customer_state)),'- ', ROW_NUMBER() OVER (PARTITION BY customer_state ORDER BY customer_state)):: VARCHAR (50) AS customer_unique_id ,
		LPAD(customer_zip_code_prefix::TEXT,5,'0'):: INT AS customer_zip_code ,
		-- SELECT MAX(LENGTH(customer_zip_code_prefix::TEXT)) FROM silver.olist_customers_dataset;
		INITCAP(TRIM(customer_city)):: VARCHAR (100)  AS customer_city ,
		UPPER(TRIM(customer_state))::VARCHAR (50) AS customer_state
FROM bronze.olist_customers_dataset;

SELECT * FROM silver.olist_customers_dataset;

SELECT DISTINCT customer_id FROM silver.olist_customers_dataset;
SELECT DISTINCT customer_unique_id FROM silver.olist_customers_dataset;
SELECT DISTINCT customer_zip_code FROM silver.olist_customers_dataset;
SELECT DISTINCT customer_city FROM silver.olist_customers_dataset;
SELECT DISTINCT customer_state FROM silver.olist_customers_dataset;

SELECT customer_state , COUNT(customer_unique_id) FROM silver.olist_customers_dataset
GROUP BY customer_state

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

-- 1. Add new column
ALTER TABLE silver.olist_customers_dataset ADD COLUMN customers_id INT;


UPDATE silver.olist_customers_dataset s
SET customers_id = nw_cust.rn
FROM (
  SELECT customer_id, ROW_NUMBER() OVER (ORDER BY customer_id) AS rn
  FROM silver.olist_customers_dataset
) nw_cust
WHERE s.customer_id = nw_cust.customer_id;

ALTER TABLE silver.olist_customers_dataset
ADD CONSTRAINT customers_id PRIMARY KEY (customers_id);


SELECT * FROM silver.olist_customers_dataset ORDER BY customers_id;
