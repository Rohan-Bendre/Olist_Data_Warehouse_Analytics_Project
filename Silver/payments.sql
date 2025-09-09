SELECT * FROM bronze.olist_order_payments_dataset ORDER BY order_id; -- 103886 
SELECT DISTINCT order_id FROM bronze.olist_order_payments_dataset; -- 99440
/*		order_id               
        payment_sequential     
        payment_type           
        payment_installments   
        payment_value		*/
		
CREATE TABLE silver.olist_order_payments_dataset AS
WITH pd AS (
	SELECT DISTINCT ON (order_id)
		order_id ,
		MAX(payment_sequential) AS payment_sequential ,
		MAX(payment_installments) AS payment_installments ,
		SUM(payment_value) AS payment_value
	FROM bronze.olist_order_payments_dataset
	GROUP BY order_id 
	
), PT AS (
SELECT
		DISTINCT ON (ORDER_ID)
		ORDER_ID,
		LAST_VALUE (payment_type ) OVER (PARTITION BY order_id ORDER BY payment_sequential
		ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS payment_type
	FROM bronze.olist_order_payments_dataset
)
SELECT 
	o.order_id ,
	o.payment_sequential ,
	oo.payment_type ,
	o.payment_installments ,
	o.payment_value
FROM pd o LEFT JOIN pt oo ON o.order_id = oo.order_id;

/* Hey see here we take unique order_id and aggregate payment_value based on order_id because same order_id 
	there is different payment type then i took last payment type base on last payment_sequential and 
	gretest payment_installments
*/
SELECT * FROM silver.olist_order_payments_dataset ORDER BY orders_id; -- 103886 

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- 1. Add new column
ALTER TABLE silver.olist_order_payments_dataset ADD COLUMN orders_id INT;


UPDATE silver.olist_order_payments_dataset s
SET orders_id = oo.orders_id
FROM (
  SELECT order_id , orders_id
  FROM silver.olist_orders_dataset
) oo
WHERE s.order_id = oo.order_id;

SELECT * FROM silver.olist_order_payments_dataset ORDER BY orders_id;