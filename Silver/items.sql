SELECT * FROM bronze.olist_order_items_dataset ;
/*		order_id             
        order_item_id        
        product_id           
        seller_id            
        shipping_limit_date  
        price                
        freight_value 		*/
		
SELECT DISTINCT order_id FROM bronze.olist_order_items_dataset; -- 98666
WITH CT AS (
 	SELECT *  , ROW_NUMBER() OVER (PARTITION BY order_id , order_item_id , product_id , seller_id , 
 	shipping_limit_date ,price ORDER BY order_id) AS rkk
 	FROM bronze.olist_order_items_dataset 
)
SELECT * FROM ct WHERE rkk > 1 ; -- 112650


-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------


CREATE TABLE silver.olist_order_items_dataset AS 
WITH CT AS (
	SELECT *  , ROW_NUMBER() OVER (PARTITION BY order_id , order_item_id , product_id , seller_id , 
	shipping_limit_date ,price ORDER BY order_id) AS rkk
	FROM bronze.olist_order_items_dataset 
)
SELECT 
	order_id ,           
    order_item_id  ,      
    product_id ,         
    seller_id  ,         
    shipping_limit_date  ,
    price  ,          
    freight_value 
FROM CT 
WHERE rkk > 1;
-- Here We Inserted Dataset Without Any Duplicate

-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

SELECT * FROM silver.olist_order_items_dataset ;

CREATE TABLE silver.olist_order_item_dataset AS 
WITH ord as (
		SELECT DISTINCT ON (order_id)
			order_id ,
			MAX(order_item_id) AS order_item_id ,
			SUM(price)  AS Total_Price,
			SUM(freight_value) AS Total_freight_value
		FROM silver.olist_order_items_dataset 
		GROUP BY order_id  ORDER BY order_id desc
 
),
ids AS (
	SELECT DISTINCT ON (order_id)
		order_id ,
		LAST_VALUE (product_id) OVER (PARTITION BY order_id order by order_item_id 
		rows between unbounded preceding and unbounded following) as product_id ,
		LAST_VALUE (seller_id) OVER (PARTITION BY order_id order by order_item_id 
		rows between unbounded preceding and unbounded following ) as seller_id ,
		shipping_limit_date
	FROM bronze.olist_order_items_dataset 
	order by order_id	
)
SELECT 
	o.order_id :: VARCHAR(50),
	o.order_item_id :: INT,
	oo.product_id :: VARCHAR(50),
	oo.seller_id :: VARCHAR(50) ,
	o.Total_Price :: NUMERIC(10,2),
	o.Total_freight_value :: NUMERIC(10,2) ,
	oo.shipping_limit_date :: TIMESTAMP
FROM ord o LEFT JOIN ids oo ON o.order_id = oo.order_id; -- 98666

/* Hey See Here we Get 98666 orders record for products and seller in this data not all product and seller exist 
   because of same order_id thats why some product , seller records are missed but there price and freight are 
   aggregated based on order_id .
   i am took latest product_id's , seller_id's based on largest item_id		*/
	
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

SELECT DISTINCT order_id   FROM silver.olist_order_item_dataset ;  
SELECT order_item_id       FROM silver.olist_order_item_dataset WHERE  order_item_id IS NULL;
SELECT DISTINCT product_id FROM silver.olist_order_item_dataset ;     
SELECT DISTINCT seller_id  FROM silver.olist_order_item_dataset ;  
SELECT shipping_limit_date FROM silver.olist_order_item_dataset WHERE shipping_limit_date IS NULL; 
SELECT Total_Price         FROM silver.olist_order_item_dataset WHERE Total_Price IS NULL ;  
SELECT Total_freight_value FROM silver.olist_order_item_dataset WHERE Total_freight_value IS NULL ; 

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
SELECT * FROM silver.olist_order_item_dataset ;
-- For Order id
-- 1. Add new column
ALTER TABLE silver.olist_order_item_dataset ADD COLUMN orders_id INT;

UPDATE silver.olist_order_item_dataset s
SET orders_id = oo.orders_id
FROM (
  SELECT order_id , orders_id
  FROM silver.olist_orders_dataset
) oo
WHERE s.order_id = oo.order_id;

SELECT * FROM silver.olist_order_item_dataset ORDER BY orders_id;

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-- For Product Id

ALTER TABLE silver.olist_order_item_dataset ADD COLUMN products_id INT;

UPDATE silver.olist_order_item_dataset s
SET products_id = oo.products_id
FROM (
  SELECT product_id , products_id
  FROM silver.olist_products_dataset
) oo
WHERE s.product_id = oo.product_id;

SELECT * FROM silver.olist_order_item_dataset ORDER BY products_id;

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-- For Seller id
ALTER TABLE silver.olist_order_item_dataset ADD COLUMN sellers_id INT;

UPDATE silver.olist_order_item_dataset s
SET sellers_id = oo.sellers_id
FROM (
  SELECT seller_id , sellers_id
  FROM silver.olist_sellers_dataset
) oo
WHERE s.seller_id = oo.seller_id;

SELECT * FROM silver.olist_order_item_dataset ORDER BY sellers_id;


