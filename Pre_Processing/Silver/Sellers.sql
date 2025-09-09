SELECT * FROM bronze.olist_sellers_dataset;

/*		seller_id               
  		seller_zip_code_prefix  
  		seller_city             
  		seller_state		*/
		  
SELECT DISTINCT seller_id FROM bronze.olist_sellers_dataset ;

SELECT * FROM bronze.olist_sellers_dataset WHERE seller_zip_code_prefix   IS NULL ;
SELECT * FROM bronze.olist_sellers_dataset WHERE seller_city              IS NULL ;
SELECT * FROM bronze.olist_sellers_dataset WHERE seller_state             IS NULL ;

CREATE EXTENSION IF NOT EXISTS UNACCENT;
CREATE TABLE silver.olist_sellers_dataset AS 
	SELECT 
		TRIM(seller_id) AS seller_id ,          
  		seller_zip_code_prefix AS seller_zip_code ,
  		INITCAP(UNACCENT(TRIM(seller_city ))):: VARCHAR(100) AS seller_city  ,           
  		UPPER(seller_state) AS seller_state
	FROM bronze.olist_sellers_dataset ;

SELECT * FROM silver.olist_sellers_dataset;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
SELECT * FROM silver.olist_sellers_dataset ;
-- 1. Add new column
ALTER TABLE silver.olist_sellers_dataset ADD COLUMN sellers_id INT;


UPDATE silver.olist_sellers_dataset s
SET sellers_id = nw_slr.rn
FROM (
  SELECT seller_id, ROW_NUMBER() OVER (ORDER BY seller_id) AS rn
  FROM silver.olist_sellers_dataset
) nw_slr
WHERE s.seller_id = nw_slr.seller_id;

ALTER TABLE silver.olist_sellers_dataset
ADD CONSTRAINT sellers_id PRIMARY KEY (sellers_id);


SELECT * FROM silver.olist_sellers_dataset ORDER BY sellers_id;