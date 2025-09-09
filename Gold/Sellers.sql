CREATE TABLE gold.Seller AS 
	SELECT 
		sellers_id ,
		seller_zip_code ,
		seller_city ,
		seller_state
	FROM silver.olist_sellers_dataset ;

SELECT * FROM gold.Seller ;