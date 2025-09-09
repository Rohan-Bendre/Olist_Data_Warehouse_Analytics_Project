CREATE TABLE gold.Customers AS 
	SELECT 
		customers_id ,
		customer_unique_id ,
		customer_zip_code ,
		customer_city ,
		customer_state
	FROM silver.olist_customers_dataset;

SELECT * FROM gold.Customers ;