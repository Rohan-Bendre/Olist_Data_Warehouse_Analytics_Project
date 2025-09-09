SELECT * FROM bronze.olist_geolocation_dataset;
/*	geolocation_zip_code_prefix 
    geolocation_lat             
    geolocation_lng             
    geolocation_city            
    geolocation_state 	*/
CREATE EXTENSION IF NOT EXISTS UNACCENT;	
CREATE TABLE silver.olist_geolocation_dataset AS 
WITH geo as (
SELECT DISTINCT ON (geolocation_zip_code_prefix)
	 	geolocation_zip_code_prefix :: INT ,
		geolocation_lat :: NUMERIC(9,6) ,           
        geolocation_lng :: NUMERIC(9,6) ,          
        INITCAP(UNACCENT(TRIM(geolocation_city ))):: VARCHAR(100)  AS geolocation_city ,         
        TRIM(UPPER(geolocation_state )) :: VARCHAR(100) AS geolocation_state
FROM bronze.olist_geolocation_dataset
)
SELECT 
		geolocation_zip_code_prefix  AS geolocation_zip_code,
		geolocation_lat  ,           
        geolocation_lng   ,          
        geolocation_city   ,         
        geolocation_state 
FROM geo;

SELECT * FROM silver.olist_geolocation_dataset;

SELECT DISTINCT geolocation_zip_code FROM silver.olist_geolocation_dataset;
SELECT DISTINCT geolocation_lat FROM silver.olist_geolocation_dataset;
SELECT DISTINCT geolocation_lng FROM silver.olist_geolocation_dataset;
SELECT DISTINCT geolocation_city FROM silver.olist_geolocation_dataset;
SELECT DISTINCT geolocation_state FROM silver.olist_geolocation_dataset;
