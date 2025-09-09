SELECT * FROM bronze.olist_products_dataset;
/*		product_id                   
  		product_category_name        
  		product_name_lenght          
  		product_description_lenght   
  		product_photos_qty           
  		product_weight_g             
  		product_length_cm            
  		product_height_cm            
  		product_width_cm 		*/
		  
SELECT DISTINCT product_id  FROM bronze.olist_products_dataset;                 
SELECT DISTINCT product_category_name FROM bronze.olist_products_dataset; 
SELECT product_category_name   FROM bronze.olist_products_dataset WHERE product_category_name  IS NULL; --610
SELECT product_name_lenght  FROM bronze.olist_products_dataset WHERE product_name_lenght  IS NULL; --610
SELECT product_description_lenght  FROM bronze.olist_products_dataset WHERE product_description_lenght IS NULL; --610
SELECT product_photos_qty  FROM bronze.olist_products_dataset WHERE product_photos_qty  IS NULL; --610
SELECT product_id , product_weight_g   FROM bronze.olist_products_dataset WHERE product_weight_g   IS NULL; -- 2
SELECT product_id , product_length_cm  FROM bronze.olist_products_dataset WHERE product_length_cm  IS NULL; -- 2
SELECT product_id , product_height_cm  FROM bronze.olist_products_dataset WHERE product_height_cm  IS NULL; -- 2
SELECT product_id , product_width_cm   FROM bronze.olist_products_dataset WHERE product_width_cm IS NULL ; -- 2

SELECT  DISTINCT product_id FROM bronze.olist_order_items_dataset WHERE product_id IN
(SELECT DISTINCT product_id FROM bronze.olist_products_dataset ); -- 32951
-- hey See all product_id's are present in bronze.olist_order_items_dataset so we dont able to remove those
-- thats why we update nulls as a 'Unkwon_Category' and others as 0
CREATE TABLE silver.olist_products_dataset AS 
	SELECT 
		product_id :: VARCHAR(100) ,  
  		product_category_name  :: VARCHAR(100) ,  
  		product_name_lenght  :: VARCHAR(100) ,  
  		product_description_lenght :: INT,  
  		product_photos_qty :: INT,  
  		product_weight_g   :: INT,  
  		product_length_cm  :: INT,  
  		product_height_cm  :: INT,  
  		product_width_cm   :: INT
	FROM bronze.olist_products_dataset ;
	
SELECT * FROM silver.olist_products_dataset ;

UPDATE silver.olist_products_dataset SET 
	product_category_name = 'Unkwon_Category' ,
	product_name_lenght  = 0 ,        
  	product_description_lenght = 0 ,  
  	product_photos_qty = 0
WHERE product_category_name IS NULL;

SELECT product_category_name       FROM silver.olist_products_dataset WHERE product_category_name      IS NULL; 
SELECT product_name_lenght         FROM silver.olist_products_dataset WHERE product_name_lenght        IS NULL; 
SELECT product_description_lenght  FROM silver.olist_products_dataset WHERE product_description_lenght IS NULL; 
SELECT product_photos_qty          FROM silver.olist_products_dataset WHERE product_photos_qty         IS NULL; 

UPDATE silver.olist_products_dataset SET 
	product_weight_g = 0 ,
	product_length_cm  = 0 ,        
  	product_height_cm = 0 ,  
  	product_width_cm = 0
WHERE product_weight_g   IS NULL;

SELECT product_id , product_weight_g            FROM silver.olist_products_dataset WHERE product_weight_g   IS NULL; -- 2
SELECT product_id , product_length_cm           FROM silver.olist_products_dataset WHERE product_length_cm  IS NULL; -- 2
SELECT product_id , product_height_cm           FROM silver.olist_products_dataset WHERE product_height_cm  IS NULL; -- 2
SELECT product_id , product_width_cm            FROM silver.olist_products_dataset WHERE product_width_cm IS NULL ; -- 2

SELECT * FROM silver.olist_products_dataset ;

---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------

SELECT * FROM bronze.product_category_name_translation;

/*		product_category_name 
        product_category_name_english  		*/
		
SELECT DISTINCT product_category_name FROM silver.olist_products_dataset; -- 74 ONE IS 'Unkwon_Category' So 2 is missing
SELECT DISTINCT product_category_name FROM bronze.product_category_name_translation;  -- 71

SELECT DISTINCT product_category_name FROM bronze.olist_products_dataset WHERE product_category_name NOT IN
(SELECT DISTINCT product_category_name FROM bronze.product_category_name_translation);
/*	'portateis_cozinha_e_preparadores_de_alimentos'
	'pc_gamer'
 	'Unkwon_Category' 	*/
	 
INSERT INTO silver.product_category_name_translation 
		(product_category_name , product_category_name_english )
VALUES	('portateis_cozinha_e_preparadores_de_alimentos' , 'portable_kitchen_food_preparator'),
		('pc_gamer' , 'pc_gamer'),
 		('Unkwon_Category' ,'Unkwon_Category');

SELECT DISTINCT product_category_name FROM silver.olist_products_dataset; -- 74 
SELECT DISTINCT product_category_name FROM silver.product_category_name_translation;  -- 74

---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
-- Now times To Merge 

UPDATE silver.olist_products_dataset pd SET product_category_name = pt.product_category_name_english
FROM silver.product_category_name_translation pt
WHERE pd.product_category_name = pt.product_category_name;

SELECT * FROM silver.olist_products_dataset ;
SELECT DISTINCT product_category_name FROM silver.olist_products_dataset; -- 74 

SELECT product_category_name       FROM silver.olist_products_dataset WHERE product_category_name      IS NULL; 
SELECT product_name_lenght         FROM silver.olist_products_dataset WHERE product_name_lenght        IS NULL; 
SELECT product_description_lenght  FROM silver.olist_products_dataset WHERE product_description_lenght IS NULL; 
SELECT product_photos_qty          FROM silver.olist_products_dataset WHERE product_photos_qty         IS NULL; 
SELECT product_id , product_weight_g  FROM silver.olist_products_dataset WHERE product_weight_g   IS NULL; 
SELECT product_id , product_length_cm FROM silver.olist_products_dataset WHERE product_length_cm  IS NULL; 
SELECT product_id , product_height_cm FROM silver.olist_products_dataset WHERE product_height_cm  IS NULL; 
SELECT product_id , product_width_cm  FROM silver.olist_products_dataset WHERE product_width_cm   IS NULL ;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
SELECT * FROM silver.olist_products_dataset ;
-- 1. Add new column
ALTER TABLE silver.olist_products_dataset ADD COLUMN products_id INT;


UPDATE silver.olist_products_dataset s
SET products_id = nw_prd.rn
FROM (
  SELECT product_id, ROW_NUMBER() OVER (ORDER BY product_id) AS rn
  FROM silver.olist_products_dataset
) nw_prd
WHERE s.product_id = nw_prd.product_id;

ALTER TABLE silver.olist_products_dataset
ADD CONSTRAINT products_id PRIMARY KEY (products_id);


SELECT * FROM silver.olist_products_dataset ORDER BY products_id;