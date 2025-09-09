CREATE TABLE gold.Product AS 
	SELECT 
	DISTINCT ON (aa.products_id )
		aa.products_id ,
		aa.product_category_name ,
		bb.price AS Total_Price ,
		bb.freight_value AS Total_Freight_Value,
		aa.product_name_lenght ,
		aa.product_description_lenght ,
		aa.product_photos_qty ,
		aa.product_weight_g ,
		aa.product_length_cm ,
		aa.product_width_cm ,
		aa.product_height_cm 
	FROM silver.olist_products_dataset aa LEFT JOIN silver.olist_order_items_dataset bb
	ON aa.product_id = bb.product_id

SELECT * FROM gold.Product ;
SELECT * FROM gold.Product WHERE Total_Price IS NULL;