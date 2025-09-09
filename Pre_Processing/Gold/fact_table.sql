CREATE TABLE gold.Olist AS 
WITH
	rv AS (
		SELECT 
			order_id ,
			reviews_id ,
			review_score 
		FROM silver.olist_order_reviews_dataset		
	),
	itn AS (
		SELECT 
			order_id ,
			order_item_id ,
	 		total_price ,               
	 		total_freight_value ,
			products_id ,
			sellers_id
		FROM silver.olist_order_item_dataset it 
	),
	Pay AS (
		SELECT 
			order_id ,
			payment_type ,
			payment_value
		FROM silver.olist_order_payments_dataset
	)
		SELECT
			oo.orders_id,
			oo.customers_id ,
			it.products_id ,
			it.sellers_id ,
			rr.reviews_id ,
			it.order_item_id ,
			it.total_price ,               
	 		it.total_freight_value ,
			py.payment_type ,
			py.payment_value ,
			oo.order_approved_at ,
			oo.order_delivered_customer_date ,
			oo.order_status ,
			rr.review_score
	FROM 	silver.olist_orders_dataset oo 		LEFT JOIN rv RR 
			ON oo.order_id = rr.order_id   		LEFT JOIN itn it
			ON oo.order_id = it.order_id   		LEFT JOIN pay py
			ON oo.order_id = py.order_id 
	ORDER BY oo.orders_id ;

SELECT * FROM gold.Olist;

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
SELECT * FROM gold.Olist;

SELECT * FROM gold.Olist WHERE orders_id  IS NULL ; -- kk
SELECT * FROM gold.Olist WHERE customers_id  IS NULL ; -- kk
SELECT * FROM gold.Olist WHERE order_approved_at  IS NULL ; -- kk
SELECT * FROM gold.Olist WHERE order_delivered_customer_date  IS NULL ; -- kk
SELECT * FROM gold.Olist WHERE order_status  IS NULL ; -- kk

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
SELECT * FROM gold.Olist;

SELECT * FROM gold.Olist WHERE reviews_id  IS NULL ; -- 768
SELECT * FROM gold.Olist WHERE review_score IS NULL ; -- 768
/*		 Its null Because In Original Dataset has only 98673 unique order_id 
		and Orders_dataset has 99441 entries
 		99441 - 98673 = 768		*/

UPDATE gold.Olist SET reviews_id = 0 ,
					  review_score = 1
WHERE reviews_id  IS NULL ;

SELECT * FROM gold.Olist WHERE reviews_id  IS NULL ; 
SELECT * FROM gold.Olist WHERE review_score IS NULL ;
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

SELECT * FROM gold.Olist;

SELECT * FROM gold.Olist WHERE payment_type  IS NULL ; -- 1
SELECT * FROM gold.Olist WHERE payment_value  IS NULL ; -- 1
/*		 Its null Because In Original Dataset has only 99440 unique order_id 
		and Orders_dataset has 99441 entries
 		99441 - 99440 = 1		*/
SELECT payment_type , COUNT(payment_type) FROM gold.Olist 
GROUP BY payment_type ORDER BY COUNT(payment_type) DESC;

/*	"credit_card"
	"boleto"
	"voucher"
	"debit_card"
	"not_defined"	*/

UPDATE gold.Olist SET payment_value = total_price + total_freight_value ,
					  payment_type = 'not_defined' -- My Choice is not_defined because already in dataset
				WHERE payment_value IS NULL ;

SELECT * FROM gold.Olist WHERE payment_type  IS NULL ; 
SELECT * FROM gold.Olist WHERE payment_value IS NULL ; 

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
SELECT * FROM gold.Olist;

SELECT * FROM gold.Olist WHERE products_id  IS NULL ; -- 775 
SELECT * FROM gold.Olist WHERE order_item_id  IS NULL ; -- 775
SELECT * FROM gold.Olist WHERE total_price  IS NULL ; -- 775
SELECT * FROM gold.Olist WHERE sellers_id  IS NULL ; -- 775
SELECT * FROM gold.Olist WHERE total_freight_value  IS NULL ; -- 775
/*		 Its null Because In Original Dataset has only 98666 unique order_id 
		and Orders_dataset has 99441 entries
 		99441 - 98666 = 775			*/

SELECT DISTINCT payment_value FROM gold.Olist; 
SELECT DISTINCT payment_value FROM gold.Olist WHERE products_id  IS NULL ORDER BY payment_value ;
SELECT  total_price , total_freight_value,payment_value , order_status FROM gold.Olist WHERE products_id  IS NULL; 

WITH aa AS (
SELECT DISTINCT ON (payment_value) * FROM gold.Olist WHERE payment_value IN 
(SELECT DISTINCT payment_value FROM gold.Olist WHERE products_id IS NULL) AND products_id IS NOT NULL
)
UPDATE gold.Olist  ss SET total_price = aa.total_price ,
					  total_freight_value = aa.total_freight_value ,
					  products_id = aa.products_id ,
					  sellers_id = aa.sellers_id ,
					  order_item_id = aa.order_item_id
FROM aa 
WHERE ss.payment_value = aa.payment_value;
-- Using This i am Able to set 634 Values

SELECT * FROM silver.olist_order_item_dataset WHERE orders_id IN
(SELECT orders_id FROM gold.Olist WHERE products_id  IS NULL );

UPDATE gold.Olist SET payment_value = total_price + total_freight_value
WHERE total_price + total_freight_value != payment_value

SELECT * FROM gold.Olist WHERE products_id  IS NULL ; -- 141 
SELECT * FROM gold.Olist WHERE order_item_id  IS NULL ; -- 141
SELECT * FROM gold.Olist WHERE total_price  IS NULL ; -- 141
SELECT * FROM gold.Olist WHERE sellers_id  IS NULL ; -- 141
SELECT * FROM gold.Olist WHERE total_freight_value  IS NULL ; -- 141

SELECT DISTINCT payment_value  FROM gold.Olist WHERE payment_value IN
(SELECT payment_value FROM gold.Olist WHERE products_id  IS NULL) AND total_price IS NOT NULL;

SELECT * FROM gold.Olist ORDER BY products_id;

