SELECT * FROM bronze.olist_order_reviews_dataset; -- 99224

/*		review_id               
        order_id                
        review_score            
        review_comment_title    
        review_comment_message  
        review_creation_date    
        review_answer_timestamp		*/
		
SELECT DISTINCT review_id FROM bronze.olist_order_reviews_dataset; -- 98410
SELECT DISTINCT order_id FROM bronze.olist_order_reviews_dataset; -- 98673

SELECT * from  bronze.olist_order_reviews_dataset where 
review_id ='2172867fd5b1a55f98fe4608e1547b4b';

/*	"2172867fd5b1a55f98fe4608e1547b4b"	"559d606ac642899e44550f194fec7e08"	5		"Entrega no prazo e produto de qualidade!"	"2018-02-15 00:00:00"	"2018-02-26 15:53:00"
	"2172867fd5b1a55f98fe4608e1547b4b"	"ac6e61336e852cdc45fe59ada3763a66"	5		"Entrega no prazo e produto de qualidade!"	"2018-02-15 00:00:00"	"2018-02-26 15:53:00"
	"2172867fd5b1a55f98fe4608e1547b4b"	"e11ba7fd8fe0728dcd89efddcda9fb11"	5		"Entrega no prazo e produto de qualidade!"	"2018-02-15 00:00:00"	"2018-02-26 15:53:00"
*/ 
SELECT * from  bronze.olist_order_reviews_dataset where 
ORDER_id ='02e0b68852217f5715fb9cc885829454';

/*	"2eab0b2e6f5bded4d9b0b2afcfdf4534"	"02e0b68852217f5715fb9cc885829454"	4		"Gostei chegou rapidinho"	"2017-12-03 00:00:00"	"2017-12-03 21:56:00"
	"fa493ead9b093fb0fa6f7d4905b0ef3b"	"02e0b68852217f5715fb9cc885829454"	4		"Gostei e entregou rapidinho"	"2017-12-01 00:00:00"	"2017-12-03 21:57:00"
*/

-- After Seen this data i get idea order_id change but review_id same and that review score and review title 
-- and review message and other are also same so i took data on distinct order_id AND SOME time order id same but review 
-- id are diff and also comment are also diff

CREATE TABLE silver.olist_order_reviews_dataset AS
WITH SS AS (
SELECT DISTINCT ON (order_id)            
    order_id  ,   
    MAX(review_score) AS review_score,           
    INITCAP(UNACCENT(TRIM(review_comment_title ))) AS review_comment_title ,   
    STRING_AGG(INITCAP(UNACCENT(TRIM(review_comment_message ))) ,' | ') AS review_comment_message ,
    MIN(review_creation_date) as review_creation_date,   
    MAX(review_answer_timestamp) as review_answer_timestamp	
FROM bronze.olist_order_reviews_dataset
GROUP BY order_id ,review_comment_title 
),
ASS AS (
SELECT DISTINCT order_id ,LAST_VALUE(review_id) OVER (PARTITION BY order_id ORDER BY review_answer_timestamp
	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS review_id
	FROM bronze.olist_order_reviews_dataset
)
SELECT a.review_id ,
		aa.order_id ,
		aa.review_score ,
		aa.review_comment_title ,
		aa.review_comment_message ,
		aa.review_creation_date ,
		aa.review_answer_timestamp
FROM ss aa LEFT JOIN ASS a ON aa.order_id = a.order_id;

SELECT * FROM silver.olist_order_reviews_dataset ;
SELECT DISTINCT order_id FROM silver.olist_order_reviews_dataset;

UPDATE silver.olist_order_reviews_dataset SET review_comment_title = 'No_Title'
WHERE review_comment_title IS NULL;

UPDATE silver.olist_order_reviews_dataset SET review_comment_message = 'No_Message'
WHERE review_comment_message IS NULL;

SELECT * FROM silver.olist_order_reviews_dataset WHERE review_score            IS NULL ;
SELECT * FROM silver.olist_order_reviews_dataset WHERE review_comment_title    IS NULL ;
SELECT * FROM silver.olist_order_reviews_dataset WHERE review_comment_message  IS NULL ;
SELECT * FROM silver.olist_order_reviews_dataset WHERE review_creation_date    IS NULL ;
SELECT * FROM silver.olist_order_reviews_dataset WHERE review_answer_timestamp IS NULL ;

SELECT * FROM silver.olist_order_reviews_dataset WHERE review_creation_date > review_answer_timestamp;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
SELECT DISTINCT review_id FROM silver.olist_order_reviews_dataset; -- 98104
SELECT * FROM silver.olist_order_reviews_dataset ;
-- 1. Add new column
ALTER TABLE silver.olist_order_reviews_dataset ADD COLUMN reviews_id INT;


UPDATE silver.olist_order_reviews_dataset s
SET reviews_id = nw_rv.rn
FROM (
  SELECT review_id, DENSE_RANK() OVER (ORDER BY review_id) AS rn
  FROM silver.olist_order_reviews_dataset 
) nw_rv
WHERE s.review_id = nw_rv.review_id;

SELECT * FROM silver.olist_order_reviews_dataset ORDER BY reviews_id;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

ALTER TABLE silver.olist_order_reviews_dataset ADD COLUMN orders_id INT;

UPDATE silver.olist_order_reviews_dataset s
SET orders_id = oo.orders_id
FROM (
  SELECT order_id , orders_id
  FROM silver.olist_orders_dataset
) oo
WHERE s.order_id = oo.order_id;

SELECT * FROM silver.olist_order_reviews_dataset ORDER BY orders_id;