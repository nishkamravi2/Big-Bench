--List all the stores with at least 10 customers who during
--a given month bought products with the price tag at least 20% higher than the
--average price of products in the same category.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
DROP TABLE IF EXISTS q07_impala_RUN_QUERY_0_result;
CREATE TABLE q07_impala_RUN_QUERY_0_result (
  state STRING,
  cnt   BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q07_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE q07_impala_RUN_QUERY_0_result
SELECT a.ca_state AS state, COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
JOIN (
  SELECT DISTINCT(d_month_seq) AS d_month_seq
  FROM date_dim 
  WHERE d_year = 2004
  AND d_moy = 7
) q07_month --subquery alias
ON q07_month.d_month_seq = d.d_month_seq
JOIN (
  SELECT
    i_category AS i_category,
    AVG(i_current_price) * 1.2 AS avg_price
  FROM item
  GROUP BY i_category
) q07_cat_avg_price --subquery alias
ON q07_cat_avg_price.i_category = i.i_category

WHERE i.i_current_price > q07_cat_avg_price.avg_price
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt
LIMIT 100
;
