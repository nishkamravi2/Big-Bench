--Find the ratio of items sold with and without promotions
--in a given month and year. Only items in certain categories sold to customers
--living in a specific time zone are considered.

-- Resources



--TODO Empty result - needs more testing

--Result  --------------------------------------------------------------------
--keep result human readable
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q17_impala_RUN_QUERY_0_result;
CREATE TABLE q17_impala_RUN_QUERY_0_result (
  promotions DOUBLE,
  total      DOUBLE,
  cnt        DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q17_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE q17_impala_RUN_QUERY_0_result
SELECT promotions, total, promotions / total * 100
--no need to cast promotions/total: SUM(COL) returns DOUBLE
FROM (
  SELECT SUM(ss_ext_sales_price) promotions
  FROM store_sales ss
  JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE ca_gmt_offset = -5
  AND s_gmt_offset = -5
  AND i_category IN ('Books', 'Music')
  AND d_year = 2001
  AND d_moy = 12
  AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
) promotional_sales
JOIN (
  SELECT SUM(ss_ext_sales_price) total
  FROM store_sales ss
  JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk 
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE ca_gmt_offset = -5
  AND s_gmt_offset = -5
  AND i_category IN ('Books', 'Music')
  AND d_year = 2001
  AND d_moy = 12
) all_sales
-- we dont need a 'ON' join condition. result is just two numbers.
ORDER BY promotions, total
LIMIT 100
;
