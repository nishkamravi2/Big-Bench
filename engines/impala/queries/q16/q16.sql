--Compute the impact of an item price change on the
--store sales by computing the total sales for items in a 30 day period before and
--after the price change. Group the items by location of warehouse where they
--were delivered from.

-- Resources

--TODO More testing needed

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
--Result  --------------------------------------------------------------------
--keep result human readable

DROP TABLE IF EXISTS q16_impala_RUN_QUERY_0_result; 
CREATE TABLE q16_impala_RUN_QUERY_0_result (
  w_state      STRING,
  i_item_id    STRING,
  sales_before DOUBLE,
  sales_after  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q16_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE  q16_impala_RUN_QUERY_0_result
SELECT w_state, i_item_id,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('2001-03-16','yyyy-MM-dd'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_before,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('2001-03-16','yyyy-MM-dd'))
    THEN ws_sales_price - coalesce(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_after
FROM (
  SELECT *
  FROM web_sales ws
  LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
  AND ws.ws_item_sk = wr.wr_item_sk)
) a1
JOIN item i ON a1.ws_item_sk = i.i_item_sk
JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('2001-03-16', 'yyyy-MM-dd') - 30*24*60*60 --substract 30 days in secconds
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('2001-03-16', 'yyyy-MM-dd') + 30*24*60*60 --add 30 days in secconds
GROUP BY w_state,i_item_id
ORDER BY w_state,i_item_id
LIMIT 100
;
