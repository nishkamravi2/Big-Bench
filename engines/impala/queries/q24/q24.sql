--For a given product, measure the effect of competitor's prices on
--products' in-store and online sales. (Compute the cross-price elasticity of demand
--for a given product)

-- Resources

-- compute the price change % for the competitor
DROP VIEW IF EXISTS q24_impala_RUN_QUERY_0_temp_competitor_price_view;
CREATE VIEW q24_impala_RUN_QUERY_0_temp_competitor_price_view AS
SELECT
  i_item_sk, (imp_competitor_price - i_current_price)/i_current_price AS price_change,
  imp_start_date, (imp_end_date - imp_start_date) AS no_days
FROM item i
JOIN item_marketprices imp ON i.i_item_sk = imp.imp_item_sk
WHERE i.i_item_sk IN (7, 17)
AND imp.imp_competitor_price < i.i_current_price
;


DROP VIEW IF EXISTS q24_impala_RUN_QUERY_0_temp_self_ws_view;
CREATE VIEW q24_impala_RUN_QUERY_0_temp_self_ws_view AS
SELECT
  ws_item_sk,
  SUM(
    CASE WHEN ws_sold_date_sk >= c.imp_start_date
    AND ws_sold_date_sk < c.imp_start_date + c.no_days
    THEN ws_quantity
    ELSE 0 END
  ) AS current_ws,
  SUM(
    CASE WHEN ws_sold_date_sk >= c.imp_start_date - c.no_days
    AND ws_sold_date_sk < c.imp_start_date
    THEN ws_quantity
    ELSE 0 END
  ) AS prev_ws
FROM web_sales ws
JOIN q24_impala_RUN_QUERY_0_temp_competitor_price_view c ON ws.ws_item_sk = c.i_item_sk
GROUP BY ws_item_sk
;


DROP VIEW IF EXISTS q24_impala_RUN_QUERY_0_temp_self_ss_view ;
CREATE VIEW q24_impala_RUN_QUERY_0_temp_self_ss_view AS
SELECT
  ss_item_sk,
  SUM(
    CASE WHEN ss_sold_date_sk >= c.imp_start_date
    AND ss_sold_date_sk < c.imp_start_date + c.no_days
    THEN ss_quantity
    ELSE 0 END
  ) AS current_ss,
  SUM(
    CASE WHEN ss_sold_date_sk >= c.imp_start_date - c.no_days
    AND ss_sold_date_sk < c.imp_start_date
    THEN ss_quantity
    ELSE 0 END
  ) AS prev_ss
FROM store_sales ss
JOIN q24_impala_RUN_QUERY_0_temp_competitor_price_view c ON c.i_item_sk = ss.ss_item_sk
GROUP BY ss_item_sk
;


--Result  --------------------------------------------------------------------
--keep result human readable

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q24_impala_RUN_QUERY_0_result;
CREATE TABLE q24_impala_RUN_QUERY_0_result (
  i_item_sk               BIGINT,
  cross_price_elasticity  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q24_impala_RUN_QUERY_0_result';

-- Begin: the real query part
INSERT INTO TABLE q24_impala_RUN_QUERY_0_result
SELECT
  i_item_sk,
  (current_ss + current_ws - prev_ss - prev_ws) / ((prev_ss + prev_ws) * price_change) AS cross_price_elasticity
FROM q24_impala_RUN_QUERY_0_temp_competitor_price_view c
JOIN q24_impala_RUN_QUERY_0_temp_self_ws_view ws ON c.i_item_sk = ws.ws_item_sk
JOIN q24_impala_RUN_QUERY_0_temp_self_ss_view ss ON c.i_item_sk = ss.ss_item_sk
;

-- clean up -----------------------------------
DROP VIEW q24_impala_RUN_QUERY_0_temp_self_ws_view;
DROP VIEW q24_impala_RUN_QUERY_0_temp_self_ss_view;
DROP VIEW q24_impala_RUN_QUERY_0_temp_competitor_price_view;
