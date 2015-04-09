--What is the ratio between the number of items sold over
--the internet in the morning (8 to 9am) to the number of items sold in the evening
--(7 to 8pm) of customers with a specified number of dependents. Consider only
--websites with a high amount of content.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q14_impala_RUN_QUERY_0_result;
CREATE TABLE q14_impala_RUN_QUERY_0_result (
  am_pm_ratio DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q14_impala_RUN_QUERY_0_result';

-- Begin: the real query part
INSERT INTO TABLE q14_impala_RUN_QUERY_0_result
SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
FROM (
  SELECT COUNT(*) amc
  FROM web_sales ws
  JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
  AND hd.hd_dep_count = 5
  JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
  AND td.t_hour >= 7
  AND td.t_hour <= 8
  JOIN web_page wp ON wp.wp_web_page_sk =ws.ws_web_page_sk
  AND wp.wp_char_count >= 5000
  AND wp.wp_char_count <= 5200
) at
JOIN (
  SELECT COUNT(*) pmc
  FROM web_sales ws
  JOIN household_demographics hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
  AND hd.hd_dep_count = 5
  JOIN time_dim td ON  td.t_time_sk =ws.ws_sold_time_sk
  AND td.t_hour >= 19
  AND td.t_hour <= 20
  JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
  AND wp.wp_char_count >= 5000
  AND wp.wp_char_count <= 5200
) pt
ORDER BY am_pm_ratio
LIMIT 100
;
