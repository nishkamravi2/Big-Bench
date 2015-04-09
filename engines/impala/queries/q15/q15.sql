--Find the categories with flat or declining sales for in store purchases
--during a given year for a given store.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q15_impala_RUN_QUERY_0_result;
CREATE TABLE q15_impala_RUN_QUERY_0_result (
  cat       INT,
  slope     DOUBLE,
  intercept DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q15_impala_RUN_QUERY_0_result';

INSERT INTO TABLE q15_impala_RUN_QUERY_0_result
SELECT *
FROM (
  SELECT
    temp.cat,
    --SUM(temp.x)as sumX,
    --SUM(temp.y)as sumY,
    --SUM(temp.xy)as sumXY,
    --SUM(temp.xx)as sumXSquared,
    --count(temp.x) as N,
    --N * sumXY - sumX * sumY AS numerator,
    --N * sumXSquared - sumX*sumX AS denom
    --numerator / denom as slope,
    --(sumY - slope * sumX) / N as intercept
    --(count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) AS numerator,
    --(count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) AS denom
    --numerator / denom as slope,
    --(sumY - slope * sumX) / N as intercept
    ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) as slope,
    (SUM(temp.y) - ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x)*SUM(temp.x)) ) * SUM(temp.x)) / count(temp.x) as intercept
  FROM (
    SELECT
      i.i_category_id AS cat, -- ranges from 1 to 10
      s.ss_sold_date_sk AS x,
      SUM(s.ss_net_paid) AS y,
      s.ss_sold_date_sk*SUM(s.ss_net_paid) AS xy,
      s.ss_sold_date_sk*s.ss_sold_date_sk AS xx
    FROM store_sales s
    -- select date range 
    LEFT SEMI JOIN (
      SELECT d_date_sk
      FROM date_dim d
      WHERE d.d_date >= '2001-09-02'
      AND   d.d_date <= '2002-09-02'
    ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
    INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
    WHERE i.i_category_id IS NOT NULL
    AND s.ss_store_sk = 10 -- for a given store ranges from 1 to 12
    GROUP BY i.i_category_id, s.ss_sold_date_sk
  ) temp
  GROUP BY temp.cat
) regression
WHERE slope < 0
;
