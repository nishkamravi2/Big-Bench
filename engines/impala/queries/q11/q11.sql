-- Impala does not support "corr" 

--For a given product, measure the correlation of sentiments, including
--the number of reviews and average review ratings, on product monthly revenues.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable

DROP TABLE IF EXISTS q11_impala_RUN_QUERY_0_result;  
CREATE TABLE q11_impala_RUN_QUERY_0_result (
  correlation DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q11_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE q11_impala_RUN_QUERY_0_result
-- SELECT corr(reviews_count,avg_rating)
SELECT (avg(reviews_count*avg_rating) - avg(reviews_count)*avg(avg_rating))/(stddev_pop(reviews_count)*stddev_pop(avg_rating))
FROM (
  SELECT
    p.pr_item_sk AS pid,
    p.r_count    AS reviews_count,
    p.avg_rating AS avg_rating,
    s.revenue    AS m_revenue
  FROM (
    SELECT
      pr_item_sk,
      count(*) AS r_count,
      avg(pr_review_rating) AS avg_rating
    FROM product_reviews
    WHERE pr_item_sk IS NOT null
    --this is GROUP BY 1 in original::same as pr_item_sk here::hive complains anyhow
    GROUP BY pr_item_sk
  ) p
  INNER JOIN (
    SELECT
      ws_item_sk,
      SUM(ws_net_paid) AS revenue
    FROM web_sales ws
    -- Select date range of interest
    LEFT SEMI JOIN (
      SELECT d_date_sk
      FROM date_dim d
      WHERE d.d_date >= '2003-01-02'
      AND   d.d_date <= '2003-02-02'
    ) dd on ( ws.ws_sold_date_sk=dd.d_date_sk )
    WHERE ws_item_sk IS NOT null
    --this is GROUP BY 1 in original::same as ws_item_sk here::hive complains anyhow
    GROUP BY ws_item_sk
  ) s
  ON p.pr_item_sk = s.ws_item_sk
) q11_review_stats
;

