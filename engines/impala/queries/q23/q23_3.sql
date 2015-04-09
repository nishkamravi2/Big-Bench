--This query contains multiple, related iterations: Iteration 1: Calculate the coeficient of variation 
--and mean of every item and warehouse of two consecutive months Iteration 2: Find items that had a coeficient
--of variation in the first months of 1.5 or larger

-- Resources

--- RESULT PART 2--------------------------------------
--keep result human readable

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q23_impala_RUN_QUERY_0_result2;
CREATE TABLE q23_impala_RUN_QUERY_0_result2 (
  inv1_w_warehouse_sk BIGINT,
  inv1_i_item_sk      BIGINT,
  inv1_d_moy          INT,
  inv1_mean           DOUBLE,
  inv1_cov            DOUBLE,
  inv2_w_warehouse_sk BIGINT,
  inv2_i_item_sk      BIGINT,
  inv2_d_moy          INT,
  inv2_mean           DOUBLE,
  inv2_cov            DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q23_impala_RUN_QUERY_0_result/q23_impala_RUN_QUERY_0_result2';

-- Begin: the real query part
INSERT INTO TABLE q23_impala_RUN_QUERY_0_result2
SELECT
  inv1.w_warehouse_sk AS inv1_w_warehouse_sk,
  inv1.i_item_sk AS inv1_i_item_sk,
  inv1.d_moy AS inv1_d_moy,
  inv1.mean AS inv1_mean,
  inv1.cov AS inv1_cov,
  inv2.w_warehouse_sk AS inv2_w_warehouse_sk,
  inv2.i_item_sk AS inv2_i_item_sk,
  inv2.d_moy AS inv2_d_moy,
  inv2.mean AS inv2_mean,
  inv2.cov AS inv2_cov
FROM q23_impala_RUN_QUERY_0_temp inv1
JOIN q23_impala_RUN_QUERY_0_temp inv2 ON (
  inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1 + 1
  AND inv2.d_moy = 1 + 2
  AND inv1.cov > 1.5
)
ORDER BY
  inv1_w_warehouse_sk,
  inv1_i_item_sk,
  inv1_d_moy,
  inv1_mean,
  inv1_cov,
  inv2_d_moy,
  inv2_mean,
  inv2_cov
LIMIT 100
;
