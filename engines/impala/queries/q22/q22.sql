--For all items whose price was changed on a given date,
--compute the percentage change in inventory between the 30-day period BEFORE
--the price change and the 30-day period AFTER the change. Group this
--information by warehouse.

-- Resources

--Result --------------------------------------------------------------------
--keep result human readable

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q22_impala_RUN_QUERY_0_result;
CREATE TABLE q22_impala_RUN_QUERY_0_result (
  w_warehouse_name STRING,
  i_item_id        STRING,
  inv_before       BIGINT,
  inv_after        BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q22_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE q22_impala_RUN_QUERY_0_result
SELECT *
FROM (
  SELECT
    w_warehouse_name,
    i_item_id,
    SUM(
      CASE WHEN datediff(d_date, '2001-05-08') < 0
      THEN inv_quantity_on_hand
      ELSE 0 END
    ) AS inv_before,
    SUM(
      CASE WHEN datediff(d_date, '2001-05-08') >= 0
      THEN inv_quantity_on_hand
      ELSE 0 END
    ) AS inv_after
  FROM (
    SELECT *
    FROM inventory inv
    JOIN (
      SELECT
        i_item_id,
        i_item_sk
      FROM item
      WHERE i_current_price > 0.98
      AND i_current_price < 1.5
    ) items
    ON inv.inv_item_sk = items.i_item_sk
    JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
    JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk
    WHERE datediff(d_date, '2001-05-08') >= -30
    AND datediff(d_date, '2001-05-08') <= 30
  ) q22_coalition_22
  GROUP BY w_warehouse_name, i_item_id
) name
WHERE inv_before > 0
AND inv_after / inv_before >= 2.0 / 3.0
AND inv_after / inv_before <= 3.0 / 2.0
ORDER BY
  w_warehouse_name,
  i_item_id
LIMIT 100
;


