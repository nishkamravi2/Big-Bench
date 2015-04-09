--Display customers with both store and web sales in
--consecutive years for whom the increase in web sales exceeds the increase in
--store sales for a specified year.

-- Resources

DROP TABLE IF EXISTS q13_impala_RUN_QUERY_0_temp;
CREATE TABLE q13_impala_RUN_QUERY_0_temp (
  customer_id         STRING,
  customer_first_name STRING,
  customer_last_name  STRING,
  year_total          DOUBLE,
  year                INT,
  sale_type           STRING
)
--PARTITIONED BY ( year   INT, sale_type  STRING)
--CLUSTERED BY (customer_id )
--SORTED BY (customer_id )
--INTO 16 BUCKETS
--STORED AS ORC
;


--table contains the values of the intersection of customer table and store_sales tables values
--that meet the necessary requirements and whose year value is either 1999 or 2000
INSERT INTO TABLE q13_impala_RUN_QUERY_0_temp
--PARTITION (year ,sale_type)
SELECT
  c_customer_id    AS customer_id,
  c_first_name     AS customer_first_name,
  c_last_name      AS customer_last_name,
  SUM(ss_net_paid) AS year_total,
  d_year           AS year,
  's'              AS sale_type
FROM customer c
INNER JOIN (
  SELECT ss_customer_sk, ss_net_paid, dd.d_year
  FROM store_sales ss
  JOIN (
    SELECT d_date_sk, d_year
    FROM date_dim d
    WHERE d.d_year in (2001, 2001 + 1)
  ) dd on ( ss.ss_sold_date_sk = dd.d_date_sk )

) ss 
ON c.c_customer_sk = ss.ss_customer_sk
GROUP BY
  c_customer_id,
  c_first_name,
  c_last_name,
  d_year
;


--table contains the values of the intersection of customer table and web_sales tables values that 
--meet the necessary requirements and whose year value is either 1999 or 2000
INSERT INTO TABLE q13_impala_RUN_QUERY_0_temp
--PARTITION (year ,sale_type)
SELECT
  c_customer_id    AS customer_id,
  c_first_name     AS customer_first_name,
  c_last_name      AS customer_last_name,
  SUM(ws_net_paid) AS year_total,
  d_year           AS year,
  'w'              AS sale_type
FROM customer c
INNER JOIN (
  SELECT ws_bill_customer_sk, ws_net_paid, dd.d_year
  FROM web_sales ws
  JOIN (
    SELECT d_date_sk, d_year
    FROM  date_dim d
    WHERE d.d_year in (2001, 2001 + 1)
  ) dd on (  ws.ws_sold_date_sk =dd.d_date_sk )
) ws
ON c.c_customer_sk = ws.ws_bill_customer_sk
GROUP BY
  c_customer_id,
  c_first_name,
  c_last_name,
  d_year
;


----hive 0.8.1 does not support self-joins
----if you have this version: create 4 different tables with same values to carry out the task
--DROP VIEW IF EXISTS q13_t_s_firstyear;
--DROP VIEW IF EXISTS q13_t_s_secyear;
--DROP VIEW IF EXISTS q13_t_w_firstyear;
--DROP VIEW IF EXISTS q13_t_w_secyear;

--CREATE VIEW IF NOT EXISTS q13_t_s_firstyear AS
--SELECT * FROM ${hiveconf:TEMP_TABLE};
--CREATE VIEW IF NOT EXISTS q13_t_s_secyear AS
--SELECT * FROM ${hiveconf:TEMP_TABLE};
--CREATE VIEW IF NOT EXISTS q13_t_w_firstyear AS
--SELECT * FROM ${hiveconf:TEMP_TABLE};
--CREATE VIEW IF NOT EXISTS q13_t_w_secyear AS
--SELECT * FROM ${hiveconf:TEMP_TABLE};

--Result  --------------------------------------------------------------------
--keep result human readable
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q13_impala_RUN_QUERY_0_result;
CREATE TABLE q13_impala_RUN_QUERY_0_result (
  customer_id         STRING,
  customer_first_name STRING,
  customer_last_name  STRING,
  cnt1                DOUBLE,
  cnt2                DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS PARQUET LOCATION '/user/jenkins/benchmarks/bigbench/temp/q13_impala_RUN_QUERY_0_result';

-- the real query part
INSERT INTO TABLE q13_impala_RUN_QUERY_0_result
SELECT
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name,
  CASE WHEN tw_f.year_total > 0
    THEN tw_s.year_total / tw_f.year_total
    ELSE null END,
  CASE WHEN ts_f.year_total > 0
    THEN ts_s.year_total / ts_f.year_total
    ELSE null END
FROM q13_impala_RUN_QUERY_0_temp ts_f
INNER JOIN q13_impala_RUN_QUERY_0_temp ts_s ON ts_f.customer_id = ts_s.customer_id
INNER JOIN q13_impala_RUN_QUERY_0_temp tw_f ON ts_f.customer_id = tw_f.customer_id
INNER JOIN q13_impala_RUN_QUERY_0_temp tw_s ON ts_f.customer_id = tw_s.customer_id
----hive 0.8.1 does not support self-joins
----if you have this version: create 4 different tables with same values to carry out the task
--  q13_t_s_firstyear ts_f
--  INNER JOIN q13_t_s_secyear   ts_s ON ts_f.customer_id = ts_s.customer_id
--  INNER JOIN q13_t_w_firstyear tw_f ON ts_f.customer_id = tw_f.customer_id
--  INNER JOIN q13_t_w_secyear   tw_s ON ts_f.customer_id = tw_s.customer_id

WHERE ts_s.customer_id = ts_f.customer_id
AND ts_f.customer_id = tw_s.customer_id
AND ts_f.customer_id = tw_f.customer_id
AND ts_f.sale_type   = 's'
AND tw_f.sale_type   = 'w'
AND ts_s.sale_type   = 's'
AND tw_s.sale_type   = 'w'
AND ts_f.year        = 2001
AND ts_s.year        = 2001 + 1
AND tw_f.year        = 2001
AND tw_s.year        = 2001 + 1
AND ts_f.year_total  > 0
AND tw_f.year_total  > 0

ORDER BY
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name
LIMIT 100;

--cleanup -----------------------------------------------------------
DROP TABLE IF EXISTS q13_impala_RUN_QUERY_0_temp;
