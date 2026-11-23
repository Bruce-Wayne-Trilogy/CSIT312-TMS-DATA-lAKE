-- RAW zone: external tables over the JSON landing files (partitioned by dt). Requires hive-hcatalog-core JsonSerDe.
-- NOT executed in the authoring environment (no Hive available) - test on the course Hive and fix dialect issues.
-- If you get 'Class org.apache.hive.hcatalog.data.JsonSerDe not found', uncomment and fix the path to the hcatalog jar:
-- ADD JAR /path/to/hive-hcatalog-core.jar;   (find it with: find $HIVE_HOME -name 'hive-hcatalog-core*.jar')
CREATE DATABASE IF NOT EXISTS tms_lake;
USE tms_lake;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_web_events (
  event_id STRING, event_ts STRING, session_id STRING, anonymous_id STRING, customer_id STRING,
  event_type STRING, page_path STRING, product_id STRING, device STRING, traffic_source STRING,
  utm_campaign STRING, country STRING,
  props STRUCT<price_aed:DOUBLE, qty:INT, query:STRING, results:INT, status:STRING, method:STRING,
               order_id:STRING, total_aed:DOUBLE, cart_value_aed:DOUBLE, referrer:STRING>,
  data_origin STRING, consent_analytics BOOLEAN)          -- consent_analytics only exists from 2026-03-01 (schema drift -> NULL before)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE
LOCATION '/datalake/raw/web_events';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_ecom_orders (
  order_id STRING, created_at STRING,
  customer STRUCT<id:STRING, emirate:STRING>, payment STRUCT<method:STRING, status:STRING>,
  shipping STRUCT<option:STRING, fee_aed:DOUBLE, emirate:STRING>,
  line_items ARRAY<STRUCT<product_id:STRING, title:STRING, size_cm:STRING, qty:INT, unit_list_price_aed:DOUBLE, unit_price_aed:DOUBLE>>,
  totals STRUCT<items_net_aed:DOUBLE, order_total_aed:DOUBLE, vat_aed:DOUBLE>,
  promo_flag BOOLEAN, status STRING, data_origin STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION '/datalake/raw/ecom_orders';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_pos_transactions (
  receipt_no STRING, store_code STRING, terminal_id STRING, cashier_id STRING, sold_at STRING, oms_order_ref STRING,
  lines ARRAY<STRUCT<sku_code:STRING, qty:INT, unit_price_aed:DOUBLE, discount_aed:DOUBLE>>,
  tender STRUCT<type:STRING, amount_aed:DOUBLE>, total_aed DOUBLE, delivery_emirate STRING, status STRING, data_origin STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION '/datalake/raw/pos_transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_support_messages (
  msg_id STRING, ts STRING, channel STRING, customer_id STRING, order_id STRING, language STRING,
  topic_hint STRING, text STRING, data_origin STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION '/datalake/raw/support_messages';

CREATE EXTERNAL TABLE IF NOT EXISTS ref_pos_item_map (sku_code STRING, product_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/datalake/reference/pos_item_map'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_product (
  product_id STRING, product_name STRING, category STRING, subcategory STRING, brand STRING, tier STRING, size_cm STRING,
  regular_price_aed DECIMAL(12,2), current_price_aed DECIMAL(12,2), markdown_observed BOOLEAN,
  price_basis STRING, source_url STRING, snapshot_age_days INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/datalake/reference/product_catalog'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE raw_web_events; MSCK REPAIR TABLE raw_ecom_orders;
MSCK REPAIR TABLE raw_pos_transactions; MSCK REPAIR TABLE raw_support_messages;
