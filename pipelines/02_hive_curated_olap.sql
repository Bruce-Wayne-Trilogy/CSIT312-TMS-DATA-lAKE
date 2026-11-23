-- CURATED zone (ORC) built from RAW with LATERAL VIEW posexplode, then OLAP queries. NOT executed here (no Hive).
USE tms_lake;
SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS curated_orders (
  order_id STRING, order_ts TIMESTAMP, channel STRING, customer_id STRING, payment_method STRING,
  delivery_emirate STRING, delivery_option STRING, order_total_aed DECIMAL(12,2), status STRING)
PARTITIONED BY (order_date STRING) STORED AS ORC;

CREATE TABLE IF NOT EXISTS curated_order_items (
  order_id STRING, line_no INT, product_id STRING, qty INT,
  unit_list_price_aed DECIMAL(12,2), unit_price_aed DECIMAL(12,2), line_total_aed DECIMAL(12,2)) STORED AS ORC;

-- online orders
INSERT OVERWRITE TABLE curated_orders PARTITION (order_date)
SELECT order_id, CAST(regexp_replace(substr(created_at,1,19),'T',' ') AS TIMESTAMP), 'online', customer.id, payment.method,
       shipping.emirate, shipping.option, CAST(totals.order_total_aed AS DECIMAL(12,2)), status, substr(created_at,1,10)
FROM raw_ecom_orders;
-- showroom orders (POS) - append
INSERT INTO TABLE curated_orders PARTITION (order_date)
SELECT oms_order_ref, CAST(regexp_replace(substr(sold_at,1,19),'T',' ') AS TIMESTAMP), 'showroom', CAST(NULL AS STRING), tender.type,
       delivery_emirate, CAST(NULL AS STRING), CAST(total_aed AS DECIMAL(12,2)), status, substr(sold_at,1,10)
FROM raw_pos_transactions;

INSERT OVERWRITE TABLE curated_order_items
SELECT o.order_id, idx + 1, li.product_id, li.qty, CAST(li.unit_list_price_aed AS DECIMAL(12,2)),
       CAST(li.unit_price_aed AS DECIMAL(12,2)), CAST(li.qty * li.unit_price_aed AS DECIMAL(12,2))
FROM raw_ecom_orders o LATERAL VIEW posexplode(o.line_items) t AS idx, li;
INSERT INTO TABLE curated_order_items
SELECT p.oms_order_ref, idx + 1, m.product_id, li.qty, CAST(li.unit_price_aed + li.discount_aed AS DECIMAL(12,2)),
       CAST(li.unit_price_aed AS DECIMAL(12,2)), CAST(li.qty * li.unit_price_aed AS DECIMAL(12,2))
FROM raw_pos_transactions p LATERAL VIEW posexplode(p.lines) t AS idx, li
JOIN ref_pos_item_map m ON m.sku_code = li.sku_code;

-- Reconciliation target (compare with orders.csv): SELECT COUNT(*), SUM(order_total_aed) FROM curated_orders;
--   expected for the 10k sample: 10000 orders, 40438942.00 AED, 14683 order lines.

-- OLAP 1: revenue by month x category with ROLLUP
SELECT substr(o.order_date,1,7) AS month, d.category, SUM(i.line_total_aed) AS revenue_aed, SUM(i.qty) AS units
FROM curated_order_items i JOIN curated_orders o ON o.order_id = i.order_id JOIN dim_product d ON d.product_id = i.product_id
GROUP BY substr(o.order_date,1,7), d.category WITH ROLLUP;
-- OLAP 2: channel x emirate cube of revenue and AOV
SELECT channel, delivery_emirate, COUNT(*) AS orders, ROUND(AVG(order_total_aed),2) AS aov, SUM(order_total_aed) AS revenue
FROM curated_orders GROUP BY channel, delivery_emirate WITH CUBE;
-- OLAP 3: web funnel by traffic source (sessions that reached each stage)
SELECT traffic_source, COUNT(DISTINCT session_id) sessions,
       COUNT(DISTINCT CASE WHEN event_type='add_to_cart' THEN session_id END) carted,
       COUNT(DISTINCT CASE WHEN event_type='purchase' THEN session_id END) purchased
FROM raw_web_events GROUP BY traffic_source;
