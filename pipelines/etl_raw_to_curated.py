#!/usr/bin/env python3
"""
Raw -> Curated -> Consumption pipeline (PySpark). SYNTHETIC data.

  python3 -I etl_raw_to_curated.py --raw LAKE_RAW --truth CURATED_CSV_DIR --out LAKE_OUT [--master local[2]]

Zones written under --out:
  curated/orders, curated/order_items   (Parquet, partitioned by order_date)  conformed from ecom + POS feeds
  curated/web_events                    (Parquet, partitioned by dt; flattened props; tolerates schema drift)
  curated/support_messages              (Parquet, partitioned by dt; keyword topic classifier added)
  consumption/*                         small marts for BI/OLAP
Reconciliation: the conformed orders must equal truth/orders.csv (per-order total & items) -> results JSON.
"""
import argparse, json, time
from pyspark.sql import SparkSession, functions as F, Window

ap = argparse.ArgumentParser()
ap.add_argument("--raw", required=True); ap.add_argument("--truth", required=True)
ap.add_argument("--out", required=True); ap.add_argument("--master", default="local[2]")
ap.add_argument("--results", default=None)
a = ap.parse_args()
t0 = time.time()
sp = (SparkSession.builder.master(a.master).appName("tms-etl").config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
sp.sparkContext.setLogLevel("ERROR")
log = {}

# ---------------- RAW -> CURATED: orders (two heterogeneous sources unified) ----------------
ecom = sp.read.json(f"{a.raw}/ecom_orders")
pos = sp.read.json(f"{a.raw}/pos_transactions")
pmap = sp.read.option("header", True).csv(f"{a.raw}/reference/pos_item_map.csv")
cat = sp.read.option("header", True).csv(f"{a.truth}/product_catalog.csv").select(
    "product_id", "product_name", "category", "subcategory", "brand", "size_cm")

e_items = (ecom.select("order_id", F.posexplode("line_items").alias("pos", "li"))
           .select("order_id", (F.col("pos") + 1).alias("line_no"), F.col("li.product_id").alias("product_id"),
                   F.col("li.qty").alias("qty"), F.col("li.unit_list_price_aed").alias("unit_list_price_aed"),
                   F.col("li.unit_price_aed").alias("unit_price_aed")))
p_items = (pos.select(F.col("oms_order_ref").alias("order_id"), F.posexplode("lines").alias("pos", "li"))
           .select("order_id", (F.col("pos") + 1).alias("line_no"), F.col("li.sku_code").alias("sku_code"),
                   F.col("li.qty").alias("qty"), F.col("li.unit_price_aed").alias("unit_price_aed"),
                   F.col("li.discount_aed").alias("discount_aed"))
           .join(pmap, "sku_code")
           .select("order_id", "line_no", "product_id", "qty",
                   F.round(F.col("unit_price_aed") + F.col("discount_aed"), 2).alias("unit_list_price_aed"), "unit_price_aed"))
items = (e_items.unionByName(p_items).join(cat, "product_id", "left")
         .withColumn("line_total_aed", F.round(F.col("qty") * F.col("unit_price_aed"), 2)))

o_e = ecom.select("order_id", F.to_timestamp("created_at").alias("order_ts"), F.lit("online").alias("channel"),
                  F.col("customer.id").alias("customer_id"), F.col("payment.method").alias("payment_method"),
                  F.col("shipping.emirate").alias("delivery_emirate"), F.col("shipping.option").alias("delivery_option"),
                  F.col("totals.order_total_aed").alias("order_total_aed"), "status")
o_p = pos.select(F.col("oms_order_ref").alias("order_id"), F.to_timestamp("sold_at").alias("order_ts"),
                 F.lit("showroom").alias("channel"), F.lit(None).cast("string").alias("customer_id"),
                 F.col("tender.type").alias("payment_method"), F.col("delivery_emirate"),
                 F.lit(None).cast("string").alias("delivery_option"), F.col("total_aed").alias("order_total_aed"), "status")
orders = (o_e.unionByName(o_p).withColumn("order_date", F.to_date("order_ts")))
orders.write.mode("overwrite").partitionBy("order_date").parquet(f"{a.out}/curated/orders")
items.write.mode("overwrite").parquet(f"{a.out}/curated/order_items")
orders = sp.read.parquet(f"{a.out}/curated/orders"); items = sp.read.parquet(f"{a.out}/curated/order_items")

# ---------------- reconciliation against ground truth ----------------
truth = sp.read.option("header", True).csv(f"{a.truth}/orders.csv").select(
    "order_id", F.col("order_total_aed").cast("double").alias("t_total"),
    F.col("items_net_aed").cast("double").alias("t_items"), F.col("channel").alias("t_channel"))
net = items.groupBy("order_id").agg(F.round(F.sum("line_total_aed"), 2).alias("c_items"))
rec = (truth.join(orders.select("order_id", "order_total_aed", "channel"), "order_id", "full_outer")
       .join(net, "order_id", "left"))
log["reconciliation"] = {
    "truth_orders": truth.count(), "curated_orders": orders.count(), "curated_lines": items.count(),
    "missing_in_curated": rec.filter(F.col("order_total_aed").isNull()).count(),
    "extra_in_curated": rec.filter(F.col("t_total").isNull()).count(),
    "total_mismatch": rec.filter(F.abs(F.col("t_total") - F.col("order_total_aed")) > 0.005).count(),
    "items_mismatch": rec.filter(F.abs(F.col("t_items") - F.col("c_items")) > 0.005).count(),
    "channel_mismatch": rec.filter(F.col("t_channel") != F.col("channel")).count(),
    "revenue_truth": round(truth.agg(F.sum("t_total")).first()[0], 2),
    "revenue_curated": round(orders.agg(F.sum("order_total_aed")).first()[0], 2)}

# ---------------- web events: flatten semi-structured props, keep drift columns ----------------
ev = sp.read.json(f"{a.raw}/web_events")
flat = ev.select("event_id", F.to_timestamp("event_ts").alias("event_ts"), "session_id", "anonymous_id", "customer_id",
                 "event_type", "page_path", "product_id", "device", "traffic_source", "utm_campaign",
                 F.col("props.price_aed").alias("price_aed"), F.col("props.qty").alias("qty"),
                 F.col("props.query").alias("search_query"), F.col("props.status").alias("payment_status"),
                 F.col("props.order_id").alias("order_id"), F.col("props.total_aed").alias("purchase_total_aed"),
                 "consent_analytics", "dt")
flat.write.mode("overwrite").partitionBy("dt").parquet(f"{a.out}/curated/web_events")
ev = sp.read.parquet(f"{a.out}/curated/web_events")
log["web_events"] = {"rows": ev.count(), "columns": ev.columns,
                     "consent_null_before_2026_03": ev.filter((F.col("dt") < "2026-03-01") & F.col("consent_analytics").isNotNull()).count() == 0}

# ---------------- consumption marts ----------------
sess = (ev.groupBy("session_id").agg(F.first("traffic_source").alias("traffic_source"), F.first("device").alias("device"),
        F.min("event_ts").alias("start_ts"), F.count("*").alias("events"),
        F.max((F.col("event_type") == "product_view").cast("int")).alias("viewed"),
        F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
        F.max((F.col("event_type") == "begin_checkout").cast("int")).alias("checkout"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased")))
funnel = (sess.groupBy("traffic_source").agg(F.count("*").alias("sessions"), F.sum("viewed").alias("viewed"),
          F.sum("carted").alias("carted"), F.sum("checkout").alias("checkout"), F.sum("purchased").alias("purchased"))
          .withColumn("conversion_pct", F.round(100 * F.col("purchased") / F.col("sessions"), 2)).orderBy(F.desc("sessions")))
funnel.write.mode("overwrite").json(f"{a.out}/consumption/funnel_by_source")
rev_cat = (items.join(orders.select("order_id", "order_date", "status"), "order_id")
           .withColumn("month", F.date_format("order_date", "yyyy-MM"))
           .groupBy("month", "category").agg(F.round(F.sum("line_total_aed"), 2).alias("revenue_aed"),
                                              F.sum("qty").alias("units")).orderBy("month", "category"))
rev_cat.write.mode("overwrite").json(f"{a.out}/consumption/revenue_by_month_category")
log["funnel"] = [r.asDict() for r in funnel.collect()]
log["revenue_by_category"] = {r["category"]: r["rev"] for r in
    items.groupBy("category").agg(F.round(F.sum("line_total_aed"), 2).alias("rev")).orderBy(F.desc("rev")).collect()}

# ---------------- unstructured support text: keyword classifier (EN + AR) vs topic_hint ----------------
sm = sp.read.json(f"{a.raw}/support_messages")
rules = [("defect", r"defect|fault|عيب"), ("cancel", r"cancel|إلغاء"), ("warranty", r"warranty|الضمان"),
         ("address", r"address|عنوان"), ("delivery_eta", r"deliver|توصيل")]
expr = F.lit("other")
for name, rx in reversed(rules):
    expr = F.when(F.lower(F.col("text")).rlike(rx), name).otherwise(expr)
sm = sm.withColumn("topic_pred", expr)
sm.write.mode("overwrite").partitionBy("dt").parquet(f"{a.out}/curated/support_messages")
tot = sm.count()
log["support_text"] = {"messages": tot, "arabic": sm.filter(F.col("language") == "ar").count(),
                       "keyword_classifier_accuracy": round(sm.filter(F.col("topic_pred") == F.col("topic_hint")).count() / tot, 4)}
log["etl_wall_seconds"] = round(time.time() - t0, 1)
print(json.dumps(log, indent=2, default=str))
if a.results:
    json.dump(log, open(a.results, "w"), indent=2, default=str)
sp.stop()
