import org.apache.spark.sql.functions._
import spark.implicits._
val RAW = "/datalake/raw"
val REF = "/datalake/reference"
val OUT = "/datalake"
val t0 = System.nanoTime
spark.conf.set("spark.sql.shuffle.partitions", "8")

val ecom = spark.read.json(RAW + "/ecom_orders")
val pos  = spark.read.json(RAW + "/pos_transactions")
val pmap = spark.read.option("header", "true").csv(REF + "/pos_item_map/pos_item_map.csv")
val cat  = spark.read.option("header", "true").csv(REF + "/product_catalog/product_catalog.csv").select("product_id", "product_name", "category", "subcategory", "brand", "size_cm")

val eItems = ecom.select($"order_id", posexplode($"line_items").as(Seq("pos", "li"))).select($"order_id", ($"pos" + 1).as("line_no"), $"li.product_id".as("product_id"), $"li.qty".as("qty"), $"li.unit_list_price_aed".as("unit_list_price_aed"), $"li.unit_price_aed".as("unit_price_aed"))
val pItems = pos.select($"oms_order_ref".as("order_id"), posexplode($"lines").as(Seq("pos", "li"))).select($"order_id", ($"pos" + 1).as("line_no"), $"li.sku_code".as("sku_code"), $"li.qty".as("qty"), $"li.unit_price_aed".as("unit_price_aed"), $"li.discount_aed".as("discount_aed")).join(pmap, "sku_code").select($"order_id", $"line_no", $"product_id", $"qty", round($"unit_price_aed" + $"discount_aed", 2).as("unit_list_price_aed"), $"unit_price_aed")
val items = eItems.unionByName(pItems).join(cat, Seq("product_id"), "left").withColumn("line_total_aed", round($"qty" * $"unit_price_aed", 2))

val oE = ecom.select($"order_id", to_timestamp($"created_at").as("order_ts"), lit("online").as("channel"), $"customer.id".as("customer_id"), $"payment.method".as("payment_method"), $"shipping.emirate".as("delivery_emirate"), $"shipping.option".as("delivery_option"), $"totals.order_total_aed".as("order_total_aed"), $"status")
val oP = pos.select($"oms_order_ref".as("order_id"), to_timestamp($"sold_at").as("order_ts"), lit("showroom").as("channel"), lit(null).cast("string").as("customer_id"), $"tender.type".as("payment_method"), $"delivery_emirate", lit(null).cast("string").as("delivery_option"), $"total_aed".as("order_total_aed"), $"status")
val orders0 = oE.unionByName(oP).withColumn("order_date", to_date($"order_ts"))
orders0.write.mode("overwrite").partitionBy("order_date").parquet(OUT + "/curated/orders")
items.write.mode("overwrite").parquet(OUT + "/curated/order_items")
val orders = spark.read.parquet(OUT + "/curated/orders")
val items2 = spark.read.parquet(OUT + "/curated/order_items")

val truth = spark.read.option("header", "true").csv(REF + "/truth_orders/orders.csv").select($"order_id", $"order_total_aed".cast("double").as("t_total"), $"items_net_aed".cast("double").as("t_items"), $"channel".as("t_channel"))
val net = items2.groupBy("order_id").agg(round(sum("line_total_aed"), 2).as("c_items"))
val rec = truth.join(orders.select("order_id", "order_total_aed", "channel"), Seq("order_id"), "full_outer").join(net, Seq("order_id"), "left").cache()
println("=== RECONCILIATION (curated vs ground truth) ===")
println("truth_orders      = " + truth.count())
println("curated_orders    = " + orders.count())
println("curated_lines     = " + items2.count())
println("missing_in_curated= " + rec.filter($"order_total_aed".isNull).count())
println("extra_in_curated  = " + rec.filter($"t_total".isNull).count())
println("total_mismatch    = " + rec.filter(abs($"t_total" - $"order_total_aed") > 0.005).count())
println("items_mismatch    = " + rec.filter(abs($"t_items" - $"c_items") > 0.005).count())
println("channel_mismatch  = " + rec.filter($"t_channel" =!= $"channel").count())
println("revenue_truth     = " + truth.agg(sum("t_total")).first().getDouble(0))
println("revenue_curated   = " + orders.agg(sum("order_total_aed")).first().getDouble(0))

val ev = spark.read.json(RAW + "/web_events")
val flat = ev.select($"event_id", to_timestamp($"event_ts").as("event_ts"), $"session_id", $"anonymous_id", $"customer_id", $"event_type", $"page_path", $"product_id", $"device", $"traffic_source", $"utm_campaign", $"props.price_aed".as("price_aed"), $"props.qty".as("qty"), $"props.query".as("search_query"), $"props.status".as("payment_status"), $"props.order_id".as("order_id"), $"props.total_aed".as("purchase_total_aed"), $"consent_analytics", lit(null).cast("string").as("dt0"))
val evc = ev.withColumn("dt", substring($"event_ts", 1, 10))
evc.select($"event_id", to_timestamp($"event_ts").as("event_ts"), $"session_id", $"traffic_source", $"event_type", $"props.total_aed".as("purchase_total_aed"), $"dt").write.mode("overwrite").partitionBy("dt").parquet(OUT + "/curated/web_events")
val evr = spark.read.parquet(OUT + "/curated/web_events")
println("web_events rows = " + evr.count())

val sess = evr.groupBy("session_id").agg(first("traffic_source").as("traffic_source"), max(($"event_type" === "product_view").cast("int")).as("viewed"), max(($"event_type" === "add_to_cart").cast("int")).as("carted"), max(($"event_type" === "begin_checkout").cast("int")).as("checkout"), max(($"event_type" === "purchase").cast("int")).as("purchased"))
val funnel = sess.groupBy("traffic_source").agg(count("*").as("sessions"), sum("viewed").as("viewed"), sum("carted").as("carted"), sum("checkout").as("checkout"), sum("purchased").as("purchased")).withColumn("conversion_pct", round(lit(100) * $"purchased" / $"sessions", 2)).orderBy(desc("sessions"))
funnel.write.mode("overwrite").json(OUT + "/consumption/funnel_by_source")
val revcat = items2.groupBy("category").agg(round(sum("line_total_aed"), 2).as("revenue_aed")).orderBy(desc("revenue_aed"))
revcat.write.mode("overwrite").json(OUT + "/consumption/revenue_by_category")
println("=== FUNNEL BY TRAFFIC SOURCE ===")
funnel.show(false)
println("=== ITEM REVENUE BY CATEGORY ===")
revcat.show(false)

val sm = spark.read.json(RAW + "/support_messages")
val rules = Seq(("defect", "defect|fault|عيب"), ("cancel", "cancel|إلغاء"), ("warranty", "warranty|الضمان"), ("address", "address|عنوان"), ("delivery_eta", "deliver|توصيل"))
var ex = lit("other")
for ((n, rx) <- rules.reverse) { ex = when(lower($"text").rlike(rx), n).otherwise(ex) }
val smp = sm.withColumn("topic_pred", ex)
val tot = smp.count()
println("support messages = " + tot + ", arabic = " + smp.filter($"language" === "ar").count() + ", keyword accuracy = " + smp.filter($"topic_pred" === $"topic_hint").count().toDouble / tot)
println("ETL wall seconds = " + (System.nanoTime - t0) / 1e9)
