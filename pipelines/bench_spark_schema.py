import glob, json, statistics, sys, time
from pyspark.sql import SparkSession, functions as F, types as T
raw = sys.argv[1]
fs = sorted(glob.glob(f"{raw}/web_events/dt=*/part-0000.jsonl"))
sp = SparkSession.builder.master("local[2]").config("spark.driver.memory", "4g").config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate()
sp.sparkContext.setLogLevel("ERROR")
schema = T.StructType([T.StructField("event_ts", T.StringType()), T.StructField("event_type", T.StringType()),
                       T.StructField("traffic_source", T.StringType()),
                       T.StructField("props", T.StructType([T.StructField("total_aed", T.DoubleType())]))])
ts = []
for _ in range(4):
    t = time.perf_counter()
    (sp.read.schema(schema).json(fs).groupBy(F.substring("event_ts", 1, 10), "traffic_source", "event_type")
     .agg(F.count("*"), F.sum(F.when(F.col("event_type") == "purchase", F.col("props.total_aed")).otherwise(0.0))).collect())
    ts.append(round(time.perf_counter() - t, 2))
print("RESULT", json.dumps({"spark_df_explicit_schema_100pct_s": ts}))
json.dump({"spark_df_explicit_schema_100pct_s": ts}, open("results/bench_spark_explicit_schema.json", "w"))
