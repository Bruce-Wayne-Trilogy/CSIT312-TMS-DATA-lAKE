#!/usr/bin/env python3
"""B1: same aggregation job on three engines at three data sizes. python3 -I bench_batch.py LAKE_RAW OUT.json"""
import glob, json, os, statistics, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce"))
from mr_emulator import run_mr
raw, outp = sys.argv[1], sys.argv[2]
parts = sorted(glob.glob(f"{raw}/web_events/dt=*/part-0000.jsonl"))
def nlines(fs): return sum(1 for f in fs for _ in open(f, "rb"))
subsets = {"25pct": parts[: len(parts) // 4], "50pct": parts[: len(parts) // 2], "100pct": parts}
from pyspark.sql import SparkSession, functions as F
t = time.perf_counter()
sp = (SparkSession.builder.master("local[2]").appName("b1").config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
sp.sparkContext.setLogLevel("ERROR"); sp.range(1).count()
startup = time.perf_counter() - t
sc = sp.sparkContext
def spark_rdd(fs):
    def parse(it):
        import json
        for line in it:
            r = json.loads(line)
            amt = r["props"].get("total_aed", 0.0) if r["event_type"] == "purchase" else 0.0
            yield ("%s|%s|%s" % (r["event_ts"][:10], r["traffic_source"], r["event_type"]), (1, amt))
    out = sc.textFile(",".join(fs)).mapPartitions(parse).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).collect()
    return {k: (v[0], v[1]) for k, v in out}
def spark_df(fs):
    df = sp.read.json(fs)
    g = (df.groupBy(F.substring("event_ts", 1, 10).alias("d"), "traffic_source", "event_type")
         .agg(F.count("*").alias("n"), F.sum(F.when(F.col("event_type") == "purchase", F.col("props.total_aed")).otherwise(0.0)).alias("amt")))
    return {"%s|%s|%s" % (r["d"], r["traffic_source"], r["event_type"]): (r["n"], r["amt"]) for r in g.collect()}
def same(a, b): return a.keys() == b.keys() and all(a[k][0] == b[k][0] and abs(a[k][1] - b[k][1]) < 0.01 for k in a)
R = {"spark_startup_s": round(startup, 2), "machine": "2 vCPU, 8 GB, single node (local mode)", "sizes": {}}
for name, fs in subsets.items():
    ev = nlines(fs); row = {"events": ev, "mb": round(sum(os.path.getsize(f) for f in fs) / 1e6, 1)}
    mr_t, rdd_t, df_t = [], [], []
    ref = None
    for rep in range(3):
        tm, res = run_mr(fs, f"/tmp/mrwork_{name}"); mr_t.append(tm); ref = res
        t = time.perf_counter(); r2 = spark_rdd(fs); rdd_t.append(time.perf_counter() - t)
        t = time.perf_counter(); r3 = spark_df(fs); df_t.append(time.perf_counter() - t)
        assert same(ref, r2) and same(ref, r3), "engine outputs differ"
    row["mr_model_total_s"] = [round(x["total_s"], 2) for x in mr_t]
    row["mr_phases_median_s"] = {k: round(statistics.median(x[k] for x in mr_t), 2) for k in ("map_s", "shuffle_sort_s", "reduce_s")}
    row["mr_spill_mb"] = round(mr_t[0]["spill_mb"], 1)
    row["spark_rdd_s"] = [round(x, 2) for x in rdd_t]
    row["spark_df_s"] = [round(x, 2) for x in df_t]
    row["groups"] = len(ref); row["outputs_identical_across_engines"] = True
    R["sizes"][name] = row; print(name, json.dumps(row), flush=True)
json.dump(R, open(outp, "w"), indent=2)
