#!/usr/bin/env python3
"""
B2: (A) Spark micro-batch streaming latency at several event rates, (B) latency of re-running a batch job over
the accumulated data as it grows (MapReduce-model emulation and Spark batch).
python3 -I bench_stream.py LAKE_RAW OUT.json     (SYNTHETIC data; single 2-vCPU node; replayer shares the CPUs)
"""
import collections, glob, json, os, shutil, statistics, sys, threading, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce"))
from mr_emulator import run_mr
from pyspark.sql import SparkSession, functions as F, types as T
raw, outp = sys.argv[1], sys.argv[2]
parts = sorted(glob.glob(f"{raw}/web_events/dt=*/part-0000.jsonl"))
def first_n(n):
    out = []
    for f in parts:
        for line in open(f, encoding="utf-8"):
            out.append(line)
            if len(out) >= n: return out
    return out
sp = (SparkSession.builder.master("local[2]").appName("b2").config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
sp.sparkContext.setLogLevel("ERROR")
schema = T.StructType([T.StructField("event_ts", T.StringType()), T.StructField("event_type", T.StringType()),
                       T.StructField("traffic_source", T.StringType()),
                       T.StructField("props", T.StructType([T.StructField("total_aed", T.DoubleType())])),
                       T.StructField("ingest_ms", T.LongType())])

def truth_counts(lines):
    c = collections.defaultdict(lambda: [0, 0.0])
    for l in lines:
        r = json.loads(l); k = (r["traffic_source"], r["event_type"])
        c[k][0] += 1
        if r["event_type"] == "purchase": c[k][1] += r["props"]["total_aed"]
    return {k: (v[0], round(v[1], 2)) for k, v in c.items()}

def stream_test(rate, per_file, total, trigger_s=2):
    d = f"/tmp/stream_in_{rate}"; shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    lines = first_n(total); n_files = len(lines) // per_file; total = n_files * per_file; lines = lines[:total]
    state = collections.defaultdict(lambda: [0, 0.0]); recs = []
    def fn(df, bid):
        a = (df.groupBy("traffic_source", "event_type")
             .agg(F.count("*").alias("n"), F.sum(F.when(F.col("event_type") == "purchase", F.col("props.total_aed")).otherwise(0.0)).alias("amt"),
                  F.min("ingest_ms").alias("lo"), F.max("ingest_ms").alias("hi")).collect())
        if not a: return
        for r in a:
            s = state[(r["traffic_source"], r["event_type"])]; s[0] += r["n"]; s[1] += r["amt"]
        now = time.time()
        recs.append({"batch": bid, "events": sum(r["n"] for r in a), "lat_oldest_s": now - min(r["lo"] for r in a) / 1000,
                     "lat_newest_s": now - max(r["hi"] for r in a) / 1000})
    q = (sp.readStream.schema(schema).option("maxFilesPerTrigger", 2000).json(d).writeStream
         .foreachBatch(fn).trigger(processingTime=f"{trigger_s} seconds").start())
    interval = per_file / rate; t_begin = time.time()
    def replay():
        for i in range(n_files):
            target = t_begin + i * interval
            time.sleep(max(0, target - time.time()))
            ms = int(time.time() * 1000)
            body = "".join(l.rstrip()[:-1] + ',"ingest_ms":%d}\n' % ms for l in lines[i * per_file:(i + 1) * per_file])
            tmp = f"{d}/.tmp_{i}"; open(tmp, "w", encoding="utf-8").write(body); os.rename(tmp, f"{d}/f_{i:05d}.jsonl")
    th = threading.Thread(target=replay); th.start(); th.join(); t_fed = time.time()
    deadline = time.time() + 120
    while sum(r["events"] for r in recs) < total and time.time() < deadline: time.sleep(0.25)
    t_done = time.time(); q.stop()
    done = sum(r["events"] for r in recs)
    ev_w = lambda key: sorted((r[key], r["events"]) for r in recs)
    def wq(key, p):
        xs = ev_w(key); tot = sum(w for _, w in xs); acc = 0
        for v, w in xs:
            acc += w
            if acc >= p * tot: return v
    got = {k: (v[0], round(v[1], 2)) for k, v in state.items()}
    return {"target_rate_events_per_s": rate, "events_sent": total, "events_processed": done,
            "feed_seconds": round(t_fed - t_begin, 1), "drain_seconds_after_feed": round(t_done - t_fed, 1),
            "batches": len(recs), "trigger_s": trigger_s,
            "latency_oldest_event_in_batch_s": {"p50": round(wq("lat_oldest_s", .5), 2), "p95": round(wq("lat_oldest_s", .95), 2), "max": round(max(r["lat_oldest_s"] for r in recs), 2)},
            "latency_newest_event_in_batch_s": {"p50": round(wq("lat_newest_s", .5), 2), "p95": round(wq("lat_newest_s", .95), 2)},
            "result_equals_batch_truth": got == truth_counts(lines)}

R = {"machine": "2 vCPU, 8 GB, single node; Spark local[2]; replayer thread shares CPUs", "streaming": [], "recompute_curve": []}
for rate, per_file, total in [(4000, 2000, 200000), (20000, 5000, 200000), (50000, 10000, 300000)]:
    r = stream_test(rate, per_file, total); R["streaming"].append(r); print(json.dumps(r), flush=True)

for n in [50000, 100000, 200000, 400000, 800000, 1600000]:
    lines = first_n(n)
    if len(lines) < n: break
    d = f"/tmp/cum_{n}"; shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    files = []
    for i in range(8):
        p = f"{d}/part-{i}.jsonl"; open(p, "w", encoding="utf-8").writelines(lines[i::8]); files.append(p)
    mr = [run_mr(files, f"/tmp/mrcum_{n}")[0]["total_s"] for _ in range(2)]
    def spark_batch():
        df = sp.read.schema(schema).json(files)
        return df.groupBy("traffic_source", "event_type").agg(F.count("*"), F.sum("props.total_aed")).collect()
    sb = []
    for _ in range(3):
        t = time.perf_counter(); spark_batch(); sb.append(time.perf_counter() - t)
    row = {"events_accumulated": n, "mr_model_job_s": [round(x, 2) for x in mr], "spark_batch_job_s": [round(x, 2) for x in sb]}
    R["recompute_curve"].append(row); print(json.dumps(row), flush=True)
json.dump(R, open(outp, "w"), indent=2)
