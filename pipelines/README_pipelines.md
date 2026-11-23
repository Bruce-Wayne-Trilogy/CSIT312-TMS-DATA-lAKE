# Pipelines package (Stage 2) - what was run and what was NOT

| File | Purpose | Executed in authoring environment? |
|---|---|---|
| `etl_raw_to_curated.py` | PySpark: unify online-platform JSON + POS JSON into curated orders/items, flatten web events (schema drift), classify support text, build marts, reconcile with ground truth | **Yes** (local[2], 10k sample; reconciliation 0 mismatches) |
| `bench_batch.py`, `bench_spark_schema.py` | Same aggregation on MapReduce-model pipeline vs Spark RDD vs Spark DataFrame, 3 sizes | **Yes** |
| `bench_stream.py` | Spark micro-batch streaming latency + batch-recompute growth curve | **Yes** |
| `mapreduce/mapper.py, reducer.py, mr_emulator.py` | Hadoop-Streaming-contract mapper/reducer + local emulation of map/spill/sort/reduce | **Yes** (emulation only - not Hadoop) |
| `00_hdfs_layout.sh`, `run_mapreduce_streaming.sh` | HDFS zones; real Hadoop Streaming run | **No** - no HDFS/YARN here; run on the course cluster |
| `01_hive_raw.sql`, `02_hive_curated_olap.sql` | Hive external raw tables (JsonSerDe), ORC curated tables, ROLLUP/CUBE OLAP | **No** - no Hive here; syntax unverified, test and fix on the course Hive |

HBase (not built): suggested use is low-latency lookups the batch tools are poor at - table `customer_profile`
(row key `customer_id`, families `p` profile and `s` last-session stats) and `order_status` (row key = order_id).
Not benchmarked; claims about HBase vs Hive point-lookup latency would need a measurement on the course cluster.
