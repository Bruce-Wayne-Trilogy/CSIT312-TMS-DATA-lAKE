# ISIT312 Project: A Data Lake for The Mattress Store

Group project for ISIT312 Big Data Management (UOW Dubai, Autumn 2026).
A data lake on Hadoop for The Mattress Store (TMS), a UAE retailer, built with
synthetic data anchored on public product, category and price information from
themattressstore.com. Real company data could not be obtained (10 companies were
contacted; one replied that it could not share sensitive data).

## Layout

| Folder | Contents |
|---|---|
| `data_generation/` | `generate_orders.py` (seed 312, 360-product catalog, 10,000 orders), `generate_lake_data.py` (raw feeds: online orders, POS receipts, web events, support messages), `verify_dataset.py` |
| `pipelines/` | HDFS layout, Hive external/ORC tables and OLAP queries, Hadoop Streaming MapReduce (`mapreduce/`), Spark benchmark scripts, run book |
| `spark_scala/` | `etl.scala`: Spark ETL (raw to curated Parquet) with reconciliation, funnel and support-text checks |
| `results/` | Benchmark and ETL result files from our runs |

## Reproduce

```bash
python3 data_generation/generate_orders.py        # seed 312
python3 data_generation/generate_lake_data.py --bounce-ratio 6
python3 data_generation/verify_dataset.py
```

Then follow `pipelines/RUNBOOK_cluster.md` on a Hadoop VM (Hive 2.1.1, Spark 3.0.3, HBase 1.2.6).
Generated data is not committed; the fixed seed regenerates it identically.

## Key results (10,000 orders)

- Reconciliation: AED 40,438,942 revenue, 0 mismatches (Hive and Spark).
- Web-events job, 162,146 events in 365 files: Hadoop Streaming 446 s; Spark local mode about 1.3 s warm, 8.8 s cold. Indicative only (different VM runs, local mode, single Hadoop run).

## Generative AI use

An AI assistant (Claude) helped collect public catalog data, generate the synthetic data, write and debug scripts, and draft report text. See the acknowledgement in the report.

## Group

[Group name] · [Members] · Lecturer: Dr Patrick Mukala
