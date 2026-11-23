#!/usr/bin/env bash
# Data-lake zones on HDFS (NOT executed in the authoring environment: no HDFS cluster was available).
# Run on the course Hadoop VM/cluster.
#   bash 00_hdfs_layout.sh /path/to/data_sample/raw /path/to/data_sample/curated
set -euo pipefail
LOCAL=${1:-./data_sample/raw}      # unpacked raw/ sample (ecom_orders, pos_transactions, web_events, support_messages, reference)
CURATED=${2:-./data_sample/curated} # unpacked curated/ folder (contains product_catalog.csv)
hdfs dfs -mkdir -p /datalake/raw /datalake/curated /datalake/consumption /datalake/reference
for src in ecom_orders pos_transactions web_events support_messages; do
  hdfs dfs -mkdir -p /datalake/raw/$src
  hdfs dfs -put -f "$LOCAL/$src/"* /datalake/raw/$src/      # keeps dt=YYYY-MM-DD partition directories
done
# one folder per reference table (Hive cannot read a table folder that contains sub-folders)
hdfs dfs -mkdir -p /datalake/reference/pos_item_map && hdfs dfs -put -f "$LOCAL/reference/pos_item_map.csv" /datalake/reference/pos_item_map/
hdfs dfs -mkdir -p /datalake/reference/product_catalog && hdfs dfs -put -f "$CURATED/product_catalog.csv" /datalake/reference/product_catalog/
hdfs dfs -du -h /datalake/raw
