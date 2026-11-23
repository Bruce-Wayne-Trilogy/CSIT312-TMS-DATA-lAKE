#!/usr/bin/env bash
# One command that runs everything that was written but never executed: HDFS zones, Hive tables + OLAP, real Hadoop Streaming.
# Run it from the code/pipelines folder on the course Hadoop VM:
#     bash run_all_on_vm.sh [RAW_DIR] [CURATED_DIR]        (defaults: ~/data_sample/raw  ~/data_sample/curated)
#     RUN_HBASE=1 bash run_all_on_vm.sh                     (also runs the small HBase demo - only if HBase is running)
# It does NOT stop when a step fails; it records the error and moves on, so send the log even if something broke.
# Output (in ./results_for_report/):  1_full_log.txt  (everything printed)   2_summary.txt  (key numbers)

cd "$(dirname "$0")" || exit 1
OUT="$PWD/results_for_report"; mkdir -p "$OUT"
LOG="$OUT/1_full_log.txt"; SUM="$OUT/2_summary.txt"; : > "$LOG"; : > "$SUM"
exec > >(tee -a "$LOG") 2>&1
RAW="${1:-$HOME/data_sample/raw}"; CUR="${2:-$HOME/data_sample/curated}"

say()  { echo; echo "=================== $* ==================="; }
note() { echo "$*" | tee -a "$SUM"; }

for p in "$RAW/web_events" "$CUR/product_catalog.csv"; do
  [ -e "$p" ] || { echo "MISSING: $p"; echo "Pass the folders where you unzipped the data: bash run_all_on_vm.sh RAW_DIR CURATED_DIR"; exit 1; }
done
if [ -z "${HADOOP_HOME:-}" ] && command -v hadoop >/dev/null; then
  export HADOOP_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v hadoop)")")")"
fi

# Prefer HiveServer2 through beeline (what the labs use); fall back to the hive CLI.
if (echo > /dev/tcp/localhost/10000) 2>/dev/null && command -v beeline >/dev/null; then HIVE_MODE=beeline
elif command -v hive >/dev/null; then HIVE_MODE=hive
else HIVE_MODE=none; fi
BL=(beeline -u jdbc:hive2://localhost:10000 -n "$(whoami)" -p '')
hive_run() { case $HIVE_MODE in beeline) "${BL[@]}" "$@";; hive) hive "$@";; *) echo "NO HIVE OR BEELINE FOUND"; return 1;; esac; }
hive_quiet() { case $HIVE_MODE in beeline) "${BL[@]}" --silent=true --outputformat=csv2 "$@";; hive) hive -S "$@";; *) echo "NO HIVE OR BEELINE FOUND"; return 1;; esac; }

say "STEP 0: environment"
{
  date; echo "user: $(whoami)   hive access via: $HIVE_MODE"
  echo "HADOOP_HOME=${HADOOP_HOME:-unset}"; uname -a
  hadoop version 2>&1 | head -1
  hive --version 2>/dev/null | grep -i '^hive' | head -1
  python3 --version 2>&1
  echo "cpus: $(nproc)"; free -m | head -2
  echo "running Java daemons:"; jps
} 2>&1 | tee -a "$SUM"

say "STEP 1: HDFS zones"
bash 00_hdfs_layout.sh "$RAW" "$CUR" || echo "!!! STEP 1 FAILED (see message above)"
{
  echo "--- HDFS layout"; hdfs dfs -ls -R /datalake | head -40
  echo "--- sizes"; hdfs dfs -du -h /datalake/raw
  echo "--- one raw showroom (POS) record"; hdfs dfs -cat '/datalake/raw/pos_transactions/dt=2025-12-23/part-0000.jsonl' | head -1
  echo "--- one raw online order"; hdfs dfs -cat '/datalake/raw/ecom_orders/dt=2025-11-0*/part-0000.jsonl' | head -1
} 2>&1 | tee -a "$SUM"

say "STEP 2a: Hive raw tables (01_hive_raw.sql)"
hive_run -f 01_hive_raw.sql || echo "!!! STEP 2a FAILED (see message above)"
say "STEP 2b: Hive curated tables + OLAP queries (02_hive_curated_olap.sql)"
hive_run -f 02_hive_curated_olap.sql || echo "!!! STEP 2b FAILED (see message above)"

cat > "$OUT/.checks.sql" <<'SQL'
USE tms_lake;
SELECT 'raw_web_events (expect 162146)', COUNT(*) FROM raw_web_events;
SELECT 'raw_ecom_orders (expect 5761)', COUNT(*) FROM raw_ecom_orders;
SELECT 'raw_pos_transactions (expect 4239)', COUNT(*) FROM raw_pos_transactions;
SELECT 'raw_support_messages (expect 943)', COUNT(*) FROM raw_support_messages;
SELECT 'curated_orders (expect 10000 and 40438942.00)', COUNT(*), SUM(order_total_aed) FROM curated_orders;
SELECT 'curated_order_items (expect 14683)', COUNT(*) FROM curated_order_items;
SQL
say "STEP 2c: Hive row-count checks"
note "--- Hive checks"
hive_quiet -f "$OUT/.checks.sql" 2>&1 | tee -a "$SUM" || echo "!!! STEP 2c FAILED"

say "STEP 3: Hadoop Streaming (real MapReduce), two runs"
for i in 1 2; do
  bash run_mapreduce_streaming.sh > "$OUT/.mr_$i.tmp" 2>&1
  cat "$OUT/.mr_$i.tmp"
  note "MapReduce run $i: $(grep -E '^real' "$OUT/.mr_$i.tmp" | head -1)   $(grep -iE 'Job job_.* completed successfully' "$OUT/.mr_$i.tmp" | head -1)"
done
note "MapReduce total events counted: $(hdfs dfs -cat '/datalake/consumption/mr_event_counts/part-*' 2>/dev/null | awk -F'\t' '{n+=$2} END{print n+0}')   (expect 162146)"
note "MapReduce output groups: $(hdfs dfs -cat '/datalake/consumption/mr_event_counts/part-*' 2>/dev/null | wc -l)"

if [ "${RUN_HBASE:-0}" = "1" ]; then
  say "STEP 4: HBase demo"
  printf "%s\n" "create 'customer_profile','p','s'" \
    "put 'customer_profile','SYN-CUST-000001','p:emirate','Dubai'" \
    "put 'customer_profile','SYN-CUST-000001','s:last_event','purchase'" \
    "get 'customer_profile','SYN-CUST-000001'" "exit" | hbase shell 2>&1 | tee -a "$SUM"
fi

rm -f "$OUT"/.mr_*.tmp "$OUT"/.checks.sql
say "DONE"
echo "Send back: $LOG  and  $SUM  (plus the screenshots zip)"
sleep 1
