# Runbook: run the written-but-unexecuted parts on the course Hadoop VM

FAST PATH (recommended): see ../FRIEND_INSTRUCTIONS.txt. `bash run_all_on_vm.sh` does steps 0 to 3 below in one go and writes the log and summary; `bash screenshot_helper.sh` then shows each screen for the screenshots.

Manual path: unzip both packages on the VM, then `cd code/pipelines`. Take a screenshot after each step (terminal showing the command and its output).

## 0. Check the VM (send me these lines)
    hadoop version | head -1; hive --version 2>/dev/null | head -1; python3 --version; nproc; free -m | head -2; jps

## 1. HDFS zones
    bash 00_hdfs_layout.sh ~/data_sample/raw ~/data_sample/curated
    hdfs dfs -ls -R /datalake | head -40
    hdfs dfs -cat /datalake/raw/pos_transactions/dt=2025-12-23/part-0000.jsonl | head -1
    hdfs dfs -ls /datalake/raw/ecom_orders | head -3        # pick any dt folder, then -cat its part-0000.jsonl | head -1

## 2. Hive
    hive -f 01_hive_raw.sql
    hive -f 02_hive_curated_olap.sql
    hive -e "USE tms_lake; SELECT COUNT(*), SUM(order_total_aed) FROM curated_orders; SELECT COUNT(*) FROM curated_order_items;"
Expected: 10000 orders, 40438942.00, 14683 lines. Any other number or any error: send it to me unedited.

## 3. Hadoop Streaming (real MapReduce)
    bash run_mapreduce_streaming.sh        # note the 'real' time printed by `time`
    bash run_mapreduce_streaming.sh        # run it a second time and note that 'real' time too

## 4. HBase demo (optional, only if HBase is installed)
    hbase shell
    create 'customer_profile','p','s'
    put 'customer_profile','SYN-CUST-000001','p:emirate','Dubai'
    put 'customer_profile','SYN-CUST-000001','s:last_event','purchase'
    get 'customer_profile','SYN-CUST-000001'
This only demonstrates the table design on one row; it does not load the dataset.
