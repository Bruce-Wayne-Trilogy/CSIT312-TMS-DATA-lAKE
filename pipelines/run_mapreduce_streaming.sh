#!/usr/bin/env bash
# Hadoop Streaming run of the SAME mapper/reducer used in the local emulation (NOT executed here; run on the cluster).
# Output: event counts and purchase revenue per (date | traffic_source | event_type).
set -euo pipefail
# Run from the folder that contains mapreduce/ (the code/pipelines folder). Needs HADOOP_HOME set and python3 on the VM.
# The dt=* glob is needed because the input folder holds dt=YYYY-MM-DD sub-folders, which Hadoop does not recurse into by default.
if [ -z "${HADOOP_HOME:-}" ]; then export HADOOP_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v hadoop)")")")"; fi
JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
hdfs dfs -rm -r -f /datalake/consumption/mr_event_counts
time hadoop jar "$JAR" \
  -files mapreduce/mapper.py,mapreduce/reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input "/datalake/raw/web_events/dt=*" -output /datalake/consumption/mr_event_counts
hdfs dfs -cat /datalake/consumption/mr_event_counts/part-* | head
# Record wall-clock time for the report: this includes YARN job launch overhead the local emulation cannot measure.
