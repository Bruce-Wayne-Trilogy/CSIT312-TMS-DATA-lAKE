#!/usr/bin/env python3
"""
Local emulation of the MapReduce execution model (Hadoop Streaming contract):
  map tasks (parallel) -> map output spilled to DISK -> external sort by key (the shuffle) -> reduce -> output file.
NOT Hadoop: no cluster, no YARN/JVM start-up, no HDFS. It reproduces the programming-model costs only
(full input scan per job, disk spill, sort-based shuffle). Real Hadoop adds fixed job-launch overhead on top.
"""
import os, shutil, subprocess, time
from multiprocessing.pool import ThreadPool

HERE = os.path.dirname(os.path.abspath(__file__))

def run_mr(files, workdir, n_splits=8, procs=2, mapper="mapper.py", reducer="reducer.py"):
    shutil.rmtree(workdir, ignore_errors=True); os.makedirs(workdir + "/map")
    splits = [s for s in (files[i::n_splits] for i in range(n_splits)) if s]
    t0 = time.perf_counter()
    def map_task(a):
        i, fs = a
        subprocess.run("cat %s | python3 %s/%s > %s/map/part-%d" % (" ".join(fs), HERE, mapper, workdir, i), shell=True, check=True)
    with ThreadPool(procs) as p:
        p.map(map_task, list(enumerate(splits)))
    t1 = time.perf_counter()
    subprocess.run("LC_ALL=C sort -S 1G -T %s -t \"$(printf '\\t')\" -k1,1 -o %s/shuffled %s/map/part-*" % (workdir, workdir, workdir), shell=True, check=True)
    t2 = time.perf_counter()
    subprocess.run("python3 %s/%s < %s/shuffled > %s/out.tsv" % (HERE, reducer, workdir, workdir), shell=True, check=True)
    t3 = time.perf_counter()
    res = {}
    for line in open(workdir + "/out.tsv"):
        k, n, a = line.rstrip("\n").split("\t"); res[k] = (int(n), float(a))
    return {"map_s": t1 - t0, "shuffle_sort_s": t2 - t1, "reduce_s": t3 - t2, "total_s": t3 - t0,
            "spill_mb": sum(os.path.getsize(f"{workdir}/map/{f}") for f in os.listdir(workdir + "/map")) / 1e6}, res
