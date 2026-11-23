#!/usr/bin/env python3
"""Hadoop-Streaming-style reducer: input sorted by key; sums count and amount per key."""
import sys
cur, n, amt = None, 0, 0.0
for line in sys.stdin:
    k, c, a = line.rstrip("\n").split("\t")
    if k != cur:
        if cur is not None:
            print("%s\t%d\t%.2f" % (cur, n, amt))
        cur, n, amt = k, 0, 0.0
    n += int(c); amt += float(a)
if cur is not None:
    print("%s\t%d\t%.2f" % (cur, n, amt))
