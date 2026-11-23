#!/usr/bin/env python3
"""Hadoop-Streaming-style mapper. Input: web_events JSON lines. Output: key<TAB>1<TAB>amount
Key = event date | traffic_source | event_type ; amount = purchase total (purchase events only)."""
import sys, json
out = sys.stdout
for line in sys.stdin:
    r = json.loads(line)
    amt = r["props"].get("total_aed", 0.0) if r["event_type"] == "purchase" else 0.0
    out.write("%s|%s|%s\t1\t%s\n" % (r["event_ts"][:10], r["traffic_source"], r["event_type"], amt))
