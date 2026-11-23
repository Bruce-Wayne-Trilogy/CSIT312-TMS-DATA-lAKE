#!/usr/bin/env python3
"""Independent integrity checks on the synthetic dataset (reads CSVs only)."""
import csv, sys, statistics, collections
from datetime import date

d = sys.argv[1]
def load(n):
    with open(f"{d}/{n}", newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        hdr = next(rd)
        rows = []
        for i, r in enumerate(rd, 2):
            assert len(r) == len(hdr), (n, i, len(r), len(hdr))
            rows.append(dict(zip(hdr, r)))
    return hdr, rows

oh, O = load("orders.csv"); ih, I = load("order_items.csv"); ch, C = load("product_catalog.csv")
fails = []
def chk(c, msg):
    if not c: fails.append(msg)

chk(len(O) == len({o["order_id"] for o in O}), "duplicate order_id")
chk(len(C) == len({c["product_id"] for c in C}), "duplicate product_id")
cat = {c["product_id"]: c for c in C}
oid = {o["order_id"]: o for o in O}
by_order = collections.defaultdict(list)
for it in I:
    by_order[it["order_id"]].append(it)
    chk(it["order_id"] in oid, f"orphan item {it['order_id']}")
    chk(it["product_id"] in cat, f"unknown product {it['product_id']}")
    c = cat[it["product_id"]]
    q = int(it["qty"]); up = float(it["unit_price_aed"]); lp = float(it["unit_list_price_aed"])
    chk(q >= 1, "qty<1")
    chk(abs(float(it["line_total_aed"]) - q * up) < 0.005, f"line_total {it['order_id']}")
    chk(up <= lp + 1e-9, "unit_price > list")
    chk(lp == float(c["regular_price_aed"]), "list price != catalog regular")
    chk(up in (float(c["regular_price_aed"]), float(c["current_price_aed"])), "unit price not a catalog price")
    chk(it["data_origin"] == "SYNTHETIC", "item origin")
    chk((it["markdown_applied"] == "true") == (up < lp), "markdown flag mismatch")

start, end = date(2025, 11, 1), date(2026, 10, 31)
for o in O:
    its = by_order.get(o["order_id"], [])
    chk(len(its) == int(o["line_count"]) and len(its) >= 1, f"line_count {o['order_id']}")
    chk(sum(int(i["qty"]) for i in its) == int(o["units"]), f"units {o['order_id']}")
    net = sum(float(i["line_total_aed"]) for i in its)
    chk(abs(net - float(o["items_net_aed"])) < 0.005, f"items_net {o['order_id']}")
    fee = float(o["delivery_fee_aed"]); tot = float(o["order_total_aed"])
    chk(abs(net + fee - tot) < 0.005, f"total {o['order_id']}")
    chk(abs(round(tot * 5 / 105, 2) - float(o["vat_aed"])) < 0.0051, f"vat {o['order_id']}")
    chk(abs(float(o["list_subtotal_aed"]) - float(o["markdown_aed"]) - net) < 0.005, f"list-markdown {o['order_id']}")
    chk(o["data_origin"] == "SYNTHETIC", "order origin")
    dt = date.fromisoformat(o["order_date"])
    chk(start <= dt <= end, "date range")
    chk(o["order_ts"].startswith(o["order_date"]), "ts/date")
    hr = int(o["order_ts"][11:13])
    if o["channel"] == "showroom":
        chk(10 <= hr <= 21, "showroom hour"); chk(o["showroom_region"] != "", "showroom region missing")
    else:
        chk(o["showroom_region"] == "", "online with showroom region"); chk(o["payment_method"] != "cash", "online cash")
    opt, em = o["delivery_option"], o["delivery_emirate"]
    if opt == "standard": chk(fee == 0, "std fee")
    if opt == "same_day":
        chk(em == "Dubai" and o["channel"] == "online", "same_day scope"); chk(fee == (50 if net < 300 else 0), "same_day fee")
    if opt == "express_next_day":
        chk(em in ("Dubai", "Sharjah", "Ajman", "Ras Al Khaimah", "Abu Dhabi"), "express scope")
        chk(fee == (599 if em == "Abu Dhabi" else 299), "express fee")
    if o["payment_method"] in ("tabby", "tamara"): chk(tot >= 150, "BNPL on tiny basket")
    chk((o["promo_flag"] == "true") == any(i["markdown_applied"] == "true" for i in its), "promo flag")
chk(not set(o["order_status"] for o in O) - {"delivered", "cancelled", "defect_return_requested"}, "status values")

print("rows: orders=%d items=%d catalog=%d" % (len(O), len(I), len(C)))
print("FAILURES:", len(fails)); [print(" -", f) for f in fails[:15]]

tot = [float(o["order_total_aed"]) for o in O]
print("AOV mean %.0f | median %.0f | units/order %.2f | lines/order %.2f" % (
    statistics.mean(tot), statistics.median(tot), statistics.mean(int(o["units"]) for o in O), statistics.mean(int(o["line_count"]) for o in O)))
print("share of orders < AED 500: %.1f%% | > AED 10,000: %.1f%% | > AED 50,000: %.2f%%" % (
    100 * sum(t < 500 for t in tot) / len(tot), 100 * sum(t > 10000 for t in tot) / len(tot), 100 * sum(t > 50000 for t in tot) / len(tot)))
rev = collections.Counter(); cnt = collections.Counter()
for it in I:
    rev[it["category"]] += float(it["line_total_aed"]); cnt[it["category"]] += 1
R = sum(rev.values())
print("category: line share / revenue share")
for k, v in rev.most_common(): print("  %-12s %5.1f%% / %5.1f%%" % (k, 100 * cnt[k] / len(I), 100 * v / R))
for k in ("channel", "delivery_option", "payment_method", "order_status", "delivery_emirate", "promo_flag"):
    c = collections.Counter(o[k] for o in O); print(k, {a: round(100 * b / len(O), 1) for a, b in c.most_common()})
m = collections.Counter(o["order_date"][:7] for o in O); print("monthly orders:", dict(sorted(m.items())))
print("repeat-customer rate: %.1f%%" % (100 * (1 - len({o["customer_id"] for o in O}) / len(O))))
ct = [float(c["current_price_aed"]) for c in C]
print("catalog current price min/median/max: %.0f / %.0f / %.0f" % (min(ct), statistics.median(ct), max(ct)))
nm = [float(o["order_total_aed"]) for o in O if o["basket_archetype"] == "value_mat"]
print("value-mattress baskets: n=%d mean %.0f median %.0f" % (len(nm), statistics.mean(nm), statistics.median(nm)))
sys.exit(1 if fails else 0)
