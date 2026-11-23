#!/usr/bin/env python3
"""
SYNTHETIC multi-source "raw zone" generator -- ISIT312 project, Stage 2.

Reads the curated CSVs produced by generate_orders.py (orders / order_items /
product_catalog) and derives the RAW source feeds a retailer like TMS would land
in a data lake. Everything is SYNTHETIC (data_origin = "SYNTHETIC").

  ecom_orders/dt=YYYY-MM-DD/part-0000.jsonl     online-platform orders, nested JSON
  pos_transactions/dt=YYYY-MM-DD/part-0000.jsonl showroom POS receipts, nested JSON, own SKU codes
  web_events/dt=YYYY-MM-DD/part-0000.jsonl      clickstream, semi-structured (schema drift from 2026-03-01)
  support_messages/dt=YYYY-MM-DD/part-0000.jsonl unstructured customer messages (EN/AR)
  reference/pos_item_map.csv                     POS sku_code -> product_id

Usage: python3 -I generate_lake_data.py --curated DIR --out RAWDIR [--bounce-ratio 6] [--seed 312]

bounce-ratio = non-converting sessions per online order. Real e-commerce conversion
(~1-3%) would imply 30-100; the default 6 keeps volumes manageable (assumption).
"""
import argparse, csv, json, os, random, re
from collections import defaultdict
from datetime import datetime, timedelta

STORE = {"Dubai": "DXB", "Abu Dhabi": "AUH", "Al Ain": "AAN", "Sharjah": "SHJ", "Umm Al Quwain": "UAQ"}
DEVICES = {"mobile": .64, "desktop": .30, "tablet": .06}
SOURCES = {"organic": .34, "direct": .22, "paid_search": .18, "social": .16, "email": .06, "referral": .04}
CAMPAIGN_BY_MONTH = {11: "white_friday", 12: "year_end", 1: "dsf_offers", 2: "ramadan_offers", 3: "ramadan_offers",
                     6: "summer_sale", 7: "summer_sale", 8: "summer_sale"}
SEARCH_TERMS = ["king mattress", "mattress 180x200", "pillow", "sofa", "recliner", "massage chair", "adjustable bed",
                "mattress topper", "vispring", "englander", "sheet set", "bedside table", "kids mattress", "duvet"]
EN = {
    "delivery_eta": ["Hi, when will my order {oid} be delivered?", "Can I schedule the delivery for order {oid}? I am home on weekends.",
                     "Order {oid} - any update on delivery time please?"],
    "defect": ["The {prod} from order {oid} arrived with a manufacturing defect. Please advise.",
               "Order {oid}: the {prod} has a fault, I have photos. How do I start a claim?"],
    "cancel": ["Please cancel order {oid}, I ordered the wrong size.", "I need to cancel order {oid} before it ships."],
    "warranty": ["How do I activate the warranty for order {oid}?", "What does the warranty cover for the {prod} (order {oid})?"],
    "address": ["Can you change the delivery address for order {oid} to Sharjah?", "Please update the delivery address on order {oid}."],
}
AR = {
    "delivery_eta": ["متى سيتم توصيل طلبي رقم {oid}؟", "هل يمكنني تحديد موعد للتوصيل لطلب {oid}؟"],
    "defect": ["المنتج الذي استلمته (رقم الطلب {oid}) به عيب تصنيع، أرجو المساعدة."],
    "cancel": ["أرغب في إلغاء الطلب رقم {oid} من فضلكم."],
    "warranty": ["كيف يمكنني تفعيل الضمان للطلب {oid}؟"],
    "address": ["أرجو تغيير عنوان التوصيل للطلب {oid}."],
}


def wc(r, d):
    k = list(d)
    return r.choices(k, weights=[d[x] for x in k], k=1)[0]


def slug(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S+04:00")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--bounce-ratio", type=float, default=6.0)
    ap.add_argument("--seed", type=int, default=312)
    a = ap.parse_args()
    r = random.Random(a.seed)
    orders = read(f"{a.curated}/orders.csv")
    items = defaultdict(list)
    for it in read(f"{a.curated}/order_items.csv"):
        items[it["order_id"]].append(it)
    cat = read(f"{a.curated}/product_catalog.csv")
    catw = [float(c["current_price_aed"]) ** -0.3 for c in cat]
    pname = {c["product_id"]: c["product_name"] for c in cat}
    sku_code = lambda pid: f"POS-{int(pid[-5:]) + 40000:05d}"

    buckets = defaultdict(lambda: defaultdict(list))   # source -> date -> [(ts_str, record)]
    def emit(src, ts, rec):
        buckets[src][ts[:10]].append((ts, rec))

    first = datetime.fromisoformat(min(o["order_ts"] for o in orders).replace(" ", "T"))
    last = datetime.fromisoformat(max(o["order_ts"] for o in orders).replace(" ", "T"))
    online_ts = []
    pos_seq = defaultdict(int)
    n_ses = 0

    def new_session(ts_start, customer_id, logged_in):
        nonlocal n_ses
        n_ses += 1
        d = wc(r, DEVICES); s = wc(r, SOURCES)
        camp = CAMPAIGN_BY_MONTH.get(ts_start.month, "always_on") if s in ("paid_search", "social", "email") else None
        return {"session_id": f"SYN-SES-{n_ses:08d}", "anonymous_id": "SYN-ANON-%010x" % r.getrandbits(40),
                "customer_id": customer_id if logged_in else None, "device": d, "traffic_source": s, "utm_campaign": camp}

    def put_events(ses, times, evs):
        for t, (etype, page, pid, props) in zip(times, evs):
            rec = {"event_ts": iso(t), "session_id": ses["session_id"], "anonymous_id": ses["anonymous_id"],
                   "customer_id": ses["customer_id"], "event_type": etype, "page_path": page, "product_id": pid,
                   "device": ses["device"], "traffic_source": ses["traffic_source"], "utm_campaign": ses["utm_campaign"],
                   "country": "AE", "props": props, "data_origin": "SYNTHETIC"}
            if t >= datetime(2026, 3, 1):          # schema drift: new field appears mid-year
                rec["consent_analytics"] = r.random() < 0.82
            emit("web_events", iso(t), rec)

    for o in orders:
        ts = datetime.fromisoformat(o["order_ts"].replace(" ", "T"))
        lines = items[o["order_id"]]
        oid, status = o["order_id"], o["order_status"]
        if o["channel"] == "online":
            online_ts.append(ts)
            emit("ecom_orders", iso(ts), {
                "order_id": oid, "created_at": iso(ts), "customer": {"id": o["customer_id"], "emirate": o["delivery_emirate"]},
                "payment": {"method": o["payment_method"], "status": "voided" if status == "cancelled" else "paid"},
                "shipping": {"option": o["delivery_option"], "fee_aed": float(o["delivery_fee_aed"]), "emirate": o["delivery_emirate"]},
                "line_items": [{"product_id": l["product_id"], "title": l["product_name"], "size_cm": l["size_cm"] or None,
                                "qty": int(l["qty"]), "unit_list_price_aed": float(l["unit_list_price_aed"]),
                                "unit_price_aed": float(l["unit_price_aed"])} for l in lines],
                "totals": {"items_net_aed": float(o["items_net_aed"]), "order_total_aed": float(o["order_total_aed"]),
                           "vat_aed": float(o["vat_aed"])},
                "promo_flag": o["promo_flag"] == "true", "status": status, "data_origin": "SYNTHETIC"})
            # converting session
            logged = r.random() < 0.7
            ses = new_session(ts - timedelta(minutes=10), o["customer_id"], logged)
            evs = [("page_view", "/", None, {"referrer": ses["traffic_source"]})]
            if r.random() < .4:
                evs.append(("search", "/search", None, {"query": r.choice(SEARCH_TERMS), "results": r.randint(3, 60)}))
            for l in lines:
                pid = l["product_id"]
                evs.append(("product_view", f"/products/{slug(l['product_name'])}", pid, {"price_aed": float(l["unit_price_aed"])}))
                evs.append(("add_to_cart", "/cart", pid, {"qty": int(l["qty"]), "price_aed": float(l["unit_price_aed"])}))
            evs.append(("begin_checkout", "/checkout", None, {"cart_value_aed": float(o["items_net_aed"])}))
            if r.random() < .07:
                evs.append(("payment_attempt", "/checkout", None, {"method": o["payment_method"], "status": "failed"}))
            evs.append(("payment_attempt", "/checkout", None, {"method": o["payment_method"], "status": "success"}))
            evs.append(("purchase", "/checkout/thank-you", None, {"order_id": oid, "total_aed": float(o["order_total_aed"])}))
            window = r.uniform(240, 1500)
            offs = sorted(r.uniform(0, window) for _ in range(len(evs) - 1)) + [window]
            put_events(ses, [ts - timedelta(seconds=window - x) for x in offs], evs)
        else:
            st = STORE[o["showroom_region"]]
            pos_seq[st] += 1
            emit("pos_transactions", iso(ts), {
                "receipt_no": f"R-{st}-{pos_seq[st]:06d}", "store_code": st, "terminal_id": f"T{r.randint(1, 3):02d}",
                "cashier_id": "CSH-%04d" % r.randint(1, 40), "sold_at": iso(ts), "oms_order_ref": oid,
                "lines": [{"sku_code": sku_code(l["product_id"]), "qty": int(l["qty"]), "unit_price_aed": float(l["unit_price_aed"]),
                           "discount_aed": round(float(l["unit_list_price_aed"]) - float(l["unit_price_aed"]), 2)} for l in lines],
                "tender": {"type": o["payment_method"], "amount_aed": float(o["order_total_aed"])},
                "total_aed": float(o["order_total_aed"]), "delivery_emirate": o["delivery_emirate"],
                "status": status, "data_origin": "SYNTHETIC"})
        # support messages (unstructured text)
        topics = []
        if status == "defect_return_requested": topics += ["defect"] * r.choice([1, 1, 2])
        if status == "cancelled": topics.append("cancel")
        if r.random() < 0.07: topics.append(wc(r, {"delivery_eta": .6, "warranty": .2, "address": .2}))
        for k, tp in enumerate(topics):
            lang = "ar" if r.random() < .15 else "en"
            tpl = r.choice((AR if lang == "ar" else EN)[tp])
            mt = ts + timedelta(hours=r.uniform(1, 96))
            if mt > last + timedelta(days=5): continue
            emit("support_messages", iso(mt), {
                "msg_id": f"SYN-MSG-{oid[-7:]}-{k + 1}", "ts": iso(mt), "channel": wc(r, {"whatsapp": .55, "email": .25, "phone_note": .20}),
                "customer_id": o["customer_id"], "order_id": oid, "language": lang, "topic_hint": tp,
                "text": tpl.format(oid=oid, prod=pname.get(lines[0]["product_id"], "item")), "data_origin": "SYNTHETIC"})

    # non-converting sessions
    n_non = int(round(a.bounce_ratio * len(online_ts)))
    cw = catw
    for _ in range(n_non):
        base = r.choice(online_ts) + timedelta(minutes=r.uniform(-180, 180))
        base = min(max(base, first), last)
        ses = new_session(base, None, False)
        evs = [("page_view", "/", None, {"referrer": ses["traffic_source"]})]
        if r.random() < .3:
            evs.append(("search", "/search", None, {"query": r.choice(SEARCH_TERMS), "results": r.randint(3, 60)}))
        picked = r.choices(cat, weights=cw, k=r.randint(1, 3))
        for c in picked:
            evs.append(("product_view", f"/products/{slug(c['product_name'])}", c["product_id"], {"price_aed": float(c["current_price_aed"])}))
        if r.random() < .12:
            c = picked[0]
            evs.append(("add_to_cart", "/cart", c["product_id"], {"qty": 1, "price_aed": float(c["current_price_aed"])}))
            if r.random() < .35:
                evs.append(("begin_checkout", "/checkout", None, {"cart_value_aed": float(c["current_price_aed"])}))
                if r.random() < .2:
                    evs.append(("payment_attempt", "/checkout", None, {"method": "card_visa_mastercard", "status": "failed"}))
        t = base; times = []
        for _ in evs:
            times.append(t); t = t + timedelta(seconds=r.uniform(8, 120))
        put_events(ses, times, evs)

    # write partitioned JSONL
    counts = {}
    seq = 0
    for src, days in buckets.items():
        n = 0
        for day in sorted(days):
            recs = sorted(days[day], key=lambda x: x[0])
            d = os.path.join(a.out, src, f"dt={day}")
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "part-0000.jsonl"), "w", encoding="utf-8") as f:
                for _, rec in recs:
                    if src == "web_events":
                        seq += 1
                        rec = {"event_id": f"SYN-EVT-{seq:09d}", **rec}
                    f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                    n += 1
        counts[src] = n
    os.makedirs(os.path.join(a.out, "reference"), exist_ok=True)
    with open(os.path.join(a.out, "reference", "pos_item_map.csv"), "w", encoding="utf-8", newline="") as f:
        f.write("sku_code,product_id\n")
        for c in cat:
            f.write(f"{sku_code(c['product_id'])},{c['product_id']}\n")
    print(json.dumps({"records": counts, "sessions": n_ses}, indent=2))


if __name__ == "__main__":
    main()
