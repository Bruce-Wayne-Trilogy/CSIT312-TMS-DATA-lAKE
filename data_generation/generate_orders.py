#!/usr/bin/env python3
"""
SYNTHETIC order generator -- ISIT312 project, Stage 1.

Everything this script writes is SYNTHETIC. Prices/product names in the catalog
were observed on public pages of themattressstore.com (UAE, AED, VAT-inclusive);
the orders, customers, timestamps, baskets, payment choices etc. are invented.

Usage:
    python3 -I generate_orders.py --out OUTDIR [--orders 10000] [--seed 312]
                                  [--no-size-ladder]   # sensitivity run

Outputs: product_catalog.csv, orders.csv, order_items.csv, stats.json
"""
import argparse
import csv
import json
import os
import random
import statistics
from datetime import date, datetime, timedelta

LIVE_ALL = "https://themattressstore.com/collections/all"
LIVE_SALE = "https://themattressstore.com/collections/sale"
B_LIVE = "live_fetch_2026-11-13"
B_SNAP = "search_snapshot"
B_DERIVED = "derived_size_ladder"

# Size ladder observed on ONE product (Dynamic Tight Top, entry 90x200 = AED 350).
LADDER = [("90x200", 350), ("100x200", 400), ("120x200", 450), ("140x200", 550),
          ("150x200", 600), ("160x200", 600), ("180x200", 700),
          ("200x200", 750), ("200x210", 800)]
LADDER_W = {"90x200": 8, "100x200": 4, "120x200": 5, "140x200": 3, "150x200": 12,
            "160x200": 14, "180x200": 38, "200x200": 12, "200x210": 4}
KIDS_W = {"90x200": 50, "100x200": 20, "120x200": 30}
LUX_SIZE_W = {"150x200": 5, "160x200": 10, "180x200": 55, "200x200": 25, "200x210": 5}


class Catalog:
    def __init__(self, use_ladder=True):
        self.use_ladder = use_ladder
        self.rows = []          # variant-level rows
        self.products = {}      # pool -> list of product dicts
        self._n = 0

    def _row(self, name, cat, sub, brand, tier, size, cur, reg, url, basis, age):
        self._n += 1
        r = {
            "product_id": f"SYN-SKU-{self._n:05d}", "product_name": name,
            "category": cat, "subcategory": sub, "brand": brand, "tier": tier,
            "size_cm": size, "regular_price_aed": reg if reg else cur,
            "current_price_aed": cur, "markdown_observed": bool(reg and reg > cur),
            "price_basis": basis, "source_url": url,
            "snapshot_age_days": "" if age is None else age,
        }
        self.rows.append(r)
        return r

    def add(self, pool, name, cat, sub, brand, tier, cur, reg=None, url=LIVE_ALL,
            basis=B_LIVE, age=None, pop=None, ladder=None, variants=None):
        """ladder: None | 'adult' | 'kids' ; variants: {size:(cur,reg)} observed."""
        if pop is None:
            pop = (cur ** -0.3) * 100.0
        vrows, weights = [], []
        if variants:
            wmap = LUX_SIZE_W
            for size, (c, rg) in variants.items():
                vrows.append(self._row(name, cat, sub, brand, tier, size, c, rg, url, basis, age))
                weights.append(wmap.get(size, 1))
        elif ladder and self.use_ladder:
            wmap = KIDS_W if ladder == "kids" else LADDER_W
            for size, ref in LADDER:
                if size not in wmap:
                    continue
                m = ref / 350.0
                c = int(round(cur * m / 5.0) * 5)
                rg = int(round(reg * m / 5.0) * 5) if reg else None
                is_entry = (size == "90x200")
                vrows.append(self._row(name, cat, sub, brand, tier, size, c, rg, url,
                                       basis if is_entry else B_DERIVED, age))
                weights.append(wmap[size])
        else:
            vrows.append(self._row(name, cat, sub, brand, tier, "", cur, reg, url, basis, age))
            weights.append(1)
        self.products.setdefault(pool, []).append(
            {"name": name, "pop": pop, "rows": vrows, "w": weights})


def build_catalog(use_ladder=True):
    C = Catalog(use_ladder)
    A = C.add
    # ---------------- Mattresses: value (Englander) ----------------
    vm = [("Dynamic Tight Top Mattress", 350, 700, "https://themattressstore.com/products/dynamic-tight-top-mattress-by-southerland", B_SNAP, 871, 6),
          ("Hotel Collection Tight Top Mattress", 600, 800, LIVE_SALE, B_LIVE, None, 8),
          ("Hotel Collection Euro Top Mattress", 750, 1000, LIVE_SALE, B_LIVE, None, 8),
          ("Hotel Collection Pillow Top Mattress", 975, 1300, LIVE_SALE, B_LIVE, None, 7),
          ("Everest Extra Firm Mattress", 800, None, LIVE_ALL, B_LIVE, None, 7),
          ("Everest Tight Top Mattress", 900, None, LIVE_ALL, B_LIVE, None, 9),
          ("Everest Euro Top Mattress", 1000, None, LIVE_ALL, B_LIVE, None, 9),
          ("Holburn Tight Top Mattress", 900, None, LIVE_ALL, B_LIVE, None, 9),
          ("Holburn Euro Top Mattress", 1000, None, LIVE_ALL, B_LIVE, None, 9),
          ("Essex Tight Top Mattress", 1300, None, LIVE_ALL, B_LIVE, None, 5),
          ("Essex Euro Top Mattress", 1600, None, LIVE_ALL, B_LIVE, None, 5),
          ("Nature's Finest Latex Tight Top Mattress", 1500, None, LIVE_ALL, B_LIVE, None, 4),
          ("Nature's Finest Latex Euro Top Mattress", 1800, None, LIVE_ALL, B_LIVE, None, 4),
          ("Bodiform Gel Tight Top Mattress", 1170, 2600, LIVE_ALL, B_LIVE, None, 5),
          ("Bodiform Gel Euro Top Mattress", 1395, 3100, LIVE_ALL, B_LIVE, None, 5),
          ("Notting Plush (2026) Mattress", 2200, None, LIVE_ALL, B_LIVE, None, 3),
          ("Notting Extra Firm (2026) Mattress", 2300, None, "https://themattressstore.com/products/notting-extra-firm-2026-mattress-by-englander", B_SNAP, 144, 3),
          ("Nature's Finest Plush Mattress", 2800, None, LIVE_ALL, B_LIVE, None, 3),
          ("O'Conner Euro Top Mattress", 2800, None, LIVE_ALL, B_LIVE, None, 2),
          ("Dreamer Elation Euro Top Mattress", 3800, None, LIVE_ALL, B_LIVE, None, 2),
          ("Dreamer Echelon Euro Top Mattress", 4500, None, LIVE_ALL, B_LIVE, None, 1.5),
          ("Camberwell Firm With Talalay Latex Mattress", 5000, None, LIVE_ALL, B_LIVE, None, 1.5)]
    for n, c, r, u, b, a, p in vm:
        A("value_mattress", n, "Mattresses", "Value", "Englander", "value", c, r, u, b, a, p, ladder="adult")
    for n, c in [("Kids Visco Mattress", 850), ("Kids Latex Mattress", 950)]:
        A("kids_mattress", n, "Mattresses", "Kids", "Englander", "value", c, None, LIVE_ALL, B_LIVE, None, 1, ladder="kids")

    # ---------------- Mattresses: luxury (Vispring; observed per-size prices) ----------------
    P = "https://themattressstore.com/products/"
    lux = [("Baronet Superb Mattress", "baronet-superb-mattress-by-vispring", 158, 6,
            {"150x200": (16500, None), "160x200": (18800, None), "180x200": (20300, None), "200x200": (23100, None)}),
           ("Herald Superb Mattress", "herald-superb-mattress-by-vispring", 936, 4,
            {"180x200": (22860, 25400), "200x200": (26100, 29000)}),
           ("Tiara Superb Mattress", "tiara-superb-mattress-by-vispring", 158, 1.5,
            {"160x200": (70700, None), "180x200": (76500, None), "200x200": (87200, None), "200x210": (94600, None)}),
           ("Sublime Superb Mattress", "sublime-superb-mattress-by-vispring", 283, 1,
            {"180x200": (80400, None), "200x200": (91700, None)}),
           ("Magnificence Mattress", "magnificence-mattress-by-vispring", 158, 1,
            {"180x200": (127400, None), "200x200": (145300, None), "200x210": (157600, None)}),
           ("Diamond Majesty Mattress", "diamond-majesty-mattress-by-vispring", 158, 0.25,
            {"180x200": (242500, None), "200x200": (261400, None), "200x210": (277000, None)})]
    for n, slug, age, pop, v in lux:
        A("luxury_mattress", n, "Mattresses", "Luxury", "Vispring", "luxury", 0, None, P + slug,
          B_SNAP, age, pop, variants=v)

    # ---------------- Bedding ----------------
    BA = "https://themattressstore.com/collections/bed-accessories"
    def bed(pool, n, sub, brand, cur, reg=None, url=LIVE_ALL, basis=B_LIVE, age=None, pop=None, variants=None):
        tier = "luxury" if (brand == "Vispring" or variants) else ("value" if cur < 1000 else "mid" if cur < 3000 else "luxury")
        A(pool, n, "Bedding", sub, brand, tier, cur, reg, url, basis, age, pop, variants=variants)
    for n, c in [("Sub-0 SoftCell Cooling Memory Foam Pillow", 400), ("Sub-0 SoftCell Chill Latex Pillow", 500),
                 ("Dr. Weil Kapok Pillow", 500), ("Prima Plush Pillow", 115), ("Rise & Shine Youth Pillow", 200),
                 ("ErgoSeven7 Pillow", 700), ("OCA Pillow", 800), ("Fresh Pillow", 500), ("Hygiencell Pillow", 500)]:
        bed("pillow", n, "Pillows", "PureCare" if n.startswith(("Sub-0", "Dr.", "Prima", "Rise")) else
            ("Ferdown" if n.split()[0] in ("ErgoSeven7", "OCA", "Fresh") else "Colunex"), c)
    for n, c, b in [("Shattered Ice Pillow", 400, "PureCare"), ("Baby Pillow", 300, "Colunex"),
                    ("Fresh Green Pillow", 600, "Colunex"), ("Fresh Mix Pillow", 700, "Colunex")]:
        bed("pillow", n, "Pillows", b, c, url=BA, basis=B_SNAP, age=969)
    for n, c, r in [("Frio Cooling Mattress Protector", 150, 200), ("Bamboo Mattress Protector", 75, 100)]:
        bed("protector", n, "Mattress Protectors", "PureCare", c, r, url=LIVE_SALE)
    bed("protector", "Kids Mattress Protector", "Mattress Protectors", "PureCare", 100)
    bed("protector", "Dr. Weil Wool 5-Sided Mattress Protector", "Mattress Protectors", "PureCare", 500, url=BA, basis=B_SNAP, age=969)
    bed("protector", "Lyocell Mattress Protector", "Mattress Protectors", "Colunex", 700, url="https://themattressstore.com/collections/all?page=2", basis=B_SNAP)
    for n, c, b in [("Frio Cooling Pillow Protector", 100, "PureCare"), ("Bamboo Pillow Protector", 50, "PureCare"),
                    ("Dr. Weil Pillow Protector", 200, "PureCare")]:
        bed("pillow_protector", n, "Pillow Protectors", b, c, url=BA if "Bamboo" in n or "Dr." in n else LIVE_ALL,
            basis=B_SNAP if ("Bamboo" in n or "Dr." in n) else B_LIVE, age=969 if ("Bamboo" in n or "Dr." in n) else None)
    for n, c, b, u, bs, ag in [("Rise & Shine Pillowcase", 50, "PureCare", LIVE_ALL, B_LIVE, None),
                               ("Bamboo Pillow Case Set", 100, "PureCare", LIVE_ALL, B_LIVE, None),
                               ("Pure Silk Pillow Case", 400, "PureCare", LIVE_ALL, B_LIVE, None),
                               ("Dr. Weil Relaxed Hemp Pillowcase", 100, "PureCare", BA, B_SNAP, 969)]:
        bed("pillow_case", n, "Pillow Cases", b, c, url=u, basis=bs, age=ag)
    bed("sheet_set", "Bamboo Rayon Sheet Set", "Sheet Sets", "PureCare", 500)
    bed("sheet_set", "Sleep Kit", "Sheet Sets", "PureCare", 400)
    bed("sheet_set", "Recovery Viscose Celliant Sheet Set", "Sheet Sets", "PureCare", 900, url=BA, basis=B_SNAP, age=969)
    bed("sheet_set", "Dr. Weil Relaxed Hemp Sheets", "Sheet Sets", "PureCare", 1050, 2100, url=LIVE_SALE)
    bed("duvet", "Cooling Frio Duvet", "Duvets", "PureCare", 600)
    bed("duvet", "Bamboo Duvet Cover", "Duvets", "PureCare", 400)
    bed("duvet", "Microseda Duvet", "Duvets", "Ferdown", 1100)
    bed("duvet", "Hygiencell Duvet", "Duvets", "Colunex", 1600, url=BA, basis=B_SNAP, age=969)
    for n, c, pop in [("Conform Memory Foam Medium Topper", 400, 4), ("Breath Latex Firm Topper", 550, 3), ("Luxe Topper", 1400, 2)]:
        bed("topper", n, "Mattress Toppers", "Englander" if "Conform" in n or "Breath" in n else "Ferdown", c, pop=pop)
    TP = "https://themattressstore.com/collections/vispring"
    bed("topper", "Dream Topper", "Mattress Toppers", "Vispring", 7800, url=TP, basis=B_SNAP, age=938, pop=0.6)
    bed("topper", "Heaven Topper", "Mattress Toppers", "Vispring", 0, url=P + "heaven-topper-by-vispring", basis=B_SNAP, age=937,
        pop=0.3, variants={"180x200": (9700, None), "200x200": (10500, None)})
    bed("topper", "Heaven Luxury Topper", "Mattress Toppers", "Vispring", 13000, url=TP, basis=B_SNAP, age=938, pop=0.2)
    bed("topper", "Heaven Luxury Supreme Topper", "Mattress Toppers", "Vispring", 15600, url=TP, basis=B_SNAP, age=938, pop=0.1)
    for n, sub, c in [("English Duck Down & Feather Luxury Pillow", "Pillows", 1800), ("Hungarian Goose Down Surround Pillow", "Pillows", 2400),
                      ("Wool Pillow", "Pillows", 1100), ("Wool Mattress Pad", "Mattress Protectors", 1800),
                      ("Luxury Mattress Protector", "Mattress Protectors", 2100)]:
        bed("lux_bedding", n + " (Vispring)", sub, "Vispring", c, url=TP, basis=B_SNAP, age=938)

    # ---------------- Bedroom: adjustable beds, beds, nightstands ----------------
    E = "https://themattressstore.com/collections/ergomotion"
    for n, c, r, pop in [("ErgoAdvance Adjustable Bed", 11300, None, 1), ("ErgoExpanding Adjustable Bed", 6800, None, 2),
                         ("ErgoBox Side Opening Adjustable Bed", 9600, None, 1.5), ("Ergosportive Adjustable Bed", 8100, None, 2),
                         ("Ergo Slim Adjustable Bed", 6900, None, 2.5)]:
        A("adj_bed", n, "Bedroom", "Adjustable Beds", "Ergomotion", "mid", c, r, E, B_SNAP, 832, pop)
    for n, c, r in [("ErgoBox Adjustable Bed 180x200 + Nature's Finest Plush Mattress", 11325, 15100),
                    ("ErgoExpanding Adjustable Bed + Nature's Finest Plush Mattress", 11775, 15700),
                    ("Ergosportive Adjustable Bed + Notting Mattress", 8700, 11600),
                    ("Ergo Slim Adjustable Bed + Nature's Finest Plush Mattress", 8625, 11500)]:
        A("adj_bundle", n, "Bedroom", "Adjustable Beds", "Ergomotion", "mid", c, r, E, B_SNAP, 832, 1)
    for n, c, r in [("Matin Supreme Collection Bed 150x200", 1375, 2750), ("Philospher's Bed 150x200", 1100, 2200),
                    ("Philospher's Reading Bed 150x200", 1600, 3200)]:
        A("bed", n, "Bedroom", "Beds", "The Mattress Store", "value", c, r, LIVE_SALE, B_LIVE, None, 4)
    A("bed", "Stream Bed", "Bedroom", "Beds", "Colunex", "luxury", 14300, None,
      P + "stream-bed-by-colunex", B_SNAP, 148, 0.3)
    NS = "https://themattressstore.com/collections/side-table-nightstand"
    for n, b, c, r, u, ag in [("Cordero Nightstand", "Sunpan", 2020, 5760, NS, None), ("Venice Nightstand", "Sunpan", 2070, 5920, NS, None),
                              ("Aniston Nightstand Dark Mango", "Sunpan", 2350, 6720, NS, None), ("Edmund Night Stand", "Sunpan", 2750, 7840, NS, None),
                              ("Abac Bedside Table", "Chateau d'Ax Italia", 1400, None, "https://themattressstore.com/collections/chateau-dax", 779),
                              ("Tratto Bedside Table", "Chateau d'Ax Italia", 1300, None, P + "tratto-bedside-table-by-chateau-dax-italia", 287),
                              ("AS789 Night Stand", "Campo de' Fiori", 700, None, "https://themattressstore.com/collections/bedroom", 969),
                              ("AS837 Night Stand", "Campo de' Fiori", 1500, None, "https://themattressstore.com/collections/bedroom", 969)]:
        A("nightstand", n, "Bedroom", "Nightstands & Bedside Tables", b, "mid", c, r, u,
          B_SNAP, ag, None)
    A("nightstand", "Cosmopolitan Bedside Table", "Bedroom", "Nightstands & Bedside Tables", "Colunex", "luxury", 15900, None,
      P + "cosmopolitan-bedside-table-by-colunex", B_SNAP, 465, 0.3)

    # ---------------- Living room ----------------
    CF = "https://themattressstore.com/collections/campo-de-fiori?page=2"
    sofas = [("5396 Sofa 3 Seater Cream", "Campo de' Fiori", 5200, None, CF, 276),
             ("5396 Sofa 2 Seater Blue", "Campo de' Fiori", 4200, None, CF, 276),
             ("CDF835 Sofa 3 Seater", "Campo de' Fiori", 5800, None, CF, 276),
             ("CDF843 Sofa 3 Seater", "Campo de' Fiori", 8500, None, CF, 276),
             ("CDF275 Sofa 3 Seater", "Campo de' Fiori", 16800, None, CF, 276),
             ("CDF848 Sofa 3 Seater", "Campo de' Fiori", 17100, None, CF, 276),
             ("CDF848 Sofa 2 Seater", "Campo de' Fiori", 12600, None, CF, 276),
             ("CDF877 Corner Sofa", "Campo de' Fiori", 12900, None, CF, 276),
             ("CDF881 Corner Sofa", "Campo de' Fiori", 17200, None, CF, 276),
             ("5341 L-Shape Sofa", "Campo de' Fiori", 6500, None, CF, 276),
             ("CDF863 Chaise Lounge", "Campo de' Fiori", 3500, None, CF, 276),
             ("5417 Chaise Lounge", "Campo de' Fiori", 2300, None, CF, 276),
             ("CDF278 Sofa 3 Seater", "Campo de' Fiori", 6800, None, "https://themattressstore.com/collections/3-seater-sofas", 225),
             ("CDF898 Sofa 3 Seater", "Campo de' Fiori", 11900, None, "https://themattressstore.com/collections/3-seater-sofas", 225),
             ("Lambert Large 3 Seater Sofa White", "Chateau d'Ax Italia", 10750, None, "https://themattressstore.com/collections/3-seater-sofas", 225),
             ("Venus 2 Seater Sofa", "Chateau d'Ax Italia", 7020, 10800, P + "venus-sofas-2-seater-by-chateau-dax", 202),
             ("CL131 Recliner Sofa 2 Seater", "Campo de' Fiori", 6400, None, P + "cl131-recliner-sofa-2-seater-by-campo-de-fiori", 275),
             ("CDF833 Recliner Sofa 3 Seater", "Campo de' Fiori", 11000, None, P + "cdf833-recliner-sofa-3-seater-by-campo-de-fiori", 272),
             ("CDF809 Recliner Sofa Set", "Campo de' Fiori", 15300, None, P + "cdf809-recliner-sofa-set-by-campo-de-fiori", 274),
             ("CDF849 Recliner Sofa 3 Seater", "Campo de' Fiori", 19300, None, P + "cdf849-recliner-sofa-3-seater-by-campo-de-fiori", 272)]
    for n, b, c, r, u, ag in sofas:
        A("sofa", n, "Living room", "Sofas", b, "mid" if c < 10000 else "luxury", c, r, u, B_SNAP, ag)
    A("sofa", "Emma Sofa (Leather)", "Living room", "Sofas", "Hilker", "mid", 8675, 34300, LIVE_SALE, B_LIVE, None)
    RC = "https://themattressstore.com/collections/recliners"
    rec = [("CL131 Recliner Chair", "Campo de' Fiori", 4600, None), ("CL136 Recliner Chair", "Campo de' Fiori", 4300, None),
           ("CL187 Recliner Chair", "Campo de' Fiori", 2900, None), ("CL145 Manual Recliner w/ Ottoman", "Campo de' Fiori", 4400, None),
           ("Urban Swing Relaxer Manual (Small)", "Fjords", 3900, 6000), ("Milan Swing Relaxer Manual (Large)", "Fjords", 5330, 8200),
           ("Admiral R Manual Recliner with Foot Stool (Small)", "Fjords", 4745, 7300),
           ("Muldal R Swing Relaxer Manual with Foot Stool (Large)", "Fjords", 3575, 5500),
           ("Urban Swing Relaxers 2 Motors (S)", "Fjords", 7200, None), ("Milan Swing Relaxers 2 Motors (L)", "Fjords", 10000, None),
           ("Admiral R Manual Recliner with Foot Stool (Large)", "Fjords", 7300, None), ("Nice Swing Relaxer (Small)", "Fjords", 5000, None)]
    for n, b, c, r in rec:
        sale_pg = (r is not None)
        A("chair", n, "Living room", "Recliners", b, "mid", c, r, LIVE_SALE if sale_pg else RC,
          B_LIVE if sale_pg else B_SNAP, None if sale_pg else 969)
    A("chair", "Emma Recliner (Leather)", "Living room", "Recliners", "Hilker", "mid", 3400, 13700,
      P + "emma-recliner-leather-blue-by-hilker", B_SNAP, 657)
    for n, c, r in [("Variable Plus Chair", 2100, 2800), ("Gravity Chair", 5250, 7000), ("Multi Chair", 1650, 2200),
                    ("Actulum Chair", 2400, 3200), ("Peel with Footrest Chair", 8650, 11500)]:
        A("chair", n, "Living room", "Chairs", "Varier", "mid", c, r, LIVE_SALE, B_LIVE, None)

    # ---------------- Tables / other furniture ----------------
    SA = LIVE_SALE
    for n, sub, c, r in [("Doncaster Coffee Table (Grey)", "Coffee Tables", 575, 2300), ("Venice Media Console and Cabinet", "Sideboards", 1175, 4700),
                         ("Renzo Entryway Cabinet Small", "Sideboards", 1025, 4100), ("Renzo Entryway Cabinet Large", "Sideboards", 1425, 5700),
                         ("Endall Ottoman Antique Brass", "Tables", 650, 2600), ("Shelburne Marble Look Bar Table", "Tables", 1350, 5400)]:
        A("table_other", n, "Living room", sub, "Sunpan", "mid", c, r, SA, B_LIVE, None)
    CD = "https://themattressstore.com/collections/chateau-dax"
    for n, sub, c in [("Ring Small Coffee Table", "Coffee Tables", 800), ("Basket Coffee Table", "Coffee Tables", 1760),
                      ("Basket Side Table", "Tables", 960)]:
        A("table_other", n, "Living room", sub, "Chateau d'Ax Italia", "mid", c, None, CD, B_SNAP, 779)
    A("dining_table", "Lambert Dining Table", "Dining", "Dining Tables", "Chateau d'Ax Italia", "mid", 5320, None, CD, B_SNAP, 779)
    A("dining_table", "Round Dining Table (Iron Gold)", "Dining", "Dining Tables", "Chateau d'Ax Italia", "mid", 5000, None, CD, B_SNAP, 779)
    A("dining_table", "Donnelly Dining Table Dark Mango", "Dining", "Dining Tables", "Sunpan", "mid", 775, 3100,
      "https://themattressstore.com/collections/dining-tables", B_SNAP, 994)
    for n, c in [("Alic Dining Chair", 1500), ("Grace Dining Chair", 1800), ("Lena Dining Chair Grey", 2150)]:
        A("dining_chair", n, "Dining", "Dining Chairs", "Chateau d'Ax Italia", "mid", c, None, CD, B_SNAP, 779)

    # ---------------- Massagers ----------------
    MS = "https://themattressstore.com/collections/massagers"
    for n, c, pop in [("Neck & Shoulder Massager", 300, 2), ("Mini Gun Massager", 400, 3), ("Cushion Massager", 500, 2),
                      ("Plate Massager", 500, 1.5), ("Scalp All Care Massager", 700, 1.5)]:
        A("massager", n, "Massagers", "Massagers", "Bodyfriend", "value", c, None, MS, B_SNAP, 276, pop)
    A("massage_chair", "Pharaoh Neo Massage Chair", "Massagers", "Massage Chairs", "Bodyfriend", "luxury", 30000, None, MS, B_SNAP, 276, 1)
    A("massage_chair", "Eden ROVO Massage Chair", "Massagers", "Massage Chairs", "Bodyfriend", "luxury", 20400, None, MS, B_SNAP, 276, 1.4)

    # ---------------- Decor ----------------
    for n, c, r in [("Herm Glass Vase", 450, 500), ("Fabrica C Ceramic Sculpture", 630, 700), ("Every Metal Age Vase Ceramic Blue", 540, 600),
                    ("Tocco Vase", 540, 600), ("Dumby Ceramic Sculptures", 360, 400), ("Linea Cono (Set of 12 Glasses)", 270, 300),
                    ("Caffettiera Square Coffeepot", 320, 400), ("Gong Glass Vase With Metal Stand", 990, 1100),
                    ("Bocca Wall Art & Pictures", 3600, 4000)]:
        A("decor", n, "Decor", "Decor", "Adriani e Rossi", "value", c, r, LIVE_SALE, B_LIVE, None)
    return C


# ---------------------------------------------------------------- generation
EMIRATES_W = {"Dubai": 0.43, "Abu Dhabi": 0.24, "Sharjah": 0.14, "Ajman": 0.06,
              "Ras Al Khaimah": 0.04, "Umm Al Quwain": 0.04, "Fujairah": 0.05}
SHOWROOM_W = {"Dubai": 0.38, "Abu Dhabi": 0.27, "Al Ain": 0.10, "Sharjah": 0.15, "Umm Al Quwain": 0.10}
SHOWROOM_TO_EMIRATE = {
    "Dubai": {"Dubai": 0.80, "Sharjah": 0.10, "Ajman": 0.05, "Abu Dhabi": 0.05},
    "Abu Dhabi": {"Abu Dhabi": 0.95, "Dubai": 0.05},
    "Al Ain": {"Abu Dhabi": 1.0},
    "Sharjah": {"Sharjah": 0.65, "Dubai": 0.15, "Ajman": 0.15, "Umm Al Quwain": 0.05},
    "Umm Al Quwain": {"Umm Al Quwain": 0.60, "Ajman": 0.20, "Ras Al Khaimah": 0.10, "Sharjah": 0.10},
}
MONTH_W = {(2025, 11): 1.25, (2025, 12): 1.15, (2026, 1): 1.05, (2026, 2): 1.10, (2026, 3): 1.05, (2026, 4): 0.95,
           (2026, 5): 0.90, (2026, 6): 0.80, (2026, 7): 0.70, (2026, 8): 0.70, (2026, 9): 0.95, (2026, 10): 1.10}
DOW_ONLINE = [1.0, 1.0, 1.0, 1.0, 1.1, 1.2, 1.15]     # Mon..Sun
DOW_SHOWROOM = [0.8, 0.8, 0.8, 0.8, 1.3, 1.5, 1.35]
HOUR_ONLINE = [1, 0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1, 1.5, 2, 2.5, 3, 3, 3, 3, 3, 3.2, 3.5, 4, 5, 6, 6, 5, 3]
# mall hours assumed ~10:00-22:00 -> orders only 10:00-21:59
HOUR_SHOWROOM = [0] * 10 + [2, 3, 3, 3, 3, 3.5, 4, 4, 4, 4, 3, 1.5] + [0, 0]
assert len(HOUR_ONLINE) == 24 and len(HOUR_SHOWROOM) == 24
START, END = date(2025, 11, 1), date(2026, 10, 31)


def wchoice(rng, d):
    keys = list(d.keys())
    return rng.choices(keys, weights=[d[k] for k in keys], k=1)[0]


class Gen:
    def __init__(self, cat, rng):
        self.cat, self.rng = cat, rng

    def pool_pick(self, pool, force_size=None):
        prods = self.cat.products[pool]
        p = self.rng.choices(prods, weights=[x["pop"] for x in prods], k=1)[0]
        return self.variant(p, force_size)

    def variant(self, p, force_size=None):
        rows, w = p["rows"], p["w"]
        if force_size:
            for r in rows:
                if r["size_cm"] == force_size:
                    return r
        return self.rng.choices(rows, weights=w, k=1)[0]

    def q(self, dist):
        return wchoice(self.rng, dist)

    def basket(self):
        r = self.rng
        lines = {}

        def add(row, qty=1):
            lines[row["product_id"]] = (row, lines.get(row["product_id"], (row, 0))[1] + qty)

        arche = wchoice(r, {"bedding": .330, "value_mat": .372, "topper": .030, "lux_mat": .008, "adj": .035,
                            "massager": .035, "mchair": .008, "sofa": .075, "chair": .040, "nightstand": .025,
                            "tables": .025, "bedframe": .005, "decor": .012})
        if arche == "bedding":
            n = self.q({1: .55, 2: .30, 3: .15})
            subw = {"pillow": .38, "protector": .14, "pillow_protector": .07, "pillow_case": .08,
                    "sheet_set": .12, "duvet": .12, "lux_bedding": .04, "topper": .05}
            for _ in range(n):
                sp = wchoice(r, subw)
                row = self.pool_pick(sp)
                add(row, self.q({1: .5, 2: .35, 3: .05, 4: .10}) if sp in ("pillow", "pillow_case") else self.q({1: .7, 2: .25, 3: .05}))
        elif arche == "value_mat":
            kids = r.random() < 0.06
            row = self.pool_pick("kids_mattress" if kids else "value_mattress")
            mq = self.q({1: .88, 2: .10, 3: .02})
            add(row, mq)
            if r.random() < .25: add(self.pool_pick("protector"), mq if r.random() < .6 else 1)
            if r.random() < .18: add(self.pool_pick("pillow"), self.q({1: .4, 2: .5, 4: .1}))
            if r.random() < .06: add(self.pool_pick("topper"))
            if r.random() < .06: add(self.pool_pick("sheet_set"))
            if r.random() < .04: add(self.pool_pick("duvet"))
        elif arche == "topper":
            add(self.pool_pick("topper"), self.q({1: .92, 2: .08}))
            if r.random() < .15: add(self.pool_pick("pillow"), self.q({1: .5, 2: .5}))
        elif arche == "lux_mat":
            add(self.pool_pick("luxury_mattress"), self.q({1: .92, 2: .08}))
            if r.random() < .45: add(self.pool_pick("lux_bedding"), self.q({1: .3, 2: .6, 4: .1}))
            if r.random() < .18: add(self.pool_pick("topper"))
        elif arche == "adj":
            add(self.pool_pick("adj_bed" if r.random() < .6 else "adj_bundle"), self.q({1: .93, 2: .07}))
            if r.random() < .10: add(self.pool_pick("protector"))
            if r.random() < .10: add(self.pool_pick("pillow"), 2)
        elif arche == "massager":
            add(self.pool_pick("massager"), self.q({1: .85, 2: .15}))
            if r.random() < .05: add(self.pool_pick("massager"))
        elif arche == "mchair":
            add(self.pool_pick("massage_chair"))
        elif arche == "sofa":
            add(self.pool_pick("sofa"), self.q({1: .95, 2: .05}))
            if r.random() < .12: add(self.pool_pick("table_other"))
            if r.random() < .08: add(self.pool_pick("decor"), self.q({1: .7, 2: .3}))
        elif arche == "chair":
            add(self.pool_pick("chair"), self.q({1: .92, 2: .08}))
        elif arche == "nightstand":
            add(self.pool_pick("nightstand"), self.q({1: .35, 2: .60, 4: .05}))
        elif arche == "tables":
            if r.random() < .35:
                add(self.pool_pick("dining_table"))
                if r.random() < .55: add(self.pool_pick("dining_chair"), self.q({2: .2, 4: .45, 6: .30, 8: .05}))
            else:
                add(self.pool_pick("table_other"), self.q({1: .95, 2: .05}))
        elif arche == "bedframe":
            add(self.pool_pick("bed"), self.q({1: .96, 2: .04}))
            if r.random() < .5:
                p = r.choices(self.cat.products["value_mattress"], weights=[x["pop"] for x in self.cat.products["value_mattress"]], k=1)[0]
                add(self.variant(p, "150x200"))
        else:  # decor
            for _ in range(self.q({1: .6, 2: .3, 3: .1})):
                add(self.pool_pick("decor"), self.q({1: .7, 2: .3}))
        return arche, list(lines.values())


def make_dates():
    days, wo, ws = [], [], []
    d = START
    dim = {}
    while d <= END:
        key = (d.year, d.month)
        dim[key] = dim.get(key, 0) + 1
        d += timedelta(days=1)
    d = START
    while d <= END:
        mw = MONTH_W[(d.year, d.month)] / dim[(d.year, d.month)]
        days.append(d)
        wo.append(mw * DOW_ONLINE[d.weekday()])
        ws.append(mw * DOW_SHOWROOM[d.weekday()])
        d += timedelta(days=1)
    return days, wo, ws


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--orders", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=312)
    ap.add_argument("--no-size-ladder", action="store_true")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rng = random.Random(a.seed)
    cat = build_catalog(use_ladder=not a.no_size_ladder)
    gen = Gen(cat, rng)
    days, wo, ws = make_dates()

    orders = []
    for _ in range(a.orders):
        channel = "online" if rng.random() < 0.58 else "showroom"
        day = rng.choices(days, weights=wo if channel == "online" else ws, k=1)[0]
        hw = HOUR_ONLINE if channel == "online" else HOUR_SHOWROOM
        hour = rng.choices(range(24), weights=hw, k=1)[0]
        ts = datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        region = ""
        if channel == "showroom":
            region = wchoice(rng, SHOWROOM_W)
            emirate = wchoice(rng, SHOWROOM_TO_EMIRATE[region])
        else:
            emirate = wchoice(rng, EMIRATES_W)
        arche, lines = gen.basket()
        orders.append({"ts": ts, "channel": channel, "region": region, "emirate": emirate,
                       "arche": arche, "lines": lines})
    orders.sort(key=lambda o: o["ts"])

    ord_rows, item_rows = [], []
    cust_ids, next_c = [], 1
    for i, o in enumerate(orders, 1):
        oid = f"SYN-ORD-{i:07d}"
        if cust_ids and rng.random() < 0.14:
            cid = rng.choice(cust_ids)
        else:
            cid = f"SYN-CUST-{next_c:06d}"
            next_c += 1
            cust_ids.append(cid)
        list_sub = markdown = 0
        units = 0
        promo = False
        for ln, (row, qty) in enumerate(o["lines"], 1):
            lp, cp = row["regular_price_aed"], row["current_price_aed"]
            applied = row["markdown_observed"] and rng.random() < 0.60
            up = cp if (applied or not row["markdown_observed"]) else lp
            promo = promo or (applied and cp < lp)
            list_sub += lp * qty
            markdown += (lp - up) * qty
            units += qty
            item_rows.append({
                "order_id": oid, "line_no": ln, "product_id": row["product_id"], "product_name": row["product_name"],
                "category": row["category"], "subcategory": row["subcategory"], "brand": row["brand"],
                "size_cm": row["size_cm"], "qty": qty, "unit_list_price_aed": f"{lp:.2f}",
                "unit_price_aed": f"{up:.2f}", "line_total_aed": f"{up * qty:.2f}",
                "markdown_applied": "true" if (applied and cp < lp) else "false", "data_origin": "SYNTHETIC"})
        items_net = list_sub - markdown
        # delivery option / fee (policy page: standard free; express 299/599; same-day Dubai only, AED 50 under 300)
        em = o["emirate"]
        opt, fee = "standard", 0
        u = rng.random()
        if em in ("Dubai", "Sharjah", "Ajman", "Ras Al Khaimah", "Abu Dhabi") and u < 0.05:
            opt, fee = "express_next_day", (599 if em == "Abu Dhabi" else 299)
        elif em == "Dubai" and o["channel"] == "online" and 0.05 <= u < 0.11:
            opt, fee = "same_day", (50 if items_net < 300 else 0)
        total = items_net + fee
        pay_w = ({"card_visa_mastercard": 45, "apple_pay": 20, "tabby": 17, "tamara": 12, "google_pay": 4, "card_other": 2}
                 if o["channel"] == "online" else
                 {"card_visa_mastercard": 58, "apple_pay": 12, "tabby": 10, "tamara": 8, "cash": 10, "google_pay": 2})
        pay_w = dict(pay_w)
        for k in ("tabby", "tamara"):
            if k in pay_w:
                pay_w[k] *= 0.0 if total < 150 else (0.3 if total < 500 else (1.5 if total >= 2000 else 1.0))
        pay = wchoice(rng, pay_w)
        s = rng.random()
        status = "cancelled" if s < 0.020 else ("defect_return_requested" if s < 0.027 else "delivered")
        ord_rows.append({
            "order_id": oid, "order_ts": o["ts"].strftime("%Y-%m-%d %H:%M:%S"), "order_date": o["ts"].strftime("%Y-%m-%d"),
            "customer_id": cid, "channel": o["channel"], "showroom_region": o["region"], "delivery_emirate": em,
            "payment_method": pay, "delivery_option": opt, "delivery_fee_aed": f"{fee:.2f}",
            "line_count": len(o["lines"]), "units": units, "list_subtotal_aed": f"{list_sub:.2f}",
            "markdown_aed": f"{markdown:.2f}", "items_net_aed": f"{items_net:.2f}",
            "order_total_aed": f"{total:.2f}", "vat_aed": f"{total * 5 / 105:.2f}",
            "promo_flag": "true" if promo else "false", "order_status": status,
            "basket_archetype": o["arche"], "data_origin": "SYNTHETIC"})

    def write(name, rows, fields):
        with open(os.path.join(a.out, name), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
            w.writeheader()
            for r in rows:
                for v in r.values():
                    assert "," not in str(v) and '"' not in str(v), (name, v)
                w.writerow(r)

    cat_fields = ["product_id", "product_name", "category", "subcategory", "brand", "tier", "size_cm",
                  "regular_price_aed", "current_price_aed", "markdown_observed", "price_basis", "source_url",
                  "snapshot_age_days"]
    for r in cat.rows:
        r["markdown_observed"] = "true" if r["markdown_observed"] else "false"
        r["product_name"] = r["product_name"].replace(",", " -")
        r["regular_price_aed"] = f"{r['regular_price_aed']:.2f}"
        r["current_price_aed"] = f"{r['current_price_aed']:.2f}"
    for r in item_rows:
        r["product_name"] = r["product_name"].replace(",", " -")
    write("product_catalog.csv", cat.rows, cat_fields)
    write("orders.csv", ord_rows, list(ord_rows[0].keys()))
    write("order_items.csv", item_rows, list(item_rows[0].keys()))

    tot = [float(r["order_total_aed"]) for r in ord_rows]
    net = [float(r["items_net_aed"]) for r in ord_rows]
    nc = [float(r["order_total_aed"]) for r in ord_rows if r["order_status"] != "cancelled"]
    q = statistics.quantiles(tot, n=100)
    stats = {"orders": len(ord_rows), "order_items": len(item_rows), "catalog_rows": len(cat.rows),
             "aov_total_mean": round(statistics.mean(tot), 2), "aov_total_median": round(statistics.median(tot), 2),
             "aov_items_net_mean": round(statistics.mean(net), 2), "aov_excl_cancelled_mean": round(statistics.mean(nc), 2),
             "p10": round(q[9], 2), "p25": round(q[24], 2), "p75": round(q[74], 2), "p90": round(q[89], 2),
             "p95": round(q[94], 2), "p99": round(q[98], 2), "max": max(tot), "min": min(tot),
             "revenue_total_aed": round(sum(tot), 2), "size_ladder": not a.no_size_ladder, "seed": a.seed}
    with open(os.path.join(a.out, "stats.json"), "w") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
