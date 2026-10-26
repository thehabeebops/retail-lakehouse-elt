"""Synthetic retail source system.

The data is synthetic, but deliberately messy in the ways real source systems are:
duplicate re-sends, late-arriving orders, invalid records, a customer dimension that
changes over time, and schema drift (new columns appear mid-stream).
"""
import csv
import datetime as dt
import json
import random
from pathlib import Path

CITIES = ["Lagos", "Abuja", "Ibadan", "Accra", "Nairobi", "Kigali", "Cairo", "Cape Town"]
SEGMENTS = ["standard", "premium", "vip"]
CATEGORIES = ["electronics", "home", "fashion", "grocery", "beauty", "sports"]
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]


def _write_csv(path: Path, rows: list[dict]) -> None:
    cols = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def _item(rnd: random.Random, products: list[dict]) -> dict:
    p = rnd.choice(products)
    return {"sku": p["sku"], "qty": rnd.randint(1, 4), "unit_price": p["price"]}


def generate(root, start="2026-01-01", days=14, customers=2000, orders_per_day=1500, seed=42) -> None:
    """Write one raw drop per day under <root>/raw/<date>/ (customers.csv, products.csv, orders.jsonl)."""
    rnd = random.Random(seed)
    root = Path(root)
    first = dt.date.fromisoformat(start)
    products = [
        {"sku": f"SKU{i:04d}", "name": f"Product {i}", "category": rnd.choice(CATEGORIES), "price": round(rnd.uniform(2, 400), 2)}
        for i in range(120)
    ]
    cust = [
        {"customer_id": f"C{i:05d}", "email": f"user{i}@example.com", "name": f"Customer {i}",
         "city": rnd.choice(CITIES), "segment": rnd.choice(SEGMENTS)}
        for i in range(customers)
    ]
    previous_day_orders: list[dict] = []
    order_seq = 0

    for d in range(days):
        day = first + dt.timedelta(days=d)
        out = root / "raw" / day.isoformat()
        out.mkdir(parents=True, exist_ok=True)

        # Customers: full snapshot on day 0, then only the records that changed.
        changed = cust if d == 0 else rnd.sample(cust, k=max(1, int(len(cust) * 0.03)))
        rows = []
        for c in changed:
            if d:
                if rnd.random() < 0.5:
                    c["city"] = rnd.choice(CITIES)
                else:
                    c["segment"] = rnd.choice(SEGMENTS)
            r = dict(c, updated_at=f"{day}T{rnd.randint(0, 9):02d}:00:00")
            if rnd.random() < 0.01:
                r["email"] = "not-an-email"
            if d >= 10:  # schema drift: a new column appears
                r["loyalty_tier"] = rnd.choice(["bronze", "silver", "gold"])
            rows.append(r)
        _write_csv(out / "customers.csv", rows)

        # Products: initial catalogue, then a price revision on day 7.
        if d in (0, 7):
            if d:
                for p in rnd.sample(products, 10):
                    p["price"] = round(p["price"] * rnd.uniform(0.8, 1.2), 2)
            _write_csv(out / "products.csv", products)

        # Orders: new orders, a few late arrivals, a few defects, plus re-sent duplicates.
        todays = []
        for _ in range(orders_per_day):
            order_seq += 1
            late = rnd.random() < 0.02
            ts_day = day - dt.timedelta(days=rnd.randint(2, 5)) if late else day
            o = {
                "order_id": f"O{order_seq:07d}",
                "customer_id": rnd.choice(cust)["customer_id"],
                "order_ts": f"{ts_day}T{rnd.randint(0, 23):02d}:{rnd.randint(0, 59):02d}:00",
                "status": rnd.choice(STATUSES),
                "currency": "USD",
                "items": [_item(rnd, products) for _ in range(rnd.randint(1, 4))],
            }
            if d >= 8:  # schema drift: a new field appears
                o["coupon_code"] = rnd.choice([None, "WELCOME10", "FREESHIP", "VIP5"])
            x = rnd.random()
            if x < 0.010:
                o["items"][0]["qty"] = rnd.choice([0, -1])
            elif x < 0.015:
                o["customer_id"] = None
            elif x < 0.020:
                o["items"][0]["sku"] = "SKU9999"
            todays.append(o)
        duplicates = rnd.sample(previous_day_orders, k=int(len(previous_day_orders) * 0.03)) if previous_day_orders else []
        with open(out / "orders.jsonl", "w") as f:
            for o in todays + duplicates:
                f.write(json.dumps(o) + "\n")
        previous_day_orders = todays
