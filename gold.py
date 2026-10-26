"""Gold layer: a star schema plus analytics marts.

* dim_customer is SCD Type 2: every change creates a new version with a validity window.
* fact_order_items joins each order to the customer version that was valid *at order time*.
* Fact partitions are synced by content checksum, so only partitions whose data actually
  changed are rewritten. This also handles late-arriving data correctly.
"""
import json
import shutil
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql" / "marts"

FACT_SQL = """
SELECT o.order_id, o.line_no, o.order_date, o.order_ts, c.customer_sk, p.product_sk, o.sku, o.qty,
       o.unit_price, o.line_amount, o.status, o.currency, o.coupon_code, o.is_late
FROM read_parquet('{s}/orders.parquet') o
JOIN dim_customer c ON c.customer_id = o.customer_id AND o.order_ts >= c.valid_from AND o.order_ts < c.valid_to
JOIN dim_product p ON p.sku = o.sku"""


def _copy(con, rel: str, path: Path) -> int:
    con.execute(f"COPY (SELECT * FROM {rel}) TO '{path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]


def build_dims(root: Path, con) -> dict:
    g, s = root / "gold", root / "silver"
    g.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_customer AS
        SELECT hash(customer_id || '|' || CAST(updated_at AS VARCHAR)) AS customer_sk,
               customer_id, email, name, city, segment, loyalty_tier,
               CASE WHEN lag(updated_at) OVER w IS NULL THEN TIMESTAMP '1900-01-01' ELSE updated_at END AS valid_from,
               coalesce(lead(updated_at) OVER w, TIMESTAMP '9999-12-31') AS valid_to,
               lead(updated_at) OVER w IS NULL AS is_current
        FROM read_parquet('{s}/customers.parquet')
        WINDOW w AS (PARTITION BY customer_id ORDER BY updated_at)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_product AS
        SELECT hash(sku) AS product_sk, sku, name, category, price AS list_price
        FROM read_parquet('{s}/products.parquet')""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_date AS
        SELECT CAST(d AS DATE) AS date_key, year(d) AS year, month(d) AS month, weekofyear(d) AS week,
               dayofweek(d) AS dow, dayofweek(d) IN (0, 6) AS is_weekend
        FROM generate_series((SELECT min(order_date) FROM read_parquet('{s}/orders.parquet')),
                             (SELECT max(order_date) FROM read_parquet('{s}/orders.parquet')), INTERVAL 1 DAY) t(d)""")
    return {name: _copy(con, name, g / f"{name}.parquet") for name in ("dim_customer", "dim_product", "dim_date")}


def sync_facts(root: Path, con) -> dict:
    g, s = root / "gold", root / "silver"
    base = g / "fact_order_items"
    chk_path = root / "audit" / "partition_checksums.json"
    old = json.loads(chk_path.read_text()) if chk_path.exists() else {}
    con.execute(f"CREATE OR REPLACE TEMP TABLE fact_stage AS {FACT_SQL.format(s=s)}")
    new = {
        d: str(chk) for d, chk in con.execute(
            "SELECT CAST(order_date AS VARCHAR), sum(hash(order_id, line_no, customer_sk, product_sk, qty, unit_price, "
            "status, coupon_code, is_late)) FROM fact_stage GROUP BY 1").fetchall()
    }
    rewritten = 0
    for d, chk in new.items():
        part = base / f"order_date={d}"
        if old.get(d) == chk and (part / "part.parquet").exists():
            continue
        part.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY (SELECT * FROM fact_stage WHERE order_date = DATE '{d}' ORDER BY order_id, line_no) "
                    f"TO '{part / 'part.parquet'}' (FORMAT PARQUET)")
        rewritten += 1
    for d in set(old) - set(new):  # partitions that no longer have data
        shutil.rmtree(base / f"order_date={d}", ignore_errors=True)
    chk_path.write_text(json.dumps(new, indent=1, sort_keys=True))
    rows = con.execute("SELECT count(*) FROM fact_stage").fetchone()[0]
    return {"partitions_total": len(new), "partitions_rewritten": rewritten, "fact_rows": rows}


def build_marts(root: Path, con) -> dict:
    g = root / "gold"
    (g / "marts").mkdir(parents=True, exist_ok=True)
    rel = {
        "fact": f"read_parquet('{g}/fact_order_items/*/*.parquet')",
        "dim_customer": f"read_parquet('{g}/dim_customer.parquet')",
        "dim_product": f"read_parquet('{g}/dim_product.parquet')",
    }
    out = {}
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        query = sql_file.read_text().format(**rel)
        target = g / "marts" / f"{sql_file.stem}.parquet"
        con.execute(f"COPY ({query}) TO '{target}' (FORMAT PARQUET)")
        out[sql_file.stem] = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
    return out
