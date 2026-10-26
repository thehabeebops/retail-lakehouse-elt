"""Silver layer: typed, de-duplicated, validated data. Bad records go to quarantine with a reason.

Silver is rebuilt deterministically from bronze on every run, so it is always a pure function
of what has been ingested. Nothing is silently dropped: every rejected record is counted
and stored with the rule it broke.
"""
from pathlib import Path


def _cols(con, rel: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {rel}").fetchall()}


def _write(con, sql: str, path: Path) -> int:
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


def build(root: Path, con) -> dict:
    b, s, q = root / "bronze", root / "silver", root / "quarantine"
    s.mkdir(parents=True, exist_ok=True)
    q.mkdir(parents=True, exist_ok=True)
    stats: dict = {}

    # ---- customers ----------------------------------------------------------
    con.execute(f"CREATE OR REPLACE TEMP VIEW b_customers AS SELECT * FROM read_parquet('{b}/customers/*/*.parquet', union_by_name=true)")
    loyalty = "loyalty_tier" if "loyalty_tier" in _cols(con, "b_customers") else "NULL"  # schema drift
    con.execute(rf"""
        CREATE OR REPLACE TEMP TABLE c_checked AS
        SELECT customer_id, email, name, city, segment, {loyalty}::VARCHAR AS loyalty_tier,
               try_cast(updated_at AS TIMESTAMP) AS updated_at, _batch_date,
               CASE WHEN customer_id IS NULL OR customer_id = '' THEN 'missing_customer_id'
                    WHEN try_cast(updated_at AS TIMESTAMP) IS NULL THEN 'bad_updated_at'
                    WHEN email IS NULL OR NOT regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$') THEN 'invalid_email'
               END AS reject_reason
        FROM b_customers""")
    stats["customers_in"] = con.execute("SELECT count(*) FROM c_checked").fetchone()[0]
    stats["customers_out"] = _write(con, """
        SELECT * EXCLUDE (reject_reason) FROM c_checked WHERE reject_reason IS NULL
        QUALIFY row_number() OVER (PARTITION BY customer_id, updated_at ORDER BY _batch_date DESC) = 1""", s / "customers.parquet")
    _write(con, "SELECT 'customers' AS entity, customer_id AS record_key, reject_reason, _batch_date FROM c_checked WHERE reject_reason IS NOT NULL", q / "customers.parquet")

    # ---- products -----------------------------------------------------------
    con.execute(f"CREATE OR REPLACE TEMP VIEW b_products AS SELECT * FROM read_parquet('{b}/products/*/*.parquet', union_by_name=true)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE p_checked AS
        SELECT sku, name, category, try_cast(price AS DOUBLE) AS price, _batch_date,
               CASE WHEN sku IS NULL OR sku = '' THEN 'missing_sku'
                    WHEN try_cast(price AS DOUBLE) IS NULL OR try_cast(price AS DOUBLE) <= 0 THEN 'bad_price' END AS reject_reason
        FROM b_products""")
    stats["products_out"] = _write(con, """
        SELECT * EXCLUDE (reject_reason) FROM p_checked WHERE reject_reason IS NULL
        QUALIFY row_number() OVER (PARTITION BY sku ORDER BY _batch_date DESC) = 1""", s / "products.parquet")
    _write(con, "SELECT 'products' AS entity, sku AS record_key, reject_reason, _batch_date FROM p_checked WHERE reject_reason IS NOT NULL", q / "products.parquet")

    # ---- orders -------------------------------------------------------------
    con.execute(f"CREATE OR REPLACE TEMP VIEW b_orders AS SELECT * FROM read_parquet('{b}/orders/*/*.parquet', union_by_name=true)")
    coupon = "coupon_code" if "coupon_code" in _cols(con, "b_orders") else "NULL"  # schema drift
    stats["orders_in"] = con.execute("SELECT count(*) FROM b_orders").fetchone()[0]
    # First arrival wins: a re-sent order never overwrites the original.
    con.execute("""CREATE OR REPLACE TEMP TABLE o_dedup AS SELECT * FROM b_orders
                   QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _batch_date ASC) = 1""")
    stats["orders_duplicates_removed"] = stats["orders_in"] - con.execute("SELECT count(*) FROM o_dedup").fetchone()[0]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE o_flat AS
        WITH u AS (SELECT order_id, customer_id, order_ts, status, currency, {coupon}::VARCHAR AS coupon_code,
                          _batch_date, unnest(items) AS it FROM o_dedup)
        SELECT order_id, customer_id, try_cast(order_ts AS TIMESTAMP) AS order_ts, status, currency, coupon_code, _batch_date,
               it.sku AS sku, try_cast(it.qty AS INTEGER) AS qty, try_cast(it.unit_price AS DOUBLE) AS unit_price
        FROM u""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE o_checked AS
        SELECT f.*, row_number() OVER (PARTITION BY order_id ORDER BY sku, unit_price, qty) AS line_no,
               CASE WHEN customer_id IS NULL OR customer_id = '' THEN 'missing_customer_id'
                    WHEN customer_id NOT IN (SELECT customer_id FROM read_parquet('{s}/customers.parquet')) THEN 'unknown_customer'
                    WHEN order_ts IS NULL THEN 'bad_order_ts'
                    WHEN status NOT IN ('placed','paid','shipped','delivered','cancelled') THEN 'bad_status'
                    WHEN qty IS NULL OR qty <= 0 THEN 'non_positive_qty'
                    WHEN unit_price IS NULL OR unit_price < 0 THEN 'bad_price'
                    WHEN sku NOT IN (SELECT sku FROM read_parquet('{s}/products.parquet')) THEN 'unknown_sku'
               END AS line_reason
        FROM o_flat f""")
    con.execute("CREATE OR REPLACE TEMP TABLE o_bad AS SELECT order_id, min(line_reason) AS reject_reason FROM o_checked WHERE line_reason IS NOT NULL GROUP BY order_id")
    stats["orders_rejected"] = con.execute("SELECT count(*) FROM o_bad").fetchone()[0]
    stats["order_lines_out"] = _write(con, """
        SELECT order_id, line_no, customer_id, order_ts, CAST(order_ts AS DATE) AS order_date, status, currency, coupon_code,
               sku, qty, unit_price, round(qty * unit_price, 2) AS line_amount,
               date_diff('day', CAST(order_ts AS DATE), CAST(_batch_date AS DATE)) >= 2 AS is_late, _batch_date
        FROM o_checked WHERE order_id NOT IN (SELECT order_id FROM o_bad)""", s / "orders.parquet")
    _write(con, """SELECT 'orders' AS entity, b.order_id AS record_key, b.reject_reason, min(c._batch_date) AS _batch_date
                   FROM o_bad b JOIN o_checked c USING (order_id) GROUP BY ALL""", q / "orders.parquet")
    return stats
