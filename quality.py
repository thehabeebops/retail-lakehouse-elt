"""Data quality gates. Critical failures stop the pipeline; warnings are recorded."""
from pathlib import Path


def run_checks(root: Path, con, run_date: str | None = None) -> list[dict]:
    g, s, q = root / "gold", root / "silver", root / "quarantine"
    fact = f"read_parquet('{g}/fact_order_items/*/*.parquet')"
    dim_c = f"read_parquet('{g}/dim_customer.parquet')"
    dim_p = f"read_parquet('{g}/dim_product.parquet')"
    silver_orders = f"read_parquet('{s}/orders.parquet')"
    checks = [
        ("silver_order_lines_unique", "critical", f"SELECT count(*) - count(DISTINCT (order_id, line_no)) FROM {silver_orders}", 0),
        ("fact_primary_key_unique", "critical", f"SELECT count(*) - count(DISTINCT (order_id, line_no)) FROM {fact}", 0),
        ("fact_reconciles_to_silver", "critical", f"SELECT abs((SELECT count(*) FROM {silver_orders}) - (SELECT count(*) FROM {fact}))", 0),
        ("fact_orphan_customers", "critical", f"SELECT count(*) FROM {fact} f ANTI JOIN {dim_c} c USING (customer_sk)", 0),
        ("fact_orphan_products", "critical", f"SELECT count(*) FROM {fact} f ANTI JOIN {dim_p} p USING (product_sk)", 0),
        ("scd2_no_gaps_or_overlaps", "critical",
         f"SELECT count(*) FROM (SELECT valid_from, lag(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from) AS prev_to "
         f"FROM {dim_c}) WHERE prev_to IS NOT NULL AND prev_to <> valid_from", 0),
        ("scd2_single_current_row", "critical",
         f"SELECT count(*) FROM (SELECT customer_id FROM {dim_c} WHERE is_current GROUP BY 1 HAVING count(*) <> 1)", 0),
        ("order_reject_rate_pct", "warn",
         f"SELECT round(100.0 * (SELECT count(*) FROM read_parquet('{q}/orders.parquet')) / "
         f"((SELECT count(*) FROM read_parquet('{q}/orders.parquet')) + (SELECT count(DISTINCT order_id) FROM {silver_orders})), 2)", 5.0),
    ]
    if run_date:
        checks.append(("freshness_days_behind", "warn",
                       f"SELECT date_diff('day', max(CAST(_batch_date AS DATE)), DATE '{run_date}') FROM {silver_orders}", 1))
    results = []
    for name, severity, sql, threshold in checks:
        value = con.execute(sql).fetchone()[0]
        results.append({"check": name, "severity": severity, "value": float(value), "threshold": threshold, "passed": value <= threshold})
    return results
