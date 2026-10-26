import shutil

import duckdb
import pytest

from generate import generate
from pipeline import run
from quality import run_checks

DAYS = dict(days=12, customers=300, orders_per_day=200, seed=7)  # long enough to trigger both schema drifts


def q(sql):
    return duckdb.connect().execute(sql).fetchall()


def fact_checksum(root):
    return q(f"SELECT count(*), sum(hash(order_id, line_no, customer_sk, product_sk, qty, unit_price, status)) "
             f"FROM read_parquet('{root}/gold/fact_order_items/*/*.parquet')")[0]


@pytest.fixture()
def lake(tmp_path):
    generate(tmp_path, **DAYS)
    return tmp_path


def test_rerun_is_idempotent(lake):
    first = run(lake)
    before = fact_checksum(lake)
    second = run(lake)
    assert first["bronze"]["files"] > 0
    assert second["bronze"]["files"] == 0, "files must be loaded exactly once"
    assert second["gold_facts"]["partitions_rewritten"] == 0, "unchanged data must not rewrite partitions"
    assert fact_checksum(lake) == before


def test_incremental_equals_full_refresh(lake, tmp_path_factory):
    run(lake, through="2026-01-06")
    run(lake, through="2026-01-12")
    full = tmp_path_factory.mktemp("full")
    shutil.copytree(lake / "raw", full / "raw")
    run(full)
    assert fact_checksum(lake) == fact_checksum(full)


def test_duplicates_removed_and_bad_records_quarantined(lake):
    stats = run(lake)["silver"]
    assert stats["orders_duplicates_removed"] > 0
    assert stats["orders_rejected"] > 0
    reasons = {r[0] for r in q(f"SELECT DISTINCT reject_reason FROM read_parquet('{lake}/quarantine/orders.parquet')")}
    assert {"non_positive_qty", "missing_customer_id", "unknown_sku"} <= reasons
    leaked = q(f"SELECT count(*) FROM read_parquet('{lake}/silver/orders.parquet') WHERE order_id IN "
               f"(SELECT record_key FROM read_parquet('{lake}/quarantine/orders.parquet'))")[0][0]
    assert leaked == 0


def test_scd2_history_is_contiguous(lake):
    run(lake)
    dim = f"read_parquet('{lake}/gold/dim_customer.parquet')"
    assert q(f"SELECT count(*) FROM (SELECT customer_id FROM {dim} GROUP BY 1 HAVING count(*) > 1)")[0][0] > 0
    assert q(f"SELECT count(*) FROM (SELECT customer_id FROM {dim} WHERE is_current GROUP BY 1 HAVING count(*) <> 1)")[0][0] == 0


def test_point_in_time_join(lake):
    run(lake)
    bad = q(f"""SELECT count(*) FROM read_parquet('{lake}/gold/fact_order_items/*/*.parquet') f
                JOIN read_parquet('{lake}/gold/dim_customer.parquet') c USING (customer_sk)
                WHERE NOT (f.order_ts >= c.valid_from AND f.order_ts < c.valid_to)""")[0][0]
    assert bad == 0


def test_late_arrivals_land_in_their_event_date_partition(lake):
    run(lake)
    late = q(f"SELECT count(*) FROM read_parquet('{lake}/silver/orders.parquet') WHERE is_late")[0][0]
    assert late > 0
    misplaced = q(f"SELECT count(*) FROM read_parquet('{lake}/gold/fact_order_items/*/*.parquet', filename=true) "
                  f"WHERE filename NOT LIKE '%order_date=' || CAST(order_date AS VARCHAR) || '%'")[0][0]
    assert misplaced == 0


def test_schema_drift_is_absorbed(lake):
    run(lake)
    cols = {r[0] for r in q(f"DESCRIBE SELECT * FROM read_parquet('{lake}/silver/orders.parquet')")}
    assert "coupon_code" in cols
    assert q(f"SELECT count(*) FROM read_parquet('{lake}/silver/orders.parquet') WHERE coupon_code IS NOT NULL")[0][0] > 0
    assert q(f"SELECT count(*) FROM read_parquet('{lake}/silver/customers.parquet') WHERE loyalty_tier IS NOT NULL")[0][0] > 0


def test_quality_gate_catches_corruption(lake):
    run(lake)
    part = next((lake / "gold" / "fact_order_items").glob("order_date=*"))
    shutil.copy(part / "part.parquet", part / "part2.parquet")  # duplicate every row in one partition
    failed = [r["check"] for r in run_checks(lake, duckdb.connect()) if not r["passed"] and r["severity"] == "critical"]
    assert "fact_primary_key_unique" in failed and "fact_reconciles_to_silver" in failed
