import argparse
import json
from pathlib import Path

import duckdb

from .generate import generate
from .pipeline import run


def _report(root: Path) -> None:
    log = [json.loads(line) for line in (root / "audit" / "run_log.jsonl").read_text().splitlines()]
    last = log[-1]["run_id"]
    print(f"Last run: {last}")
    for rec in (r for r in log if r["run_id"] == last):
        print(f"  {rec['task']:<11} {rec['status']:<6} {rec.get('seconds', '-'):>7}s  {rec.get('metrics', rec.get('error'))}")
    dq = [json.loads(line) for line in (root / "audit" / "dq_results.jsonl").read_text().splitlines()]
    print("\nData quality (this run):")
    for r in (d for d in dq if d["run_id"] == last):
        print(f"  {'PASS' if r['passed'] else 'FAIL':<5} {r['severity']:<9} {r['check']:<30} value={r['value']} (limit {r['threshold']})")
    con = duckdb.connect()
    marts = root / "gold" / "marts"
    print("\nRevenue by segment, as of order time (first rows):")
    for row in con.execute(f"SELECT * FROM read_parquet('{marts}/revenue_by_segment_asof.parquet') LIMIT 6").fetchall():
        print("  ", row)


def main() -> None:
    p = argparse.ArgumentParser(prog="lakehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="create synthetic raw source drops")
    g.add_argument("--root", default="lake")
    g.add_argument("--days", type=int, default=14)
    g.add_argument("--customers", type=int, default=2000)
    g.add_argument("--orders-per-day", type=int, default=1500)
    g.add_argument("--seed", type=int, default=42)
    r = sub.add_parser("run", help="run the pipeline")
    r.add_argument("--root", default="lake")
    r.add_argument("--through", help="only ingest raw drops up to this date (YYYY-MM-DD)")
    rep = sub.add_parser("report", help="print the last run, quality results and a mart sample")
    rep.add_argument("--root", default="lake")
    a = p.parse_args()
    if a.cmd == "generate":
        generate(a.root, days=a.days, customers=a.customers, orders_per_day=a.orders_per_day, seed=a.seed)
        print(f"generated {a.days} daily drops in {a.root}/raw")
    elif a.cmd == "run":
        print(json.dumps(run(a.root, through=a.through), indent=1))
    else:
        _report(Path(a.root))


if __name__ == "__main__":
    main()
