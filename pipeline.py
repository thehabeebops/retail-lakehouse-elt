"""A small DAG runner: dependency ordering, retries with backoff, and an audit trail per task."""
import datetime as dt
import json
import time
import uuid
from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Callable

import duckdb

from import bronze
import gold
import quality
import silver


class PipelineFailed(RuntimeError):
    """Raised when a critical data-quality gate fails. Never retried."""


@dataclass
class Task:
    name: str
    fn: Callable[[], dict]
    deps: tuple[str, ...] = ()


def _append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def run(root, through: str | None = None, retries: int = 2) -> dict:
    root = Path(root)
    con = duckdb.connect()
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    audit = root / "audit"
    run_date = through or max((p.name for p in (root / "raw").glob("*")), default=None)

    def quality_gate() -> dict:
        results = quality.run_checks(root, con, run_date)
        for r in results:
            _append(audit / "dq_results.jsonl", {"run_id": run_id, **r})
        failed = [r["check"] for r in results if r["severity"] == "critical" and not r["passed"]]
        if failed:
            raise PipelineFailed(f"critical data-quality checks failed: {', '.join(failed)}")
        return {"checks": len(results), "warnings": sum(1 for r in results if not r["passed"])}

    tasks = {
        "bronze": Task("bronze", lambda: bronze.ingest(root, con, through)),
        "silver": Task("silver", lambda: silver.build(root, con), ("bronze",)),
        "gold_dims": Task("gold_dims", lambda: gold.build_dims(root, con), ("silver",)),
        "gold_facts": Task("gold_facts", lambda: gold.sync_facts(root, con), ("gold_dims",)),
        "quality": Task("quality", quality_gate, ("gold_facts",)),  # gate runs BEFORE marts are published
        "marts": Task("marts", lambda: gold.build_marts(root, con), ("quality",)),
    }
    results: dict = {}
    for name in TopologicalSorter({n: t.deps for n, t in tasks.items()}).static_order():
        started = time.time()
        for attempt in range(1, retries + 2):
            try:
                metrics = tasks[name].fn()
                _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "ok", "attempt": attempt,
                                                  "seconds": round(time.time() - started, 3), "metrics": metrics})
                results[name] = metrics
                break
            except PipelineFailed as exc:
                _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "failed", "error": str(exc)})
                raise
            except Exception as exc:  # transient failure: retry with backoff
                if attempt > retries:
                    _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "failed", "error": repr(exc)})
                    raise
                time.sleep(0.2 * attempt)
    return results
