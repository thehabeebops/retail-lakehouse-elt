"""Bronze layer: raw files landed as append-only Parquet, exactly once.

Idempotency comes from a manifest keyed by the SHA-256 of each source file, so re-running
the pipeline (or a retry after a crash) never double-loads a file.
"""
import hashlib
import json
from pathlib import Path

ENTITIES = {"customers": "customers.csv", "products": "products.csv", "orders": "orders.jsonl"}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _reader(path: Path) -> str:
    if path.suffix == ".csv":
        return f"read_csv_auto('{path}', header=true, all_varchar=true)"
    return f"read_json_auto('{path}', format='newline_delimited', sample_size=-1)"


def ingest(root: Path, con, through: str | None = None) -> dict:
    manifest_path = root / "audit" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    seen = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    stats = {"files": 0, "rows": 0, "skipped_already_loaded": 0}

    for day_dir in sorted((root / "raw").glob("*")):
        if through and day_dir.name > through:
            continue
        for entity, filename in ENTITIES.items():
            src = day_dir / filename
            if not src.exists():
                continue
            digest = _sha256(src)
            if digest in seen:
                stats["skipped_already_loaded"] += 1
                continue
            out = root / "bronze" / entity / f"batch_date={day_dir.name}"
            out.mkdir(parents=True, exist_ok=True)
            target = out / "part.parquet"
            con.execute(
                f"COPY (SELECT *, '{filename}' AS _source_file, '{day_dir.name}' AS _batch_date "
                f"FROM {_reader(src)}) TO '{target}' (FORMAT PARQUET)"
            )
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
            seen[digest] = {"file": f"{day_dir.name}/{filename}", "rows": rows}
            stats["files"] += 1
            stats["rows"] += rows
    manifest_path.write_text(json.dumps(seen, indent=1, sort_keys=True))
    return stats
