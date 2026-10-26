# Architecture notes

| Layer | Contents | Written by | Properties |
|---|---|---|---|
| raw | Daily CSV and JSONL drops from the source system | `generate` (stand-in for a real source) | Immutable input |
| bronze | Raw rows plus `_source_file` and `_batch_date`, as Parquet | `bronze.ingest` | Append-only, exactly-once via file hash |
| silver | Typed, de-duplicated, validated tables | `silver.build` | Deterministic function of bronze |
| quarantine | Rejected records with the rule broken | `silver.build` | Nothing is silently dropped |
| gold | Star schema: `dim_customer` (SCD2), `dim_product`, `dim_date`, `fact_order_items` | `gold.build_dims`, `gold.sync_facts` | Partitioned by `order_date`, synced by checksum |
| marts | Analytics tables built from SQL files | `gold.build_marts` | Published only after the quality gate passes |
| audit | `manifest.json`, `run_log.jsonl`, `dq_results.jsonl`, `partition_checksums.json` | all steps | Full run history |

Task order: bronze, silver, gold_dims, gold_facts, quality, marts.
