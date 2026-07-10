# STATE — sprint handoff file

> Purpose: any model or agent (Claude, Codex, or a human) must be able to resume this project cold from this file. Update + push at every milestone.

**Last updated:** 2026-07-10 (Fri) — Spark stage code complete, pending live verification

## Sprint context
3-day sprint (Fri Jul 10 → Sun Jul 12). This repo is the flagship of Gokul Arumugam's DE portfolio. Master plan lives in Claude's memory + `~/projects/portfolio-site/STATE.md`. Workflow: Claude/any-orchestrator plans and reviews; heavy implementation is delegated to Codex (`/codex:rescue --model gpt-5.6-terra --effort high`), and every Codex result is inspected before acceptance.

## Status
- ✅ Architecture designed: `docs/ARCHITECTURE.md` + 6 ADRs in `docs/adr/` — these are **binding decisions**, implement to them (Redpanda, Spark Structured Streaming, Iceberg on MinIO w/ REST catalog, effectively-once semantics per ADR-0005, 2-min watermark per ADR-0006).
- ✅ Docker Compose stack + Python SSE ingest service: Redpanda topic `wiki.recentchange` (6 partitions), MinIO `warehouse` bucket, Iceberg REST catalog, Prometheus, Grafana, and resumable/idempotent Wikimedia SSE ingestion. **Verified live 2026-07-10 ~20:35 IST**: `make up` + `make verify` green, ~7.8k events produced, 0 errors/reconnects. Post-review fixes applied by Claude: rpk healthcheck flag, compose command shlex-split trap on init containers (keep `command` as single-element lists!), iceberg-rest healthcheck wget→curl, Makefile compose-ps JSON-lines parsing. Host docker = colima 4CPU/8GB; `~/.docker/config.json` must NOT have `credsStore: desktop`.
- ✅ Spark Structured Streaming stage **verified live 2026-07-10 ~22:15 IST from a cold `make nuke` start**: `make up && make spark-up && make verify-spark` → PASS (bronze 8056→10443, gold 0→132, freshness 84s < 120s SLO). Grafana dashboard "Wiki Stream Pipeline" provisions correctly. Post-review fixes this stage (details in commit): bitnami/spark image is gone from Docker Hub → apache/spark:3.5.5-python3 (ships Python 3.8 → datetime.UTC shim); REST catalog needed CATALOG_S3_PATH__STYLE__ACCESS + file-backed sqlite on a volume + user 0:0 (in-memory sqlite evaporates on pool idle → 500s); watermark redefinition after dropDuplicatesWithinWatermark crash-looped the job (Spark 3.5 forbids it); verify counts now read REST-catalog snapshot summaries from the host (second spark-submit in the 2g container OOM-kills the driver); down/nuke include --profile spark; retention trimmed (Kafka 24h, Prometheus 2d) to keep disk small per Gokul.
- ⬜ NEXT: (a) switch Iceberg sinks from foreachBatch+append (at-least-once — replays duplicate rows) to the native Iceberg streaming sink `.writeStream.format("iceberg").toTable()` which dedups replayed epochs, keeping metrics via QueryListener numInputRows — REQUIRED before the chaos demo or it will demonstrate duplicates, contradicting ADR-0005; (b) Iceberg snapshot expiry + compaction scheduled task (also keeps data small); (c) chaos demo recording (kill wiki-spark mid-batch, restart, show counts/aggregates match an uninterrupted control); (d) export gold tables as Parquet + a few thousand sample events JSON into exports/ for the website's DuckDB-WASM widget and diagram replay.

## How to run
```sh
make up
make verify
```

## Repos in this portfolio
- `portfolio-site` — Next.js site (projects + blog), deploys to Vercel
- `wiki-stream-pipeline` — this repo
- `payments-reconciliation` — starts Sunday
