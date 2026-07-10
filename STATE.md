# STATE — sprint handoff file

> Purpose: any model or agent (Claude, Codex, or a human) must be able to resume this project cold from this file. Update + push at every milestone.

**Last updated:** 2026-07-10 (Fri) — compose + ingest stage

## Sprint context
3-day sprint (Fri Jul 10 → Sun Jul 12). This repo is the flagship of Gokul Arumugam's DE portfolio. Master plan lives in Claude's memory + `~/projects/portfolio-site/STATE.md`. Workflow: Claude/any-orchestrator plans and reviews; heavy implementation is delegated to Codex (`/codex:rescue --model gpt-5.6-terra --effort high`), and every Codex result is inspected before acceptance.

## Status
- ✅ Architecture designed: `docs/ARCHITECTURE.md` + 6 ADRs in `docs/adr/` — these are **binding decisions**, implement to them (Redpanda, Spark Structured Streaming, Iceberg on MinIO w/ REST catalog, effectively-once semantics per ADR-0005, 2-min watermark per ADR-0006).
- ✅ Docker Compose stack + Python SSE ingest service: Redpanda topic `wiki.recentchange` (6 partitions), MinIO `warehouse` bucket, Iceberg REST catalog, Prometheus, Grafana, and resumable/idempotent Wikimedia SSE ingestion. **Verified live 2026-07-10 ~20:35 IST**: `make up` + `make verify` green, ~7.8k events produced, 0 errors/reconnects. Post-review fixes applied by Claude: rpk healthcheck flag, compose command shlex-split trap on init containers (keep `command` as single-element lists!), iceberg-rest healthcheck wget→curl, Makefile compose-ps JSON-lines parsing. Host docker = colima 4CPU/8GB; `~/.docker/config.json` must NOT have `credsStore: desktop`.
- ⬜ NEXT: Spark Structured Streaming job per ADR-0003/0005/0006 — blocking DQ gate to bronze, then 2-minute-watermark windowed aggregates to Iceberg gold with checkpoint + atomic commit semantics.
- ⬜ Then: Grafana dashboards, chaos demo recording, Parquet/sample-JSON export for the website.

## Next task spec (ready to hand to Codex)
Add the Spark Structured Streaming stage per ADR-0003/0005/0006: (1) `spark/` job (PySpark, containerized, added to compose but NOT run by `make up` by default — `make spark-up` target): read `wiki.recentchange` from earliest committed offset with checkpointing to a volume; (2) blocking DQ gate — schema-validate; malformed → `wiki.recentchange.dlq` topic with reason, valid → append Iceberg `bronze.recentchange` via the REST catalog (s3://warehouse on MinIO); (3) dedup on `meta.id` within 2-min watermark; gold tables: `gold.edits_per_minute_by_wiki` (1-min tumbling), `gold.bot_vs_human_per_minute`, `gold.top_pages_10min`, plus `gold.late_arrivals` count; all windows event-time on `meta.dt`, 2-min watermark; (4) `make verify-spark` asserting bronze + gold row counts grow across two checks; (5) Grafana provisioned dashboard reading gold via... (decide: Grafana Infinity plugin vs exporting metrics — orchestrator to decide before dispatch). Acceptance: `make up && make spark-up && make verify-spark` green on this machine.

## How to run
```sh
make up
make verify
```

## Repos in this portfolio
- `portfolio-site` — Next.js site (projects + blog), deploys to Vercel
- `wiki-stream-pipeline` — this repo
- `payments-reconciliation` — starts Sunday
