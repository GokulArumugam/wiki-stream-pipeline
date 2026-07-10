# STATE — sprint handoff file

> Purpose: any model or agent (Claude, Codex, or a human) must be able to resume this project cold from this file. Update + push at every milestone.

**Last updated:** 2026-07-10 (Fri) — by Claude (Fable 5), architecture phase

## Sprint context
3-day sprint (Fri Jul 10 → Sun Jul 12). This repo is the flagship of Gokul Arumugam's DE portfolio. Master plan lives in Claude's memory + `~/projects/portfolio-site/STATE.md`. Workflow: Claude/any-orchestrator plans and reviews; heavy implementation is delegated to Codex (`/codex:rescue --model gpt-5.6-terra --effort high`), and every Codex result is inspected before acceptance.

## Status
- ✅ Architecture designed: `docs/ARCHITECTURE.md` + 6 ADRs in `docs/adr/` — these are **binding decisions**, implement to them (Redpanda, Spark Structured Streaming, Iceberg on MinIO w/ REST catalog, effectively-once semantics per ADR-0005, 2-min watermark per ADR-0006).
- ⬜ NEXT: Docker Compose stack + Python SSE ingest service (Codex task; see "Next task spec" below).
- ⬜ Then: Spark job (DQ gate → bronze; windowed aggregates → gold), Grafana dashboards, chaos demo recording, Parquet/sample-JSON export for the website.

## Next task spec (ready to hand to Codex)
Build in this repo: (1) `docker-compose.yml` with redpanda, minio, iceberg REST catalog, spark, prometheus, grafana — laptop-sized memory limits; (2) `ingest/` Python service: consume https://stream.wikimedia.org/v2/stream/recentchange (SSE), resume via Last-Event-ID, idempotent Kafka producer, acks=all, produce raw JSON to topic `wiki.recentchange` keyed by `wiki`, Prometheus metrics (events consumed/produced, lag, reconnects); (3) `make up`, `make verify` (asserts events flowing broker-side), `make down`. Acceptance: fresh clone + `make up` + `make verify` passes.

## How to run
Nothing runnable yet — docs only. After next task: `make up`.

## Repos in this portfolio
- `portfolio-site` — Next.js site (projects + blog), deploys to Vercel
- `wiki-stream-pipeline` — this repo
- `payments-reconciliation` — starts Sunday
