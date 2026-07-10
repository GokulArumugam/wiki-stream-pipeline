# wiki-stream-pipeline

Real-time pipeline over the live Wikimedia recent-changes firehose — **SSE → Redpanda → Spark Structured Streaming → Iceberg on MinIO → Grafana** — the whole thing runs on a laptop with one command.

> 🚧 Built during a 3-day sprint, Jul 10–12 2026. See [STATE.md](STATE.md) for live progress.

```mermaid
flowchart LR
    WM[Wikimedia SSE] --> ING[Ingest] --> RP[(Redpanda)] --> SS[Spark SS] --> ICE[(Iceberg / MinIO)] --> GF[Grafana]
```

- **Architecture & decisions:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · ADRs in [docs/adr/](docs/adr/)
- **Delivery semantics:** effectively-once into gold tables, stage-by-stage contract in [ADR-0005](docs/adr/0005-delivery-semantics.md)
- **Failure modes & chaos demo, cost section, quickstart:** coming as the sprint lands them
