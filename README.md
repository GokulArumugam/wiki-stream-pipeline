# wiki-stream-pipeline

Real-time pipeline over the live Wikimedia recent-changes firehose — **SSE → Redpanda → Spark Structured Streaming → Iceberg on MinIO → Grafana** — the whole thing runs on a laptop with one command.

> 🚧 Built during a 3-day sprint, Jul 10–12 2026. See [STATE.md](STATE.md) for live progress.

```mermaid
flowchart LR
    WM[Wikimedia SSE] --> ING[Ingest] --> RP[(Redpanda)] --> SS[Spark SS] --> ICE[(Iceberg / MinIO)] --> GF[Grafana]
```

- **Architecture & decisions:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · ADRs in [docs/adr/](docs/adr/)
- **Delivery semantics:** effectively-once into gold tables, stage-by-stage contract in [ADR-0005](docs/adr/0005-delivery-semantics.md)

## Quickstart

Requirements: Docker Desktop or Colima with Docker Compose v2 and at least 4 CPU / 8 GB allocated.

```sh
make up
make verify
```

`make up` starts Redpanda, MinIO (including its `warehouse` bucket), the Iceberg REST catalog, the resumable Wikimedia SSE ingest service, Prometheus, and Grafana. `make verify` waits up to 60 seconds for 100 broker-side events, then prints a sample payload and the six Redpanda partition counts.

Useful local endpoints: Grafana at <http://localhost:3000> (`admin` / `admin`), Prometheus at <http://localhost:9090>, MinIO Console at <http://localhost:9001> (`minioadmin` / `minioadmin`), Redpanda Kafka API at `localhost:19092`, and ingest metrics at <http://localhost:9108/metrics>.

```sh
make down  # stop the stack and retain data
make nuke  # stop the stack and remove all named volumes
```

The next stage is the Spark Structured Streaming job: a blocking DQ gate to bronze, then watermark-bounded gold aggregates using the 2-minute policy and effectively-once checkpoint/Iceberg commits specified in ADR-0003, ADR-0005, and ADR-0006.
