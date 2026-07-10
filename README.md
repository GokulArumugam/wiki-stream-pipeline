# wiki-stream-pipeline

Real-time pipeline over the live Wikimedia recent-changes firehose — **SSE → Redpanda → Spark Structured Streaming → Iceberg on MinIO → Grafana** — the whole thing runs on a laptop with one command.

> 🚧 Built during a 3-day sprint, Jul 10–12 2026. See [STATE.md](STATE.md) for live progress.

```mermaid
flowchart LR
    WM[Wikimedia SSE] --> ING[Ingest] --> RP[(Redpanda)] --> SS[Spark SS] --> ICE[(Iceberg / MinIO)] --> GF[Grafana]
```

- **Architecture & decisions:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · ADRs in [docs/adr/](docs/adr/)
- **Delivery semantics:** native Iceberg streaming sinks make bronze and gold effectively-once on replay; the Kafka DLQ remains intentionally at-least-once. See the stage-by-stage contract in [ADR-0005](docs/adr/0005-delivery-semantics.md).

## Quickstart

Requirements: Docker Desktop or Colima with Docker Compose v2 and at least 4 CPU / 8 GB allocated.

```sh
make up
make verify
make spark-up
make verify-spark
```

`make up` starts Redpanda, MinIO (including its `warehouse` bucket), the Iceberg REST catalog, the resumable Wikimedia SSE ingest service, Prometheus, and Grafana. `make verify` waits up to 60 seconds for 100 broker-side events, then prints a sample payload and the six Redpanda partition counts.

Spark is deliberately opt-in so the base laptop stack remains small. `make spark-up` starts a local two-thread Spark Structured Streaming driver (2 GB limit) with the Iceberg REST catalog and a named checkpoint volume. It reads `wiki.recentchange` from the earliest checkpointed Kafka offset, routes malformed required fields to `wiki.recentchange.dlq`, appends deduplicated valid events to `local.bronze.recentchange`, and writes 2-minute-watermarked gold windows. `make verify-spark` takes two Iceberg count samples 60 seconds apart, requires bronze and gold growth, and checks the Spark freshness metric is below 300 seconds.

Useful local endpoints: Grafana at <http://localhost:3000> (`admin` / `admin`), Prometheus at <http://localhost:9090>, MinIO Console at <http://localhost:9001> (`minioadmin` / `minioadmin`), Redpanda Kafka API at `localhost:19092`, ingest metrics at <http://localhost:9108/metrics>, and Spark metrics at <http://localhost:9109/metrics>. Grafana provisions the **Wiki Stream Pipeline** dashboard automatically; its freshness panel has the 120-second SLO line.

```sh
make down  # stop the stack and retain data
make nuke  # stop the stack and remove all named volumes
```

Use `make spark-down` to stop only the opt-in Spark service while retaining its checkpoint. The Spark stage follows the blocking DQ, effectively-once, and 2-minute event-time watermark decisions in ADR-0003, ADR-0005, and ADR-0006. Bronze and all gold tables use Iceberg's native streaming sink: a replayed `(queryId, epochId)` is deduplicated by the Iceberg snapshot commit protocol. The malformed-record DLQ deliberately stays on a `foreachBatch` Kafka writer, so it is at-least-once.

## Iceberg maintenance

After the base stack is running, run the batch maintenance job as needed:

```sh
make maintain
```

It discovers every table in the `bronze` and `gold` namespaces, expires snapshots older than 24 hours while retaining the newest five, removes orphan files older than 24 hours, and compacts small files with `rewrite_data_files`. The one-off Spark container has its own driver memory, so it does not contend with the streaming driver.

## Website exports

With the stack and Spark tables running, create website artifacts with:

```sh
make export
```

This writes each gold table and a 50,000-row bronze sample as one-data-file Parquet datasets under MinIO's `s3://warehouse/exports/` prefix, copies them to the ignored local `./exports/` directory, and writes `exports/sample_events.json` from 2,000 recent raw Redpanda events. Copy these artifacts into the portfolio-site repository; they are not committed here.
