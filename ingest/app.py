"""Resumable Wikimedia EventStreams to Kafka-API ingest service."""

import asyncio
import json
import logging
import os
import signal
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
from confluent_kafka import KafkaException, Producer
from prometheus_client import Counter, start_http_server

EVENTS_CONSUMED = Counter("events_consumed_total", "SSE events read from Wikimedia")
EVENTS_PRODUCED = Counter("events_produced_total", "Events acknowledged by Redpanda")
SSE_RECONNECTS = Counter("sse_reconnects_total", "SSE reconnect attempts")
PRODUCE_ERRORS = Counter("produce_errors_total", "Redpanda produce failures")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        return json.dumps(payload, default=str)


def configure_logging() -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("wiki_ingest")
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    logger.handlers[:] = [handler]
    logger.propagate = False
    return logger


LOGGER = configure_logging()


def log(level: int, message: str, **fields: object) -> None:
    LOGGER.log(level, message, extra={"extra_fields": fields})


def read_checkpoint(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
        return value or None
    except FileNotFoundError:
        return None


def write_checkpoint(path: Path, event_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        temporary.write(event_id)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = temporary.name
    os.replace(temporary_path, path)


async def deliver(producer: Producer, topic: str, payload: dict, event_id: str, state_file: Path) -> None:
    """Await one broker acknowledgement before advancing the SSE checkpoint."""
    loop = asyncio.get_running_loop()
    delivered: asyncio.Future[None] = loop.create_future()

    def on_delivery(error, message) -> None:
        if error is not None:
            if not delivered.done():
                loop.call_soon_threadsafe(delivered.set_exception, KafkaException(error))
            return
        if not delivered.done():
            loop.call_soon_threadsafe(delivered.set_result, None)

    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    key = str(payload.get("wiki", "unknown")).encode("utf-8")
    while True:
        try:
            producer.produce(topic, value=encoded, key=key, on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.1)
            await asyncio.sleep(0.05)

    while not delivered.done():
        producer.poll(0)
        await asyncio.sleep(0.01)
    await delivered
    EVENTS_PRODUCED.inc()
    write_checkpoint(state_file, event_id)


async def consume(stop_event: asyncio.Event) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    topic = os.environ.get("KAFKA_TOPIC", "wiki.recentchange")
    sse_url = os.environ.get("SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    state_file = Path(os.environ.get("STATE_FILE", "/data/last-event-id"))
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "wiki-sse-ingest",
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,
            "delivery.timeout.ms": 120000,
        }
    )
    reconnect_delay = 1
    reconnecting = False
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not stop_event.is_set():
                headers = {
                    "Accept": "text/event-stream",
                    "User-Agent": "wiki-stream-pipeline/1.0 (local portfolio ingest)",
                }
                last_event_id = read_checkpoint(state_file)
                if last_event_id:
                    headers["Last-Event-ID"] = last_event_id
                if reconnecting:
                    SSE_RECONNECTS.inc()
                    log(logging.INFO, "reconnecting_to_sse", delay_seconds=reconnect_delay)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 30)
                try:
                    async with session.get(sse_url, headers=headers) as response:
                        response.raise_for_status()
                        log(logging.INFO, "sse_connected", resumed=bool(last_event_id))
                        reconnect_delay = 1
                        reconnecting = True
                        current_id: str | None = None
                        data_lines: list[str] = []
                        async for raw_line in response.content:
                            if stop_event.is_set():
                                break
                            line = raw_line.decode("utf-8").rstrip("\r\n")
                            if not line:
                                if data_lines and current_id:
                                    try:
                                        event = json.loads("\n".join(data_lines))
                                        if not isinstance(event, dict):
                                            raise ValueError("SSE data is not a JSON object")
                                        event["ingested_at"] = datetime.now(UTC).isoformat()
                                        EVENTS_CONSUMED.inc()
                                        await deliver(producer, topic, event, current_id, state_file)
                                    except (json.JSONDecodeError, ValueError, KafkaException) as error:
                                        PRODUCE_ERRORS.inc()
                                        log(logging.ERROR, "event_not_produced", error=str(error), event_id=current_id)
                                        raise
                                current_id = None
                                data_lines = []
                            elif line.startswith("id:"):
                                current_id = line[3:].lstrip()
                            elif line.startswith("data:"):
                                data_lines.append(line[5:].lstrip())
                except (aiohttp.ClientError, asyncio.TimeoutError, KafkaException, OSError) as error:
                    if stop_event.is_set():
                        break
                    log(logging.WARNING, "sse_connection_failed", error=str(error))
                    reconnecting = True
    finally:
        remaining = producer.flush(15)
        if remaining:
            log(logging.WARNING, "producer_shutdown_with_pending_messages", pending=remaining)
        else:
            log(logging.INFO, "producer_shutdown_complete")


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    metrics_port = int(os.environ.get("METRICS_PORT", "9108"))
    start_http_server(metrics_port)
    log(logging.INFO, "ingest_started", metrics_port=metrics_port)
    await consume(stop_event)


if __name__ == "__main__":
    asyncio.run(main())
