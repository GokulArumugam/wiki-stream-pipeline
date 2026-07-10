"""Kafka to Iceberg Structured Streaming pipeline for Wikimedia changes.

Each sink owns a distinct checkpoint. Kafka offsets only advance after its
Iceberg (or DLQ Kafka) write succeeds, so a failed micro-batch is replayed.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

UTC = timezone.utc  # datetime.UTC needs Python 3.11; the Spark image ships 3.8

from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


KAFKA_BOOTSTRAP = "redpanda:9092"
SOURCE_TOPIC = "wiki.recentchange"
DLQ_TOPIC = "wiki.recentchange.dlq"
CHECKPOINT_ROOT = "/opt/spark/checkpoints"
WATERMARK = "2 minutes"
TRIGGER = "15 seconds"

ROWS_WRITTEN = Counter(
    "spark_rows_written", "Rows successfully written by Spark", ["table"]
)
LAST_BATCH_DURATION = Gauge(
    "spark_last_batch_duration_seconds", "Most recent micro-batch duration", ["query"]
)
FRESHNESS = Gauge(
    "spark_pipeline_freshness_seconds", "Seconds since the newest event time"
)
DLQ_RECORDS = Counter("dlq_records", "Rejected records written to the DLQ")
LATE_ARRIVALS = Counter("late_arrivals", "Events more than two minutes late")

latest_event_time: datetime | None = None
latest_event_lock = threading.Lock()


def build_spark() -> SparkSession:
    """Create the session; package/catalog configuration is supplied by compose."""
    return (
        SparkSession.builder.appName("wiki-stream-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )


def create_tables(spark: SparkSession) -> None:
    for namespace in ("bronze", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.bronze.recentchange (
          event_time TIMESTAMP, ingested_at TIMESTAMP, wiki STRING, type STRING,
          title STRING, user STRING, bot BOOLEAN, server_name STRING,
          meta_id STRING, raw_json STRING
        ) USING iceberg
        PARTITIONED BY (days(event_time), wiki)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.gold.edits_per_minute_by_wiki (
          window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING, edit_count BIGINT
        ) USING iceberg PARTITIONED BY (days(window_start), wiki)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.gold.bot_vs_human_per_minute (
          window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING,
          bot BOOLEAN, edit_count BIGINT
        ) USING iceberg PARTITIONED BY (days(window_start), wiki)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.gold.top_pages_10min (
          window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING,
          title STRING, edit_count BIGINT
        ) USING iceberg PARTITIONED BY (days(window_start), wiki)
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.gold.late_arrivals (
          window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING,
          late_event_count BIGINT
        ) USING iceberg PARTITIONED BY (days(window_start), wiki)
        """
    )


def start_freshness_updater() -> None:
    """Keep the gauge honest while no new micro-batch is completing."""

    def update() -> None:
        while True:
            with latest_event_lock:
                newest = latest_event_time
            if newest is not None:
                FRESHNESS.set(max(0.0, (datetime.now(UTC) - newest).total_seconds()))
            time.sleep(5)

    threading.Thread(target=update, name="freshness-updater", daemon=True).start()


class QueryMetricsListener(StreamingQueryListener):
    """Exports driver-side timing independently of the sink implementation."""

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark callback API
        return None

    def onQueryProgress(self, event) -> None:  # noqa: N802 - Spark callback API
        progress = event.progress
        duration_ms = progress.durationMs.get("triggerExecution", 0)
        LAST_BATCH_DURATION.labels(query=progress.name or progress.id).set(
            float(duration_ms) / 1000.0
        )

    def onQueryTerminated(self, event) -> None:  # noqa: N802 - Spark callback API
        return None

    def onQueryIdle(self, event) -> None:  # noqa: N802 - Spark 3.5 callback API
        return None


def kafka_source(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
        .select(F.col("value").cast("string").alias("raw_json"))
    )


def parsed_records(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    schema = StructType(
        [
            StructField(
                "meta",
                StructType(
                    [StructField("id", StringType()), StructField("dt", StringType())]
                ),
            ),
            StructField("wiki", StringType()),
            StructField("type", StringType()),
            StructField("title", StringType()),
            StructField("user", StringType()),
            StructField("bot", BooleanType()),
            StructField("server_name", StringType()),
            StructField("ingested_at", StringType()),
        ]
    )
    parsed = raw.withColumn("payload", F.from_json("raw_json", schema))
    event_time = F.to_timestamp(F.col("payload.meta.dt"))
    ingested_at = F.coalesce(F.to_timestamp(F.col("payload.ingested_at")), F.current_timestamp())
    # `from_json` can coerce JSON scalars into Spark strings. Check the raw
    # token too: the contract requires these fields to be JSON strings.
    meta_id_is_string = F.col("raw_json").rlike(r'"meta"\s*:\s*\{[^{}]*"id"\s*:\s*"')
    wiki_is_string = F.col("raw_json").rlike(r'"wiki"\s*:\s*"')
    type_is_string = F.col("raw_json").rlike(r'"type"\s*:\s*"')
    reason = F.concat_ws(
        "; ",
        F.when(F.col("payload").isNull(), F.lit("invalid_json")),
        F.when(
            (F.trim(F.coalesce(F.col("payload.meta.id"), F.lit(""))) == "")
            | ~meta_id_is_string,
            F.lit("missing_or_invalid_meta.id"),
        ),
        F.when(event_time.isNull(), F.lit("missing_or_invalid_meta.dt")),
        F.when(
            (F.trim(F.coalesce(F.col("payload.wiki"), F.lit(""))) == "") | ~wiki_is_string,
            F.lit("missing_or_invalid_wiki"),
        ),
        F.when(
            (F.trim(F.coalesce(F.col("payload.type"), F.lit(""))) == "") | ~type_is_string,
            F.lit("missing_or_invalid_type"),
        ),
    )
    checked = parsed.withColumn("reason", reason).withColumn("event_time", event_time).withColumn(
        "ingested_at", ingested_at
    )
    invalid = checked.filter(F.col("reason") != "").select(
        "raw_json", "reason", "ingested_at"
    )
    valid = checked.filter(F.col("reason") == "").select(
        "event_time",
        "ingested_at",
        F.col("payload.wiki").alias("wiki"),
        F.col("payload.type").alias("type"),
        F.col("payload.title").alias("title"),
        F.col("payload.user").alias("user"),
        F.coalesce(F.col("payload.bot"), F.lit(False)).alias("bot"),
        F.col("payload.server_name").alias("server_name"),
        F.col("payload.meta.id").alias("meta_id"),
        "raw_json",
    )
    return valid, invalid


def write_iceberg(table: str, count_late: bool = False):
    """Return a foreachBatch writer with metrics after a successful append."""

    def writer(batch: DataFrame, _: int) -> None:
        global latest_event_time
        batch.cache()
        try:
            row_count = batch.count()
            if row_count:
                batch.writeTo(table).append()
                ROWS_WRITTEN.labels(table=table).inc(row_count)
                if count_late:
                    late_events = batch.agg(
                        F.coalesce(F.sum("late_event_count"), F.lit(0)).alias("count")
                    ).first()["count"]
                    LATE_ARRIVALS.inc(int(late_events))
                if "event_time" in batch.columns:
                    newest = batch.agg(F.max("event_time").alias("newest")).first()["newest"]
                    if newest is not None:
                        if newest.tzinfo is None:
                            newest = newest.replace(tzinfo=UTC)
                        with latest_event_lock:
                            if latest_event_time is None or newest > latest_event_time:
                                latest_event_time = newest
        finally:
            batch.unpersist()

    return writer


def write_dlq(batch: DataFrame, _: int) -> None:
    batch.cache()
    try:
        row_count = batch.count()
        if row_count:
            payload = batch.select(
                F.to_json(F.struct("reason", "raw_json", "ingested_at")).cast("string").alias("value")
            )
            (
                payload.write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", DLQ_TOPIC)
                .save()
            )
            DLQ_RECORDS.inc(row_count)
    finally:
        batch.unpersist()


def stream_to_iceberg(frame: DataFrame, table: str, checkpoint: str, *, late: bool = False):
    return (
        frame.writeStream.queryName(table.rsplit(".", 1)[-1])
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{checkpoint}")
        .trigger(processingTime=TRIGGER)
        .foreachBatch(write_iceberg(table, count_late=late))
        .start()
    )


def main() -> None:
    start_http_server(9109)
    start_freshness_updater()
    spark = build_spark()
    spark.streams.addListener(QueryMetricsListener())
    create_tables(spark)

    valid, invalid = parsed_records(kafka_source(spark))
    deduplicated = valid.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(
        ["meta_id"]
    )

    queries = [
        (
            invalid.writeStream.queryName("dlq")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq")
            .trigger(processingTime=TRIGGER)
            .foreachBatch(write_dlq)
            .start()
        ),
        stream_to_iceberg(deduplicated, "local.bronze.recentchange", "bronze"),
    ]

    # Every aggregate is a separate streaming query, giving each Iceberg sink
    # its own atomic-commit and Kafka-offset checkpoint boundary. The watermark
    # set before dedup propagates; redefining it after a stateful operator is
    # disallowed in Spark 3.5.
    event_time = deduplicated
    edits = (
        event_time.groupBy(F.window("event_time", "1 minute"), "wiki")
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki",
            F.col("count").alias("edit_count"),
        )
    )
    bots = (
        event_time.groupBy(F.window("event_time", "1 minute"), "wiki", "bot")
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki",
            "bot",
            F.col("count").alias("edit_count"),
        )
    )
    top_pages = (
        event_time.groupBy(F.window("event_time", "10 minutes"), "wiki", "title")
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki",
            "title",
            F.col("count").alias("edit_count"),
        )
    )
    # Late is a business classification based on ingest skew, rather than an
    # inference from records Spark discards after a watermark has advanced.
    late = event_time.filter(F.col("ingested_at") > F.col("event_time") + F.expr("INTERVAL 2 MINUTES"))
    late = (
        late.groupBy(F.window("event_time", "1 minute"), "wiki")
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki",
            F.col("count").alias("late_event_count"),
        )
    )
    queries.extend(
        [
            stream_to_iceberg(edits, "local.gold.edits_per_minute_by_wiki", "gold-edits"),
            stream_to_iceberg(bots, "local.gold.bot_vs_human_per_minute", "gold-bots"),
            stream_to_iceberg(top_pages, "local.gold.top_pages_10min", "gold-pages"),
            stream_to_iceberg(late, "local.gold.late_arrivals", "gold-late", late=True),
        ]
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
