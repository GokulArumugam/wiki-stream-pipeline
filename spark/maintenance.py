"""Apply routine Iceberg retention and compaction to every pipeline table."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


RETENTION = timedelta(hours=24)
NAMESPACES = ("bronze", "gold")
# remove_orphan_files lists the warehouse through Hadoop's FileSystem API,
# which needs hadoop-aws + fs.s3.impl wiring that the S3FileIO-based jobs
# don't. Orphans only appear after crashes and this warehouse is tiny, so
# it's opt-in rather than pulling in that dependency stack.
ORPHAN_CLEANUP = os.environ.get("ORPHAN_CLEANUP", "false").lower() == "true"


def build_spark() -> SparkSession:
    """Create a batch Spark session; catalog configuration is supplied by Make."""
    return (
        SparkSession.builder.appName("wiki-iceberg-maintenance")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def table_names(spark: SparkSession, namespace: str) -> list[str]:
    """Return every non-temporary table in one Iceberg namespace."""
    return [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN local.{namespace}").collect()
        if not row.isTemporary
    ]


def main() -> None:
    spark = build_spark()
    cutoff = datetime.now(timezone.utc) - RETENTION
    cutoff_sql = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    for namespace in NAMESPACES:
        for table_name in table_names(spark, namespace):
            table = f"{namespace}.{table_name}"
            print(f"Maintaining local.{table}", flush=True)
            spark.sql(
                "CALL local.system.expire_snapshots("
                f"table => '{table}', "
                f"older_than => TIMESTAMP '{cutoff_sql}', retain_last => 5)"
            )
            if ORPHAN_CLEANUP:
                spark.sql(
                    "CALL local.system.remove_orphan_files("
                    f"table => '{table}', older_than => TIMESTAMP '{cutoff_sql}')"
                )
            spark.sql(f"CALL local.system.rewrite_data_files(table => '{table}')")

    spark.stop()


if __name__ == "__main__":
    main()
