"""Export website-ready Parquet snapshots from the Iceberg pipeline tables.

Writes to a local directory (bind-mounted to the host by `make export`)
rather than back to MinIO: plain-parquet writes to s3a:// would drag in the
hadoop-aws stack, and copying out of MinIO's on-disk layout isn't valid.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


EXPORT_ROOT = os.environ.get("EXPORT_DIR", "/out")
GOLD_TABLES = (
    "edits_per_minute_by_wiki",
    "bot_vs_human_per_minute",
    "top_pages_10min",
    "late_arrivals",
)


def build_spark() -> SparkSession:
    """Create a batch Spark session; catalog configuration is supplied by Make."""
    return (
        SparkSession.builder.appName("wiki-data-export")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_single_parquet(frame, name: str) -> None:
    """Write one data file per dataset under the MinIO S3 exports prefix."""
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .parquet(f"{EXPORT_ROOT}/{name}")
    )


def main() -> None:
    spark = build_spark()

    for table_name in GOLD_TABLES:
        print(f"Exporting local.gold.{table_name}", flush=True)
        write_single_parquet(spark.table(f"local.gold.{table_name}"), table_name)

    print("Exporting a 50,000-row bronze sample", flush=True)
    write_single_parquet(
        spark.table("local.bronze.recentchange").limit(50_000),
        "bronze_recentchange_sample",
    )
    spark.stop()


if __name__ == "__main__":
    main()
