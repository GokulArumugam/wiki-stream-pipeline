"""Print the Iceberg row counts used by `make verify-spark`."""

from pyspark.sql import SparkSession


TABLES = {
    "bronze": "local.bronze.recentchange",
    "edits": "local.gold.edits_per_minute_by_wiki",
    "bots": "local.gold.bot_vs_human_per_minute",
    "pages": "local.gold.top_pages_10min",
    "late": "local.gold.late_arrivals",
}


spark = SparkSession.builder.appName("wiki-stream-counts").getOrCreate()
counts = {name: spark.table(table).count() for name, table in TABLES.items()}
print("COUNTS " + " ".join(f"{name}={count}" for name, count in counts.items()))
spark.stop()
