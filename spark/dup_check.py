"""Print total vs distinct-key row counts for bronze — chaos demo evidence."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wiki-dup-check").getOrCreate()
row = spark.sql(
    "SELECT COUNT(*) AS total, COUNT(DISTINCT meta_id) AS distinct_ids "
    "FROM local.bronze.recentchange"
).first()
verdict = "OK-NO-DUPLICATES" if row.total == row.distinct_ids else "DUPLICATES-FOUND"
print(f"DUPCHECK total={row.total} distinct={row.distinct_ids} {verdict}")
spark.stop()
