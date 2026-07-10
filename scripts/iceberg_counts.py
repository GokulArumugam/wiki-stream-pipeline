#!/usr/bin/env python3
"""Row counts for verify-spark, read from Iceberg REST catalog snapshot summaries.

Runs on the host with stdlib only. Counting via a second spark-submit inside
the streaming container OOM-kills the driver (2g cgroup shared by two JVMs);
the current snapshot's `total-records` summary property gives the same answer
for free.
"""

import json
import sys
from urllib.error import URLError
from urllib.request import urlopen

CATALOG = "http://localhost:8181/v1/namespaces/{ns}/tables/{table}"
TABLES = {
    "bronze": ("bronze", "recentchange"),
    "edits": ("gold", "edits_per_minute_by_wiki"),
    "bots": ("gold", "bot_vs_human_per_minute"),
    "pages": ("gold", "top_pages_10min"),
    "late": ("gold", "late_arrivals"),
}


def total_records(ns: str, table: str) -> int:
    with urlopen(CATALOG.format(ns=ns, table=table), timeout=10) as response:
        metadata = json.load(response)["metadata"]
    current = metadata.get("current-snapshot-id")
    if current is None or current == -1:
        return 0
    for snapshot in metadata.get("snapshots", []):
        if snapshot["snapshot-id"] == current:
            return int(snapshot.get("summary", {}).get("total-records", 0))
    return 0


def main() -> None:
    try:
        counts = {name: total_records(ns, tbl) for name, (ns, tbl) in TABLES.items()}
    except (URLError, KeyError, ValueError) as error:
        print(f"ERROR reading catalog: {error}", file=sys.stderr)
        sys.exit(1)
    print("COUNTS " + " ".join(f"{name}={count}" for name, count in counts.items()))


if __name__ == "__main__":
    main()
