# bitnami/spark was discontinued on Docker Hub (Aug 2025) — use the official image
FROM apache/spark:3.5.5-python3

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
COPY wiki_stream.py verify_counts.py maintenance.py export_data.py dup_check.py /opt/spark/app/
RUN mkdir -p /opt/spark/checkpoints && chown -R spark:spark /opt/spark/app /opt/spark/checkpoints
USER spark
