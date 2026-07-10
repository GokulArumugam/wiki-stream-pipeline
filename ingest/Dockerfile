FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --system --uid 10001 ingest
COPY app.py .
RUN mkdir -p /data && chown -R ingest:ingest /app /data

USER ingest
CMD ["python", "app.py"]
