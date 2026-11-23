FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/app

WORKDIR /app
COPY spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY spark /app/spark
RUN mkdir -p /app/data/streaming /app/data/checkpoints

CMD ["python", "/app/common/wait_for.py", "kafka:29092", "--", "python", "-m", "spark.streaming_job"]
