from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from spark.metrics import (
    PHYSICAL_BOUNDS,
    WindowMetrics,
    compute_health_index,
    evaluate_realtime_alerts,
    normalize_status,
)
from spark.sinks import persist_to_postgres, write_json_snapshot

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cnc.telemetry.raw")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
OUTPUT_DIR = Path(os.getenv("SPARK_OUTPUT_DIR", "/app/data/streaming"))
CHECKPOINT_DIR = Path(os.getenv("SPARK_CHECKPOINT_DIR", "/app/data/checkpoints"))

SPARK_VERSION = "3.5.1"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("CNC-Predictive-Maintenance-Streaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _telemetry_schema() -> T.StructType:
    data_schema = T.StructType(
        [
            T.StructField("vibration_rms_mms", T.DoubleType(), True),
            T.StructField("peak_g", T.DoubleType(), True),
            T.StructField("spindle_temp_c", T.DoubleType(), True),
            T.StructField("rpm", T.DoubleType(), True),
            T.StructField("load_pct", T.DoubleType(), True),
            T.StructField("parts_count", T.IntegerType(), True),
            T.StructField("status", T.StringType(), True),
        ]
    )
    return T.StructType(
        [
            T.StructField("device_id", T.StringType(), True),
            T.StructField("device_type", T.StringType(), True),
            T.StructField("machine_id", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("data", data_schema, True),
        ]
    )


def _sanitized_col(col_name: str, field_key: str) -> F.Column:
    low, high = PHYSICAL_BOUNDS[field_key]
    col = F.col(col_name)
    return F.when((col >= low) & (col <= high), col).otherwise(F.lit(None))


def make_batch_writer(window_label: str):
    def _write_batch(batch_df: DataFrame, epoch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return

        window_rows: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []

        for row in rows:
            avg_vib = float(row["avg_vibration_mms"]) if row["avg_vibration_mms"] is not None else None
            max_vib = float(row["max_vibration_mms"]) if row["max_vibration_mms"] is not None else None
            avg_temp = float(row["avg_spindle_temp_c"]) if row["avg_spindle_temp_c"] is not None else None
            max_temp = float(row["max_spindle_temp_c"]) if row["max_spindle_temp_c"] is not None else None
            avg_load = float(row["avg_load_pct"]) if row["avg_load_pct"] is not None else None

            health_index = compute_health_index(avg_vib, avg_temp, avg_load)
            window_start_str = row["window_start"].strftime("%Y-%m-%dT%H:%M:%SZ")
            window_end_str = row["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
            parts_produced = int(row["parts_produced"] or 0)
            parts_per_hour = parts_produced * 60 if window_label == "1m" else parts_produced * 6

            window_rows.append(
                {
                    "machine_id": row["machine_id"],
                    "window_size": window_label,
                    "window_start": window_start_str,
                    "window_end": window_end_str,
                    "avg_vibration_mms": round(avg_vib, 3) if avg_vib is not None else None,
                    "max_vibration_mms": round(max_vib, 3) if max_vib is not None else None,
                    "avg_peak_g": round(float(row["avg_peak_g"]), 3) if row["avg_peak_g"] is not None else None,
                    "avg_spindle_temp_c": round(avg_temp, 2) if avg_temp is not None else None,
                    "max_spindle_temp_c": round(max_temp, 2) if max_temp is not None else None,
                    "avg_rpm": round(float(row["avg_rpm"]), 1) if row["avg_rpm"] is not None else None,
                    "avg_load_pct": round(avg_load, 2) if avg_load is not None else None,
                    "parts_produced": parts_produced,
                    "parts_per_hour": parts_per_hour,
                    "latest_status": normalize_status(row["latest_status"]),
                    "health_index": health_index,
                    "reading_count": int(row["reading_count"]),
                }
            )

            if window_label == "1m":
                alerts.extend(
                    evaluate_realtime_alerts(
                        WindowMetrics(
                            machine_id=row["machine_id"],
                            window_end=window_end_str,
                            avg_vibration_mms=avg_vib,
                            max_vibration_mms=max_vib,
                            avg_spindle_temp_c=avg_temp,
                            max_spindle_temp_c=max_temp,
                            avg_load_pct=avg_load,
                        )
                    )
                )

        write_json_snapshot(OUTPUT_DIR, window_label, window_rows, alerts)
        persist_to_postgres(POSTGRES_DSN, window_label, window_rows, alerts)
        print(
            f"[spark-streaming] epoch={epoch_id} window={window_label} machines={len(window_rows)} alerts={len(alerts)}",
            flush=True,
        )

    return _write_batch


def build_window_aggregation(parsed_df: DataFrame, window_duration: str) -> DataFrame:
    # Only consider non-null status rows when selecting the chronologically latest status
    status_order_ts = F.when(F.col("raw_status").isNotNull(), F.col("event_time"))
    return (
        parsed_df.withWatermark("event_time", "2 minutes")
        .groupBy(F.window(F.col("event_time"), window_duration).alias("win"), F.col("machine_id"))
        .agg(
            F.avg("clean_vib").alias("avg_vibration_mms"),
            F.max("clean_vib").alias("max_vibration_mms"),
            F.avg("clean_peak_g").alias("avg_peak_g"),
            F.avg("clean_temp").alias("avg_spindle_temp_c"),
            F.max("clean_temp").alias("max_spindle_temp_c"),
            F.avg("clean_rpm").alias("avg_rpm"),
            F.avg("clean_load").alias("avg_load_pct"),
            F.sum(F.coalesce(F.col("clean_parts"), F.lit(0))).alias("parts_produced"),
            F.max_by("raw_status", status_order_ts).alias("latest_status"),
            F.count("*").alias("reading_count"),
        )
        .select(
            F.col("machine_id"),
            F.col("win.start").alias("window_start"),
            F.col("win.end").alias("window_end"),
            "avg_vibration_mms",
            "max_vibration_mms",
            "avg_peak_g",
            "avg_spindle_temp_c",
            "max_spindle_temp_c",
            "avg_rpm",
            "avg_load_pct",
            "parts_produced",
            "latest_status",
            "reading_count",
        )
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream.select(F.from_json(F.col("value").cast("string"), _telemetry_schema()).alias("msg"))
        .select("msg.*")
        .withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.to_timestamp(F.col("timestamp"), "yyyy/MM/dd HH:mm:ss"),
            ),
        )
        .dropna(subset=["event_time", "machine_id", "device_id"])
        .withColumn("clean_vib", _sanitized_col("data.vibration_rms_mms", "vibration_rms_mms"))
        .withColumn("clean_peak_g", _sanitized_col("data.peak_g", "peak_g"))
        .withColumn("clean_temp", _sanitized_col("data.spindle_temp_c", "spindle_temp_c"))
        .withColumn("clean_rpm", _sanitized_col("data.rpm", "rpm"))
        .withColumn("clean_load", _sanitized_col("data.load_pct", "load_pct"))
        .withColumn("clean_parts", _sanitized_col("data.parts_count", "parts_count"))
        .withColumn("raw_status", F.col("data.status"))
    )

    agg_1m = build_window_aggregation(parsed, "1 minute")
    agg_10m = build_window_aggregation(parsed, "10 minutes")

    (
        agg_1m.writeStream.outputMode("update")
        .foreachBatch(make_batch_writer("1m"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "window_1m"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    (
        agg_10m.writeStream.outputMode("update")
        .foreachBatch(make_batch_writer("10m"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "window_10m"))
        .trigger(processingTime="15 seconds")
        .start()
    )

    print(
        f"[spark-streaming] Started 1m & 10m windowed queries from Kafka {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}",
        flush=True,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
