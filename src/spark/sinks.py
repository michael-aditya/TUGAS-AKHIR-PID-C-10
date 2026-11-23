from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any


def write_json_snapshot(
    output_dir: Path,
    window_label: str,
    window_rows: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
) -> None:
    """Atomically write latest windowed aggregates for Checkpoint 1 standalone verification."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"latest_{window_label}.json"
    tmp = output_dir / f"latest_{window_label}.json.tmp"
    payload = {
        "updated_at": int(time.time()),
        "window_size": window_label,
        "records": window_rows,
        "alerts": alerts,
    }
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    os.replace(tmp, target)


def persist_to_postgres(
    postgres_dsn: str,
    window_label: str,
    window_rows: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
) -> None:
    """Upsert Spark real-time windowed metrics and active alerts into PostgreSQL serving tables."""
    if not postgres_dsn:
        return
    try:
        import psycopg2
    except ImportError:
        return

    try:
        with psycopg2.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                for row in window_rows:
                    cur.execute(
                        """
                        INSERT INTO rt_window_metrics (
                            machine_id, window_size, window_start, window_end,
                            avg_vibration_mms, max_vibration_mms, avg_peak_g,
                            avg_spindle_temp_c, max_spindle_temp_c, avg_rpm, avg_load_pct,
                            parts_produced, parts_per_hour, latest_status, health_index,
                            reading_count, updated_at
                        ) VALUES (
                            %(machine_id)s, %(window_size)s, %(window_start)s, %(window_end)s,
                            %(avg_vibration_mms)s, %(max_vibration_mms)s, %(avg_peak_g)s,
                            %(avg_spindle_temp_c)s, %(max_spindle_temp_c)s, %(avg_rpm)s, %(avg_load_pct)s,
                            %(parts_produced)s, %(parts_per_hour)s, %(latest_status)s, %(health_index)s,
                            %(reading_count)s, NOW()
                        )
                        ON CONFLICT (machine_id, window_size, window_start)
                        DO UPDATE SET
                            window_end = EXCLUDED.window_end,
                            avg_vibration_mms = EXCLUDED.avg_vibration_mms,
                            max_vibration_mms = EXCLUDED.max_vibration_mms,
                            avg_peak_g = EXCLUDED.avg_peak_g,
                            avg_spindle_temp_c = EXCLUDED.avg_spindle_temp_c,
                            max_spindle_temp_c = EXCLUDED.max_spindle_temp_c,
                            avg_rpm = EXCLUDED.avg_rpm,
                            avg_load_pct = EXCLUDED.avg_load_pct,
                            parts_produced = EXCLUDED.parts_produced,
                            parts_per_hour = EXCLUDED.parts_per_hour,
                            latest_status = EXCLUDED.latest_status,
                            health_index = EXCLUDED.health_index,
                            reading_count = EXCLUDED.reading_count,
                            updated_at = NOW()
                        """,
                        row,
                    )

                for alert in alerts:
                    cur.execute(
                        """
                        INSERT INTO rt_active_alerts (
                            machine_id, failure_code, severity, metric_value,
                            threshold_value, window_end, source_layer, created_at
                        ) VALUES (
                            %(machine_id)s, %(failure_code)s, %(severity)s, %(metric_value)s,
                            %(threshold_value)s, %(window_end)s, %(source_layer)s, NOW()
                        )
                        ON CONFLICT (machine_id, failure_code, window_end)
                        DO UPDATE SET
                            severity = EXCLUDED.severity,
                            metric_value = EXCLUDED.metric_value,
                            threshold_value = EXCLUDED.threshold_value,
                            created_at = NOW()
                        """,
                        alert,
                    )
            conn.commit()
    except Exception as exc:
        print(f"[spark-streaming] PostgreSQL sink unavailable ({window_label}): {exc}", flush=True)
