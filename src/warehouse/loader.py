from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable, Iterator, Mapping, Protocol, Sequence


class Cursor(Protocol):
    description: Sequence[Sequence[Any]] | None

    def execute(self, operation: str, params: Sequence[Any] | None = None) -> Any: ...

    def executemany(self, operation: str, seq_of_params: Sequence[Sequence[Any]]) -> Any: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


FAILURE_TYPES: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "VIB_WARNING",
        "Vibration Warning",
        "VIBRATION",
        "WARNING",
        "Vibration reached the warning threshold of 4.5 mm/s.",
    ),
    (
        "VIB_CRITICAL",
        "Critical Vibration",
        "VIBRATION",
        "CRITICAL",
        "Vibration reached the critical threshold of 7.1 mm/s.",
    ),
    (
        "TEMP_HIGH",
        "High Spindle Temperature",
        "TEMPERATURE",
        "WARNING",
        "Spindle temperature reached a warning or critical threshold.",
    ),
    (
        "OVERLOAD",
        "Spindle Overload",
        "LOAD",
        "WARNING",
        "Spindle load reached the overload threshold.",
    ),
    (
        "MAINTENANCE_DUE_RUL",
        "Maintenance Due by RUL",
        "MAINTENANCE",
        "CRITICAL",
        "Estimated remaining useful life is below 48 hours.",
    ),
    (
        "UNPLANNED_DOWNTIME",
        "Unplanned Downtime",
        "DOWNTIME",
        "CRITICAL",
        "Machine entered an unplanned downtime state.",
    ),
)

MACHINE_FIELDS = (
    "machine_id",
    "model",
    "production_line",
    "installation_date",
    "service_interval_days",
    "last_service_date",
    "vibration_warning_threshold_mms",
    "vibration_critical_threshold_mms",
    "spindle_temp_warning_threshold_c",
    "spindle_temp_critical_threshold_c",
)

DEVICE_FIELDS = (
    "device_id",
    "machine_id",
    "device_type",
    "engineering_unit",
    "normal_range_min",
    "normal_range_max",
    "sampling_rate_hz",
)

HOURLY_MEASURES = (
    "avg_vibration_mms",
    "max_vibration_mms",
    "avg_peak_g",
    "avg_spindle_temp_c",
    "max_spindle_temp_c",
    "avg_rpm",
    "avg_load_pct",
    "parts_produced",
    "parts_per_hour",
    "health_index",
    "rul_hours",
    "latest_status",
    "reading_count",
)


@contextmanager
def _transaction(connection: Connection) -> Iterator[Cursor]:
    cursor = connection.cursor()
    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _read_mysql_records(connection: Connection, query: str) -> list[dict[str, Any]]:
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if cursor.description is None:
            raise RuntimeError("Master-data query did not return column metadata")
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("%s must be timezone-aware" % field_name)
    return value.astimezone(timezone.utc)


def _hour_start(value: datetime, field_name: str, require_aligned: bool = False) -> datetime:
    value = _as_utc(value, field_name)
    if require_aligned and (value.minute or value.second or value.microsecond):
        raise ValueError("%s must be aligned to an hour" % field_name)
    return value.replace(minute=0, second=0, microsecond=0)


def _record_values(record: Mapping[str, Any], fields: Sequence[str]) -> tuple[Any, ...]:
    return tuple(record.get(field) for field in fields)


def _execute_upserts(
    cursor: Cursor,
    table: str,
    columns: Sequence[str],
    values: Iterable[Sequence[Any]],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
) -> None:
    rows = list(values)
    if not rows:
        return
    column_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_sql = ", ".join(conflict_columns)
    updates_sql = ", ".join("%s = EXCLUDED.%s" % (column, column) for column in update_columns)
    cursor.executemany(
        "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s"
        % (table, column_sql, placeholders, conflict_sql, updates_sql),
        rows,
    )


class WarehouseLoader:
    """Synchronize CNC master data and idempotently load Star Schema rows.

    The caller owns the supplied DB-API connections and is responsible for closing them.
    """

    def __init__(self, mysql_connection: Connection, postgres_connection: Connection) -> None:
        self._mysql_connection = mysql_connection
        self._postgres_connection = postgres_connection

    def sync_dimensions(self) -> dict[str, int]:
        machines = _read_mysql_records(
            self._mysql_connection,
            "SELECT %s FROM machine ORDER BY machine_id" % ", ".join(MACHINE_FIELDS),
        )
        devices = _read_mysql_records(
            self._mysql_connection,
            "SELECT %s FROM device ORDER BY device_id" % ", ".join(DEVICE_FIELDS),
        )

        with _transaction(self._postgres_connection) as cursor:
            _execute_upserts(
                cursor,
                "dim_machine",
                MACHINE_FIELDS,
                (_record_values(machine, MACHINE_FIELDS) for machine in machines),
                ("machine_id",),
                MACHINE_FIELDS[1:],
            )

            machine_keys: dict[str, int] = {}
            for machine in machines:
                cursor.execute(
                    "SELECT machine_key FROM dim_machine WHERE machine_id = %s",
                    (machine["machine_id"],),
                )
                row = cursor.fetchone()
                if row is None:
                    raise LookupError("Machine %s was not synchronized" % machine["machine_id"])
                machine_keys[machine["machine_id"]] = row[0]

            device_columns = ("device_id", "machine_key") + DEVICE_FIELDS[1:]
            device_values = []
            for device in devices:
                machine_id = device["machine_id"]
                if machine_id not in machine_keys:
                    raise LookupError(
                        "Device %s references unknown machine %s"
                        % (device["device_id"], machine_id)
                    )
                device_values.append(
                    (device["device_id"], machine_keys[machine_id])
                    + _record_values(device, DEVICE_FIELDS[1:])
                )

            _execute_upserts(
                cursor,
                "dim_sensor",
                device_columns,
                device_values,
                ("device_id",),
                ("machine_key",) + DEVICE_FIELDS[1:],
            )
            _execute_upserts(
                cursor,
                "dim_failure_type",
                ("failure_code", "failure_name", "category", "default_severity", "description"),
                FAILURE_TYPES,
                ("failure_code",),
                ("failure_name", "category", "default_severity", "description"),
            )

        return {
            "machines": len(machines),
            "sensors": len(devices),
            "failure_types": len(FAILURE_TYPES),
        }

    def ensure_time_dimension(self, start: datetime, end: datetime) -> int:
        first_hour = _hour_start(start, "start")
        last_hour = _hour_start(end, "end")
        if last_hour < first_hour:
            raise ValueError("end must be greater than or equal to start")

        time_rows = []
        current = first_hour
        while current <= last_hour:
            current_date = current.date()
            weekday = current.weekday()
            hour = current.hour
            if 6 <= hour < 14:
                production_shift = "Shift 1 Pagi"
            elif 14 <= hour < 22:
                production_shift = "Shift 2 Sore"
            else:
                production_shift = "Shift 3 Malam"
            time_rows.append(
                (
                    current,
                    current_date,
                    current.year,
                    current.month,
                    current.day,
                    weekday,
                    current.strftime("%A"),
                    hour,
                    weekday >= 5,
                    production_shift,
                )
            )
            current += timedelta(hours=1)

        fields = (
            "hour_start",
            "calendar_date",
            "year_number",
            "month_number",
            "day_of_month",
            "day_of_week",
            "day_name",
            "hour_of_day",
            "is_weekend",
            "production_shift",
        )
        with _transaction(self._postgres_connection) as cursor:
            cursor.executemany(
                "INSERT INTO dim_time (%s) VALUES (%s) "
                "ON CONFLICT (hour_start) DO UPDATE SET %s"
                % (
                    ", ".join(fields),
                    ", ".join(["%s"] * len(fields)),
                    ", ".join("%s = EXCLUDED.%s" % (field, field) for field in fields[1:]),
                ),
                time_rows,
            )
        return len(time_rows)

    def upsert_machine_hourly(self, records: Iterable[Mapping[str, Any]]) -> int:
        rows = list(records)
        columns = ("machine_key", "time_key") + HOURLY_MEASURES
        values = []
        with _transaction(self._postgres_connection) as cursor:
            for record in rows:
                machine_key = self._machine_key(cursor, record["machine_id"])
                hour_start = _hour_start(record["hour_start"], "hour_start", require_aligned=True)
                time_key = self._time_key(cursor, hour_start)
                measures = tuple(
                    record.get(field, 0 if field == "reading_count" else None)
                    for field in HOURLY_MEASURES
                )
                values.append((machine_key, time_key) + measures)
            _execute_upserts(
                cursor,
                "fact_machine_hourly",
                columns,
                values,
                ("machine_key", "time_key"),
                HOURLY_MEASURES,
            )
        return len(rows)

    def upsert_downtime_events(self, records: Iterable[Mapping[str, Any]]) -> int:
        rows = list(records)
        columns = (
            "machine_key",
            "start_time_key",
            "end_time_key",
            "failure_type_key",
            "start_time",
            "end_time",
            "duration_minutes",
            "status",
            "description",
        )
        values = []
        with _transaction(self._postgres_connection) as cursor:
            for record in rows:
                start_time = _as_utc(record["start_time"], "start_time")
                end_time = record.get("end_time")
                if end_time is not None:
                    end_time = _as_utc(end_time, "end_time")
                    if end_time < start_time:
                        raise ValueError("end_time must be greater than or equal to start_time")
                values.append(
                    (
                        self._machine_key(cursor, record["machine_id"]),
                        self._time_key(cursor, _hour_start(start_time, "start_time")),
                        self._time_key(cursor, _hour_start(end_time, "end_time"))
                        if end_time is not None
                        else None,
                        self._failure_type_key(cursor, record["failure_code"]),
                        start_time,
                        end_time,
                        record.get("duration_minutes"),
                        record.get("status", "CLOSED" if end_time is not None else "OPEN"),
                        record.get("description"),
                    )
                )
            _execute_upserts(
                cursor,
                "fact_downtime_event",
                columns,
                values,
                ("machine_key", "start_time", "failure_type_key"),
                (
                    "start_time_key",
                    "end_time_key",
                    "end_time",
                    "duration_minutes",
                    "status",
                    "description",
                ),
            )
        return len(rows)

    def upsert_alerts(self, records: Iterable[Mapping[str, Any]]) -> int:
        rows = list(records)
        columns = (
            "machine_key",
            "time_key",
            "failure_type_key",
            "event_timestamp",
            "severity",
            "metric_value",
            "threshold_value",
            "source_layer",
            "details",
        )
        values = []
        with _transaction(self._postgres_connection) as cursor:
            for record in rows:
                event_timestamp = _as_utc(record["event_timestamp"], "event_timestamp")
                values.append(
                    (
                        self._machine_key(cursor, record["machine_id"]),
                        self._time_key(cursor, _hour_start(event_timestamp, "event_timestamp")),
                        self._failure_type_key(cursor, record["failure_code"]),
                        event_timestamp,
                        record["severity"],
                        record.get("metric_value"),
                        record.get("threshold_value"),
                        record.get("source_layer", "BATCH"),
                        json.dumps(record.get("details", {})),
                    )
                )
            cursor.executemany(
                "INSERT INTO fact_alert ({columns}) "
                "VALUES ({placeholders}, %s::jsonb) "
                "ON CONFLICT (machine_key, failure_type_key, event_timestamp, source_layer) "
                "DO UPDATE SET time_key = EXCLUDED.time_key, severity = EXCLUDED.severity, "
                "metric_value = EXCLUDED.metric_value, threshold_value = EXCLUDED.threshold_value, "
                "details = EXCLUDED.details"
                .format(
                    columns=", ".join(columns),
                    placeholders=", ".join(["%s"] * 8),
                ),
                values,
            )
        return len(rows)

    def upsert_dq_audit_summaries(self, records: Iterable[Mapping[str, Any]]) -> int:
        rows = list(records)
        values = [
            (
                record["run_id"],
                record["fault_type"],
                record.get("injected_count", 0),
                record.get("caught_count", 0),
                record.get("unmatched_count", 0),
                json.dumps(record.get("details", {})),
            )
            for record in rows
        ]
        with _transaction(self._postgres_connection) as cursor:
            cursor.executemany(
                "INSERT INTO dq_audit_summary "
                "(run_id, fault_type, injected_count, caught_count, unmatched_count, details) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (run_id, fault_type) DO UPDATE SET "
                "injected_count = EXCLUDED.injected_count, "
                "caught_count = EXCLUDED.caught_count, "
                "unmatched_count = EXCLUDED.unmatched_count, "
                "processed_at = CURRENT_TIMESTAMP, details = EXCLUDED.details",
                values,
            )
        return len(rows)

    @staticmethod
    def _machine_key(cursor: Cursor, machine_id: str) -> int:
        cursor.execute("SELECT machine_key FROM dim_machine WHERE machine_id = %s", (machine_id,))
        row = cursor.fetchone()
        if row is None:
            raise LookupError("Unknown machine_id: %s" % machine_id)
        return row[0]

    @staticmethod
    def _time_key(cursor: Cursor, hour_start: datetime) -> int:
        cursor.execute("SELECT time_key FROM dim_time WHERE hour_start = %s", (hour_start,))
        row = cursor.fetchone()
        if row is None:
            raise LookupError("Missing dim_time hour: %s" % hour_start.isoformat())
        return row[0]

    @staticmethod
    def _failure_type_key(cursor: Cursor, failure_code: str) -> int:
        cursor.execute(
            "SELECT failure_type_key FROM dim_failure_type WHERE failure_code = %s",
            (failure_code,),
        )
        row = cursor.fetchone()
        if row is None:
            raise LookupError("Unknown failure_code: %s" % failure_code)
        return row[0]
