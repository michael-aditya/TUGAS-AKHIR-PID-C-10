from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Protocol, Sequence


class Cursor(Protocol):
    description: Sequence[Sequence[Any]] | None

    def execute(self, operation: str, params: Sequence[Any] | None = None) -> Any: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


DOWNTIME_AND_ALERT_RANKING_SQL = """
WITH downtime AS (
    SELECT machine_key,
           SUM(COALESCE(
               duration_minutes,
               EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time)) / 60.0
           )) AS unplanned_downtime_minutes
    FROM fact_downtime_event
    JOIN dim_failure_type USING (failure_type_key)
    WHERE failure_code = 'UNPLANNED_DOWNTIME'
    GROUP BY machine_key
),
alerts AS (
    SELECT machine_key, COUNT(*) AS alert_count
    FROM fact_alert
    GROUP BY machine_key
)
SELECT machine.machine_id,
       COALESCE(downtime.unplanned_downtime_minutes, 0) AS unplanned_downtime_minutes,
       COALESCE(alerts.alert_count, 0) AS alert_count
FROM dim_machine AS machine
LEFT JOIN downtime USING (machine_key)
LEFT JOIN alerts USING (machine_key)
ORDER BY unplanned_downtime_minutes DESC, alert_count DESC, machine.machine_id
"""

DAILY_HEALTH_RUL_TRENDS_SQL = """
SELECT machine.machine_id,
       calendar.calendar_date,
       AVG(hourly.health_index)::double precision AS avg_health_index,
       AVG(hourly.rul_hours)::double precision AS avg_rul_hours
FROM fact_machine_hourly AS hourly
JOIN dim_machine AS machine USING (machine_key)
JOIN dim_time AS calendar USING (time_key)
WHERE calendar.calendar_date BETWEEN %s AND %s
GROUP BY machine.machine_id, calendar.calendar_date
ORDER BY calendar.calendar_date, machine.machine_id
"""

HOURLY_PARTS_BY_SHIFT_SQL = """
SELECT machine.machine_id,
       calendar.hour_start,
       calendar.production_shift,
       SUM(hourly.parts_produced) AS parts_produced
FROM fact_machine_hourly AS hourly
JOIN dim_machine AS machine USING (machine_key)
JOIN dim_time AS calendar USING (time_key)
WHERE calendar.calendar_date BETWEEN %s AND %s
GROUP BY machine.machine_id, calendar.hour_start, calendar.production_shift
ORDER BY calendar.hour_start, machine.machine_id
"""

DIRTY_DATA_RECONCILIATION_SQL = """
WITH categories(sort_order, fault_type) AS (
    VALUES
        (1, 'missing_value'),
        (2, 'duplicate'),
        (3, 'out_of_range'),
        (4, 'stuck_sensor'),
        (5, 'inconsistent_format'),
        (6, 'late_event')
),
totals AS (
    SELECT fault_type,
           SUM(injected_count) AS injected_count,
           SUM(caught_count) AS caught_count,
           SUM(unmatched_count) AS unmatched_count
    FROM dq_audit_summary
    GROUP BY fault_type
)
SELECT categories.fault_type,
       COALESCE(totals.injected_count, 0) AS injected_count,
       COALESCE(totals.caught_count, 0) AS caught_count,
       COALESCE(totals.unmatched_count, 0) AS unmatched_count,
       COALESCE(totals.injected_count, 0) - COALESCE(totals.caught_count, 0)
           AS reconciliation_delta
FROM categories
LEFT JOIN totals USING (fault_type)
ORDER BY categories.sort_order
"""


class WarehouseRepository:
    """Read Star Schema analytical views from an existing PostgreSQL connection."""

    def __init__(self, postgres_connection: Connection) -> None:
        self._postgres_connection = postgres_connection

    def fetch_downtime_and_alert_ranking(self) -> list[dict[str, Any]]:
        return self._fetch_all(DOWNTIME_AND_ALERT_RANKING_SQL)

    def fetch_daily_health_rul_trends(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        if end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self._fetch_all(DAILY_HEALTH_RUL_TRENDS_SQL, (start_date, end_date))

    def fetch_hourly_parts_by_shift(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        if end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self._fetch_all(HOURLY_PARTS_BY_SHIFT_SQL, (start_date, end_date))

    def fetch_dirty_data_reconciliation(self) -> list[dict[str, Any]]:
        return self._fetch_all(DIRTY_DATA_RECONCILIATION_SQL)

    def _fetch_all(
        self, query: str, params: Sequence[Any] | None = None
    ) -> list[dict[str, Any]]:
        cursor = self._postgres_connection.cursor()
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                raise RuntimeError("Analytical query did not return column metadata")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
