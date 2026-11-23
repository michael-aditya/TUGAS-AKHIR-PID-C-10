import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from datetime import datetime, timedelta, timezone


ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from warehouse.loader import WarehouseLoader
from warehouse.repository import WarehouseRepository


MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "cnc_master")
MYSQL_USER = os.environ.get("MYSQL_USER", "cnc_app")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "cnc_app_password")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DB", "cnc_dwh")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cnc_app")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "cnc_app_password")


class TestDatabaseServicesConfigured(unittest.TestCase):
    @unittest.skipUnless(shutil.which("docker"), "Docker Compose CLI is not installed")
    def test_compose_declares_mysql_and_postgres_services(self) -> None:
        result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "--services"],
            cwd=ROOT,
            capture_output=True,
            check=True,
            text=True,
        )
        self.assertIn("mysql", result.stdout.splitlines())
        self.assertIn("postgres", result.stdout.splitlines())


@unittest.skipUnless(
    os.environ.get("RUN_DB_INTEGRATION") == "1",
    "Set RUN_DB_INTEGRATION=1 to boot isolated MySQL/PostgreSQL containers",
)
class TestDatabaseBootstrapIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if os.environ.get("WAREHOUSE_TEST_IN_CONTAINER") != "1":
            raise unittest.SkipTest(
                "Run via `docker compose --profile test run --rm warehouse-integration-tests`"
            )

        import pymysql
        import psycopg2

        cls.mysql_connection = pymysql.connect(
            host=os.environ.get("MYSQL_HOST", "mysql"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
        )
        cls.postgres_connection = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DATABASE,
        )
        cls.addClassCleanup(cls.mysql_connection.close)
        cls.addClassCleanup(cls.postgres_connection.close)

    @staticmethod
    def _query_values(connection, query: str) -> list[str]:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return [str(row[0]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _mysql(self, query: str) -> list[str]:
        return self._query_values(self.mysql_connection, query)

    def _postgres(self, query: str) -> list[str]:
        return self._query_values(self.postgres_connection, query)

    def test_mysql_starts_with_authoritative_machine_and_device_master_data(self) -> None:
        machine_ids = self._mysql("SELECT machine_id FROM machine ORDER BY machine_id")
        self.assertEqual(machine_ids, ["CNC-%02d" % number for number in range(1, 11)])
        self.assertEqual(self._mysql("SELECT COUNT(*) FROM device"), ["30"])
        self.assertEqual(
            self._mysql(
                "SELECT COUNT(*) FROM ("
                "SELECT machine_id FROM device GROUP BY machine_id "
                "HAVING COUNT(*) = 3 AND COUNT(DISTINCT device_type) = 3"
                ") AS sensors_per_machine"
            ),
            ["10"],
        )
        self.assertEqual(
            self._mysql("SELECT device_id FROM device ORDER BY device_id"),
            sorted(
                "CNC-%02d-%s" % (number, suffix)
                for number in range(1, 11)
                for suffix in ("VIB", "SPD", "PRD")
            ),
        )
        self.assertEqual(
            self._mysql(
                "SELECT COUNT(*) FROM machine "
                "WHERE vibration_warning_threshold_mms = 4.5 "
                "AND vibration_critical_threshold_mms = 7.1 "
                "AND spindle_temp_warning_threshold_c = 70.0 "
                "AND spindle_temp_critical_threshold_c = 85.0"
            ),
            ["10"],
        )
        self.assertEqual(
            self._mysql(
                "SELECT COUNT(*) FROM device "
                "WHERE engineering_unit IS NOT NULL "
                "AND normal_range_min IS NOT NULL "
                "AND normal_range_max IS NOT NULL "
                "AND sampling_rate_hz > 0"
            ),
            ["30"],
        )
        self.assertEqual(
            self._mysql(
                "SELECT COUNT(*) FROM device "
                "WHERE device_type = 'spindle' AND engineering_unit = '°C' "
                "AND normal_range_min = 30.0 AND normal_range_max = 70.0"
            ),
            ["10"],
        )

    def test_postgres_starts_with_star_schema_catalog_and_streaming_sink(self) -> None:
        tables = self._postgres(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        expected_tables = {
            "dim_machine",
            "dim_sensor",
            "dim_time",
            "dim_failure_type",
            "fact_machine_hourly",
            "fact_downtime_event",
            "fact_alert",
            "dq_audit_summary",
            "rt_window_metrics",
            "rt_active_alerts",
        }
        self.assertTrue(expected_tables.issubset(set(tables)))

        self.assertEqual(
            self._postgres("SELECT failure_code FROM dim_failure_type ORDER BY failure_code"),
            [
                "MAINTENANCE_DUE_RUL",
                "OVERLOAD",
                "TEMP_HIGH",
                "UNPLANNED_DOWNTIME",
                "VIB_CRITICAL",
                "VIB_WARNING",
            ],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'dim_time' "
                "AND column_name IN ('hour_start', 'day_of_week', 'production_shift')"
            ),
            ["3"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM pg_constraint "
                "WHERE conrelid = 'dim_time'::regclass AND contype = 'c' "
                "AND pg_get_constraintdef(oid) LIKE '%Shift 1 Pagi%' "
                "AND pg_get_constraintdef(oid) LIKE '%Shift 2 Sore%' "
                "AND pg_get_constraintdef(oid) LIKE '%Shift 3 Malam%'"
            ),
            ["1"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT kcu.table_name || ':' || kcu.column_name || ':' || "
                "ccu.table_name || ':' || ccu.column_name "
                "FROM information_schema.key_column_usage kcu "
                "JOIN information_schema.constraint_column_usage ccu "
                "ON ccu.constraint_catalog = kcu.constraint_catalog "
                "AND ccu.constraint_schema = kcu.constraint_schema "
                "AND ccu.constraint_name = kcu.constraint_name "
                "WHERE kcu.table_schema = 'public' "
                "AND kcu.table_name IN "
                "('fact_machine_hourly', 'fact_downtime_event', 'fact_alert') "
                "AND kcu.position_in_unique_constraint IS NOT NULL "
                "ORDER BY 1"
            ),
            [
                "fact_alert:failure_type_key:dim_failure_type:failure_type_key",
                "fact_alert:machine_key:dim_machine:machine_key",
                "fact_alert:time_key:dim_time:time_key",
                "fact_downtime_event:end_time_key:dim_time:time_key",
                "fact_downtime_event:failure_type_key:dim_failure_type:failure_type_key",
                "fact_downtime_event:machine_key:dim_machine:machine_key",
                "fact_downtime_event:start_time_key:dim_time:time_key",
                "fact_machine_hourly:machine_key:dim_machine:machine_key",
                "fact_machine_hourly:time_key:dim_time:time_key",
            ],
        )
        self.assertEqual(
            self._postgres(
                "SELECT column_name || ':' || is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'fact_downtime_event' "
                "AND column_name IN ('start_time_key', 'end_time_key') "
                "ORDER BY column_name"
            ),
            ["end_time_key:YES", "start_time_key:NO"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'rt_window_metrics' "
                "AND column_name IN ("
                "'machine_id', 'window_size', 'window_start', 'window_end', "
                "'avg_vibration_mms', 'max_vibration_mms', 'avg_peak_g', "
                "'avg_spindle_temp_c', 'max_spindle_temp_c', 'avg_rpm', 'avg_load_pct', "
                "'parts_produced', 'parts_per_hour', 'latest_status', 'health_index', "
                "'reading_count', 'updated_at')"
            ),
            ["17"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM pg_constraint "
                "WHERE conrelid = 'rt_window_metrics'::regclass "
                "AND contype = 'u' "
                "AND pg_get_constraintdef(oid) LIKE "
                "'% (machine_id, window_size, window_start)%'"
            ),
            ["1"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'rt_active_alerts' "
                "AND column_name IN ("
                "'machine_id', 'failure_code', 'severity', 'metric_value', "
                "'threshold_value', 'window_end', 'source_layer', 'created_at')"
            ),
            ["8"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM pg_constraint "
                "WHERE conrelid = 'rt_active_alerts'::regclass "
                "AND contype = 'u' "
                "AND pg_get_constraintdef(oid) LIKE "
                "'% (machine_id, failure_code, window_end)%'"
            ),
            ["1"],
        )

    def test_warehouse_loader_is_idempotent_and_analytics_match_facts(self) -> None:
        loader = WarehouseLoader(self.mysql_connection, self.postgres_connection)
        first_sync = loader.sync_dimensions()
        machine_keys = self._postgres(
            "SELECT machine_id || ':' || machine_key FROM dim_machine ORDER BY machine_id"
        )
        sensor_keys = self._postgres(
            "SELECT device_id || ':' || sensor_key FROM dim_sensor ORDER BY device_id"
        )

        self.assertEqual(first_sync, {"machines": 10, "sensors": 30, "failure_types": 6})

        def restore_master_source() -> None:
            cursor = self.mysql_connection.cursor()
            try:
                cursor.execute(
                    "UPDATE machine SET vibration_warning_threshold_mms = 4.5 "
                    "WHERE machine_id = 'CNC-01'"
                )
                cursor.execute(
                    "UPDATE device SET normal_range_max = 4.5 WHERE device_id = 'CNC-01-VIB'"
                )
                self.mysql_connection.commit()
            finally:
                cursor.close()
            loader.sync_dimensions()

        self.addCleanup(restore_master_source)
        master_cursor = self.mysql_connection.cursor()
        try:
            master_cursor.execute(
                "UPDATE machine SET vibration_warning_threshold_mms = 4.6 "
                "WHERE machine_id = 'CNC-01'"
            )
            master_cursor.execute(
                "UPDATE device SET normal_range_max = 4.4 WHERE device_id = 'CNC-01-VIB'"
            )
            self.mysql_connection.commit()
        finally:
            master_cursor.close()

        self.assertEqual(loader.sync_dimensions(), first_sync)
        self.assertEqual(
            self._postgres("SELECT COUNT(*) FROM dim_machine UNION ALL SELECT COUNT(*) FROM dim_sensor"),
            ["10", "30"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT machine_id || ':' || machine_key FROM dim_machine ORDER BY machine_id"
            ),
            machine_keys,
        )
        self.assertEqual(
            self._postgres(
                "SELECT device_id || ':' || sensor_key FROM dim_sensor ORDER BY device_id"
            ),
            sensor_keys,
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM dim_machine "
                "WHERE machine_id = 'CNC-01' AND vibration_warning_threshold_mms = 4.6"
            ),
            ["1"],
        )
        self.assertEqual(
            self._postgres(
                "SELECT COUNT(*) FROM dim_sensor "
                "WHERE device_id = 'CNC-01-VIB' AND normal_range_max = 4.4"
            ),
            ["1"],
        )

        horizon_start = datetime(2026, 11, 16, 5, tzinfo=timezone.utc)
        horizon_end = horizon_start + timedelta(hours=167)
        self.assertEqual(loader.ensure_time_dimension(horizon_start, horizon_end), 168)
        self.assertEqual(loader.ensure_time_dimension(horizon_start, horizon_end), 168)
        self.assertEqual(
            self._postgres(
                "SELECT hour_of_day || ':' || production_shift FROM dim_time "
                "WHERE hour_start IN ("
                "'2026-11-16 05:00:00+00', '2026-11-16 06:00:00+00', "
                "'2026-11-16 13:00:00+00', '2026-11-16 14:00:00+00', "
                "'2026-11-16 21:00:00+00', '2026-11-16 22:00:00+00') "
                "ORDER BY hour_of_day"
            ),
            [
                "5:Shift 3 Malam",
                "6:Shift 1 Pagi",
                "13:Shift 1 Pagi",
                "14:Shift 2 Sore",
                "21:Shift 2 Sore",
                "22:Shift 3 Malam",
            ],
        )
        self.assertEqual(
            self._postgres("SELECT COUNT(*) FROM dim_time WHERE hour_start BETWEEN "
                           "'2026-11-16 05:00:00+00' AND '2026-11-23 04:00:00+00'"),
            ["168"],
        )

        hourly = [
            {
                "machine_id": "CNC-01",
                "hour_start": horizon_start,
                "avg_vibration_mms": 2.0,
                "max_vibration_mms": 2.2,
                "parts_produced": 2,
                "health_index": 90,
                "rul_hours": 168,
                "latest_status": "RUN",
                "reading_count": 3600,
            },
            {
                "machine_id": "CNC-01",
                "hour_start": horizon_start + timedelta(hours=1),
                "avg_vibration_mms": 2.2,
                "max_vibration_mms": 2.4,
                "parts_produced": 7,
                "health_index": 80,
                "rul_hours": 160,
                "latest_status": "RUN",
                "reading_count": 3600,
            },
            {
                "machine_id": "CNC-03",
                "hour_start": horizon_start + timedelta(hours=2),
                "avg_vibration_mms": 7.1,
                "max_vibration_mms": 7.3,
                "parts_produced": 0,
                "health_index": 60,
                "rul_hours": 0,
                "latest_status": "DOWN",
                "reading_count": 3600,
            },
            {
                "machine_id": "CNC-03",
                "hour_start": horizon_start + timedelta(hours=3),
                "avg_vibration_mms": 7.2,
                "max_vibration_mms": 7.4,
                "parts_produced": 0,
                "health_index": 40,
                "rul_hours": 0,
                "latest_status": "DOWN",
                "reading_count": 3600,
            },
            {
                "machine_id": "CNC-07",
                "hour_start": horizon_start + timedelta(hours=33),
                "avg_vibration_mms": 5.8,
                "max_vibration_mms": 5.9,
                "parts_produced": 12,
                "health_index": 75,
                "rul_hours": 28,
                "latest_status": "RUN",
                "reading_count": 3600,
            },
        ]
        loader.upsert_machine_hourly(hourly)
        hourly[0]["parts_produced"] = 4
        loader.upsert_machine_hourly(hourly)

        downtime = [
            {
                "machine_id": "CNC-03",
                "failure_code": "UNPLANNED_DOWNTIME",
                "start_time": horizon_start + timedelta(hours=2),
                "end_time": horizon_start + timedelta(hours=4),
                "duration_minutes": 120,
                "status": "CLOSED",
            },
            {
                "machine_id": "CNC-01",
                "failure_code": "UNPLANNED_DOWNTIME",
                "start_time": horizon_start + timedelta(hours=1),
                "end_time": horizon_start + timedelta(hours=2),
                "duration_minutes": 30,
                "status": "CLOSED",
            },
        ]
        loader.upsert_downtime_events(downtime)
        downtime[0]["duration_minutes"] = 125
        loader.upsert_downtime_events(downtime)

        alerts = [
            {
                "machine_id": "CNC-03",
                "failure_code": "VIB_CRITICAL",
                "event_timestamp": horizon_start + timedelta(hours=2),
                "severity": "CRITICAL",
                "metric_value": 7.3,
                "threshold_value": 7.1,
                "source_layer": "BATCH",
            },
            {
                "machine_id": "CNC-03",
                "failure_code": "TEMP_HIGH",
                "event_timestamp": horizon_start + timedelta(hours=3),
                "severity": "CRITICAL",
                "metric_value": 86,
                "threshold_value": 85,
                "source_layer": "BATCH",
            },
            {
                "machine_id": "CNC-01",
                "failure_code": "VIB_WARNING",
                "event_timestamp": horizon_start + timedelta(hours=1),
                "severity": "WARNING",
                "metric_value": 4.5,
                "threshold_value": 4.5,
                "source_layer": "BATCH",
            },
            {
                "machine_id": "CNC-07",
                "failure_code": "MAINTENANCE_DUE_RUL",
                "event_timestamp": horizon_start + timedelta(hours=33),
                "severity": "CRITICAL",
                "metric_value": 28,
                "threshold_value": 48,
                "source_layer": "BATCH",
            },
        ]
        loader.upsert_alerts(alerts)
        alerts[0]["metric_value"] = 7.4
        loader.upsert_alerts(alerts)

        audits = [
            {
                "run_id": "test-run",
                "fault_type": "missing_value",
                "injected_count": 10,
                "caught_count": 8,
                "unmatched_count": 2,
            },
            {
                "run_id": "test-run",
                "fault_type": "duplicate",
                "injected_count": 5,
                "caught_count": 5,
                "unmatched_count": 0,
            },
            {
                "run_id": "test-run",
                "fault_type": "out_of_range",
                "injected_count": 4,
                "caught_count": 3,
                "unmatched_count": 1,
            },
            {
                "run_id": "test-run",
                "fault_type": "stuck_sensor",
                "injected_count": 2,
                "caught_count": 2,
                "unmatched_count": 0,
            },
            {
                "run_id": "test-run",
                "fault_type": "inconsistent_format",
                "injected_count": 2,
                "caught_count": 1,
                "unmatched_count": 1,
            },
            {
                "run_id": "test-run",
                "fault_type": "late_event",
                "injected_count": 7,
                "caught_count": 7,
                "unmatched_count": 0,
            },
        ]
        loader.upsert_dq_audit_summaries(audits)
        audits[0].update(injected_count=12, caught_count=9, unmatched_count=3)
        loader.upsert_dq_audit_summaries(audits)

        repository = WarehouseRepository(self.postgres_connection)
        ranking = repository.fetch_downtime_and_alert_ranking()
        self.assertEqual(
            (
                ranking[0]["machine_id"],
                ranking[0]["unplanned_downtime_minutes"],
                ranking[0]["alert_count"],
            ),
            ("CNC-03", 125, 2),
        )
        self.assertEqual(
            repository.fetch_daily_health_rul_trends(
                horizon_start.date(), horizon_start.date()
            ),
            [
                {
                    "machine_id": "CNC-01",
                    "calendar_date": horizon_start.date(),
                    "avg_health_index": 85.0,
                    "avg_rul_hours": 164.0,
                },
                {
                    "machine_id": "CNC-03",
                    "calendar_date": horizon_start.date(),
                    "avg_health_index": 50.0,
                    "avg_rul_hours": 0.0,
                },
            ],
        )
        parts = repository.fetch_hourly_parts_by_shift(
            horizon_start.date(), horizon_start.date()
        )
        self.assertEqual(
            [
                (
                    row["machine_id"],
                    row["hour_start"].hour,
                    row["production_shift"],
                    row["parts_produced"],
                )
                for row in parts
            ],
            [
                ("CNC-01", 5, "Shift 3 Malam", 4),
                ("CNC-01", 6, "Shift 1 Pagi", 7),
                ("CNC-03", 7, "Shift 1 Pagi", 0),
                ("CNC-03", 8, "Shift 1 Pagi", 0),
            ],
        )
        reconciliation = repository.fetch_dirty_data_reconciliation()
        self.assertEqual(
            [
                (
                    row["fault_type"],
                    row["injected_count"],
                    row["caught_count"],
                    row["unmatched_count"],
                )
                for row in reconciliation
            ],
            [
                ("missing_value", 12, 9, 3),
                ("duplicate", 5, 5, 0),
                ("out_of_range", 4, 3, 1),
                ("stuck_sensor", 2, 2, 0),
                ("inconsistent_format", 2, 1, 1),
                ("late_event", 7, 7, 0),
            ],
        )


if __name__ == "__main__":
    unittest.main()
