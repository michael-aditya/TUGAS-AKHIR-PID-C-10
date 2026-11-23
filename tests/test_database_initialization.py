import os
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"
MYSQL_DATABASE = "cnc_master"
MYSQL_USER = "cnc_test"
MYSQL_PASSWORD = "cnc_test_password"
POSTGRES_DATABASE = "cnc_dwh"
POSTGRES_USER = "cnc_test"
POSTGRES_PASSWORD = "cnc_test_password"


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
        cls.project_name = "cnc-db-tests-" + str(os.getpid())
        cls.environment = os.environ.copy()
        cls.environment.update(
            {
                "MYSQL_DATABASE": MYSQL_DATABASE,
                "MYSQL_USER": MYSQL_USER,
                "MYSQL_PASSWORD": MYSQL_PASSWORD,
                "MYSQL_ROOT_PASSWORD": "cnc_test_root_password",
                "POSTGRES_DB": POSTGRES_DATABASE,
                "POSTGRES_USER": POSTGRES_USER,
                "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            }
        )
        cls.addClassCleanup(cls._compose, "down", "--volumes", "--remove-orphans", check=False)
        cls._compose("up", "--detach", "--wait", "--wait-timeout", "180", "mysql", "postgres")

    @classmethod
    def _compose(cls, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                cls.project_name,
                "-f",
                str(COMPOSE_FILE),
                *arguments,
            ],
            cwd=ROOT,
            env=cls.environment,
            capture_output=True,
            check=False,
            text=True,
        )
        if check and result.returncode:
            raise RuntimeError(result.stderr or result.stdout)
        return result

    @classmethod
    def _mysql(cls, query: str) -> list[str]:
        result = cls._compose(
            "exec",
            "--no-TTY",
            "mysql",
            "mysql",
            "--batch",
            "--skip-column-names",
            "--user",
            MYSQL_USER,
            "--password=" + MYSQL_PASSWORD,
            MYSQL_DATABASE,
            "--execute",
            query,
        )
        return result.stdout.strip().splitlines()

    @classmethod
    def _postgres(cls, query: str) -> list[str]:
        result = cls._compose(
            "exec",
            "--no-TTY",
            "postgres",
            "psql",
            "--no-align",
            "--tuples-only",
            "--username",
            POSTGRES_USER,
            "--dbname",
            POSTGRES_DATABASE,
            "--command",
            query,
        )
        return result.stdout.strip().splitlines()

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
                "SELECT t.relname || ':' || r.relname "
                "FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "JOIN pg_class r ON r.oid = c.confrelid "
                "WHERE t.relname IN ('fact_machine_hourly', 'fact_downtime_event', 'fact_alert') "
                "AND c.contype = 'f' ORDER BY 1"
            ),
            [
                "fact_alert:dim_failure_type",
                "fact_alert:dim_machine",
                "fact_alert:dim_time",
                "fact_downtime_event:dim_failure_type",
                "fact_downtime_event:dim_machine",
                "fact_downtime_event:dim_time",
                "fact_machine_hourly:dim_machine",
                "fact_machine_hourly:dim_time",
            ],
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


if __name__ == "__main__":
    unittest.main()
