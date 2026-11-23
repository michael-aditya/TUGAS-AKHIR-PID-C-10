from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
TESTS_ROOT = Path(__file__).resolve().parent
for p in (SRC_ROOT, TESTS_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from fakes import FakeMongoCollection, FakeMqttClient
from simulator.models import FaultRates
from simulator.runner import LiveMqttPublisher, run_backfill_if_needed


class TestSimulatorRuntime(unittest.TestCase):
    def test_backfill_is_idempotent_and_persists_raw_docs_and_audit_log(self) -> None:
        telemetry_col = FakeMongoCollection()
        audit_col = FakeMongoCollection()
        now = datetime(2026, 11, 23, 0, 0, 0, tzinfo=timezone.utc)

        first = run_backfill_if_needed(
            telemetry_collection=telemetry_col,
            audit_collection=audit_col,
            end_time=now,
            days=1,
            interval_seconds=1800,
            fault_rates=FaultRates(
                missing_value=0.05,
                duplicate=0.05,
                out_of_range=0.05,
                stuck_sensor=0.0,
                inconsistent_format=0.05,
                late_event=0.05,
            ),
            seed=11,
        )
        self.assertTrue(first["seeded"])
        self.assertGreater(len(telemetry_col.docs), 0)
        self.assertGreater(len(audit_col.docs), 0)
        for doc in telemetry_col.docs:
            self.assertIn("ingested_at", doc)
            self.assertEqual(doc["source"], "backfill")

        count_before = len(telemetry_col.docs)
        second = run_backfill_if_needed(
            telemetry_collection=telemetry_col,
            audit_collection=audit_col,
            end_time=now,
            days=1,
            interval_seconds=1800,
            fault_rates=FaultRates(),
            seed=11,
        )
        self.assertFalse(second["seeded"])
        self.assertEqual(len(telemetry_col.docs), count_before)

    def test_live_publisher_emits_30_devices_and_supports_multi_tick_stuck_sensor_runs(self) -> None:
        mqtt_client = FakeMqttClient()
        audit_col = FakeMongoCollection()
        publisher = LiveMqttPublisher(
            mqtt_client=mqtt_client,
            audit_collection=audit_col,
            fault_rates=FaultRates(
                missing_value=0.0,
                duplicate=0.0,
                out_of_range=0.0,
                stuck_sensor=0.25,
                inconsistent_format=0.0,
                late_event=0.0,
            ),
            seed=21,
        )

        start = datetime(2026, 11, 23, 6, 0, 0, tzinfo=timezone.utc)
        for sec in range(6):
            emitted = publisher.publish_tick(start + timedelta(seconds=sec))
            self.assertEqual(emitted, 30)

        self.assertEqual(len(mqtt_client.published), 180)
        topics = {topic for topic, _ in mqtt_client.published}
        self.assertEqual(len(topics), 30)
        for topic, raw_payload in mqtt_client.published[:30]:
            self.assertTrue(topic.startswith("sensors/cnc/CNC-"))
            parsed = json.loads(raw_payload)
            self.assertEqual(set(parsed.keys()), {"device_id", "device_type", "machine_id", "timestamp", "data"})

        # Verify live publisher actually injects multi-tick Stuck Sensor runs
        stuck_audits = [d for d in audit_col.docs if d["fault_type"] == "stuck_sensor"]
        self.assertGreaterEqual(len(stuck_audits), 4)


if __name__ == "__main__":
    unittest.main()
