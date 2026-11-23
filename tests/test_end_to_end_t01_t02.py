from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
TESTS_ROOT = Path(__file__).resolve().parent
for p in (SRC_ROOT, TESTS_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from bridge.handler import process_mqtt_message
from fakes import FakeKafkaProducer, FakeMongoCollection, FakeMqttClient
from simulator.generator import default_machines, generate_telemetry_batch
from simulator.models import FaultRates
from simulator.runner import LiveMqttPublisher
from spark.metrics import WindowMetrics, compute_health_index, evaluate_realtime_alerts, is_physical_reading


class TestTicket01And02Integration(unittest.TestCase):
    def test_seam1_unpacks_as_readings_and_audit_entries(self) -> None:
        start = datetime(2026, 11, 16, 0, 0, 0, tzinfo=timezone.utc)
        readings, audit_entries = generate_telemetry_batch(
            machines=default_machines(),
            start_time=start,
            hours=1,
            interval_seconds=3600,
            fault_rates=FaultRates.zero(),
            seed=1,
        )
        self.assertEqual(len(readings), 30)
        self.assertEqual(len(audit_entries), 0)

    def test_live_simulator_through_bridge_into_spark_metrics_and_alerts(self) -> None:
        mqtt_broker = FakeMqttClient()
        kafka = FakeKafkaProducer()
        mongo_raw = FakeMongoCollection()
        mongo_audit = FakeMongoCollection()

        publisher = LiveMqttPublisher(
            mqtt_client=mqtt_broker,
            audit_collection=mongo_audit,
            fault_rates=FaultRates.zero(),
            seed=77,
            base_elapsed_hours=168.0,
        )

        now = datetime(2026, 11, 23, 6, 45, 0, tzinfo=timezone.utc)
        publisher.publish_tick(now)

        for _topic, payload_str in mqtt_broker.published:
            ok = process_mqtt_message(
                payload_bytes=payload_str.encode("utf-8"),
                kafka_producer=kafka,
                kafka_topic="cnc.telemetry.raw",
                mongo_collection=mongo_raw,
                received_at=now,
            )
            self.assertTrue(ok)

        self.assertEqual(len(kafka.messages), 30)
        self.assertEqual(len(mongo_raw.docs), 30)

        by_machine: dict[str, dict[str, float | None]] = {}
        for _topic, machine_id, reading in kafka.messages:
            self.assertTrue(is_physical_reading(reading["device_type"], reading["data"]))
            slot = by_machine.setdefault(machine_id, {})
            if reading["device_type"] == "vibration":
                slot["vib"] = reading["data"]["vibration_rms_mms"]
            elif reading["device_type"] == "spindle":
                slot["temp"] = reading["data"]["spindle_temp_c"]
                slot["load"] = reading["data"]["load_pct"]

        cnc03 = by_machine["CNC-03"]
        cnc03_hi = compute_health_index(cnc03["vib"], cnc03["temp"], cnc03["load"])
        cnc03_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-03", "2026-11-23T06:46:00Z", cnc03["vib"], cnc03["vib"], cnc03["temp"], cnc03["temp"], cnc03["load"])
        )
        self.assertLess(cnc03_hi, 25.0)
        self.assertIn("VIB_CRITICAL", {a["failure_code"] for a in cnc03_alerts})

        cnc07 = by_machine["CNC-07"]
        cnc07_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-07", "2026-11-23T06:46:00Z", cnc07["vib"], cnc07["vib"], cnc07["temp"], cnc07["temp"], cnc07["load"])
        )
        self.assertIn("VIB_WARNING", {a["failure_code"] for a in cnc07_alerts})

        cnc01 = by_machine["CNC-01"]
        cnc01_hi = compute_health_index(cnc01["vib"], cnc01["temp"], cnc01["load"])
        cnc01_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-01", "2026-11-23T06:46:00Z", cnc01["vib"], cnc01["vib"], cnc01["temp"], cnc01["temp"], cnc01["load"])
        )
        self.assertGreater(cnc01_hi, 70.0)
        self.assertEqual(cnc01_alerts, [])


if __name__ == "__main__":
    unittest.main()
