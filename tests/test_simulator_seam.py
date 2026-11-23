from datetime import datetime, timezone
import sys
from pathlib import Path
import unittest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from simulator.generator import default_machines, generate_telemetry_batch
from simulator.models import FaultRates


class TestSimulatorSeam(unittest.TestCase):
    def test_generates_30_devices_across_10_machines_with_required_envelope(self) -> None:
        start_time = datetime(2026, 11, 16, 0, 0, 0, tzinfo=timezone.utc)
        machines = default_machines()

        batch = generate_telemetry_batch(
            machines=machines,
            start_time=start_time,
            hours=1,
            interval_seconds=1800,
            fault_rates=FaultRates.zero(),
            seed=42,
        )

        machine_ids = {r["machine_id"] for r in batch.readings}
        device_ids = {r["device_id"] for r in batch.readings}
        device_types = {r["device_type"] for r in batch.readings}

        self.assertEqual(len(machine_ids), 10)
        self.assertEqual(len(device_ids), 30)
        self.assertEqual(device_types, {"vibration", "spindle", "production_counter"})
        self.assertEqual(len(batch.audit_entries), 0)

        required_keys = {"device_id", "device_type", "machine_id", "timestamp", "data"}
        for reading in batch.readings:
            self.assertEqual(set(reading.keys()), required_keys)
            self.assertIsInstance(reading["data"], dict)

    def test_7_day_gradual_degradation_trajectories_match_spec(self) -> None:
        start_time = datetime(2026, 11, 16, 0, 0, 0, tzinfo=timezone.utc)
        batch = generate_telemetry_batch(
            machines=default_machines(),
            start_time=start_time,
            hours=168,
            interval_seconds=3600,
            fault_rates=FaultRates.zero(),
            seed=7,
        )

        vib_by_machine: dict[str, list[float]] = {}
        status_by_machine: dict[str, list[str]] = {}
        for r in batch.readings:
            mid = r["machine_id"]
            if r["device_type"] == "vibration":
                vib_by_machine.setdefault(mid, []).append(r["data"]["vibration_rms_mms"])
            elif r["device_type"] == "production_counter":
                status_by_machine.setdefault(mid, []).append(r["data"]["status"])

        # CNC-03 breaches 7.1 mm/s and enters DOWN (Unplanned Downtime) on Day 6
        self.assertGreaterEqual(max(vib_by_machine["CNC-03"]), 7.1)
        self.assertIn("DOWN", status_by_machine["CNC-03"])

        # CNC-07 actively degrades over final 72h toward ~5.8 mm/s (+0.045 mm/s/h)
        cnc07_end = vib_by_machine["CNC-07"][-1]
        cnc07_48h_ago = vib_by_machine["CNC-07"][-49]
        self.assertGreater(cnc07_end, 5.2)
        self.assertLess(cnc07_end, 6.4)
        self.assertGreater(cnc07_end - cnc07_48h_ago, 1.5)

        # CNC-09 exhibits early wear toward ~4.6 mm/s
        cnc09_end = vib_by_machine["CNC-09"][-1]
        self.assertGreater(cnc09_end, 4.1)
        self.assertLess(cnc09_end, 5.1)

        # Healthy machines remain within normal operating range (0.5 - 3.8 mm/s)
        for healthy_id in ["CNC-01", "CNC-02", "CNC-04", "CNC-05", "CNC-06", "CNC-08", "CNC-10"]:
            self.assertLess(max(vib_by_machine[healthy_id]), 4.0)
            self.assertGreater(min(vib_by_machine[healthy_id]), 0.5)

    def test_injects_all_6_dirty_data_categories_out_of_band_without_payload_leakage(self) -> None:
        start_time = datetime(2026, 11, 16, 0, 0, 0, tzinfo=timezone.utc)
        fault_rates = FaultRates(
            missing_value=0.03,
            duplicate=0.03,
            out_of_range=0.03,
            stuck_sensor=0.02,
            inconsistent_format=0.04,
            late_event=0.03,
        )
        batch = generate_telemetry_batch(
            machines=default_machines(),
            start_time=start_time,
            hours=24,
            interval_seconds=600,
            fault_rates=fault_rates,
            seed=123,
        )

        # Out-of-band audit log contains all 6 categories
        audit_types = {entry.fault_type for entry in batch.audit_entries}
        expected_types = {
            "missing_value",
            "duplicate",
            "out_of_range",
            "stuck_sensor",
            "inconsistent_format",
            "late_event",
        }
        self.assertEqual(audit_types, expected_types)

        # Telemetry payload must never expose dirty-data marker flags (ADR-0001)
        allowed_envelope = {"device_id", "device_type", "machine_id", "timestamp", "data"}
        for reading in batch.readings:
            self.assertEqual(set(reading.keys()), allowed_envelope)
            for k in reading["data"]:
                self.assertFalse(k.startswith("_"))
                self.assertNotIn("dirty", k.lower())
                self.assertNotIn("fault", k.lower())

        # Verify actual injected artifacts exist in the stream
        has_null = any(
            v is None for r in batch.readings for v in r["data"].values()
        )
        has_spike = any(
            r["device_type"] == "vibration"
            and r["data"].get("vibration_rms_mms") is not None
            and (r["data"]["vibration_rms_mms"] > 25.0 or r["data"]["vibration_rms_mms"] < 0)
            for r in batch.readings
        )
        has_bad_status_or_ts = any(
            (r["device_type"] == "production_counter" and r["data"].get("status") not in {"RUN", "IDLE", "DOWN"})
            or ("/" in str(r["timestamp"]))
            for r in batch.readings
        )
        seen_pairs: set[tuple[str, str]] = set()
        dup_count = 0
        for r in batch.readings:
            pair = (r["device_id"], r["timestamp"])
            if pair in seen_pairs:
                dup_count += 1
            seen_pairs.add(pair)

        self.assertTrue(has_null)
        self.assertTrue(has_spike)
        self.assertTrue(has_bad_status_or_ts)
        self.assertGreater(dup_count, 0)

        # Verify exact late_event metadata is tracked via event vs scheduled timestamps
        late_entries = [e for e in batch.audit_entries if e.fault_type == "late_event"]
        self.assertGreater(len(late_entries), 0)
        for entry in late_entries:
            self.assertGreater(entry.delay_seconds, 120)

    def test_stuck_sensor_runs_are_at_least_4_consecutive_identical_readings(self) -> None:
        start_time = datetime(2026, 11, 16, 0, 0, 0, tzinfo=timezone.utc)
        # Clean stream should have zero 4-in-a-row identical float runs
        clean_batch = generate_telemetry_batch(
            machines=default_machines(),
            start_time=start_time,
            hours=12,
            interval_seconds=600,
            fault_rates=FaultRates.zero(),
            seed=99,
        )
        self.assertEqual(self._count_stuck_readings(clean_batch.readings), 0)

        # Stuck-only stream should reconcile detected stuck readings with audit log
        stuck_only = FaultRates(
            missing_value=0.0,
            duplicate=0.0,
            out_of_range=0.0,
            stuck_sensor=0.05,
            inconsistent_format=0.0,
            late_event=0.0,
        )
        dirty_batch = generate_telemetry_batch(
            machines=default_machines(),
            start_time=start_time,
            hours=12,
            interval_seconds=600,
            fault_rates=stuck_only,
            seed=99,
        )
        injected_stuck = sum(1 for e in dirty_batch.audit_entries if e.fault_type == "stuck_sensor")
        detected_stuck = self._count_stuck_readings(dirty_batch.readings)
        self.assertGreater(injected_stuck, 0)
        self.assertEqual(detected_stuck, injected_stuck)

    @staticmethod
    def _count_stuck_readings(readings: list[dict]) -> int:
        by_device: dict[str, list[float]] = {}
        for r in readings:
            if r["device_type"] == "vibration":
                val = r["data"].get("vibration_rms_mms")
                if val is not None:
                    by_device.setdefault(r["device_id"], []).append(float(val))
            elif r["device_type"] == "spindle":
                val = r["data"].get("spindle_temp_c")
                if val is not None:
                    by_device.setdefault(r["device_id"], []).append(float(val))

        total_stuck = 0
        for series in by_device.values():
            run_len = 1
            for i in range(1, len(series)):
                if series[i] == series[i - 1]:
                    run_len += 1
                else:
                    if run_len >= 4:
                        total_stuck += run_len
                    run_len = 1
            if run_len >= 4:
                total_stuck += run_len
        return total_stuck


if __name__ == "__main__":
    unittest.main()
