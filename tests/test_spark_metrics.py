from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.metrics import (
    WindowMetrics,
    compute_health_index,
    evaluate_realtime_alerts,
    is_physical_reading,
    normalize_status,
)


class TestSparkMetrics(unittest.TestCase):
    def test_health_index_matches_50_30_20_weighted_formula(self) -> None:
        self.assertEqual(compute_health_index(0.5, 30.0, 20.0), 100.0)
        self.assertEqual(compute_health_index(7.1, 85.0, 100.0), 0.0)
        self.assertEqual(compute_health_index(3.8, 57.5, 60.0), 50.0)
        self.assertEqual(compute_health_index(0.1, 20.0, 5.0), 100.0)
        self.assertEqual(compute_health_index(12.0, 105.0, 110.0), 0.0)

    def test_filters_non_physical_spikes_while_keeping_real_degradation(self) -> None:
        self.assertTrue(is_physical_reading("vibration", {"vibration_rms_mms": 7.8, "peak_g": 2.4}))
        self.assertTrue(is_physical_reading("spindle", {"spindle_temp_c": 88.5, "rpm": 9000.0, "load_pct": 85.0}))

        self.assertFalse(is_physical_reading("vibration", {"vibration_rms_mms": 350.0, "peak_g": 1.2}))
        self.assertFalse(is_physical_reading("vibration", {"vibration_rms_mms": -5.0, "peak_g": 1.2}))
        self.assertFalse(is_physical_reading("spindle", {"spindle_temp_c": 260.0, "rpm": 8000.0, "load_pct": 50.0}))
        self.assertFalse(is_physical_reading("spindle", {"spindle_temp_c": 50.0, "rpm": 35000.0, "load_pct": 50.0}))
        self.assertFalse(is_physical_reading("spindle", {"spindle_temp_c": 50.0, "rpm": 8000.0, "load_pct": 180.0}))

    def test_evaluates_realtime_threshold_alerts_including_70c_and_85c_spindle_temp(self) -> None:
        healthy_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-01", "2026-11-23T06:40:00Z", 2.2, 2.5, 48.0, 50.0, 52.0)
        )
        self.assertEqual(healthy_alerts, [])

        critical_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-03", "2026-11-23T06:40:00Z", 7.4, 7.6, 86.5, 88.0, 92.0)
        )
        self.assertEqual(
            {(a["failure_code"], a["severity"]) for a in critical_alerts},
            {("VIB_CRITICAL", "CRITICAL"), ("TEMP_HIGH", "CRITICAL"), ("OVERLOAD", "WARNING")},
        )

        # Warning vibration (>= 4.5) and warning spindle temp (>= 70.0, < 85.0)
        warning_alerts = evaluate_realtime_alerts(
            WindowMetrics("CNC-07", "2026-11-23T06:40:00Z", 5.8, 6.0, 68.0, 72.5, 74.0)
        )
        self.assertEqual(
            {(a["failure_code"], a["severity"]) for a in warning_alerts},
            {("VIB_WARNING", "WARNING"), ("TEMP_HIGH", "WARNING")},
        )

    def test_normalizes_inconsistent_operational_status(self) -> None:
        self.assertEqual(normalize_status("run"), "RUN")
        self.assertEqual(normalize_status("Running"), "RUN")
        self.assertEqual(normalize_status("idle "), "IDLE")
        self.assertEqual(normalize_status("down_err"), "DOWN")
        self.assertEqual(normalize_status(None), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
