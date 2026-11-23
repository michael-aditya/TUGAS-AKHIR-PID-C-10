from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Physical validity bounds separating non-physical Out-of-Range Spikes from real Gradual Degradation (ADR-0004)
PHYSICAL_BOUNDS: dict[str, tuple[float, float]] = {
    "vibration_rms_mms": (0.0, 25.0),
    "peak_g": (0.0, 15.0),
    "spindle_temp_c": (0.0, 150.0),
    "rpm": (0.0, 20000.0),
    "load_pct": (0.0, 120.0),
    "parts_count": (0.0, 5000.0),
}

# ISO 10816 and CNC operational thresholds
VIB_WARNING_MMS = 4.5
VIB_CRITICAL_MMS = 7.1
TEMP_WARNING_C = 70.0
TEMP_CRITICAL_C = 85.0
LOAD_OVERLOAD_PCT = 90.0


@dataclass(frozen=True)
class WindowMetrics:
    """Aggregated windowed telemetry metrics for one CNC Machine."""

    machine_id: str
    window_end: str
    avg_vibration_mms: float | None
    max_vibration_mms: float | None
    avg_spindle_temp_c: float | None
    max_spindle_temp_c: float | None
    avg_load_pct: float | None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _build_alert(
    machine_id: str,
    window_end: str,
    failure_code: str,
    severity: str,
    metric_value: float,
    threshold_value: float,
    precision: int = 2,
) -> dict[str, Any]:
    return {
        "machine_id": machine_id,
        "failure_code": failure_code,
        "severity": severity,
        "metric_value": round(metric_value, precision),
        "threshold_value": threshold_value,
        "window_end": window_end,
        "source_layer": "STREAMING",
    }


def compute_health_index(
    vibration_rms_mms: float | None,
    spindle_temp_c: float | None,
    load_pct: float | None,
) -> float:
    """Compute composite CNC Machine Health Index (0-100) weighted 50% vibration, 30% temp, 20% load."""
    vib = vibration_rms_mms if vibration_rms_mms is not None else 0.5
    temp = spindle_temp_c if spindle_temp_c is not None else 30.0
    load = load_pct if load_pct is not None else 20.0

    p_vib = _clamp(((vib - 0.5) / (VIB_CRITICAL_MMS - 0.5)) * 100.0, 0.0, 100.0)
    p_temp = _clamp(((temp - 30.0) / (TEMP_CRITICAL_C - 30.0)) * 100.0, 0.0, 100.0)
    p_load = _clamp(((load - 20.0) / (100.0 - 20.0)) * 100.0, 0.0, 100.0)

    score = 100.0 - (0.50 * p_vib + 0.30 * p_temp + 0.20 * p_load)
    return round(_clamp(score, 0.0, 100.0), 2)


def is_physical_reading(device_type: str, data: dict[str, Any]) -> bool:
    """Return True if all non-null numeric metrics fall inside physical sensor bounds (not an Out-of-Range Spike)."""
    fields_by_type: dict[str, tuple[str, ...]] = {
        "vibration": ("vibration_rms_mms", "peak_g"),
        "spindle": ("spindle_temp_c", "rpm", "load_pct"),
        "production_counter": ("parts_count",),
    }
    for field in fields_by_type.get(device_type, ()):
        val = data.get(field)
        if val is None:
            continue
        low, high = PHYSICAL_BOUNDS[field]
        try:
            num = float(val)
        except (TypeError, ValueError):
            return False
        if num < low or num > high:
            return False
    return True


def normalize_status(raw_status: Any) -> str:
    """Standardize inconsistent Operational Status strings into RUN, IDLE, DOWN, or UNKNOWN."""
    if raw_status is None:
        return "UNKNOWN"
    cleaned = str(raw_status).strip().upper()
    if cleaned.startswith("RUN"):
        return "RUN"
    if cleaned.startswith("IDLE"):
        return "IDLE"
    if cleaned.startswith("DOWN"):
        return "DOWN"
    return "UNKNOWN"


def evaluate_realtime_alerts(metrics: WindowMetrics) -> list[dict[str, Any]]:
    """Evaluate real-time windowed Machine metrics and return active Failure Type alerts."""
    alerts: list[dict[str, Any]] = []

    vib = metrics.max_vibration_mms if metrics.max_vibration_mms is not None else metrics.avg_vibration_mms
    if vib is not None:
        if vib >= VIB_CRITICAL_MMS:
            alerts.append(_build_alert(metrics.machine_id, metrics.window_end, "VIB_CRITICAL", "CRITICAL", vib, VIB_CRITICAL_MMS, precision=3))
        elif vib >= VIB_WARNING_MMS:
            alerts.append(_build_alert(metrics.machine_id, metrics.window_end, "VIB_WARNING", "WARNING", vib, VIB_WARNING_MMS, precision=3))

    temp = metrics.max_spindle_temp_c if metrics.max_spindle_temp_c is not None else metrics.avg_spindle_temp_c
    if temp is not None:
        if temp >= TEMP_CRITICAL_C:
            alerts.append(_build_alert(metrics.machine_id, metrics.window_end, "TEMP_HIGH", "CRITICAL", temp, TEMP_CRITICAL_C))
        elif temp >= TEMP_WARNING_C:
            alerts.append(_build_alert(metrics.machine_id, metrics.window_end, "TEMP_HIGH", "WARNING", temp, TEMP_WARNING_C))

    if metrics.avg_load_pct is not None and metrics.avg_load_pct >= LOAD_OVERLOAD_PCT:
        alerts.append(_build_alert(metrics.machine_id, metrics.window_end, "OVERLOAD", "WARNING", metrics.avg_load_pct, LOAD_OVERLOAD_PCT))

    return alerts
