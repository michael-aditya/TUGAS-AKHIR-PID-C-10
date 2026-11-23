from __future__ import annotations

from dataclasses import dataclass
import math
import random

from .models import MachineProfile


@dataclass(frozen=True)
class MachinePhysicsSnapshot:
    """Physical state of a single CNC Machine at a given elapsed hour."""

    vibration_rms_mms: float
    peak_g: float
    spindle_temp_c: float
    rpm: float
    load_pct: float
    status: str
    parts_delta: int


def default_machines() -> list[MachineProfile]:
    """Return the canonical 10 CNC Machines (CNC-01 .. CNC-10) per Topik 2A spec."""
    return [
        MachineProfile("CNC-01", "healthy", 1.6, 44.0, 8200.0, 48.0),
        MachineProfile("CNC-02", "healthy", 2.1, 47.0, 7800.0, 52.0),
        MachineProfile("CNC-03", "breakdown_day6", 1.8, 46.0, 8600.0, 54.0),
        MachineProfile("CNC-04", "healthy", 1.9, 45.0, 8100.0, 50.0),
        MachineProfile("CNC-05", "healthy", 2.4, 50.0, 7500.0, 56.0),
        MachineProfile("CNC-06", "healthy", 1.5, 42.0, 8800.0, 46.0),
        MachineProfile("CNC-07", "active_degradation_rul", 2.56, 48.0, 8400.0, 54.0),
        MachineProfile("CNC-08", "healthy", 2.7, 51.0, 7200.0, 58.0),
        MachineProfile("CNC-09", "early_degradation", 2.8, 47.0, 9200.0, 50.0),
        MachineProfile("CNC-10", "healthy", 2.0, 46.0, 9000.0, 49.0),
    ]


def _ensure_distinct_float(value: float, previous: float | None, min_step: float = 0.002) -> float:
    rounded = round(value, 4)
    if previous is not None and abs(rounded - previous) < min_step:
        rounded = round(rounded + min_step, 4)
    return rounded


def compute_machine_physics(
    machine: MachineProfile,
    elapsed_hours: float,
    total_hours: float,
    interval_seconds: int,
    rng: random.Random,
    prev_vib: float | None = None,
    prev_temp: float | None = None,
) -> MachinePhysicsSnapshot:
    """Compute realistic physical sensor readings for one Machine at elapsed_hours."""
    shift_wave = math.sin(2.0 * math.pi * (elapsed_hours / 24.0))
    status = "RUN"

    if machine.profile_type == "breakdown_day6":
        degrade_start = max(0.0, total_hours - 96.0)
        breakdown_hour = max(degrade_start + 1.0, total_hours - 30.0)
        if elapsed_hours < degrade_start:
            vib_target = machine.baseline_vibration_mms + 0.15 * shift_wave
            temp_target = machine.baseline_temp_c + 2.0 * shift_wave
            load_target = machine.baseline_load_pct + 4.0 * shift_wave
        elif elapsed_hours < breakdown_hour:
            progress = (elapsed_hours - degrade_start) / (breakdown_hour - degrade_start)
            vib_target = machine.baseline_vibration_mms + progress * 5.7
            temp_target = machine.baseline_temp_c + progress * 41.0
            load_target = machine.baseline_load_pct + progress * 34.0
        else:
            status = "DOWN"
            vib_target = 7.45 + 0.15 * shift_wave
            temp_target = 86.5 + 1.0 * shift_wave
            load_target = 88.0 + 1.5 * shift_wave

    elif machine.profile_type == "active_degradation_rul":
        degrade_start = max(0.0, total_hours - 72.0)
        if elapsed_hours < degrade_start:
            vib_target = machine.baseline_vibration_mms + 0.12 * shift_wave
            temp_target = machine.baseline_temp_c + 2.0 * shift_wave
            load_target = machine.baseline_load_pct + 3.0 * shift_wave
        else:
            hours_degrading = elapsed_hours - degrade_start
            vib_target = machine.baseline_vibration_mms + 0.045 * hours_degrading
            temp_target = machine.baseline_temp_c + 0.30 * hours_degrading
            load_target = machine.baseline_load_pct + 0.25 * hours_degrading

    elif machine.profile_type == "early_degradation":
        degrade_start = max(0.0, total_hours - 60.0)
        if elapsed_hours < degrade_start:
            vib_target = machine.baseline_vibration_mms + 0.12 * shift_wave
            temp_target = machine.baseline_temp_c + 2.0 * shift_wave
            load_target = machine.baseline_load_pct + 3.0 * shift_wave
        else:
            hours_degrading = elapsed_hours - degrade_start
            vib_target = machine.baseline_vibration_mms + 0.030 * hours_degrading
            temp_target = machine.baseline_temp_c + 0.22 * hours_degrading
            load_target = machine.baseline_load_pct + 0.20 * hours_degrading

    else:
        vib_target = machine.baseline_vibration_mms + 0.20 * shift_wave
        temp_target = machine.baseline_temp_c + 3.0 * shift_wave
        load_target = machine.baseline_load_pct + 5.0 * shift_wave
        if int(elapsed_hours) % 16 == 15:
            status = "IDLE"

    vib_raw = max(0.55, min(14.0, vib_target + rng.gauss(0.0, 0.07)))
    temp_raw = max(31.0, min(105.0, temp_target + rng.gauss(0.0, 0.45)))

    vibration_rms = _ensure_distinct_float(vib_raw, prev_vib)
    spindle_temp = _ensure_distinct_float(temp_raw, prev_temp)
    peak_g = round(max(0.15, vibration_rms * rng.uniform(0.28, 0.36)), 4)

    if status == "DOWN":
        rpm = 0.0
        load_pct = round(max(5.0, min(98.0, load_target + rng.gauss(0.0, 0.8))), 2)
        parts_delta = 0
    elif status == "IDLE":
        rpm = round(max(0.0, rng.uniform(0.0, 400.0)), 1)
        load_pct = round(max(12.0, min(25.0, 18.0 + rng.gauss(0.0, 1.5))), 2)
        parts_delta = 0
    else:
        rpm = round(max(1500.0, min(11800.0, machine.baseline_rpm + rng.gauss(0.0, 180.0))), 1)
        load_pct = round(max(20.0, min(96.0, load_target + rng.gauss(0.0, 1.8))), 2)
        expected_parts = max(0.0, (interval_seconds / 100.0) * rng.uniform(0.85, 1.15))
        parts_delta = int(round(expected_parts)) if interval_seconds >= 60 else (1 if rng.random() < (interval_seconds / 100.0) else 0)

    return MachinePhysicsSnapshot(
        vibration_rms_mms=vibration_rms,
        peak_g=peak_g,
        spindle_temp_c=spindle_temp,
        rpm=rpm,
        load_pct=load_pct,
        status=status,
        parts_delta=parts_delta,
    )
