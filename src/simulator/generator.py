from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random
from typing import Any

from common.time_utils import iso_utc

from .degradation import compute_machine_physics, default_machines
from .models import (
    FaultRates,
    GeneratedReading,
    InjectionAuditEntry,
    MachineProfile,
    TelemetryBatch,
)

STUCK_RUN_LENGTH = 4
PRIMARY_FIELD_BY_DEVICE_TYPE: dict[str, str] = {
    "vibration": "vibration_rms_mms",
    "spindle": "spindle_temp_c",
    "production_counter": "status",
}
NON_CANONICAL_STATUSES: dict[str, tuple[str, ...]] = {
    "RUN": ("run", "Running", "RUN "),
    "IDLE": ("idle ", "Idle", "IDLE_WAIT"),
    "DOWN": ("down_err", "Down", "down "),
}


def _non_iso_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y/%m/%d %H:%M:%S")


def _inject_out_of_range(device_type: str, data: dict[str, Any], rng: random.Random) -> str:
    if device_type == "vibration":
        data["vibration_rms_mms"] = round(rng.choice([rng.uniform(250.0, 500.0), -rng.uniform(5.0, 25.0)]), 2)
        return "vibration_rms_mms"
    if device_type == "spindle":
        target = rng.choice(["spindle_temp_c", "rpm", "load_pct"])
        if target == "spindle_temp_c":
            data["spindle_temp_c"] = round(rng.choice([rng.uniform(220.0, 450.0), -rng.uniform(10.0, 30.0)]), 2)
        elif target == "rpm":
            data["rpm"] = round(rng.uniform(28000.0, 45000.0), 1)
        else:
            data["load_pct"] = round(rng.uniform(150.0, 240.0), 2)
        return target
    data["parts_count"] = -rng.randint(10, 50)
    return "parts_count"


class TelemetryStreamGenerator:
    """Stateful generator shared by both batch backfill and live 1-second MQTT streaming."""

    def __init__(
        self,
        machines: list[MachineProfile],
        fault_rates: FaultRates | None = None,
        seed: int = 42,
        total_timeline_hours: float = 168.0,
    ) -> None:
        self._machines = machines
        self._rates = fault_rates if fault_rates is not None else FaultRates()
        self._rng = random.Random(seed)
        self._total_timeline_hours = total_timeline_hours

        self._prev_vib: dict[str, float | None] = {m.machine_id: None for m in machines}
        self._prev_temp: dict[str, float | None] = {m.machine_id: None for m in machines}
        self._active_stuck: dict[str, tuple[str, float, int]] = {}
        self._stuck_cooldown: dict[str, int] = {}

    def generate_step(
        self,
        step_dt: datetime,
        elapsed_hours: float,
        interval_seconds: int,
        remaining_steps: int | None = None,
    ) -> TelemetryBatch:
        """Generate one tick across all 10 Machines (30 Devices), preserving Stuck Sensor state."""
        batch = TelemetryBatch()
        can_start_stuck = remaining_steps is None or remaining_steps >= STUCK_RUN_LENGTH

        for machine in self._machines:
            snapshot = compute_machine_physics(
                machine=machine,
                elapsed_hours=elapsed_hours,
                total_hours=self._total_timeline_hours,
                interval_seconds=interval_seconds,
                rng=self._rng,
                prev_vib=self._prev_vib[machine.machine_id],
                prev_temp=self._prev_temp[machine.machine_id],
            )
            self._prev_vib[machine.machine_id] = snapshot.vibration_rms_mms
            self._prev_temp[machine.machine_id] = snapshot.spindle_temp_c

            device_payloads: list[tuple[str, str, dict[str, Any]]] = [
                (
                    f"{machine.machine_id}-VIB",
                    "vibration",
                    {"vibration_rms_mms": snapshot.vibration_rms_mms, "peak_g": snapshot.peak_g},
                ),
                (
                    f"{machine.machine_id}-SPD",
                    "spindle",
                    {
                        "spindle_temp_c": snapshot.spindle_temp_c,
                        "rpm": snapshot.rpm,
                        "load_pct": snapshot.load_pct,
                    },
                ),
                (
                    f"{machine.machine_id}-PRD",
                    "production_counter",
                    {"parts_count": snapshot.parts_delta, "status": snapshot.status},
                ),
            ]

            for device_id, device_type, data in device_payloads:
                self._emit_device_reading(
                    batch=batch,
                    machine_id=machine.machine_id,
                    device_id=device_id,
                    device_type=device_type,
                    data=data,
                    step_dt=step_dt,
                    can_start_stuck=can_start_stuck,
                )

        return batch

    def _emit_device_reading(
        self,
        batch: TelemetryBatch,
        machine_id: str,
        device_id: str,
        device_type: str,
        data: dict[str, Any],
        step_dt: datetime,
        can_start_stuck: bool,
    ) -> None:
        timestamp_str = iso_utc(step_dt)
        ingested_str = iso_utc(step_dt + timedelta(seconds=1))

        def record_audit(fault_type: str, affected_field: str, ts: str, ing: str, delay_seconds: int = 0) -> None:
            batch.audit_entries.append(
                InjectionAuditEntry(
                    fault_type=fault_type,
                    device_id=device_id,
                    machine_id=machine_id,
                    device_type=device_type,
                    reading_timestamp=ts,
                    ingested_at=ing,
                    affected_field=affected_field,
                    delay_seconds=delay_seconds,
                )
            )

        # 1. Continue active Stuck Sensor run if in progress
        if device_id in self._active_stuck:
            stuck_field, frozen_val, remaining = self._active_stuck[device_id]
            data[stuck_field] = frozen_val
            remaining -= 1
            if remaining <= 0:
                del self._active_stuck[device_id]
                self._stuck_cooldown[device_id] = 2
            else:
                self._active_stuck[device_id] = (stuck_field, frozen_val, remaining)

            batch.items.append(
                GeneratedReading(
                    payload={
                        "device_id": device_id,
                        "device_type": device_type,
                        "machine_id": machine_id,
                        "timestamp": timestamp_str,
                        "data": data,
                    },
                    ingested_at=ingested_str,
                )
            )
            record_audit("stuck_sensor", stuck_field, timestamp_str, ingested_str)
            return

        if self._stuck_cooldown.get(device_id, 0) > 0:
            self._stuck_cooldown[device_id] -= 1

        injected = False

        # 2a. Initiate a 4-reading Stuck Sensor run on continuous float devices
        if (
            not injected
            and can_start_stuck
            and device_type in {"vibration", "spindle"}
            and self._stuck_cooldown.get(device_id, 0) == 0
            and self._rng.random() < self._rates.stuck_sensor
        ):
            stuck_field = PRIMARY_FIELD_BY_DEVICE_TYPE[device_type]
            frozen_val = float(data[stuck_field])
            self._active_stuck[device_id] = (stuck_field, frozen_val, STUCK_RUN_LENGTH - 1)
            record_audit("stuck_sensor", stuck_field, timestamp_str, ingested_str)
            injected = True

        # 2b. Missing value (null metric)
        if not injected and self._rng.random() < self._rates.missing_value:
            target_field = PRIMARY_FIELD_BY_DEVICE_TYPE[device_type]
            data[target_field] = None
            record_audit("missing_value", target_field, timestamp_str, ingested_str)
            injected = True

        # 2c. Non-physical Out-of-Range Spike
        if not injected and self._rng.random() < self._rates.out_of_range:
            target_field = _inject_out_of_range(device_type, data, self._rng)
            record_audit("out_of_range", target_field, timestamp_str, ingested_str)
            injected = True

        # 2d. Inconsistent format (status string or non-ISO timestamp)
        if not injected and self._rng.random() < self._rates.inconsistent_format:
            if device_type == "production_counter":
                canonical_status = str(data["status"])
                data["status"] = self._rng.choice(NON_CANONICAL_STATUSES[canonical_status])
                affected_field = "status"
            else:
                timestamp_str = _non_iso_timestamp(step_dt)
                affected_field = "timestamp"
            record_audit("inconsistent_format", affected_field, iso_utc(step_dt), ingested_str)
            injected = True

        # 2e. Late-arriving Telemetry Reading (> 120s behind ingested_at; non-grid offset prevents duplicate collision)
        if not injected and self._rng.random() < self._rates.late_event:
            delay_seconds = self._rng.randint(3, 9) * 60 + self._rng.randint(7, 53)
            late_dt = step_dt - timedelta(seconds=delay_seconds)
            timestamp_str = iso_utc(late_dt)
            ingested_str = iso_utc(step_dt)
            record_audit("late_event", "timestamp", timestamp_str, ingested_str, delay_seconds=delay_seconds)
            injected = True

        payload = {
            "device_id": device_id,
            "device_type": device_type,
            "machine_id": machine_id,
            "timestamp": timestamp_str,
            "data": data,
        }
        batch.items.append(GeneratedReading(payload=payload, ingested_at=ingested_str))

        # 2f. Duplicate Telemetry Reading (exact device_id + timestamp copy)
        if not injected and self._rng.random() < self._rates.duplicate:
            duplicate_payload = {
                "device_id": device_id,
                "device_type": device_type,
                "machine_id": machine_id,
                "timestamp": timestamp_str,
                "data": dict(data),
            }
            batch.items.append(GeneratedReading(payload=duplicate_payload, ingested_at=ingested_str))
            record_audit("duplicate", "device_id+timestamp", timestamp_str, ingested_str)


def generate_telemetry_batch(
    machines: list[MachineProfile],
    start_time: datetime,
    hours: float,
    interval_seconds: int = 300,
    fault_rates: FaultRates | None = None,
    seed: int = 42,
    initial_elapsed_hours: float = 0.0,
    total_timeline_hours: float | None = None,
) -> TelemetryBatch:
    """Seam 1: Generate pure CNC Telemetry Readings and out-of-band Injection Audit Log."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if hours <= 0:
        raise ValueError("hours must be positive")

    timeline_hours = total_timeline_hours if total_timeline_hours is not None else max(hours, 168.0)
    total_steps = max(1, int(round((hours * 3600.0) / interval_seconds)))

    stream = TelemetryStreamGenerator(
        machines=machines,
        fault_rates=fault_rates,
        seed=seed,
        total_timeline_hours=timeline_hours,
    )
    combined = TelemetryBatch()
    for step in range(total_steps):
        step_dt = start_time + timedelta(seconds=step * interval_seconds)
        elapsed_h = initial_elapsed_hours + (step * interval_seconds) / 3600.0
        step_batch = stream.generate_step(
            step_dt=step_dt,
            elapsed_hours=elapsed_h,
            interval_seconds=interval_seconds,
            remaining_steps=total_steps - step,
        )
        combined.items.extend(step_batch.items)
        combined.audit_entries.extend(step_batch.audit_entries)

    return combined
