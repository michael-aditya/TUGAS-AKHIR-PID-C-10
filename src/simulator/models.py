from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class MachineProfile:
    """Physical operating baseline and degradation profile of one CNC Machine."""

    machine_id: str
    profile_type: str  # "healthy" | "breakdown_day6" | "active_degradation_rul" | "early_degradation"
    baseline_vibration_mms: float
    baseline_temp_c: float
    baseline_rpm: float
    baseline_load_pct: float


@dataclass(frozen=True)
class FaultRates:
    """Configurable per-reading probability for each of the 6 Dirty Data categories."""

    missing_value: float = 0.015
    duplicate: float = 0.015
    out_of_range: float = 0.015
    stuck_sensor: float = 0.005  # Probability of initiating a 4-reading stuck run
    inconsistent_format: float = 0.020
    late_event: float = 0.015

    @classmethod
    def zero(cls) -> FaultRates:
        return cls(
            missing_value=0.0,
            duplicate=0.0,
            out_of_range=0.0,
            stuck_sensor=0.0,
            inconsistent_format=0.0,
            late_event=0.0,
        )


@dataclass(frozen=True)
class InjectionAuditEntry:
    """Out-of-band ground-truth entry for one intentionally injected Dirty Data instance."""

    fault_type: str
    device_id: str
    machine_id: str
    device_type: str
    reading_timestamp: str
    ingested_at: str
    affected_field: str
    delay_seconds: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "fault_type": self.fault_type,
            "device_id": self.device_id,
            "machine_id": self.machine_id,
            "device_type": self.device_type,
            "event_timestamp": self.reading_timestamp,
            "ingested_at": self.ingested_at,
            "affected_field": self.affected_field,
            "delay_seconds": self.delay_seconds,
        }


@dataclass(frozen=True)
class GeneratedReading:
    """Pairs one pure Telemetry Reading payload with its wall-clock ingestion timestamp."""

    payload: dict[str, Any]
    ingested_at: str


@dataclass
class TelemetryBatch:
    """Result returned by Seam 1: pure Telemetry Readings and out-of-band Injection Audit Log."""

    items: list[GeneratedReading] = field(default_factory=list)
    audit_entries: list[InjectionAuditEntry] = field(default_factory=list)

    @property
    def readings(self) -> list[dict[str, Any]]:
        return [item.payload for item in self.items]

    def __iter__(self):
        """Allow unpacking as (readings, audit_entries) per SPEC.md Seam 1 contract."""
        yield self.readings
        yield self.audit_entries
