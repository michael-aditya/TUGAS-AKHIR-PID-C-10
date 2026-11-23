from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Protocol

from .degradation import default_machines
from .generator import TelemetryStreamGenerator, generate_telemetry_batch
from .models import FaultRates, MachineProfile


class MongoCollectionLike(Protocol):
    def count_documents(self, query: dict[str, Any]) -> int: ...
    def insert_many(self, docs: list[dict[str, Any]]) -> Any: ...
    def create_index(self, keys: list[tuple[str, int]]) -> Any: ...


class MqttClientLike(Protocol):
    def publish(self, topic: str, payload: str, qos: int = 0) -> Any: ...


def ensure_raw_indexes(
    telemetry_collection: MongoCollectionLike,
    audit_collection: MongoCollectionLike,
) -> None:
    telemetry_collection.create_index([("device_id", 1), ("timestamp", 1)])
    telemetry_collection.create_index([("machine_id", 1), ("timestamp", 1)])
    telemetry_collection.create_index([("source", 1)])
    audit_collection.create_index([("fault_type", 1), ("device_id", 1)])


def run_backfill_if_needed(
    telemetry_collection: MongoCollectionLike,
    audit_collection: MongoCollectionLike,
    end_time: datetime | None = None,
    days: int = 7,
    interval_seconds: int = 300,
    fault_rates: FaultRates | None = None,
    seed: int = 42,
    machines: list[MachineProfile] | None = None,
    chunk_size: int = 5000,
) -> dict[str, Any]:
    """Seed historical raw Telemetry Readings and Injection Audit Log into MongoDB if absent (ADR-0002)."""
    ensure_raw_indexes(telemetry_collection, audit_collection)

    existing = telemetry_collection.count_documents({"source": "backfill"})
    if existing > 0:
        return {"seeded": False, "existing_docs": existing, "inserted_readings": 0, "inserted_audits": 0}

    now = end_time.astimezone(timezone.utc) if end_time is not None else datetime.now(timezone.utc).replace(microsecond=0)
    total_hours = float(days * 24)
    start_time = now - timedelta(hours=total_hours)
    fleet = machines if machines is not None else default_machines()

    batch = generate_telemetry_batch(
        machines=fleet,
        start_time=start_time,
        hours=total_hours,
        interval_seconds=interval_seconds,
        fault_rates=fault_rates,
        seed=seed,
        initial_elapsed_hours=0.0,
        total_timeline_hours=total_hours,
    )

    raw_docs: list[dict[str, Any]] = []
    for item in batch.items:
        doc = dict(item.payload)
        doc["ingested_at"] = item.ingested_at
        doc["source"] = "backfill"
        raw_docs.append(doc)

    for i in range(0, len(raw_docs), chunk_size):
        telemetry_collection.insert_many(raw_docs[i : i + chunk_size])

    audit_docs = [{**entry.to_dict(), "source": "backfill"} for entry in batch.audit_entries]
    for i in range(0, len(audit_docs), chunk_size):
        audit_collection.insert_many(audit_docs[i : i + chunk_size])

    return {
        "seeded": True,
        "inserted_readings": len(raw_docs),
        "inserted_audits": len(audit_docs),
    }


class LiveMqttPublisher:
    """Stateful 1-second live MQTT publisher across all 30 CNC Devices, supporting multi-tick Stuck Sensor runs."""

    def __init__(
        self,
        mqtt_client: MqttClientLike,
        audit_collection: MongoCollectionLike | None = None,
        machines: list[MachineProfile] | None = None,
        fault_rates: FaultRates | None = None,
        seed: int = 1000,
        base_elapsed_hours: float = 168.0,
    ) -> None:
        fleet = machines if machines is not None else default_machines()
        self._mqtt_client = mqtt_client
        self._audit_collection = audit_collection
        self._base_elapsed_hours = base_elapsed_hours
        self._tick_counter = 0
        self._stream = TelemetryStreamGenerator(
            machines=fleet,
            fault_rates=fault_rates,
            seed=seed,
            total_timeline_hours=base_elapsed_hours,
        )

    def publish_tick(self, tick_time: datetime | None = None, interval_seconds: int = 1) -> int:
        now = tick_time.astimezone(timezone.utc) if tick_time is not None else datetime.now(timezone.utc).replace(microsecond=0)
        elapsed_hours = self._base_elapsed_hours + (self._tick_counter * interval_seconds) / 3600.0
        batch = self._stream.generate_step(
            step_dt=now,
            elapsed_hours=elapsed_hours,
            interval_seconds=interval_seconds,
            remaining_steps=None,
        )
        self._tick_counter += 1

        for reading in batch.readings:
            topic = f"sensors/cnc/{reading['machine_id']}/{reading['device_type']}"
            self._mqtt_client.publish(topic, json.dumps(reading), qos=0)

        if self._audit_collection is not None and batch.audit_entries:
            audit_docs = [{**entry.to_dict(), "source": "live"} for entry in batch.audit_entries]
            self._audit_collection.insert_many(audit_docs)

        return len(batch.readings)
