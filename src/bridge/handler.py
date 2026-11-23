from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Protocol

from common.time_utils import iso_utc

REQUIRED_ENVELOPE_FIELDS: frozenset[str] = frozenset(
    {"device_id", "device_type", "machine_id", "timestamp", "data"}
)


class KafkaProducerLike(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> Any: ...


class MongoInsertable(Protocol):
    def insert_one(self, doc: dict[str, Any]) -> Any: ...


def process_mqtt_message(
    payload_bytes: bytes,
    kafka_producer: KafkaProducerLike,
    kafka_topic: str,
    mongo_collection: MongoInsertable,
    received_at: datetime | None = None,
) -> bool:
    """Validate incoming MQTT Telemetry Reading, forward to Kafka, and persist raw to MongoDB."""
    try:
        reading = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False

    if not isinstance(reading, dict):
        return False
    if not REQUIRED_ENVELOPE_FIELDS.issubset(reading.keys()):
        return False
    if not isinstance(reading.get("data"), dict):
        return False

    now = received_at if received_at is not None else datetime.now(timezone.utc)
    machine_key = str(reading["machine_id"])

    kafka_producer.send(kafka_topic, key=machine_key, value=reading)

    raw_doc = dict(reading)
    raw_doc["ingested_at"] = iso_utc(now)
    raw_doc["source"] = "live"
    mongo_collection.insert_one(raw_doc)
    return True
