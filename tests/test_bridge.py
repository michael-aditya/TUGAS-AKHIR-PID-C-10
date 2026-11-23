from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"
TESTS_ROOT = Path(__file__).resolve().parent
for p in (SRC_ROOT, TESTS_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from bridge.handler import process_mqtt_message
from fakes import FakeKafkaProducer, FakeMongoCollection


class TestMqttKafkaBridge(unittest.TestCase):
    def test_valid_telemetry_is_forwarded_to_kafka_and_persisted_raw_in_mongodb(self) -> None:
        kafka = FakeKafkaProducer()
        mongo = FakeMongoCollection()
        received_at = datetime(2026, 11, 23, 6, 30, 5, tzinfo=timezone.utc)

        payload = {
            "device_id": "CNC-07-VIB",
            "device_type": "vibration",
            "machine_id": "CNC-07",
            "timestamp": "2026-11-23T06:30:04Z",
            "data": {"vibration_rms_mms": 5.82, "peak_g": 1.85},
        }

        ok = process_mqtt_message(
            payload_bytes=json.dumps(payload).encode("utf-8"),
            kafka_producer=kafka,
            kafka_topic="cnc.telemetry.raw",
            mongo_collection=mongo,
            received_at=received_at,
        )

        self.assertTrue(ok)
        self.assertEqual(len(kafka.messages), 1)
        topic, key, forwarded = kafka.messages[0]
        self.assertEqual(topic, "cnc.telemetry.raw")
        self.assertEqual(key, "CNC-07")
        self.assertEqual(forwarded, payload)

        self.assertEqual(len(mongo.docs), 1)
        saved = mongo.docs[0]
        self.assertEqual(saved["device_id"], "CNC-07-VIB")
        self.assertEqual(saved["ingested_at"], "2026-11-23T06:30:05Z")
        self.assertEqual(saved["source"], "live")

    def test_rejects_malformed_json_or_missing_required_envelope_fields(self) -> None:
        kafka = FakeKafkaProducer()
        mongo = FakeMongoCollection()

        self.assertFalse(
            process_mqtt_message(
                payload_bytes=b"not-valid-json",
                kafka_producer=kafka,
                kafka_topic="cnc.telemetry.raw",
                mongo_collection=mongo,
            )
        )
        self.assertFalse(
            process_mqtt_message(
                payload_bytes=json.dumps({"device_id": "CNC-01-VIB"}).encode("utf-8"),
                kafka_producer=kafka,
                kafka_topic="cnc.telemetry.raw",
                mongo_collection=mongo,
            )
        )
        self.assertEqual(len(kafka.messages), 0)
        self.assertEqual(len(mongo.docs), 0)


if __name__ == "__main__":
    unittest.main()
