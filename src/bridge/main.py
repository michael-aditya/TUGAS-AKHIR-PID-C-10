from __future__ import annotations

import json
import os
import signal

from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from pymongo import MongoClient

from bridge.handler import process_mqtt_message


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "cnc.telemetry.raw")
    mqtt_broker = os.getenv("MQTT_BROKER", "mosquitto")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_topic = os.getenv("MQTT_TOPIC", "sensors/cnc/+/+")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
    mongo_db_name = os.getenv("MONGO_DB", "cnc_raw")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        linger_ms=20,
        acks="all",
        retries=5,
    )

    mongo_client = MongoClient(mongo_uri)
    telemetry_col = mongo_client[mongo_db_name]["telemetry_raw"]
    counters = {"ok": 0, "bad": 0}

    def on_connect(client: mqtt.Client, _userdata: object, _flags: dict, rc: int) -> None:
        print(f"[bridge] MQTT connected (rc={rc}), subscribing to {mqtt_topic}", flush=True)
        client.subscribe(mqtt_topic)

    def on_message(_client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage) -> None:
        ok = process_mqtt_message(
            payload_bytes=msg.payload,
            kafka_producer=producer,
            kafka_topic=kafka_topic,
            mongo_collection=telemetry_col,
        )
        if ok:
            counters["ok"] += 1
            if counters["ok"] % 300 == 0:
                print(f"[bridge] Bridged {counters['ok']} messages to Kafka & MongoDB (bad={counters['bad']})", flush=True)
        else:
            counters["bad"] += 1
            print(f"[bridge] Dropped malformed payload on topic {msg.topic}", flush=True)

    client = mqtt.Client(client_id="cnc-mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_broker, mqtt_port, keepalive=60)

    def _stop(_signum: int, _frame: object) -> None:
        client.disconnect()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        client.loop_forever()
    finally:
        producer.flush()
        producer.close()
        mongo_client.close()
        print("[bridge] Shutdown complete.", flush=True)


if __name__ == "__main__":
    main()
