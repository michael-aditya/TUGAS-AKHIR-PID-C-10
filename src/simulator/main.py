from __future__ import annotations

import os
import signal
import sys
import time

import paho.mqtt.client as mqtt
from pymongo import MongoClient

from simulator.models import FaultRates
from simulator.runner import LiveMqttPublisher, run_backfill_if_needed


def _float_env(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _load_fault_rates() -> FaultRates:
    return FaultRates(
        missing_value=_float_env("FAULT_RATE_MISSING", 0.015),
        duplicate=_float_env("FAULT_RATE_DUPLICATE", 0.015),
        out_of_range=_float_env("FAULT_RATE_OUT_OF_RANGE", 0.015),
        stuck_sensor=_float_env("FAULT_RATE_STUCK_SENSOR", 0.005),
        inconsistent_format=_float_env("FAULT_RATE_INCONSISTENT_FORMAT", 0.020),
        late_event=_float_env("FAULT_RATE_LATE_EVENT", 0.015),
    )


def main() -> None:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
    mongo_db_name = os.getenv("MONGO_DB", "cnc_raw")
    mqtt_host = os.getenv("MQTT_BROKER", "mosquitto")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    backfill_days = int(os.getenv("BACKFILL_DAYS", "7"))
    backfill_interval = int(os.getenv("BACKFILL_INTERVAL_SECONDS", "300"))
    live_interval = float(os.getenv("LIVE_INTERVAL_SECONDS", "1.0"))
    seed = int(os.getenv("SIMULATOR_SEED", "42"))

    fault_rates = _load_fault_rates()

    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    telemetry_col = mongo_db["telemetry_raw"]
    audit_col = mongo_db["injection_audit_log"]

    print(
        f"[simulator] Checking {backfill_days}-day historical backfill in MongoDB ({mongo_db_name})...",
        flush=True,
    )
    summary = run_backfill_if_needed(
        telemetry_collection=telemetry_col,
        audit_collection=audit_col,
        days=backfill_days,
        interval_seconds=backfill_interval,
        fault_rates=fault_rates,
        seed=seed,
    )
    print(f"[simulator] Backfill summary: {summary}", flush=True)

    mqtt_client = mqtt.Client(client_id="cnc-simulator")
    mqtt_client.connect(mqtt_host, mqtt_port, keepalive=60)
    mqtt_client.loop_start()
    print(f"[simulator] Connected to MQTT {mqtt_host}:{mqtt_port}. Publishing 30 devices live...", flush=True)

    publisher = LiveMqttPublisher(
        mqtt_client=mqtt_client,
        audit_collection=audit_col,
        fault_rates=fault_rates,
        seed=seed + 1000,
        base_elapsed_hours=float(backfill_days * 24),
    )

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            tick_start = time.monotonic()
            emitted = publisher.publish_tick()
            elapsed = time.monotonic() - tick_start
            if int(tick_start) % 15 == 0:
                print(f"[simulator] Published {emitted} live readings in {elapsed:.3f}s", flush=True)
            sleep_for = max(0.05, live_interval - elapsed)
            time.sleep(sleep_for)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        mongo_client.close()
        print("[simulator] Shutdown complete.", flush=True)


if __name__ == "__main__":
    main()
