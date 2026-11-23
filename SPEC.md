---
title: "Spec: Topik 2A — Industrial Predictive Maintenance (CNC Production Line)"
label: ready-for-agent
status: ready-for-agent
---

## Problem Statement

A manufacturing plant operates a production line of ten Computer Numerical Control (**Machine**) assets (`CNC-01` through `CNC-10`) that suffer from **Unplanned Downtime** when mechanical wear goes undetected until catastrophic breakdown. Each **Machine** is instrumented with three **Device** endpoints—a vibration sensor, a spindle thermal/load sensor, and a production counter (30 **Device** endpoints in total)—emitting continuous **Telemetry Reading** streams at roughly one message per second per **Device**.

However, raw industrial telemetry cannot be trusted directly for operational or predictive decisions:
1. Sensor and gateway faults introduce **Dirty Data**—specifically missing values, duplicate messages, non-physical **Out-of-Range Spike** glitches, **Stuck Sensor** zero-variance flatlines, inconsistent **Operational Status** strings, and late-arriving messages—which can masquerade as mechanical anomalies or distort regression models if not separated from real **Gradual Degradation**.
2. Operators lack a unified real-time and historical view that combines multi-sensor penalties into an actionable **Health Index** (`0–100`), projects **Remaining Useful Life (RUL)** in hours toward the ISO 10816 **Critical Vibration Threshold** (`7.1 mm/s`), and triggers a **Maintenance Due Alert** when **Remaining Useful Life (RUL)** drops below `48 hours`.
3. The entire end-to-end cyber-physical data pipeline—spanning simulation, MQTT ingestion, Kafka streaming, Spark Structured Streaming, MongoDB raw storage, MySQL master data, Airflow batch ETL with Pandas data quality reconciliation, a PostgreSQL Star Schema Data Warehouse, and a custom Flask dashboard—must run reproducibly on a single evaluator laptop via `docker compose up` without paid cloud services.

## Solution

We will build a reproducible, Docker Compose–orchestrated end-to-end data pipeline for **Topik 2A — Lini Produksi CNC** that simulates 10 **Machine** assets (30 **Device** endpoints), seeds 7 days of historical raw telemetry into MongoDB prior to switching to live 1 msg/sec/device MQTT streaming, processes real-time 1-minute and 10-minute windows in Spark Structured Streaming, and executes an Airflow Pandas ETL pipeline that reconciles **Dirty Data** against an out-of-band **Injection Audit Log**, computes each **Machine**'s **Health Index** and linear-regression **Remaining Useful Life (RUL)**, populates a 4-dimension / 3-fact Star Schema Data Warehouse in PostgreSQL, and serves both real-time and historical insights through an interactive Flask dashboard.

## User Stories

1. As a plant reliability engineer, I want the simulator to model 10 **Machine** assets (`CNC-01` through `CNC-10`), each equipped with 3 **Device** endpoints (`vibration`, `spindle`, and `production_counter`, totaling 30 **Device** endpoints), so that every **Machine** reports complete vibration, thermal/load, and production telemetry.
2. As a plant reliability engineer, I want every emitted **Telemetry Reading** to include `device_id`, `device_type`, `machine_id`, `timestamp`, and metric `data`, so that downstream streaming and batch consumers can correlate readings across sensors on the same **Machine**.
3. As a course evaluator, I want the simulator to automatically backfill 7 days of historical **Telemetry Reading** records into raw MongoDB storage on startup before transitioning to live 1 msg/sec/device MQTT publishing, so that the Data Warehouse and dashboard immediately contain 7 days of verifiable history upon `docker compose up`.
4. As a plant reliability engineer, I want the 7-day dataset to model **Gradual Degradation** across at least three distinct **Machine** profiles—`CNC-03` reaching the **Critical Vibration Threshold** (`7.1 mm/s`) and entering **Unplanned Downtime** (`DOWN`) on Day 6, `CNC-07` actively degrading with an estimated **Remaining Useful Life (RUL)** around 28 hours (`< 48 hours`), and `CNC-09` exhibiting early-stage wear (`RUL ~ 83 hours`) while the remaining 7 **Machine** assets stay healthy—so that all predictive maintenance scenarios are observable together.
5. As a data quality engineer, I want the simulator to intentionally inject six configurable categories of **Dirty Data** (`missing_value`, `duplicate`, `out_of_range` / **Out-of-Range Spike**, `stuck_sensor` / **Stuck Sensor**, `inconsistent_format`, and `late_event`), so that the pipeline's data quality defenses can be stress-tested.
6. As a data quality auditor, I want the simulator to record every injected fault in an out-of-band **Injection Audit Log** rather than embedding a dirty flag inside the MQTT JSON payload, so that the ETL pipeline must detect **Dirty Data** purely from domain validation rules while still supporting an exact injected-versus-caught reconciliation report.
7. As a data platform engineer, I want an ingestion bridge that subscribes to MQTT topics (`sensors/cnc/+/+`), forwards raw **Telemetry Reading** payloads to Apache Kafka (`cnc.telemetry.raw`), and persists raw documents to MongoDB, so that both real-time stream processing and batch ETL operate over the same ingested telemetry.
8. As a shop-floor supervisor, I want Spark Structured Streaming to consume `cnc.telemetry.raw` from Kafka and compute 1-minute and 10-minute windowed aggregations (average and maximum vibration RMS, peak G, spindle temperature, RPM, load percentage, and hourly parts produced per **Machine**), so that I can monitor live **Machine** performance.
9. As a shop-floor supervisor, I want Spark Structured Streaming to evaluate real-time threshold breaches and instant **Health Index** scores and write active alerts and windowed metrics to PostgreSQL serving tables, so that the Flask dashboard reflects live conditions within seconds.
10. As a data architect, I want MySQL to serve as the OLTP master data source storing reference attributes for all 10 **Machine** assets (model, production line, install date, service interval, last service date, warning and critical thresholds) and 30 **Device** sensors, so that the ETL pipeline enriches raw telemetry with authoritative reference metadata.
11. As a data quality engineer, I want the Airflow Pandas ETL pipeline to distinguish real **Gradual Degradation** (`vibration_rms_mms` in `4.5–15.0 mm/s`, `spindle_temp_c` in `70–110 °C`) from non-physical **Out-of-Range Spike** faults (`vibration_rms_mms > 25.0 mm/s` or `< 0`, `spindle_temp_c > 150 °C` or `< 0`, `rpm > 20000` or `< 0`, `load_pct > 120%` or `< 0%`), so that genuine mechanical wear triggers maintenance alerts while sensor glitches do not corrupt **Remaining Useful Life (RUL)** linear regression slopes.
12. As a data quality engineer, I want the ETL pipeline to detect **Stuck Sensor** faults whenever a continuous floating-point metric (`vibration_rms_mms` or `spindle_temp_c`) repeats the exact same value (`diff == 0.0000`) for 4 or more consecutive readings, log the fault count, and impute the affected values using the device's rolling median, so that sensor flatlines are caught and repaired.
13. As a data quality engineer, I want the ETL pipeline to detect and standardize inconsistent **Operational Status** strings (`"run"`, `"Running"`, `"idle "`, `"down_err"` → `"RUN"`, `"IDLE"`, `"DOWN"`) and non-ISO timestamps, deduplicate exact `(device_id, timestamp)` collisions, impute `missing_value` nulls via per-device rolling median / forward-fill, and bucket `late_event` readings (`ingested_at - timestamp > 120s`) into their true event hour, so that downstream analytics remain complete and accurate.
14. As a plant reliability engineer, I want the pipeline to compute each **Machine**'s **Health Index** (`0–100`) as `100 - (0.50 * P_vib + 0.30 * P_temp + 0.20 * P_load)` using normalized linear penalties over vibration (`0.5–7.1 mm/s`), spindle temperature (`30–85 °C`), and spindle load (`20–100%`), so that multi-sensor wear is summarized in a single explainable metric.
15. As a plant reliability engineer, I want the pipeline to estimate **Remaining Useful Life (RUL)** in hours for each **Machine** by fitting an ordinary least squares linear regression (`y = a * t + b`) over the last 48 hours of clean hourly vibration averages and projecting the remaining hours until `y` reaches the **Critical Vibration Threshold** (`7.1 mm/s`), so that I know when each degrading **Machine** will fail.
16. As a maintenance planner, I want the system to automatically emit a **Maintenance Due Alert** (`MAINTENANCE_DUE_RUL`) whenever a **Machine**'s estimated **Remaining Useful Life (RUL)** is less than `48 hours`, so that preventive maintenance can be scheduled before **Unplanned Downtime** occurs.
17. As a data analyst, I want the Airflow DAG to load cleaned, enriched metrics into a PostgreSQL Star Schema Data Warehouse (`cnc_dwh`) comprising four dimension tables (`dim_machine`, `dim_sensor`, `dim_time` with **Production Shift**, and `dim_failure_type`) and three fact tables (`fact_machine_hourly`, `fact_downtime_event`, and `fact_alert`), so that historical BI queries run against a dimensional model.
18. As a plant manager, I want to view a custom Flask web dashboard that auto-refreshes real-time **Machine** status cards (`RUN`/`IDLE`/`DOWN`), 1m/10m windowed telemetry, live **Health Index** gauges, **Remaining Useful Life (RUL)** badges, and flashing **Maintenance Due Alert** banners, so that I have immediate situational awareness of the shop floor.
19. As a plant manager, I want the Flask dashboard to display 7-day historical degradation charts with linear regression projection lines toward `7.1 mm/s`, daily **Health Index** trends, rankings of **Machine** assets by **Unplanned Downtime** duration and alert count, hourly parts produced per **Machine**, and an **Injection Audit Log** vs. ETL caught-count reconciliation table, so that every evaluation rubric requirement can be demonstrated from one interface.
20. As a course evaluator, I want the entire stack to run within 4–5 GB of RAM via a single `docker compose up` command using Kafka KRaft mode, Airflow `LocalExecutor`, and Spark `local[*]`, and to include a manual "Trigger ETL Refresh" button in the Flask dashboard, so that grading and live demonstration succeed reliably on any laptop.
21. As a 3-person student team, I want clear module ownership and report/reflection templates mapped to our three roles (Member 1: Ingestion & Streaming; Member 2: Storage, Master Data & Star Schema Data Warehouse; Member 3: Airflow Orchestration, Data Quality & Flask Dashboard), so that each team member has distinct Git commits and can confidently explain and modify their assigned modules during the oral examination.

## Implementation Decisions

- **Domain & Asset Topology**:
  - The shop floor consists of 10 **Machine** assets (`CNC-01` .. `CNC-10`) and 30 **Device** sensors (3 per **Machine**: `CNC-XX-VIB` for `vibration`, `CNC-XX-SPD` for `spindle`, and `CNC-XX-PRD` for `production_counter`).
  - Three **Machine** assets follow deterministic-plus-noise **Gradual Degradation** curves across the 7-day window:
    - `CNC-03`: rapid wear starting Day 3, breaching `7.1 mm/s` on Day 6 (`Health Index < 25`, `RUL = 0h`), transitioning to `DOWN` (**Unplanned Downtime**).
    - `CNC-07`: progressive 72-hour degradation reaching `~5.8 mm/s` at `+0.045 mm/s/hour`, yielding an OLS 48-hour **Remaining Useful Life (RUL)** of `~28 hours` (`< 48h`) and triggering an active **Maintenance Due Alert**.
    - `CNC-09`: early degradation reaching `~4.6 mm/s` at `+0.030 mm/s/hour`, yielding `RUL ~ 83 hours` (trending downward, above the 48h alert threshold).
    - Remaining 7 **Machine** assets: stable operation (`1.2–3.2 mm/s`, slope `<= 0`, `RUL` capped at `168 hours`).
- **Out-of-Band Dirty Data Audit (ADR-0001)**:
  - The simulator injects 6 configurable **Dirty Data** fault types (`missing_value`, `duplicate`, `out_of_range`, `stuck_sensor`, `inconsistent_format`, `late_event`) controlled by environment variables.
  - Telemetry JSON payloads never contain a dirty flag; instead, the simulator records ground-truth injection events in a separate MongoDB collection (`injection_audit_log`).
- **7-Day Historical Backfill via Raw Storage & Batch ETL (ADR-0002)**:
  - On startup, if the raw MongoDB collection lacks the 7-day historical baseline, the simulator generates 7 days of historical **Telemetry Reading** documents (and corresponding **Injection Audit Log** entries) directly into MongoDB raw storage, then switches to continuous live MQTT publishing (`1 msg/sec/device`).
  - The Airflow ETL pipeline processes those raw MongoDB documents against MySQL master data to populate the 7-day Star Schema in PostgreSQL.
- **Storage Split & Serving Layer (ADR-0003)**:
  - **MySQL (`cnc_master`)**: holds authoritative master data tables for machines and sensors, initialized automatically on startup.
  - **MongoDB (`cnc_raw`)**: holds `telemetry_raw` and `injection_audit_log`.
  - **PostgreSQL (`cnc_dwh` & `airflow_db`)**: hosts Airflow metadata, the 4-dimension / 3-fact Star Schema (`dim_machine`, `dim_sensor`, `dim_time`, `dim_failure_type`, `fact_machine_hourly`, `fact_downtime_event`, `fact_alert`), the data quality reconciliation summary table (`dq_audit_summary`), and the Spark Structured Streaming real-time serving tables (`rt_window_metrics`, `rt_active_alerts`).
- **Dirty Data Separation & Imputation Rules (ADR-0004)**:
  - Real **Gradual Degradation** (`vibration_rms_mms` in `4.5–15.0 mm/s`, `spindle_temp_c` in `70–110 °C`) passes data quality validation and drives alerts and RUL regression.
  - Non-physical **Out-of-Range Spike** values (`< 0`, `vibration_rms_mms > 25.0 mm/s`, `spindle_temp_c > 150 °C`, `rpm > 20000`, `load_pct > 120%`), **Stuck Sensor** runs (`>= 4` consecutive readings with `0.0000` float difference), and `missing_value` nulls are logged to the data quality audit and imputed with the device's rolling median so hourly buckets remain contiguous without skewing linear regression slopes.
  - `duplicate` records (`(device_id, timestamp)` collisions) are logged and deduplicated (`keep='first'`).
  - `inconsistent_format` values (non-canonical **Operational Status** strings and non-ISO timestamps) are logged and normalized in-place.
  - `late_event` readings (`ingested_at - timestamp > 120s`) are logged and assigned to their true event-time hour bucket.
- **Signature Challenge Mathematics (Health Index & RUL)**:
  - Normalized component penalties $P \in [0, 100]$:
    - $P_{\text{vib}} = \text{clamp}\left(\frac{\text{vib} - 0.5}{7.1 - 0.5} \times 100, 0, 100\right)$
    - $P_{\text{temp}} = \text{clamp}\left(\frac{\text{temp} - 30.0}{85.0 - 30.0} \times 100, 0, 100\right)$
    - $P_{\text{load}} = \text{clamp}\left(\frac{\text{load} - 20.0}{100.0 - 20.0} \times 100, 0, 100\right)$
  - Composite score: $\text{Health Index} = \text{round}(100 - (0.50 \cdot P_{\text{vib}} + 0.30 \cdot P_{\text{temp}} + 0.20 \cdot P_{\text{load}}), 2)$.
  - **Remaining Useful Life (RUL)** uses Ordinary Least Squares linear regression ($y = a \cdot t + b$) over the last 48 hours of clean hourly average vibration per **Machine** toward `7.1 mm/s`. When $a > 0$ and $y_{\text{now}} < 7.1$, $\text{RUL} = (7.1 - y_{\text{now}}) / a$; when $\text{RUL} < 48\text{ hours}$, a **Maintenance Due Alert** (`MAINTENANCE_DUE_RUL`) is emitted.
- **Lightweight Single-Host Container Topology (ADR-0005)**:
  - Uses Kafka in KRaft mode (no Zookeeper), Airflow in `LocalExecutor` mode (no Redis or separate Celery worker containers), and Spark Structured Streaming in `local[*]` mode to keep total memory consumption within 4–5 GB RAM.

## Testing Decisions

- **Philosophy — Test External Behavior at High-Level Seams**:
  - Tests must verify observable domain behavior across deep module interfaces rather than internal helper functions or private state.
- **Three High-Level Testing Seams**:
  1. **Seam 1 — Telemetry & Fault Generation Module (`generate_telemetry_batch`)**:
     - *Interface*: Accepts simulation parameters `(machines, start_time, hours, interval_seconds, fault_rates, seed)` and returns `(telemetry_readings, injection_audit_log)`.
     - *Verified Behavior*: Generates all 30 **Device** streams across 10 **Machine** assets, reproduces the 7-day **Gradual Degradation** trajectories for `CNC-03`, `CNC-07`, and `CNC-09`, injects all 6 **Dirty Data** categories at the requested rates without polluting payload schemas with dirty flags, and produces an exact matching **Injection Audit Log**.
  2. **Seam 2 — Batch ETL, Data Quality & Signature Analytics Module (`run_cnc_etl_pipeline`)**:
     - *Interface*: Accepts `(raw_telemetry_readings, injection_audit_log, master_machines, master_sensors)` and returns the dimensional and audit dataset `(dim_machine, dim_sensor, dim_time, dim_failure_type, fact_machine_hourly, fact_downtime_event, fact_alert, dq_audit_summary)`.
     - *Verified Behavior*: Accurately catches and reconciles all 6 **Dirty Data** fault types against the **Injection Audit Log**, preserves real **Gradual Degradation** without false-positive spike filtering, computes **Health Index** (`0–100`) and 48-hour OLS **Remaining Useful Life (RUL)**, triggers `MAINTENANCE_DUE_RUL` (`< 48h`) for `CNC-07` and `UNPLANNED_DOWNTIME` / `VIB_CRITICAL` for `CNC-03`, and populates valid Star Schema fact/dimension structures.
  3. **Seam 3 — Flask Dashboard Application Seam (`create_app(repository)`)**:
     - *Interface*: Flask WSGI application factory backed by a repository adapter, tested via the Flask HTTP test client (`GET /`, `GET /api/realtime`, `GET /api/historical`, `GET /api/data-quality`, `POST /api/trigger-etl`).
     - *Verified Behavior*: Returns well-formed JSON and HTML views containing real-time 1m/10m windowed metrics, 7-day **Health Index** and **Remaining Useful Life (RUL)** trends, top downtime/alert rankings, and **Injection Audit Log** vs. caught-count reconciliation.
- **End-to-End Docker Compose Verification**:
  - In addition to automated tests across Seams 1–3, the live Docker Compose stack is verified by checking container readiness, MQTT-to-Kafka-to-Spark streaming output, Airflow ETL DAG completion into PostgreSQL `cnc_dwh`, and HTTP responses from the running Flask container.

## Out of Scope

- Physical hardware sensors or PLC/SCADA integrations (all data originates from the Python simulator).
- Paid or external cloud services (AWS, GCP, Azure, Confluent Cloud, Databricks).
- Grafana dashboard configuration (replaced by the custom Flask web dashboard per team preference).
- Complex deep-learning RUL models (e.g., LSTM/Transformer neural networks); linear regression over the 24–72h window is the canonical estimator.
- Authoring the final prose narrative of `laporan.md`, `slides.pdf`, `video-demo.mp4`, or individual reflection essays (templates and data-backed SQL/charts will be provided for the 3 team members to complete).

## Further Notes

- **Team Role & Ticket Mapping (3 Members)**:
  - **Member 1 (Ingestion & Streaming)**: Owns **T2** (CNC Telemetry Simulator, 7-day backfill, gradual degradation, dirty data injector), **T3** (MQTT-to-Kafka bridge & raw MongoDB sink), and **T4** (Spark Structured Streaming 1m/10m windows & real-time alerts).
  - **Member 2 (Storage, Master Data & Data Warehouse)**: Owns **T1** (Docker Compose foundation, MySQL `cnc_master` schema/seed, PostgreSQL `cnc_dwh` Star Schema DDL, MongoDB collections) and the Star Schema warehouse loading & analytical SQL queries in **T5**.
  - **Member 3 (Orchestration, Data Quality & Flask Dashboard)**: Owns the Airflow DAG, 6-rule Pandas Data Quality engine, and Health Index / RUL signature calculations in **T5**, **T6** (Flask real-time & historical dashboard), and leads **T7** (end-to-end integration, `README.md`, and report/reflection templates).
