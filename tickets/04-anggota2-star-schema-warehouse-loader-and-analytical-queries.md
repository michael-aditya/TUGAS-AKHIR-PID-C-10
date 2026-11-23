# 04: [Anggota 2] Star Schema Dimensional Warehouse Loader & Analytical BI Queries

**What to build:** Deliver the idempotent Data Warehouse dimensional loader and the validated analytical SQL query suite for `cnc_dwh`. The loader synchronizes **Machine** and **Device** master records from MySQL (`cnc_master`) into PostgreSQL `dim_machine` and `dim_sensor`, populates `dim_time` across the historical and live time horizon (assigning each hour to its **Production Shift**), ensures `dim_failure_type` is populated, and provides a clean idempotent upsert interface for `fact_machine_hourly`, `fact_downtime_event`, `fact_alert`, and `dq_audit_summary` so re-running an ETL batch never creates duplicate dimensional or fact rows. Also delivers the analytical SQL queries and repository accessors that answer every required business intelligence question from the rubric.

**Blocked by:** 03: [Anggota 2] MySQL Master Data Reference & PostgreSQL Star Schema Data Warehouse Foundation

**Status:** ready-for-agent

- [ ] Idempotently synchronizes 10 **Machine** rows from MySQL into `dim_machine` and 30 **Device** rows into `dim_sensor`, maintaining stable surrogate keys and up-to-date threshold attributes.
- [ ] Populates `dim_time` for all hourly buckets across the 7-day historical and live window, accurately classifying each hour into `Shift 1 Pagi` (06:00–13:59), `Shift 2 Sore` (14:00–21:59), or `Shift 3 Malam` (22:00–05:59).
- [ ] Provides an idempotent warehouse upsert interface for `fact_machine_hourly`, `fact_downtime_event`, `fact_alert`, and `dq_audit_summary` that safely replaces or updates existing hourly facts without duplicating records on repeated ETL runs.
- [ ] Includes verified analytical SQL queries that join the Star Schema fact and dimension tables to answer: (1) which **Machine** assets have the highest **Unplanned Downtime** duration and alert counts, (2) daily **Health Index** and **Remaining Useful Life (RUL)** trends per **Machine**, (3) hourly parts produced per **Machine** and **Production Shift**, and (4) **Injection Audit Log** vs. ETL caught-count reconciliation per **Dirty Data** category.
