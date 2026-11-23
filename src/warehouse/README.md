# CNC Warehouse Loader

`WarehouseLoader` accepts open MySQL and PostgreSQL DB-API connections. The caller owns
and closes both connections.

```python
from warehouse import WarehouseLoader, WarehouseRepository

loader = WarehouseLoader(mysql_connection, postgres_connection)
loader.sync_dimensions()
loader.ensure_time_dimension(history_start, live_horizon_end)

loader.upsert_machine_hourly(hourly_records)
loader.upsert_downtime_events(downtime_events)
loader.upsert_alerts(alerts)
loader.upsert_dq_audit_summaries(audit_summaries)

analytics = WarehouseRepository(postgres_connection)
ranking = analytics.fetch_downtime_and_alert_ranking()
trends = analytics.fetch_daily_health_rul_trends(start_date, end_date)
parts = analytics.fetch_hourly_parts_by_shift(start_date, end_date)
reconciliation = analytics.fetch_dirty_data_reconciliation()
```

Dimension and fact writes use PostgreSQL natural-key constraints for idempotent
upserts. `ensure_time_dimension` normalizes timezone-aware datetimes to UTC hour
boundaries and maps shifts as 06:00–13:59, 14:00–21:59, and 22:00–05:59 UTC.
Analytical accessors return lists of dictionaries keyed by their selected columns.

Install the DB-API drivers for the MySQL and PostgreSQL connections with:

```sh
pip install -r src/warehouse/requirements.txt
```

Run the integration tests against isolated Compose databases with:

```sh
docker compose --project-name cnc-warehouse-test --profile test run --build --rm warehouse-integration-tests
```

To avoid colliding with local database ports, set `MYSQL_PUBLISHED_PORT=13306` and
`POSTGRES_PUBLISHED_PORT=15432` before running. In PowerShell:

```powershell
$env:MYSQL_PUBLISHED_PORT = "13306"
$env:POSTGRES_PUBLISHED_PORT = "15432"
docker compose --project-name cnc-warehouse-test --profile test run --build --rm warehouse-integration-tests
```

The test project uses its own database volumes. Remove only those test volumes when
finished:

```sh
docker compose --project-name cnc-warehouse-test down --volumes
docker compose --profile test run --rm warehouse-integration-tests
```
