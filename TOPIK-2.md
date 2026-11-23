# Topik 2 — Industrial Predictive Maintenance

Pabrik ingin mencegah kerusakan mesin yang tidak terencana dengan memantau kondisi mesin secara real-time dan memperkirakan kapan perawatan diperlukan.

## Varian A — Lini Produksi CNC

Lini produksi dengan mesin CNC yang dipantau getaran, suhu spindle, dan kecepatan putar.

| Perangkat | Jumlah min. | Field | Rentang normal |
|---|---|---|---|
| Sensor getaran | 10 | `vibration_rms_mms`, `peak_g` | 0.5–4.5 mm/s |
| Sensor spindle | 10 | `spindle_temp_c`, `rpm`, `load_pct` | 30–70 °C, 0–12000 rpm |
| Penghitung produksi | 5 | `parts_count`, `status` (`RUN`/`IDLE`/`DOWN`) | — |

Simulator wajib memodelkan **degradasi bertahap**: beberapa mesin mengalami kenaikan getaran perlahan selama beberapa hari sebelum "rusak".

**Tantangan signature — Health index dan estimasi sisa umur (RUL)**
- Hitung *health index* 0–100 per mesin dari kombinasi getaran, suhu, dan beban (bobot ditentukan kelompok dan dijelaskan).
- Estimasi **Remaining Useful Life** dengan tren linear getaran 24–72 jam terakhir: kapan getaran diperkirakan mencapai ambang kritis (mis. 7.1 mm/s, ISO 10816).
- Alert *maintenance due* bila RUL < 48 jam.

## Varian B — Stasiun Pompa Air

Stasiun pompa distribusi air bersih yang dipantau tekanan, debit, dan arus motor.

| Perangkat | Jumlah min. | Field | Rentang normal |
|---|---|---|---|
| Sensor tekanan (hisap & buang) | 10 | `suction_bar`, `discharge_bar` | 0.5–2 bar, 3–6 bar |
| Flowmeter | 5 | `flow_m3h` | 50–300 m³/jam |
| Motor pompa | 5 | `current_a`, `motor_temp_c`, `status` | 20–60 A, 30–80 °C |

**Tantangan signature — Deteksi *dry-run*, kavitasi, dan ketersediaan mesin**
- *Dry-run*: arus motor turun tajam dan debit ≈ 0 sementara status `RUN`.
- Kavitasi: tekanan hisap di bawah ambang disertai fluktuasi debit (standar deviasi window tinggi).
- Hitung **availability** per pompa per hari = waktu `RUN` / (waktu `RUN` + waktu `DOWN` tak terencana).

## Contoh Skema Pesan

```json
{
  "device_id": "CNC-07-VIB",
  "device_type": "vibration",
  "machine_id": "CNC-07",
  "timestamp": "2026-11-20T09:15:02Z",
  "data": { "vibration_rms_mms": 3.8, "peak_g": 1.2 }
}
```

Field `device_id`, `device_type`, `machine_id`, `timestamp`, dan `data` wajib ada.

## Data Kotor yang Disarankan

- Sensor *stuck*: nilai sama persis berulang lama.
- Lonjakan sesaat (*spike*) yang tidak fisik, mis. getaran 500 mm/s.
- `status` dengan format tidak konsisten (`run`, `RUN`, `Running`).
- Pesan terlambat atau tidak berurutan dari gateway mesin.

## KPI Streaming dan Alert

- Window 1 dan 10 menit: rata-rata dan maksimum getaran/arus per mesin.
- Alert real-time: ambang kritis, kondisi signature.
- Varian A: produksi per jam per mesin. Varian B: total debit stasiun.

## Saran Star Schema

- **Fakta**: `fact_machine_hourly`, `fact_downtime_event`, `fact_alert`.
- **Dimensi**: `dim_machine` (tipe, tanggal pasang, jadwal servis dari MySQL), `dim_sensor`, `dim_time`, `dim_failure_type`.

## Kriteria Penerimaan

1. Simulator menghasilkan minimal 2 mesin dengan degradasi atau kegagalan yang dapat diamati pada data 7 hari.
2. Tantangan signature mendeteksi mesin bermasalah **sebelum** atau saat kegagalan terjadi, dan ditunjukkan pada dashboard.
3. Warehouse dapat menjawab: mesin mana dengan downtime/alert terbanyak dan tren health index atau availability per hari.
4. Data kotor (mis. sensor stuck dan spike) dibedakan dari anomali nyata, dan jumlahnya dilaporkan.
