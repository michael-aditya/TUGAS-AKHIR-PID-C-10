SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS machine (
    machine_id VARCHAR(16) NOT NULL PRIMARY KEY,
    model VARCHAR(64) NOT NULL,
    production_line VARCHAR(32) NOT NULL,
    installation_date DATE NOT NULL,
    service_interval_days SMALLINT UNSIGNED NOT NULL,
    last_service_date DATE NOT NULL,
    vibration_warning_threshold_mms DECIMAL(5, 2) NOT NULL,
    vibration_critical_threshold_mms DECIMAL(5, 2) NOT NULL,
    spindle_temp_warning_threshold_c DECIMAL(5, 1) NOT NULL,
    spindle_temp_critical_threshold_c DECIMAL(5, 1) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_machine_service_interval CHECK (service_interval_days > 0),
    CONSTRAINT chk_machine_vibration_thresholds CHECK (
        vibration_warning_threshold_mms < vibration_critical_threshold_mms
    ),
    CONSTRAINT chk_machine_temperature_thresholds CHECK (
        spindle_temp_warning_threshold_c < spindle_temp_critical_threshold_c
    )
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS device (
    device_id VARCHAR(24) NOT NULL PRIMARY KEY,
    machine_id VARCHAR(16) NOT NULL,
    device_type VARCHAR(32) NOT NULL,
    engineering_unit VARCHAR(32) NOT NULL,
    normal_range_min DECIMAL(12, 3) NOT NULL,
    normal_range_max DECIMAL(12, 3) NOT NULL,
    sampling_rate_hz DECIMAL(6, 3) NOT NULL DEFAULT 1.000,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_device_machine FOREIGN KEY (machine_id)
        REFERENCES machine (machine_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT chk_device_type CHECK (
        device_type IN ('vibration', 'spindle', 'production_counter')
    ),
    CONSTRAINT chk_device_normal_range CHECK (normal_range_min < normal_range_max),
    CONSTRAINT chk_device_sampling_rate CHECK (sampling_rate_hz > 0),
    INDEX idx_device_machine_id (machine_id)
) ENGINE=InnoDB;

INSERT INTO machine (
    machine_id,
    model,
    production_line,
    installation_date,
    service_interval_days,
    last_service_date,
    vibration_warning_threshold_mms,
    vibration_critical_threshold_mms,
    spindle_temp_warning_threshold_c,
    spindle_temp_critical_threshold_c
) VALUES
    ('CNC-01', 'DMG MORI CMX 600 V', 'Line A', '2019-03-15', 90, '2026-10-15', 4.5, 7.1, 70.0, 85.0),
    ('CNC-02', 'DMG MORI CMX 600 V', 'Line A', '2019-06-20', 90, '2026-10-20', 4.5, 7.1, 70.0, 85.0),
    ('CNC-03', 'Haas VF-2', 'Line A', '2018-11-02', 90, '2026-09-28', 4.5, 7.1, 70.0, 85.0),
    ('CNC-04', 'Haas VF-2', 'Line A', '2020-01-10', 90, '2026-10-10', 4.5, 7.1, 70.0, 85.0),
    ('CNC-05', 'Mazak VCN-530C', 'Line B', '2019-08-05', 90, '2026-10-05', 4.5, 7.1, 70.0, 85.0),
    ('CNC-06', 'Mazak VCN-530C', 'Line B', '2020-04-22', 90, '2026-10-22', 4.5, 7.1, 70.0, 85.0),
    ('CNC-07', 'Okuma GENOS M560-V', 'Line B', '2018-07-17', 90, '2026-09-17', 4.5, 7.1, 70.0, 85.0),
    ('CNC-08', 'Okuma GENOS M560-V', 'Line B', '2021-02-11', 90, '2026-10-11', 4.5, 7.1, 70.0, 85.0),
    ('CNC-09', 'Brother SPEEDIO S700X1', 'Line C', '2020-09-30', 90, '2026-09-30', 4.5, 7.1, 70.0, 85.0),
    ('CNC-10', 'Brother SPEEDIO S700X1', 'Line C', '2022-05-09', 90, '2026-11-09', 4.5, 7.1, 70.0, 85.0)
ON DUPLICATE KEY UPDATE
    model = VALUES(model),
    production_line = VALUES(production_line),
    installation_date = VALUES(installation_date),
    service_interval_days = VALUES(service_interval_days),
    last_service_date = VALUES(last_service_date),
    vibration_warning_threshold_mms = VALUES(vibration_warning_threshold_mms),
    vibration_critical_threshold_mms = VALUES(vibration_critical_threshold_mms),
    spindle_temp_warning_threshold_c = VALUES(spindle_temp_warning_threshold_c),
    spindle_temp_critical_threshold_c = VALUES(spindle_temp_critical_threshold_c);

INSERT INTO device (
    device_id,
    machine_id,
    device_type,
    engineering_unit,
    normal_range_min,
    normal_range_max,
    sampling_rate_hz
) VALUES
    ('CNC-01-VIB', 'CNC-01', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-01-SPD', 'CNC-01', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-01-PRD', 'CNC-01', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-02-VIB', 'CNC-02', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-02-SPD', 'CNC-02', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-02-PRD', 'CNC-02', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-03-VIB', 'CNC-03', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-03-SPD', 'CNC-03', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-03-PRD', 'CNC-03', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-04-VIB', 'CNC-04', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-04-SPD', 'CNC-04', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-04-PRD', 'CNC-04', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-05-VIB', 'CNC-05', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-05-SPD', 'CNC-05', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-05-PRD', 'CNC-05', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-06-VIB', 'CNC-06', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-06-SPD', 'CNC-06', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-06-PRD', 'CNC-06', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-07-VIB', 'CNC-07', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-07-SPD', 'CNC-07', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-07-PRD', 'CNC-07', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-08-VIB', 'CNC-08', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-08-SPD', 'CNC-08', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-08-PRD', 'CNC-08', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-09-VIB', 'CNC-09', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-09-SPD', 'CNC-09', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-09-PRD', 'CNC-09', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000),
    ('CNC-10-VIB', 'CNC-10', 'vibration', 'mm/s RMS', 0.000, 4.500, 1.000),
    ('CNC-10-SPD', 'CNC-10', 'spindle', '°C', 30.000, 70.000, 1.000),
    ('CNC-10-PRD', 'CNC-10', 'production_counter', 'parts/reading', 0.000, 5000.000, 1.000)
ON DUPLICATE KEY UPDATE
    machine_id = VALUES(machine_id),
    device_type = VALUES(device_type),
    engineering_unit = VALUES(engineering_unit),
    normal_range_min = VALUES(normal_range_min),
    normal_range_max = VALUES(normal_range_max),
    sampling_rate_hz = VALUES(sampling_rate_hz);
