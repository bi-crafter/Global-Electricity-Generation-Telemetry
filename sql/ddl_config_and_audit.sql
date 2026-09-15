-- =============================================================================
-- Global Renewable Energy & Grid Decarbonization Command Center
-- DDL Script: Configuration, Control, Audit, and Data Quality Tables
-- Engine: Fabric SQL Analytics Endpoint (T-SQL Dialect)
-- =============================================================================

-- 1. Configuration Table for Data Sources
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'config_data_sources')
BEGIN
    CREATE TABLE config_data_sources (
        source_id INT NOT NULL,
        source_name VARCHAR(100) NOT NULL,
        source_type VARCHAR(50) NOT NULL,
        endpoint VARCHAR(500) NOT NULL,
        authentication_type VARCHAR(50) NOT NULL,
        target_table VARCHAR(100) NOT NULL,
        frequency VARCHAR(50) NOT NULL,
        active BIT NOT NULL DEFAULT 1,
        priority INT NOT NULL DEFAULT 1,
        watermark_column VARCHAR(50) NULL,
        load_type VARCHAR(50) NOT NULL DEFAULT 'INCREMENTAL',
        created_at DATETIME2 DEFAULT CURRENT_TIMESTAMP
    );
END;

-- Seed Metadata Configuration
INSERT INTO config_data_sources (source_id, source_name, source_type, endpoint, authentication_type, target_table, frequency, active, priority, watermark_column, load_type, created_at)
VALUES
(1, 'Electricity Maps', 'REST_API', 'https://api.electricitymap.org/v3/power-breakdown/latest', 'API_KEY_HEADER', 'bronze_electricity_maps', 'HOURLY', 1, 1, 'datetime', 'INCREMENTAL', CURRENT_TIMESTAMP),
(2, 'Ember Climate', 'CSV_HTTP', 'https://raw.githubusercontent.com/ember-climate/global-electricity-data/main/data/monthly_full_release_long_format.csv', 'NONE', 'bronze_ember', 'MONTHLY', 1, 2, 'Year_Month', 'FULL', CURRENT_TIMESTAMP),
(3, 'World Bank Open Data', 'REST_API', 'https://api.worldbank.org/v2/country/all/indicator/', 'NONE', 'bronze_world_bank', 'ANNUAL', 1, 3, 'date', 'FULL', CURRENT_TIMESTAMP),
(4, 'NASA POWER API', 'REST_API', 'https://power.larc.nasa.gov/api/temporal/daily/point', 'NONE', 'bronze_nasa_power', 'DAILY', 1, 4, 'date', 'INCREMENTAL', CURRENT_TIMESTAMP),
(5, 'Open-Meteo API', 'REST_API', 'https://api.open-meteo.com/v1/forecast', 'NONE', 'bronze_open_meteo', 'HOURLY', 1, 5, 'time', 'INCREMENTAL', CURRENT_TIMESTAMP);

-- 2. State Control Table for Incremental Ingestion
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'control_ingestion_state')
BEGIN
    CREATE TABLE control_ingestion_state (
        source_id INT NOT NULL,
        source_name VARCHAR(100) NOT NULL,
        last_successful_timestamp DATETIME2 NULL,
        last_watermark VARCHAR(100) NULL,
        last_run_id VARCHAR(100) NULL,
        last_status VARCHAR(50) NOT NULL,
        records_ingested BIGINT NOT NULL DEFAULT 0,
        last_error VARCHAR(2000) NULL,
        updated_at DATETIME2 DEFAULT CURRENT_TIMESTAMP
    );
END;

-- Seed State Control Table
INSERT INTO control_ingestion_state (source_id, source_name, last_successful_timestamp, last_watermark, last_run_id, last_status, records_ingested, last_error, updated_at)
VALUES
(1, 'Electricity Maps', NULL, NULL, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP),
(2, 'Ember Climate', NULL, NULL, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP),
(3, 'World Bank Open Data', NULL, NULL, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP),
(4, 'NASA POWER API', NULL, NULL, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP),
(5, 'Open-Meteo API', NULL, NULL, NULL, 'PENDING', 0, NULL, CURRENT_TIMESTAMP);

-- 3. Audit Pipeline Execution Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'audit_pipeline_execution')
BEGIN
    CREATE TABLE audit_pipeline_execution (
        run_id VARCHAR(100) NOT NULL,
        pipeline_name VARCHAR(100) NOT NULL,
        source_name VARCHAR(100) NOT NULL,
        start_time DATETIME2 NOT NULL,
        end_time DATETIME2 NULL,
        status VARCHAR(50) NOT NULL,
        records_processed BIGINT DEFAULT 0,
        records_quarantined BIGINT DEFAULT 0,
        error_message VARCHAR(2000) NULL
    );
END;

-- 4. Data Quality Check Results Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_results')
BEGIN
    CREATE TABLE dq_results (
        dq_check_id VARCHAR(100) NOT NULL,
        run_id VARCHAR(100) NOT NULL,
        source_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        check_name VARCHAR(100) NOT NULL,
        records_checked BIGINT NOT NULL,
        records_failed BIGINT NOT NULL,
        failure_percentage FLOAT NOT NULL,
        status VARCHAR(50) NOT NULL,
        execution_timestamp DATETIME2 DEFAULT CURRENT_TIMESTAMP
    );
END;
