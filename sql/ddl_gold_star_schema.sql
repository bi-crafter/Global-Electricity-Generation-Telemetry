-- =============================================================================
-- Global Renewable Energy & Grid Decarbonization Command Center
-- DDL Script: Gold Star Schema (Dimensions & Facts)
-- Engine: Fabric SQL Analytics Endpoint (T-SQL Dialect)
-- =============================================================================

-- 1. Date Dimension
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
BEGIN
    CREATE TABLE dim_date (
        date_key INT NOT NULL,
        full_date DATE NOT NULL,
        year INT NOT NULL,
        quarter INT NOT NULL,
        month INT NOT NULL,
        month_name VARCHAR(20) NOT NULL,
        day_of_month INT NOT NULL,
        day_of_week INT NOT NULL,
        day_name VARCHAR(20) NOT NULL,
        is_weekend BIT NOT NULL,
        year_month VARCHAR(7) NOT NULL,
        fiscal_year INT NOT NULL
    );
END;

-- 2. Energy Technology Dimension
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_energy_technology')
BEGIN
    CREATE TABLE dim_energy_technology (
        technology_key INT NOT NULL,
        technology_name VARCHAR(50) NOT NULL,
        source_category VARCHAR(50) NOT NULL,
        is_clean_energy BIT NOT NULL,
        is_intermittent_renewable BIT NOT NULL,
        typical_capacity_factor FLOAT NULL,
        default_emission_factor_gco2_kwh FLOAT NULL
    );
END;

-- 3. Scenario Dimension
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_scenario')
BEGIN
    CREATE TABLE dim_scenario (
        scenario_key INT NOT NULL,
        scenario_name VARCHAR(50) NOT NULL,
        target_penetration_pct FLOAT NOT NULL,
        fossil_displacement_factor FLOAT NOT NULL
    );
END;

-- 4. Fact Energy Generation
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_energy_generation')
BEGIN
    CREATE TABLE fact_energy_generation (
        country_key INT NOT NULL,
        date_key INT NOT NULL,
        technology_key INT NOT NULL,
        generation_gwh FLOAT NOT NULL DEFAULT 0.0,
        generation_mwh FLOAT NOT NULL DEFAULT 0.0,
        pct_of_total_country_generation FLOAT NULL,
        emissions_mtco2 FLOAT NULL
    );
END;

-- 5. Fact Carbon Intensity
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_carbon_intensity')
BEGIN
    CREATE TABLE fact_carbon_intensity (
        zone_key INT NOT NULL,
        date_key INT NOT NULL,
        hour INT NOT NULL,
        carbon_intensity_gco2_kwh FLOAT NOT NULL,
        direct_carbon_intensity_gco2_kwh FLOAT NULL,
        is_anomaly BIT NOT NULL DEFAULT 0
    );
END;
