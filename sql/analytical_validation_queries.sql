-- =============================================================================
-- Global Renewable Energy & Grid Decarbonization Command Center
-- Analytical Validation & Quality Audit Queries
-- Execution Engine: Fabric SQL Analytics Endpoint / PySpark SQL
-- =============================================================================

-- 1. Daily Average Carbon Intensity by Grid Zone
SELECT 
    zone_id,
    CAST(timestamp_utc AS DATE) AS date,
    ROUND(AVG(carbon_intensity_gco2_kwh), 2) AS avg_carbon_intensity_gco2_kwh,
    MIN(carbon_intensity_gco2_kwh) AS min_carbon_intensity,
    MAX(carbon_intensity_gco2_kwh) AS max_carbon_intensity,
    COUNT(*) AS hourly_readings_count
FROM silver_carbon_intensity
GROUP BY zone_id, CAST(timestamp_utc AS DATE)
ORDER BY avg_carbon_intensity_gco2_kwh ASC;

-- 2. Top Clean Energy Generating Countries (Ember Generation Metric)
SELECT 
    iso_code_3,
    country_name,
    year,
    ROUND(SUM(CASE WHEN technology_name IN ('Solar', 'Wind', 'Hydro', 'Bioenergy', 'Geothermal') THEN generation_gwh ELSE 0 END), 2) AS renewable_generation_gwh,
    ROUND(SUM(generation_gwh), 2) AS total_generation_gwh,
    ROUND((SUM(CASE WHEN technology_name IN ('Solar', 'Wind', 'Hydro', 'Bioenergy', 'Geothermal') THEN generation_gwh ELSE 0 END) / NULLIF(SUM(generation_gwh), 0)) * 100, 2) AS renewable_share_pct
FROM silver_energy_generation
WHERE year = 2024
GROUP BY iso_code_3, country_name, year
HAVING SUM(generation_gwh) > 1000
ORDER BY renewable_share_pct DESC;

-- 3. Data Quality Pipeline Execution Audit Summary
SELECT 
    source_name,
    table_name,
    check_name,
    records_checked,
    records_failed,
    failure_percentage,
    status,
    execution_timestamp
FROM dq_results
ORDER BY execution_timestamp DESC;

-- 4. Anomaly Detection: Carbon Intensity Readings > 3 Standard Deviations
WITH stats AS (
    SELECT 
        zone_id,
        AVG(carbon_intensity_gco2_kwh) AS mean_ci,
        STDDEV(carbon_intensity_gco2_kwh) AS stddev_ci
    FROM silver_carbon_intensity
    GROUP BY zone_id
)
SELECT 
    c.zone_id,
    c.timestamp_utc,
    c.carbon_intensity_gco2_kwh,
    s.mean_ci,
    s.stddev_ci,
    ROUND((c.carbon_intensity_gco2_kwh - s.mean_ci) / NULLIF(s.stddev_ci, 0), 2) AS z_score
FROM silver_carbon_intensity c
JOIN stats s ON c.zone_id = s.zone_id
WHERE ABS((c.carbon_intensity_gco2_kwh - s.mean_ci) / NULLIF(s.stddev_ci, 0)) > 3.0
ORDER BY z_score DESC;
