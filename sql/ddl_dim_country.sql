-- =============================================================================
-- Global Renewable Energy & Grid Decarbonization Command Center
-- DDL Script: dim_country Dimension Table
-- Engine: Fabric SQL Analytics Endpoint / SQL Warehouse
-- =============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_country')
BEGIN
    CREATE TABLE dim_country (
        country_sk INT NOT NULL,
        iso_code_3 VARCHAR(3) NOT NULL,
        country_name VARCHAR(100) NOT NULL,
        region VARCHAR(50) NOT NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL
    );
END;

-- Populate dim_country Master Records
INSERT INTO dim_country (country_sk, iso_code_3, country_name, region, latitude, longitude) VALUES
(1, 'USA', 'United States', 'North America', 37.0902, -95.7129),
(2, 'DEU', 'Germany', 'Europe', 51.1657, 10.4515),
(3, 'FRA', 'France', 'Europe', 46.2276, 2.2137),
(4, 'CHN', 'China', 'Asia-Pacific', 35.8617, 104.1954),
(5, 'IND', 'India', 'Asia-Pacific', 20.5937, 78.9629),
(6, 'BRA', 'Brazil', 'South America', -14.2350, -51.9253),
(7, 'JPN', 'Japan', 'Asia-Pacific', 36.2048, 138.2529),
(8, 'GBR', 'United Kingdom', 'Europe', 55.3781, -3.4360);
