# Microsoft Fabric Architect Resume & Portfolio Defense Guide

## Project Title
**Global Renewable Energy & Grid Decarbonization Command Center**

---

## 1. Resume Executive Summary (For Fabric Architect / Senior Data Engineer Roles)

> **Principal Data Architect / Senior Microsoft Fabric Engineer** with expertise in designing enterprise-grade Medallion Lakehouses, metadata-driven Fabric Data Factory pipelines, PySpark Delta Lake transformations, and Direct Lake semantic models. Architected an enterprise energy analytics command center ingesting multi-source REST APIs and multi-gigabyte open datasets, reducing carbon intensity data processing latency by 85% and enabling real-time decarbonization scenario modeling for global grid networks.

---

## 2. Impact-Driven Resume Bullet Points (XYZ Format)

### Primary Experience / Key Accomplishments

- **Architected & Deployed Enterprise Medallion Lakehouse:** Designed a 3-tier (Bronze, Silver, Gold) Microsoft Fabric Lakehouse architecture using PySpark and Delta Lake, processing real-time REST API feeds (Electricity Maps, Open-Meteo) and 23K+ global electricity generation records across 215+ countries.
- **Built Metadata-Driven Data Factory Orchestration:** Created a metadata-driven ingestion framework using Fabric Data Factory driven by a `config_data_sources` control table, implementing dynamic sequence execution, zero-downtime retries, and high-concurrency Spark session optimization.
- **Implemented Automated Data Quality & Governance Engine:** Developed an automated PySpark Data Quality validation engine logging schema compliance, range boundaries, and null checks into a `dq_results` audit table, increasing dataset reliability to 99.8% and preventing downstream reporting errors.
- **Designed Direct Lake Semantic Model & DAX Catalog:** Formulated a Gold Star Schema (`dim_date`, `dim_energy_technology`, `dim_scenario`, `fact_energy_generation`, `fact_carbon_intensity`) in Fabric SQL Analytics Endpoint with 15+ advanced DAX measures (CAGR, YoY Carbon Reduction %, Displaced Coal Generation).
- **Engineered What-If Decarbonization Scenario Simulator:** Integrated a disconnected parameter dimension and Power BI What-If parameter model enabling executive scenario simulation (+5% to +30% renewable penetration) to project 2030 emissions reduction targets.
- **Delivered 7-Page Executive Command Center:** Developed a modern, dark-theme Power BI report featuring interactive 3D global carbon intensity heatmaps, stacked area energy mix trends, and real-time grid telemetry alerts.

---

## 3. ATS Technical Skills Matrix

| Skill Category | Microsoft Fabric & Azure Architect Skills |
| :--- | :--- |
| **Platform & Storage** | Microsoft Fabric, Lakehouse, SQL Analytics Endpoint, Delta Lake, Parquet, OneLake, Azure Key Vault |
| **Orchestration & ETL** | Fabric Data Factory, Pipelines, Metadata-Driven Pipelines, Dynamic Expressions, Triggers, Schedule Refresh |
| **Processing & Code** | PySpark, Python (`requests`, `urllib3`), Delta MERGE, Window Functions, Spark SQL, T-SQL |
| **Semantic & Analytics**| Direct Lake, Import Mode, Star Schema Dimensional Modeling, DAX, Time Intelligence, What-If Parameters |
| **Visualization & UX** | Power BI Desktop & Service, Glassmorphism UI, Geospatial Heatmaps, Dynamic Tooltips, Drill-Through |
| **Governance & Security**| Data Quality Scorecard, Audit Lineage, MD5 Hashing, RLS, Secret Scopes (`mssparkutils`) |

---

## 4. GitHub / Portfolio Project Description

```markdown
### Global Renewable Energy & Grid Decarbonization Command Center
**Tech Stack:** Microsoft Fabric | PySpark | Delta Lake | Data Factory | T-SQL | DAX | Power BI

#### Architectural Highlights:
1. **Multi-Source REST API Ingestion:** Built fault-tolerant PySpark ingestion notebooks for Electricity Maps API, Ember Open Data, World Bank API, NASA POWER, and Open-Meteo, with exponential backoff and secret manager parameterization.
2. **Medallion Standardization:** Normalizes disparate raw payloads into clean Silver Delta tables with unit conversion (GWh to MWh), timezone alignment (UTC), and windowing deduplication.
3. **Gold Star Schema:** Features `dim_date`, `dim_energy_technology`, `dim_scenario`, `fact_energy_generation`, and `fact_carbon_intensity` optimized for Fabric Direct Lake queries.
4. **Data Quality Governance:** Automated PySpark DQ engine recording check results into `dq_results` table for complete compliance transparency.
```

---

## 5. Top Interview Questions & Technical Defense Strategies

### Q1: How did you design the orchestration layer in Microsoft Fabric?
> **Answer:** *"I implemented a metadata-driven orchestration model using Fabric Data Factory. Instead of hardcoding static pipelines, a `config_data_sources` table stores endpoints, frequencies, and priority tags. The master pipeline (`PL_01_Master_Orchestrator`) executes a Lookup activity to read active sources, iterates sequentially to prevent Delta write locks, and executes Silver standardization and Gold Star Schema builds once all raw data is staged."*

### Q2: How did you handle API rate limits and external service downtime?
> **Answer:** *"I designed a multi-layer resilience model: first, at the Data Factory level using 3-attempt retries with a 30-second backoff. Second, inside PySpark using `requests.Session` with exponential backoff (`urllib3.util.retry`). Third, I implemented a circuit breaker fallback that loads cached Delta snapshots if an external API experiences extended downtime."*

### Q3: Why did you choose Direct Lake / Import mode for the Power BI Semantic Model?
> **Answer:** *"Fabric Direct Lake mode queries Delta tables directly in OneLake without copying data or executing traditional SQL query translation. This delivers sub-second query performance over millions of records while maintaining strict 1-to-many single-direction star schema relationships."*
