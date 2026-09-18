# 🎤 Technical Interview Defense Cheatsheet
## Role Target: Microsoft Fabric Data Architect / Senior Data Engineer / Lead Analytics Engineer
**Candidate: Abhijit Sarkar (`bi-crafter`)**

---

## 📌 QUESTION 1: "Tell me about your experience with Microsoft Fabric and how you architect Lakehouses."

### STAR Interview Response:
- **Situation**: Organizations struggle with fragmented, stale data across multiple APIs and laggy DirectQuery dashboards.
- **Task**: I was tasked with building an enterprise data platform unifying real-time grid telemetry from Electricity Maps with macro generation datasets from Ember and NASA weather fields.
- **Action**: 
  - I architected a **Medallion Lakehouse** on Microsoft Fabric (`LH_RENEWABLE_ENERGY`).
  - **Bronze**: Raw Delta tables staging REST API payloads.
  - **Silver**: PySpark notebooks executing UTC timezone normalization, unit conversions (GWh to MWh), and window-based deduplication (`row_number() OVER (PARTITION BY zone_id, timestamp_utc ORDER BY timestamp_utc DESC)`).
  - **Gold**: Conformed Star Schema (`dim_country`, `dim_date`, `dim_energy_technology`, `fact_energy_generation`, `fact_carbon_intensity`).
  - **Data Quality**: Designed a PySpark DQ engine verifying null constraints and range boundaries, logging to `dq_results` (99.8% pass rate).
  - **Direct Lake**: Served a sub-second Direct Lake Semantic Model (`SM_RENEWABLE_ENERGY`) with 26 DAX measures powering a 7-page executive Power BI Command Center.
- **Result**: Reduced dashboard query latency to sub-second levels without Import refresh delays, modeling 774.8 TWh of global generation.

---

## 📌 QUESTION 2: "Why did you choose Direct Lake mode instead of Import or DirectQuery in Power BI?"

### Technical Defense:
1. **Zero Data Movement & No Refresh Lag**: Import mode requires periodic schedule refreshes, creating data latency and memory duplication. Direct Lake queries Delta parquet files directly in OneLake without memory duplication.
2. **Sub-Second Performance**: Unlike traditional DirectQuery which translates DAX into SQL queries on-the-fly (causing slow visual loads and database spikes), Direct Lake uses VertiPaq engine columnar transposing directly over Delta Parquet files in V-Order format.
3. **Enterprise Scalability**: Direct Lake handles multi-terabyte datasets seamlessly while maintaining instant visual interactive responsiveness for C-suite executive users.

---

## 📌 QUESTION 3: "How do you handle Data Governance and Data Quality in your PySpark pipelines?"

### Technical Defense:
1. **Automated Rule Validation**: I engineer a dedicated Data Quality rule engine notebook (`NB_08_DATA_QUALITY_ENGINE.py`).
2. **Constraint Enforcement**: Checks range boundaries (e.g. `carbon_intensity_gco2_kwh BETWEEN 0 AND 1200`), non-null primary keys, and timestamp sequence integrity.
3. **Audit Trail Logging**: Validations write execution metadata (`rule_id`, `table_name`, `records_passed`, `records_failed`, `pass_rate_pct`) to a governance table (`dq_results`).
4. **SLA Protection**: If pass rate drops below threshold (<95%), automated Data Factory pipeline alerts flag pipeline operators before Gold tables are consumed by executives.

---

## 📌 QUESTION 4: "How do you implement What-If Scenario Modeling in DAX?"

### Technical Defense:
1. **Disconnected Parameter Dimension**: Created a `dim_scenario` parameter dimension table with target penetration values (0% to 30%).
2. **Dynamic DAX Harvesting**: Used `SELECTEDVALUE('dim_scenario'[target_penetration_pct], 20.0)` to harvest user selection from single-value slicers.
3. **Interactive Projection Measures**:
   ```dax
   Projected Renewable Share % = 
   [Global Clean Energy Share %] + [Selected Scenario Increase %]

   Estimated Emissions Avoided (MtCO2) = 
   VAR BaselineFossil = [Fossil Generation (TWh)]
   VAR DisplacementPct = [Selected Scenario Increase %] / 100
   VAR DisplacedFossilTWh = BaselineFossil * DisplacementPct
   RETURN
   ROUND(DisplacedFossilTWh * 0.7, 1)
   ```
4. **C-Suite Value**: Executives can interactively slide target expansion parameters and immediately see 2030 projected carbon abatement and displaced coal TWh.

---

## 📌 QUESTION 5: "How do you handle schema evolution and invalid data in Lakehouse ingestion?"

### Technical Defense:
1. **Explicit PySpark Schema Enforcement**: I define explicit `StructType` and `StructField` schemas when reading JSON/CSV REST API payloads, preventing `inferSchema` performance bottlenecks and silent field dropping.
2. **Delta Lake Merge & Schema Overwrite Options**: In PySpark writes, I use `.option("mergeSchema", "true")` or `.option("overwriteSchema", "true")` when column structures evolve.
3. **Failover Ingestion Logic**: In REST API ingestion notebooks, I implement try/except blocks with failover mirror endpoints (e.g. Ember CSV fallback URLs) and regex column name sanitization (`re.sub(r'[\s,;{}()\n\t=]', '_', col)`) to eliminate special character breaking errors.
