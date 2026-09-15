# Deep-Dive API Data Breakdown & Executive Storytelling Guide

## Project Title
**Global Renewable Energy & Grid Decarbonization Command Center**

---

## SECTION 1: DETAILED DATA SOURCE & FIELD DICTIONARY

### 1. Electricity Maps REST API (Real-Time & Hourly Grid Emissions)

#### Endpoints:
- `https://api.electricitymap.org/v3/power-breakdown/latest?zone={zone}`
- `https://api.electricitymap.org/v3/carbon-intensity/history?zone={zone}`

#### Key Data Fields:
- `zone` (STRING): Grid control zone code (`DE`, `FR`, `GB`, `US-CAL-CISO`, `IN-WE`, `BR`, `JP-TK`).
- `datetime` (TIMESTAMP): UTC timestamp of observation (`2026-09-11T04:00:00.000Z`).
- `carbonIntensity` (DOUBLE): Lifecycle grid emissions in **gCO2eq/kWh** (e.g., `425`).
- `fossilFreePercentage` (DOUBLE): Percentage of power generated from non-fossil fuel sources (`57%`).
- `renewablePercentage` (DOUBLE): Percentage generated specifically from renewables (`55%`).
- `powerProductionBreakdown` (JSON Object): Real-time generation in MW by fuel:
  - `solar` (MW), `wind` (MW), `hydro` (MW), `nuclear` (MW), `biomass` (MW), `geothermal` (MW), `coal` (MW), `gas` (MW), `oil` (MW), `hydro discharge` (MW).

#### Executive Storytelling Angle:
> *"Electricity Maps provides real-time grid telemetry. By monitoring carbon intensity gCO2eq/kWh at 1-hour resolution, grid operators and corporate energy buyers can execute carbon-aware load shifting—running energy-intensive computing tasks during zero-carbon wind/solar peak hours."*

---

### 2. Ember Climate Open Data (Global Yearly & Monthly Generation Mix)

#### Data Sources:
- Ember Global Electricity Datasets / Our World in Data Open Energy Repo (`owid-energy-data.csv`).

#### Key Data Fields:
- `Area` / `country` (STRING): Country or region name (`United States`, `Germany`, `France`, `China`, `India`).
- `Country_code` / `iso_code` (STRING): ISO 3-letter country code (`USA`, `DEU`, `FRA`, `CHN`, `IND`).
- `Year` / `year` (INT): Calendar year (`2000` – `2024`).
- `Category` (STRING): Generation, capacity, or emissions category.
- `Variable` / `technology_name` (STRING): Energy technology (`Solar`, `Wind`, `Hydro`, `Nuclear`, `Coal`, `Gas`, `Oil`).
- `Value` / `electricity_generation` (DOUBLE): Total annual electricity generation in **TWh** or **GWh**.

#### Executive Storytelling Angle:
> *"Ember data provides the historical decarbonization macro-trend. It answers high-level executive questions: Which nations are accelerating renewable capacity fastest? What is the 5-year CAGR of wind vs solar? How much coal generation has been displaced by clean energy over the last decade?"*

---

### 3. World Bank Open Data API (Socio-Economic Context & Normalization)

#### Base Endpoint:
- `https://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json`

#### Key Data Indicators:
- `SP.POP.TOTL`: Total Country Population (`population`).
- `NY.GDP.MKTP.CD`: Gross Domestic Product in USD (`gdp_usd`).
- `EG.USE.ELEC.KH.PC`: Electric Power Consumption per Capita in **kWh** (`electricity_kwh_per_capita`).

#### Executive Storytelling Angle:
> *"Raw generation figures favor large countries. World Bank indicators normalize energy metrics against population and economic activity, allowing executives to compare clean energy efficiency per capita and carbon intensity per unit of GDP across developing and developed economies."*

---

### 4. NASA POWER API (Solar Radiation & Wind Resource Potential)

#### Base Endpoint:
- `https://power.larc.nasa.gov/api/temporal/daily/point`

#### Key Environmental Variables:
- `ALLSKY_SFC_SW_DWN` (DOUBLE): All Sky Surface Shortwave Downward Irradiance in **kWh/m²/day** (Solar Potential).
- `WS50M` (DOUBLE): Wind Speed at 50 Meters above surface in **m/s** (Wind Potential).
- `T2M` (DOUBLE): Temperature at 2 Meters above surface in **°C**.

#### Executive Storytelling Angle:
> *"NASA POWER data provides climatological resource potential. By overlaying solar irradiance (kWh/m²/day) and 50m wind speed against actual generation output, we calculate theoretical capacity factors to identify underperforming grid nodes and optimal sites for new renewable asset deployment."*

---

### 5. Open-Meteo API (Real-Time Weather & Wind Forecasts)

#### Base Endpoint:
- `https://api.open-meteo.com/v1/forecast`

#### Key Forecast Variables:
- `direct_normal_irradiance` (DOUBLE): Direct Normal Solar Irradiance in **W/m²**.
- `wind_speed_80m` (DOUBLE): Wind Speed at turbine hub height (80m) in **m/s**.
- `temperature_2m` (DOUBLE): Ambient surface temperature in **°C**.

#### Executive Storytelling Angle:
> *"Open-Meteo supplies 7-day hourly forward-looking weather context. Integrating wind speed at 80m hub height enables short-term renewable dispatch forecasting, alerting grid operators to impending wind ramp events or solar cloud cover dips."*

---

## SECTION 2: 7-PAGE POWER BI DASHBOARD MAPPING & STORYTELLING FLOW

| Page # | Page Name | Primary Data Sources Used | Key Visuals & Interactive Elements | Executive Insight / Narrative |
| :--- | :--- | :--- | :--- | :--- |
| **Page 1** | **Executive Command Center** | Electricity Maps + Ember | KPI Cards, Glowing 3D Global Grid Map, Stacked Area Generation Trend, Grid Alert Panel | High-level executive overview of global generation, clean energy share (48.5%), and real-time grid carbon intensity. |
| **Page 2** | **Global Renewable Transition** | Ember Climate Data | Country Ranking Bar Chart, Renewable CAGR Line Plot, Solar/Wind Breakdown Donut | Macro decarbonization trends, identifying top transition leaders (China, USA, Brazil, Germany). |
| **Page 3** | **Grid Carbon Intelligence** | Electricity Maps API | Carbon Intensity Heatmap (Zone × Hour), Carbon vs Renewable Scatter Plot | Granular hourly grid emissions analysis, identifying peak carbon hours for load shifting. |
| **Page 4** | **Energy Mix & Generation** | Ember + OWID Datasets | Stacked Area Chart by Fuel, Technology Contribution Treemap, Fossil Dependency Gauge | Deep-dive into energy technology contribution (Coal, Gas, Oil vs Solar, Wind, Hydro, Nuclear). |
| **Page 5** | **Renewable Potential & Weather** | NASA POWER + Open-Meteo | Solar Irradiance Map, 80m Wind Speed Line Chart, Capacity Factor Heatmap | Climatological context comparing theoretical solar/wind potential against actual GWh output. |
| **Page 6** | **Forecast & Scenario Simulator** | Gold Star Schema + DAX What-If | What-If Scenario Slider (+5% to +30%), 2030 Baseline vs Target Forecast Chart, Impact Cards | Interactive What-If simulation calculating estimated emissions avoided (`1.45 MtCO2`) and displaced coal. |
| **Page 7** | **Data & Operational Intelligence** | `dq_results` + `audit_pipeline_execution` | Pipeline Status Card, DQ Scorecard %, Failed Record Grid, Latency Monitor | Engineering maturity page displaying automated pipeline health, DQ scores (99.8%), and refresh logs. |

---

## SECTION 3: JOB-LANDING INTERVIEW DEFENSE STRATEGY

When interviewing for **Microsoft Fabric Architect**, **Senior Analyst**, or **Lead Data Engineer** roles:

1. **Demonstrate Domain & Data Mastery:**
   - *"I didn't just build dashboards—I modeled multi-source energy telemetry. I integrated real-time carbon intensity (gCO2eq/kWh) from Electricity Maps, macro generation TWh from Ember, socio-economic indicators from World Bank, and 50m wind vector fields from NASA POWER."*

2. **Explain the Medallion Data Engineering Strategy:**
   - *"Raw JSON payloads from Open-Meteo and Electricity Maps are staged in Bronze Delta tables. PySpark cleans, normalizes timezones to UTC, and converts units (GWh to MWh) in Silver. Finally, Gold builds a Star Schema (`dim_date`, `dim_energy_technology`, `dim_scenario`, `fact_energy_generation`, `fact_carbon_intensity`) optimized for Fabric Direct Lake queries."*

3. **Highlight Business Impact & Executive Storytelling:**
   - *"The 7-page Power BI Command Center features a What-If scenario simulator that allows C-suite executives to slide renewable penetration targets (+5% to +30%) and instantly see projected 2030 emissions reductions and displaced fossil fuel TWh."*
