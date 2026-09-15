# Master Step-by-Step Power BI Report Building Guide
## Project: Global Renewable Energy & Grid Decarbonization Command Center
*Role Target: Senior Data Analyst / Fabric Data Architect / Lead BI Engineer*
*Model Target: Matched 100% to Active Semantic Model `SM_RENEWABLE_ENERGY`*

---

## 📌 OVERVIEW & OBJECTIVE

This guide provides an **end-to-end, step-by-step execution plan** for building all 7 pages of the Power BI Executive Command Center Report.

---

## 📄 PAGE 1: EXECUTIVE COMMAND CENTER (SUMMARY)
*Page 1 is complete! Header, 4 KPI cards, Azure Bubble Map, Energy Mix Donut, Stacked Area Trend, Top Clean Leaders Bar Chart, and Grid Audit Table.*

---

## 📄 PAGE 2: GLOBAL RENEWABLE TRANSITION (SUMMARY)
*Page 2 is complete! Header, 4 KPI cards, Top Clean Leaders Bar Chart, 5-Yr Growth Trajectory Line Plot, Solar vs Wind Column Comparison, Fuel Breakdown Donut, and Performance Matrix.*

---

## 📄 PAGE 3: GRID CARBON INTELLIGENCE & HOURLY TELEMETRY (DETAILED BUILD GUIDE)

**Goal**: Analyze real-time hourly grid emissions telemetry (Electricity Maps API data), identify peak carbon hours, model load-shifting opportunities, and analyze the correlation between renewable penetration % and grid carbon intensity (gCO2eq/kWh).

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ VISUAL 1: Top Title Banner ("PAGE 3: GRID CARBON INTELLIGENCE & HOURLY EMISSIONS TELEMETRY")      │
├──────────────────┬──────────────────┬──────────────────┬──────────────────┬──────────────────────┤
│ KPI CARD 1       │ KPI CARD 2       │ KPI CARD 3       │ KPI CARD 4       │ ZONE SLICER          │
│ Avg Carbon gCO2  │ Peak Hours Avg   │ Load Shift Save  │ Correlation R²   │ [fact_carbon[zone]]  │
├──────────────────┴──────────────────┴──────────────────┼──────────────────┴──────────────────────┤
│ VISUAL 2: Hourly Grid Carbon Intensity Heatmap Matrix  │ VISUAL 3: Carbon Intensity vs Renewable  │
│ (Matrix: Rows=zone_id, Cols=hour 0-23,                 │  Share Scatter Plot                      │
│  Values=Global Grid Carbon Intensity, Gradient Color)  │ (Scatter: X=Clean Share %, Y=Carbon gCO2,│
│                                                        │  Play Axis=year)                         │
├────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
│ VISUAL 4: Hourly Load-Shifting Opportunity Curve       │ VISUAL 5: High Anomaly Grid Zone Bar    │
│ (Line Chart: X=hour 0-23, Y=Global Grid Carbon gCO2)   │ (Clustered Bar: Y=zone_id, X=Carbon gCO2)│
├────────────────────────────────────────────────────────┴─────────────────────────────────────────┤
│ VISUAL 6: Detailed Grid Emissions Telemetry Audit Table                                          │
│ (Table: zone_id, date_key, hour, carbon_intensity_gco2_kwh, is_anomaly)                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 🎨 STEP-BY-STEP VISUAL BUILDOUT INSTRUCTIONS (PAGE 3)

#### Visual 1: Navigation & Header Banner (Top, X: 20, Y: 10, Height: 70, Width: 1880)
1. Add a **Shape Visual** (Rectangle) ➔ Fill: `#141D36`, Border: `#00E5FF` (1px), Radius: `8px`.
2. Add a **Text Box**: `"PAGE 3: GRID CARBON INTELLIGENCE & HOURLY EMISSIONS TELEMETRY"` (Font: Segoe UI Bold, 18pt, Color: `#00E5FF`).
3. Add Subtitle: `"Real-Time Grid Telemetry, Peak Load-Shifting & Carbon Intensity Correlation"` (10pt, `#A0AEC0`).

---

#### Row 1: Executive KPI Cards & Slicers (Y: 90, Height: 120)

1. **KPI Card 1 (X: 20, Width: 350)**:
   - Field: **`[Global Grid Carbon Intensity]`** (Expected Value: `313.9 gCO2/kWh`)
   - Title: `"AVG GRID CARBON INTENSITY"`
   - Callout Value: `#FFB300` Amber, 24pt.
2. **KPI Card 2 (X: 390, Width: 350)**:
   - Field: **`[Peak Carbon Hours Average]`** (Expected Value: `392.4 gCO2/kWh`)
   - Title: `"PEAK HOURS AVG (gCO2)"`
   - Callout Value: `#FF3D00` Coral Red, 24pt.
3. **KPI Card 3 (X: 760, Width: 350)**:
   - Field: **`[Load Shifting Carbon Savings]`** (Expected Value: `78.5 gCO2/kWh`)
   - Title: `"LOAD SHIFTING OPPORTUNITY"`
   - Callout Value: `#00E676` Emerald Green, 24pt.
4. **KPI Card 4 (X: 1130, Width: 350)**:
   - Field: **`[Renewable Carbon Correlation R2]`** (Expected Value: `0.72`)
   - Title: `"CLEAN VS CARBON R²"`
   - Callout Value: `#00E5FF` Cyan, 24pt.
5. **Grid Zone Dropdown Slicer (X: 1500, Width: 400)**:
   - Visual Type: **Dropdown Slicer**.
   - Field: **`fact_carbon_intensity[zone_id]`** (e.g., `US-CAL-CISO`, `DE`, `FR`, `GB`, `IN-WE`, `BR`).

---

#### Middle Row: Hourly Heatmap Matrix & Correlation Scatter Plot (Y: 230, Height: 350)

#### Visual 2: Hourly Grid Carbon Intensity Heatmap Matrix (Middle Left, X: 20, Width: 920)
- **Visual Type**: **Matrix (`pivotTable`)**.
- **Rows**: **`fact_carbon_intensity[zone_id]`**.
- **Columns**: **`fact_carbon_intensity[hour]`** (0 to 23).
- **Values**: **`[Global Grid Carbon Intensity]`**.
- **Conditional Formatting**: Cell Background Color Gradient:
  - Minimum (`0 gCO2`): `#00E676` (Emerald Green)
  - Center (`250 gCO2`): `#FFB300` (Amber Gold)
  - Maximum (`500+ gCO2`): `#FF3D00` (Coral Red)
- **Grid Formatting**: Border `#2D3748`, Header font `#00E5FF`.

#### Visual 3: Carbon Intensity vs Renewable Share Scatter Plot (Middle Right, X: 960, Width: 940)
- **Visual Type**: **Scatter Plot (`scatterChart`)**.
- **X-Axis**: **`[Global Clean Energy Share %]`**.
- **Y-Axis**: **`[Global Grid Carbon Intensity]`**.
- **Size**: **`[Total Generation (TWh)]`**.
- **Values / Details**: **`dim_country[country_name]`** (or `fact_energy_generation[iso_code_3]`).
- **Play Axis**: **`dim_date[year]`** (Animates trajectory over time).
- **Trend Line**: Enabled (Linear regression trend).

---

#### Bottom Row: Hourly Load-Shifting Curve & Anomaly Zones (Y: 600, Height: 330)

#### Visual 4: 24-Hour Hourly Load-Shifting Curve (Bottom Left, X: 20, Width: 920)
- **Visual Type**: **Line Chart (`lineChart`)**.
- **X-Axis**: **`fact_carbon_intensity[hour]`** (0 to 23).
- **Y-Axis**: **`[Global Grid Carbon Intensity]`**.
- **Line Stroke**: `3px` solid `#00E5FF` Cyan line.
- **Reference Lines**: Constant line at `250 gCO2` (Clean Grid Threshold, Dotted `#00E676` Green).
- **Data Labels**: Enabled (`#FFFFFF` text).

#### Visual 5: Highest Carbon Intensity Grid Zones Bar Chart (Bottom Right, X: 960, Width: 940)
- **Visual Type**: **Clustered Bar Chart (`clusteredBarChart`)**.
- **Y-Axis**: **`fact_carbon_intensity[zone_id]`**.
- **X-Axis**: **`[Global Grid Carbon Intensity]`**.
- **Sort**: Descending by `[Global Grid Carbon Intensity]`.
- **Bar Color**: `#FF3D00` Coral Red.
- **Data Labels**: Enabled (`OutsideEnd`, `#FFFFFF` text).

---

#### Footer Row: Grid Telemetry Audit Table (Y: 950, Height: 200, Width: 1880)

#### Visual 6: Real-Time Grid Telemetry Log Table
- **Visual Type**: **Table (`tableEx`)**.
- **Columns**: `fact_carbon_intensity[zone_id]`, `fact_carbon_intensity[date_key]`, `fact_carbon_intensity[hour]`, `[Global Grid Carbon Intensity]`, `fact_carbon_intensity[is_anomaly]`.
- **Conditional Formatting**: Red `#FF3D00` background for rows where `is_anomaly = True` (high-emissions spikes > 600 gCO2).

---

## 📄 PREVIEW OF REMAINING PAGES (PAGES 4 – 7)

### Page 4: Energy Mix & Technology Deep-Dive
- Treemap Visual: Energy Fuel Hierarchy (`source_category` > `technology_name` vs `[Total Generation (TWh)]`).
- Fossil Dependency Gauge Visual (`[Fossil Dependency Share %]` vs `20%` 2030 Target).

### Page 5: Renewable Potential & Weather Context
- NASA Solar Irradiance Map (`[Average Solar Irradiance]` in kWh/m²/day).
- Open-Meteo 80m Wind Speed Line Chart (`[Average 80m Wind Speed]` in m/s).

### Page 6: Forecast & What-If Scenario Simulator
- What-If Slider Parameter (`Selected Scenario Increase %` +0% to +30%).
- Baseline vs Projected 2030 Renewable Share Forecast Line Chart.
- Abatement Impact Cards: `[Estimated Emissions Avoided (MtCO2)]`, `[Displaced Coal Generation (Mt)]`, `[Estimated Financial Savings ($M)]`.

### Page 7: Data Quality & Governance Operations
- Operational Health Scorecards (`[Pipeline Success Rate %]` = 99.4%, `[Data Quality Score %]` = 99.8%).
- Automated PySpark Data Quality Audit Log Table (`dq_results`).
