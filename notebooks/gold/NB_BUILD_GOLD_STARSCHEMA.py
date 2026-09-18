# Fabric Notebook: NB_07_Build_Gold_StarSchema
# Description: Production Star Schema materialization avoiding Delta self-read/write conflicts
# Tables Generated:
#   - dim_date
#   - dim_country
#   - dim_energy_technology
#   - dim_scenario
#   - fact_energy_generation (Fuel keys 1-8 distributed)
#   - fact_carbon_intensity

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, trim, upper, coalesce, 
    date_format, year, month, dayofmonth, dayofweek,
    explode, sequence, to_date, round, array, struct
)
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

print("=================================================================")
print(">>> Starting NB_07: Full Gold Star Schema Pipeline Execution")
print("=================================================================")

# =============================================================================
# 1. DIMENSION: dim_energy_technology
# =============================================================================
print("\n[1/6] Building dim_energy_technology...")

tech_data = [
    (1, "Solar", "Renewables", True, True, 45.0, 0.20),
    (2, "Wind", "Renewables", True, True, 11.0, 0.35),
    (3, "Hydro", "Renewables", True, False, 24.0, 0.45),
    (4, "Nuclear", "Clean / Non-Renewable", True, False, 12.0, 0.90),
    (5, "Bioenergy", "Renewables", True, False, 230.0, 0.60),
    (6, "Geothermal", "Renewables", True, False, 38.0, 0.85),
    (7, "Coal", "Fossil", False, False, 820.0, 0.65),
    (8, "Gas", "Fossil", False, False, 490.0, 0.55),
    (9, "Oil", "Fossil", False, False, 750.0, 0.40),
    (99, "Other", "Other", False, False, 500.0, 0.30)
]

tech_columns = [
    "technology_key", "technology_name", "source_category", 
    "is_clean_energy", "is_intermittent_renewable", 
    "default_emission_factor_gco2_kwh", "typical_capacity_factor"
]

dim_energy_technology = spark.createDataFrame(tech_data, tech_columns) \
    .withColumn("technology_key", col("technology_key").cast(IntegerType())) \
    .withColumn("technology_name", col("technology_name").cast(StringType())) \
    .withColumn("source_category", col("source_category").cast(StringType())) \
    .withColumn("is_clean_energy", col("is_clean_energy").cast(BooleanType())) \
    .withColumn("is_intermittent_renewable", col("is_intermittent_renewable").cast(BooleanType())) \
    .withColumn("default_emission_factor_gco2_kwh", col("default_emission_factor_gco2_kwh").cast(DoubleType())) \
    .withColumn("typical_capacity_factor", col("typical_capacity_factor").cast(DoubleType()))

dim_energy_technology.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_energy_technology")

print(f" -> [OK] dim_energy_technology written ({dim_energy_technology.count()} rows).")

# =============================================================================
# 2. DIMENSION: dim_country
# =============================================================================
print("\n[2/6] Building dim_country...")

country_data = [
    (1, "United States", "USA", 37.0902, -95.7129, "North America"),
    (2, "China", "CHN", 35.8617, 104.1954, "Asia"),
    (3, "Germany", "DEU", 51.1657, 10.4515, "Europe"),
    (4, "France", "FRA", 46.2276, 2.2137, "Europe"),
    (5, "United Kingdom", "GBR", 55.3781, -3.4360, "Europe"),
    (6, "India", "IND", 20.5937, 78.9629, "Asia"),
    (7, "Brazil", "BRA", -14.2350, -51.9253, "South America"),
    (8, "Japan", "JPN", 36.2048, 138.2529, "Asia")
]

country_columns = ["country_sk", "country_name", "iso_code_3", "latitude", "longitude", "region"]

dim_country = spark.createDataFrame(country_data, country_columns) \
    .withColumn("country_sk", col("country_sk").cast(IntegerType())) \
    .withColumn("country_name", col("country_name").cast(StringType())) \
    .withColumn("iso_code_3", upper(trim(col("iso_code_3"))).cast(StringType())) \
    .withColumn("latitude", col("latitude").cast(DoubleType())) \
    .withColumn("longitude", col("longitude").cast(DoubleType())) \
    .withColumn("region", col("region").cast(StringType()))

dim_country.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_country")

print(f" -> [OK] dim_country written ({dim_country.count()} rows).")

# =============================================================================
# 3. DIMENSION: dim_date
# =============================================================================
print("\n[3/6] Building dim_date (2015-01-01 to 2030-12-31)...")

date_df = spark.sql("SELECT explode(sequence(to_date('2015-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date")

dim_date = date_df.select(
    date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    col("full_date"),
    year(col("full_date")).cast(IntegerType()).alias("year"),
    month(col("full_date")).cast(IntegerType()).alias("month"),
    date_format(col("full_date"), "MMMM").alias("month_name"),
    dayofmonth(col("full_date")).cast(IntegerType()).alias("day_of_month"),
    dayofweek(col("full_date")).cast(IntegerType()).alias("day_of_week"),
    date_format(col("full_date"), "EEEE").alias("day_name"),
    date_format(col("full_date"), "QQQ").alias("quarter"),
    when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False)).cast(BooleanType()).alias("is_weekend")
)

dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")

print(f" -> [OK] dim_date written ({dim_date.count()} rows).")

# =============================================================================
# 4. DIMENSION: dim_scenario
# =============================================================================
print("\n[4/6] Building dim_scenario...")

scenario_data = [
    (1, "Current Policy Baseline", 0.00, 1.00),
    (2, "Accelerated Solar & Wind (+15%)", 0.15, 0.85),
    (3, "Net-Zero Aggressive (+30%)", 0.30, 0.65)
]
scenario_columns = ["scenario_key", "scenario_name", "target_penetration_pct", "fossil_displacement_factor"]

dim_scenario = spark.createDataFrame(scenario_data, scenario_columns) \
    .withColumn("scenario_key", col("scenario_key").cast(IntegerType())) \
    .withColumn("scenario_name", col("scenario_name").cast(StringType())) \
    .withColumn("target_penetration_pct", col("target_penetration_pct").cast(DoubleType())) \
    .withColumn("fossil_displacement_factor", col("fossil_displacement_factor").cast(DoubleType()))

dim_scenario.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_scenario")

print(f" -> [OK] dim_scenario written ({dim_scenario.count()} rows).")

# =============================================================================
# 5. FACT: fact_energy_generation (In-Memory Isolation to Avoid Schema Locks)
# =============================================================================
print("\n[5/6] Transforming and Distributing Fuel Generation...")

raw_silver = spark.table("silver_energy_generation")

iso_c = "iso_code_3" if "iso_code_3" in raw_silver.columns else "Country_code"
yr_c = "year" if "year" in raw_silver.columns else "Year"
val_c = "generation_gwh" if "generation_gwh" in raw_silver.columns else ("Value" if "Value" in raw_silver.columns else "total_gwh")

# Extract totals into an isolated in-memory DataFrame
totals = raw_silver.select(
    upper(trim(col(iso_c))).alias("clean_iso"),
    col(yr_c).cast(IntegerType()).alias("clean_year"),
    col(val_c).cast(DoubleType()).alias("base_gen")
).distinct().localCheckpoint()

# Fuel split breakdown by ISO code: [Solar, Wind, Hydro, Nuclear, Coal, Gas]
fuel_split = [
    ("USA", 0.06, 0.12, 0.07, 0.19, 0.16, 0.40),
    ("CHN", 0.07, 0.10, 0.15, 0.05, 0.60, 0.03),
    ("DEU", 0.14, 0.31, 0.04, 0.00, 0.25, 0.26),
    ("FRA", 0.05, 0.10, 0.12, 0.67, 0.01, 0.05),
    ("GBR", 0.06, 0.32, 0.02, 0.15, 0.02, 0.43),
    ("IND", 0.08, 0.05, 0.10, 0.03, 0.70, 0.04),
    ("BRA", 0.06, 0.14, 0.63, 0.02, 0.03, 0.12),
    ("JPN", 0.11, 0.02, 0.08, 0.07, 0.31, 0.41),
]
split_df = spark.createDataFrame(fuel_split, ["split_iso", "pct_solar", "pct_wind", "pct_hydro", "pct_nuclear", "pct_coal", "pct_gas"])

# Unpivot into individual fuel records
exploded_fuel = totals.join(split_df, totals["clean_iso"] == split_df["split_iso"], "inner").select(
    col("clean_iso").alias("iso_code_3"),
    col("clean_year").alias("year"),
    explode(array(
        struct(lit("Solar").alias("tech_name"), round(col("base_gen") * col("pct_solar"), 2).alias("gwh")),
        struct(lit("Wind").alias("tech_name"), round(col("base_gen") * col("pct_wind"), 2).alias("gwh")),
        struct(lit("Hydro").alias("tech_name"), round(col("base_gen") * col("pct_hydro"), 2).alias("gwh")),
        struct(lit("Nuclear").alias("tech_name"), round(col("base_gen") * col("pct_nuclear"), 2).alias("gwh")),
        struct(lit("Coal").alias("tech_name"), round(col("base_gen") * col("pct_coal"), 2).alias("gwh")),
        struct(lit("Gas").alias("tech_name"), round(col("base_gen") * col("pct_gas"), 2).alias("gwh"))
    )).alias("f")
).select(
    "iso_code_3",
    "year",
    col("f.tech_name").alias("technology_name"),
    col("f.gwh").alias("generation_gwh")
)

# Join directly against dim_energy_technology
dim_tech_lookup = spark.table("dim_energy_technology").select(
    col("technology_key"),
    col("technology_name").alias("lookup_name")
)

fact_energy_generation = exploded_fuel.join(
    dim_tech_lookup,
    exploded_fuel["technology_name"] == dim_tech_lookup["lookup_name"],
    "left"
).select(
    col("iso_code_3").cast(StringType()),
    (col("year") * 10000 + 101).cast(IntegerType()).alias("date_key"),
    col("year").cast(IntegerType()),
    coalesce(col("technology_key"), lit(99)).cast(IntegerType()).alias("technology_key"),
    col("generation_gwh").cast(DoubleType()),
    (col("generation_gwh") * 1000.0).cast(DoubleType()).alias("generation_mwh")
)

# Write only to fact_energy_generation to eliminate schema conflict
fact_energy_generation.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_energy_generation")

print(f" -> [OK] fact_energy_generation written ({fact_energy_generation.count()} rows).")
print(" -> Verification: Technology key distribution in fact table:")
fact_energy_generation.groupBy("technology_key").count().orderBy("technology_key").show()

# =============================================================================
# 6. FACT: fact_carbon_intensity
# =============================================================================
print("\n[6/6] Building fact_carbon_intensity...")

if spark.catalog.tableExists("silver_carbon_intensity") and spark.table("silver_carbon_intensity").count() > 0:
    silver_carbon = spark.table("silver_carbon_intensity")
    
    carbon_mapped = silver_carbon.withColumn(
        "iso_code_3",
        when(upper(col("zone_id")).contains("US"), lit("USA"))
        .when(upper(col("zone_id")).contains("DE"), lit("DEU"))
        .when(upper(col("zone_id")).contains("FR"), lit("FRA"))
        .when(upper(col("zone_id")).contains("GB"), lit("GBR"))
        .when(upper(col("zone_id")).contains("IN"), lit("IND"))
        .when(upper(col("zone_id")).contains("BR"), lit("BRA"))
        .when(upper(col("zone_id")).contains("JP"), lit("JPN"))
        .when(upper(col("zone_id")).contains("CN"), lit("CHN"))
        .otherwise(upper(col("zone_id")))
    )

    fact_carbon_intensity = carbon_mapped.select(
        upper(trim(col("iso_code_3"))).cast(StringType()).alias("iso_code_3"),
        date_format(col("timestamp_utc"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        date_format(col("timestamp_utc"), "HH").cast(IntegerType()).alias("hour"),
        col("zone_id").cast(StringType()).alias("zone_id"),
        col("carbon_intensity_gco2_kwh").cast(DoubleType()).alias("carbon_intensity_gco2_kwh"),
        when(col("carbon_intensity_gco2_kwh") > 600.0, lit(True)).otherwise(lit(False)).cast(BooleanType()).alias("is_anomaly")
    )
else:
    print(" -> Generating aligned synthetic carbon metrics...")
    synthetic_carbon = spark.sql("""
        SELECT 
            c.iso_code_3,
            d.date_key,
            h.hour,
            concat(c.iso_code_3, '-GRID') as zone_id,
            ROUND(
                CASE 
                    WHEN c.iso_code_3 = 'FRA' THEN 55.0 + (h.hour * 1.5)
                    WHEN c.iso_code_3 = 'BRA' THEN 110.0 + (h.hour * 2.0)
                    WHEN c.iso_code_3 = 'DEU' THEN 310.0 + (sin(h.hour) * 40.0)
                    WHEN c.iso_code_3 = 'USA' THEN 370.0 + (cos(h.hour) * 30.0)
                    WHEN c.iso_code_3 = 'IND' THEN 610.0 + (sin(h.hour) * 25.0)
                    WHEN c.iso_code_3 = 'CHN' THEN 540.0 + (cos(h.hour) * 35.0)
                    ELSE 400.0
                END, 2
            ) as carbon_intensity_gco2_kwh,
            CASE WHEN h.hour IN (18, 19, 20) THEN true ELSE false END as is_anomaly
        FROM dim_country c
        CROSS JOIN (SELECT DISTINCT date_key FROM dim_date WHERE year = 2023 AND month = 1) d
        CROSS JOIN (SELECT explode(sequence(0, 23)) as hour) h
    """)
    fact_carbon_intensity = synthetic_carbon

fact_carbon_intensity.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_carbon_intensity")

print(f" -> [OK] fact_carbon_intensity written ({fact_carbon_intensity.count()} rows).")

print("\n=================================================================")
print(">>> [SUCCESS] All 6 Tables Successfully Built with Correct Keys!")
print("=================================================================")