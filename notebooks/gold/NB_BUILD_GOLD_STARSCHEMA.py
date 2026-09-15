# Fabric Notebook: NB_BUILD_GOLD_STARSCHEMA
# Description: Fail-safe PySpark Gold Star Schema Builder (Dimensions & Facts)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, year, month, dayofmonth, dayofweek, quarter, lit, coalesce, hour

spark = SparkSession.builder.getOrCreate()

print("Starting Gold Star Schema Build...")

# -----------------------------------------------------------------------------
# 1. Build dim_date
# -----------------------------------------------------------------------------
try:
    date_df = spark.sql("SELECT explode(sequence(to_date('2000-01-01'), to_date('2030-12-31'), interval 1 day)) AS full_date")
    dim_date = date_df.select(
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        dayofmonth(col("full_date")).alias("day_of_month"),
        dayofweek(col("full_date")).alias("day_of_week"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        when(dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
    )
    dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
    print("[SUCCESS] Built dim_date table.")
except Exception as e:
    print(f"[WARN] dim_date build: {str(e)}")

# -----------------------------------------------------------------------------
# 2. Build dim_energy_technology
# -----------------------------------------------------------------------------
try:
    tech_data = [
        (1, "Solar", "Renewable", True, True, 0.22, 0.0),
        (2, "Wind", "Renewable", True, True, 0.35, 0.0),
        (3, "Hydro", "Hydro", True, False, 0.45, 0.0),
        (4, "Nuclear", "Nuclear", True, False, 0.90, 0.0),
        (5, "Bioenergy", "Renewable", True, False, 0.60, 230.0),
        (6, "Geothermal", "Renewable", True, False, 0.80, 40.0),
        (7, "Coal", "Fossil", False, False, 0.65, 820.0),
        (8, "Gas", "Fossil", False, False, 0.55, 490.0),
        (9, "Oil", "Fossil", False, False, 0.40, 730.0)
    ]
    tech_schema = ["technology_key", "technology_name", "source_category", "is_clean_energy", "is_intermittent_renewable", "typical_capacity_factor", "default_emission_factor_gco2_kwh"]
    dim_tech = spark.createDataFrame(tech_data, tech_schema)
    dim_tech.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_energy_technology")
    print("[SUCCESS] Built dim_energy_technology table.")
except Exception as e:
    print(f"[WARN] dim_energy_technology build: {str(e)}")

# -----------------------------------------------------------------------------
# 3. Build dim_scenario
# -----------------------------------------------------------------------------
try:
    scenario_data = [
        (1, "Baseline", 0.0, 1.0),
        (2, "+5% Renewable Penetration", 5.0, 0.95),
        (3, "+10% Renewable Penetration", 10.0, 0.90),
        (4, "+20% Renewable Penetration", 20.0, 0.80),
        (5, "+30% Renewable Penetration", 30.0, 0.70)
    ]
    dim_scenario = spark.createDataFrame(scenario_data, ["scenario_key", "scenario_name", "target_penetration_pct", "fossil_displacement_factor"])
    dim_scenario.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_scenario")
    print("[SUCCESS] Built dim_scenario table.")
except Exception as e:
    print(f"[WARN] dim_scenario build: {str(e)}")

# -----------------------------------------------------------------------------
# 4. Build fact_energy_generation
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("silver_energy_generation"):
    try:
        silver_gen = spark.table("silver_energy_generation")
        dim_t = spark.table("dim_energy_technology")
        fact_gen = silver_gen.join(dim_t, silver_gen["technology_name"] == dim_t["technology_name"], "left") \
            .select(
                col("iso_code_3"),
                col("year"),
                coalesce(col("technology_key"), lit(99)).alias("technology_key"),
                col("generation_gwh"),
                (col("generation_gwh") * 1000).alias("generation_mwh")
            )
        fact_gen.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_energy_generation")
        print("[SUCCESS] Built fact_energy_generation table.")
    except Exception as e:
        print(f"[WARN] fact_energy_generation build: {str(e)}")
else:
    print("[SKIP] silver_energy_generation does not exist yet.")

# -----------------------------------------------------------------------------
# 5. Build fact_carbon_intensity
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("silver_carbon_intensity"):
    try:
        silver_carbon = spark.table("silver_carbon_intensity")
        fact_carbon = silver_carbon.select(
            col("zone_id"),
            date_format(col("timestamp_utc"), "yyyyMMdd").cast("int").alias("date_key"),
            hour(col("timestamp_utc")).alias("hour"),
            col("carbon_intensity_gco2_kwh"),
            when(col("carbon_intensity_gco2_kwh") > 600, True).otherwise(False).alias("is_anomaly")
        )
        fact_carbon.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_carbon_intensity")
        print("[SUCCESS] Built fact_carbon_intensity table.")
    except Exception as e:
        print(f"[WARN] fact_carbon_intensity build: {str(e)}")
else:
    print("[SKIP] silver_carbon_intensity does not exist yet.")

print("\nGold Star Schema Build Complete!")
