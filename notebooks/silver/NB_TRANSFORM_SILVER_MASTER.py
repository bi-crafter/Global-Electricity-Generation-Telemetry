# Fabric Notebook: NB_TRANSFORM_SILVER_MASTER
# Description: Advanced Enterprise Silver Transformation Engine (Sanitization, ISO Standardization, Deduplication & Outlier Handling)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, to_timestamp, to_utc_timestamp, year, month, date_format, lit, upper, trim, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("Starting Advanced Enterprise Silver Transformation Engine...")

# -----------------------------------------------------------------------------
# 1. Transform Electricity Maps -> silver_carbon_intensity
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("bronze_electricity_maps"):
    raw_em = spark.table("bronze_electricity_maps")
    
    carbon_df = raw_em.filter(col("data_type") == "CARBON_INTENSITY_HISTORY") \
        .select(
            upper(trim(col("zone"))).alias("zone_id"),
            to_utc_timestamp(to_timestamp(col("datetime")), "UTC").alias("timestamp_utc"),
            # Outlier Handling: Cap carbon intensity between 0 and 1500 gCO2eq/kWh
            when(col("carbon_intensity") < 0, 0.0)
            .when(col("carbon_intensity") > 1500, 1500.0)
            .otherwise(col("carbon_intensity").cast("double"))
            .alias("carbon_intensity_gco2_kwh"),
            col("pipeline_run_id"),
            col("ingestion_timestamp")
        )
    
    # Deduplication Window: Keep latest record for each zone + timestamp
    w_carbon = Window.partitionBy("zone_id", "timestamp_utc").orderBy(col("ingestion_timestamp").desc())
    silver_carbon = carbon_df.withColumn("rn", row_number().over(w_carbon)).filter(col("rn") == 1).drop("rn")
    
    silver_carbon.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_carbon_intensity")
    print(f"[SUCCESS] Created silver_carbon_intensity Delta table ({silver_carbon.count()} rows).")

# -----------------------------------------------------------------------------
# 2. Transform Ember/OWID -> silver_energy_generation
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("bronze_ember"):
    raw_ember = spark.table("bronze_ember")
    
    iso_col = "Country_code" if "Country_code" in raw_ember.columns else ("Country code" if "Country code" in raw_ember.columns else "Country_Code")
    area_col = "Area" if "Area" in raw_ember.columns else "country"
    year_col = "Year" if "Year" in raw_ember.columns else "year"
    tech_col = "Variable" if "Variable" in raw_ember.columns else "Variable"
    val_col = "Value" if "Value" in raw_ember.columns else "electricity_generation"
    
    silver_ember_gen = raw_ember \
        .filter(col(val_col).isNotNull()) \
        .select(
            upper(trim(col(area_col))).alias("country_name"),
            coalesce(upper(trim(col(iso_col))), lit("UNKNOWN")).alias("iso_code_3"),
            col(year_col).cast("int").alias("year"),
            col(tech_col).alias("technology_name"),
            # Outlier & Negative Generation Handling
            when(col(val_col) < 0, 0.0).otherwise(col(val_col).cast("double")).alias("generation_gwh"),
            col("ingestion_timestamp")
        )
    
    # Deduplication Window: Keep latest record for each country + year + tech
    w_ember = Window.partitionBy("iso_code_3", "year", "technology_name").orderBy(col("ingestion_timestamp").desc())
    silver_ember_clean = silver_ember_gen.withColumn("rn", row_number().over(w_ember)).filter(col("rn") == 1).drop("rn")
    
    silver_ember_clean.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_energy_generation")
    print(f"[SUCCESS] Created silver_energy_generation Delta table ({silver_ember_clean.count()} rows).")

print("\nAdvanced Enterprise Silver Transformation Complete!")
