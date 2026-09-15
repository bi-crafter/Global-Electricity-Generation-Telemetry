# Fabric Notebook: NB_08_DATA_QUALITY_ENGINE
# Description: Production PySpark Data Quality Check Engine (Duplicate Checks, Null Checks, Range Checks, & Data Quality Scorecard)

import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, count, when, sum as _sum

spark = SparkSession.builder.getOrCreate()
run_id = str(uuid.uuid4())

print(f"Starting Production Data Quality Validation Engine. Run ID: {run_id}")

dq_checks = []

# -----------------------------------------------------------------------------
# 1. Validation Checks: silver_carbon_intensity
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("silver_carbon_intensity"):
    df_carbon = spark.table("silver_carbon_intensity")
    total_carbon_records = df_carbon.count()
    
    if total_carbon_records > 0:
        # A. DUPLICATE CHECK: Verify no duplicate (zone_id, timestamp_utc) pairs exist
        distinct_carbon_keys = df_carbon.select("zone_id", "timestamp_utc").distinct().count()
        duplicate_carbon_count = total_carbon_records - distinct_carbon_keys
        
        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "source_name": "Electricity Maps",
            "table_name": "silver_carbon_intensity",
            "check_name": "DUPLICATE_KEY_CHECK",
            "records_checked": total_carbon_records,
            "records_failed": duplicate_carbon_count,
            "failure_percentage": float(duplicate_carbon_count / total_carbon_records * 100),
            "status": "PASSED" if duplicate_carbon_count == 0 else "FAILED"
        })
        
        # B. NULL CHECK: Verify zone_id and timestamp_utc are non-null
        null_zones = df_carbon.filter(col("zone_id").isNull() | col("timestamp_utc").isNull()).count()
        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "source_name": "Electricity Maps",
            "table_name": "silver_carbon_intensity",
            "check_name": "NULL_KEY_CHECK",
            "records_checked": total_carbon_records,
            "records_failed": null_zones,
            "failure_percentage": float(null_zones / total_carbon_records * 100),
            "status": "PASSED" if null_zones == 0 else "FAILED"
        })
        
        # C. RANGE CHECK: Carbon Intensity between 0 and 1500 gCO2eq/kWh
        range_failures = df_carbon.filter((col("carbon_intensity_gco2_kwh") < 0) | (col("carbon_intensity_gco2_kwh") > 1500)).count()
        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "source_name": "Electricity Maps",
            "table_name": "silver_carbon_intensity",
            "check_name": "CARBON_INTENSITY_RANGE_CHECK",
            "records_checked": total_carbon_records,
            "records_failed": range_failures,
            "failure_percentage": float(range_failures / total_carbon_records * 100),
            "status": "PASSED" if range_failures == 0 else "WARNING"
        })

# -----------------------------------------------------------------------------
# 2. Validation Checks: silver_energy_generation
# -----------------------------------------------------------------------------
if spark.catalog.tableExists("silver_energy_generation"):
    df_gen = spark.table("silver_energy_generation")
    total_gen_records = df_gen.count()
    
    if total_gen_records > 0:
        # A. DUPLICATE CHECK: Verify no duplicate (iso_code_3, year, technology_name) tuples exist
        distinct_gen_keys = df_gen.select("iso_code_3", "year", "technology_name").distinct().count()
        duplicate_gen_count = total_gen_records - distinct_gen_keys
        
        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "source_name": "Ember/OWID",
            "table_name": "silver_energy_generation",
            "check_name": "DUPLICATE_KEY_CHECK",
            "records_checked": total_gen_records,
            "records_failed": duplicate_gen_count,
            "failure_percentage": float(duplicate_gen_count / total_gen_records * 100),
            "status": "PASSED" if duplicate_gen_count == 0 else "FAILED"
        })
        
        # B. NEGATIVE VALUE CHECK: Verify generation_gwh >= 0
        negative_gen = df_gen.filter(col("generation_gwh") < 0).count()
        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "source_name": "Ember/OWID",
            "table_name": "silver_energy_generation",
            "check_name": "NEGATIVE_GENERATION_CHECK",
            "records_checked": total_gen_records,
            "records_failed": negative_gen,
            "failure_percentage": float(negative_gen / total_gen_records * 100),
            "status": "PASSED" if negative_gen == 0 else "FAILED"
        })

# -----------------------------------------------------------------------------
# 3. Calculate Overall Data Quality Score & Save Results
# -----------------------------------------------------------------------------
if dq_checks:
    df_dq_results = spark.createDataFrame(dq_checks) \
        .withColumn("execution_timestamp", current_timestamp())
    
    # Save to dq_results Delta Table
    df_dq_results.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("dq_results")
        
    passed_count = sum(1 for c in dq_checks if c["status"] == "PASSED")
    total_checks = len(dq_checks)
    dq_score = round((passed_count / total_checks) * 100, 2)
    
    print("\n=======================================================")
    print(f"DATA QUALITY ENGINE SUMMARY:")
    print(f"  Total Checks Executed: {total_checks}")
    print(f"  Passed Checks:         {passed_count}")
    print(f"  Overall DQ Score:      {dq_score}%")
    print("=======================================================\n")
    
    # Display check breakdown
    for c in dq_checks:
        print(f"  [{c['status']}] {c['table_name']} -> {c['check_name']}: Failed {c['records_failed']}/{c['records_checked']} ({c['failure_percentage']:.2f}%)")
else:
    print("[WARN] No Silver tables available for Data Quality validation.")
