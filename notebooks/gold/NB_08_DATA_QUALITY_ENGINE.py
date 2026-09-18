import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, lit, count, when, sum as _sum, row_number

spark = SparkSession.builder.getOrCreate()
run_id = str(uuid.uuid4())
dq_checks = []

print(f"=== Starting Enterprise DQ Gate | Run ID: {run_id} ===")

# =============================================================================
# 1. VALIDATION & DLQ: silver_carbon_intensity
# Single-pass scan for Nulls & Range Checks + Single Window for Duplicates
# =============================================================================
if spark.catalog.tableExists("silver_carbon_intensity"):
    df_carbon = spark.table("silver_carbon_intensity")
    total_carbon = df_carbon.count()

    if total_carbon > 0:
        # A. Deduplication & DLQ Isolation using Windowing (1 Shuffle)
        w_carbon = Window.partitionBy("zone_id", "timestamp_utc").orderBy(col("timestamp_utc").desc())
        df_carbon_flagged = df_carbon.withColumn("_row_num", row_number().over(w_carbon))

        df_carbon_valid = df_carbon_flagged.filter(col("_row_num") == 1).drop("_row_num")
        df_carbon_duplicates = df_carbon_flagged.filter(col("_row_num") > 1).drop("_row_num")
        
        dup_count = df_carbon_duplicates.count()
        if dup_count > 0:
            (df_carbon_duplicates
                .withColumn("_dq_failure_reason", lit("DUPLICATE_ZONE_TIMESTAMP_KEY"))
                .withColumn("_quarantine_timestamp", current_timestamp())
                .write.format("delta").mode("append").saveAsTable("bronze_dlq_quarantine"))

        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()), "run_id": run_id, "source_name": "Electricity Maps",
            "table_name": "silver_carbon_intensity", "check_name": "DUPLICATE_KEY_CHECK",
            "criticality": "BLOCKER", "records_checked": total_carbon, "records_failed": dup_count,
            "failure_percentage": float((dup_count / total_carbon) * 100),
            "status": "PASSED" if dup_count == 0 else "FAILED"
        })

        # B. Single-pass Aggregation for Nulls & Out-of-Range Checks (0 Shuffles)
        carbon_metrics = df_carbon.select(
            _sum(when(col("zone_id").isNull() | col("timestamp_utc").isNull(), 1).otherwise(0)).alias("null_keys"),
            _sum(when((col("carbon_intensity_gco2_kwh") < 0) | (col("carbon_intensity_gco2_kwh") > 1500), 1).otherwise(0)).alias("range_fails")
        ).first()

        null_count = carbon_metrics["null_keys"] or 0
        range_count = carbon_metrics["range_fails"] or 0

        dq_checks.extend([
            {
                "dq_check_id": str(uuid.uuid4()), "run_id": run_id, "source_name": "Electricity Maps",
                "table_name": "silver_carbon_intensity", "check_name": "NULL_KEY_CHECK",
                "criticality": "BLOCKER", "records_checked": total_carbon, "records_failed": null_count,
                "failure_percentage": float((null_count / total_carbon) * 100),
                "status": "PASSED" if null_count == 0 else "FAILED"
            },
            {
                "dq_check_id": str(uuid.uuid4()), "run_id": run_id, "source_name": "Electricity Maps",
                "table_name": "silver_carbon_intensity", "check_name": "CARBON_INTENSITY_RANGE_CHECK",
                "criticality": "WARNING", "records_checked": total_carbon, "records_failed": range_count,
                "failure_percentage": float((range_count / total_carbon) * 100),
                "status": "PASSED" if range_count == 0 else "WARNING"
            }
        ])

# =============================================================================
# 2. VALIDATION & DLQ: silver_energy_generation
# =============================================================================
if spark.catalog.tableExists("silver_energy_generation"):
    df_gen = spark.table("silver_energy_generation")
    total_gen = df_gen.count()

    if total_gen > 0:
        # Single-pass Aggregation for Negative Values
        gen_metrics = df_gen.select(
            _sum(when(col("generation_gwh") < 0, 1).otherwise(0)).alias("neg_gen")
        ).first()

        neg_count = gen_metrics["neg_gen"] or 0

        dq_checks.append({
            "dq_check_id": str(uuid.uuid4()), "run_id": run_id, "source_name": "Ember/OWID",
            "table_name": "silver_energy_generation", "check_name": "NEGATIVE_GENERATION_CHECK",
            "criticality": "BLOCKER", "records_checked": total_gen, "records_failed": neg_count,
            "failure_percentage": float((neg_count / total_gen) * 100),
            "status": "PASSED" if neg_count == 0 else "FAILED"
        })

# =============================================================================
# 3. SCORECARD, AUDIT TRAIL, AND PIPELINE CIRCUIT BREAKER
# =============================================================================
if dq_checks:
    df_dq_results = spark.createDataFrame(dq_checks) \
        .withColumn("execution_timestamp", current_timestamp())

    # Write audit log to Delta
    df_dq_results.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("dq_audit_log")

    passed_count = sum(1 for c in dq_checks if c["status"] == "PASSED")
    total_checks = len(dq_checks)
    dq_score = round((passed_count / total_checks) * 100, 2)
    
    # Check for Blocker Failures
    blocker_failures = [c for c in dq_checks if c["criticality"] == "BLOCKER" and c["status"] == "FAILED"]

    print(f"\n=======================================================")
    print(f"DATA QUALITY AUDIT COMPLETED. Overall DQ Score: {dq_score}%")
    print(f"Blocker Failures: {len(blocker_failures)}")
    print(f"=======================================================\n")

    # If critical contract checks fail, break the circuit and fail the pipeline activity
    if blocker_failures:
        failure_msg = f"CRITICAL DQ CIRCUIT BREAKER TRIGGERED: {len(blocker_failures)} blocker check(s) failed."
        # Pass exit payload to Fabric Data Factory pipeline
        import json
        error_payload = json.dumps({"run_id": run_id, "status": "FAILED", "score": dq_score, "blockers": blocker_failures})
        
        # This guarantees Fabric Pipeline stops and marks NB_08 as Failed
        raise RuntimeError(f"{failure_msg} Details: {error_payload}")

    # Return clean run telemetry to the caller
    import json
    notebook_summary = json.dumps({"run_id": run_id, "status": "PASSED", "score": dq_score})
    # try:
    #     # import notebookutils
    #     # notebookutils.notebook.exit(notebook_summary)
    # except Exception:
    #     pass