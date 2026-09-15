# Fabric Notebook: NB_INGEST_ELECTRICITY_MAPS
# Description: Fail-safe production PySpark ingestion for Electricity Maps API

import requests
import json
import uuid
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# Parameters
dbutils_secret_scope = "akv-renewable-energy"
secret_key_name = "ELECTRICITY-MAPS-API-KEY"
target_table = "bronze_electricity_maps"
pipeline_run_id = str(uuid.uuid4())

# Verified Fallback API Key for testing/demo
DEFAULT_API_KEY = "em_k3zusf96XNVp759Huvmk6qxFtwNgK9hY"

def get_credential():
    try:
        from notebookutils import mssparkutils
        key = mssparkutils.credentials.getSecret(dbutils_secret_scope, secret_key_name)
        if key and len(key) > 5:
            return key
    except Exception:
        pass
    import os
    env_key = os.environ.get("ELECTRICITY_MAPS_API_KEY", "")
    if env_key and len(env_key) > 5:
        return env_key
    return DEFAULT_API_KEY

api_key = get_credential()

print(f"Starting Electricity Maps Ingestion. Run ID: {pipeline_run_id}")
print(f"Using API Key: ...{api_key[-6:] if len(api_key)>=6 else 'NONE'}")

headers = {
    "auth-token": api_key,
    "User-Agent": "FabricEnergyCommandCenter/1.0"
}

zones = ["DE", "FR", "GB", "US-CAL-CISO", "IN-WE", "BR", "JP-TK"]
records = []

for zone in zones:
    print(f"Ingesting zone: {zone}...")
    
    # 1. Power Breakdown
    breakdown_url = "https://api.electricitymap.org/v3/power-breakdown/latest"
    try:
        res = requests.get(breakdown_url, headers=headers, params={"zone": zone}, timeout=10)
        if res.status_code == 200:
            payload = res.json()
            raw_str = json.dumps(payload)
            rec_hash = hashlib.md5(raw_str.encode('utf-8')).hexdigest()
            records.append({
                "zone": zone,
                "data_type": "POWER_BREAKDOWN",
                "datetime": payload.get("datetime"),
                "carbon_intensity": None,
                "fossil_free_percentage": payload.get("fossilFreePercentage"),
                "renewable_percentage": payload.get("renewablePercentage"),
                "power_production_breakdown": json.dumps(payload.get("powerProductionBreakdown", {})),
                "power_consumption_breakdown": json.dumps(payload.get("powerConsumptionBreakdown", {})),
                "raw_payload": raw_str,
                "source_system": "Electricity Maps API",
                "source_file": f"electricity_maps_{zone}_breakdown.json",
                "api_endpoint": breakdown_url,
                "pipeline_run_id": pipeline_run_id,
                "record_hash": rec_hash
            })
            print(f"  -> [OK] Power breakdown fetched for {zone}")
        else:
            print(f"  -> [WARN] Power breakdown status {res.status_code}: {res.text[:100]}")
    except Exception as e:
        print(f"  -> [ERROR] Breakdown request failed for {zone}: {str(e)}")

    # 2. Carbon Intensity History (24 hours)
    history_url = "https://api.electricitymap.org/v3/carbon-intensity/history"
    try:
        res = requests.get(history_url, headers=headers, params={"zone": zone}, timeout=10)
        if res.status_code == 200:
            payload = res.json()
            history = payload.get("history", [])
            for item in history:
                raw_str = json.dumps(item)
                rec_hash = hashlib.md5(raw_str.encode('utf-8')).hexdigest()
                records.append({
                    "zone": zone,
                    "data_type": "CARBON_INTENSITY_HISTORY",
                    "datetime": item.get("datetime"),
                    "carbon_intensity": item.get("carbonIntensity"),
                    "fossil_free_percentage": None,
                    "renewable_percentage": None,
                    "power_production_breakdown": None,
                    "power_consumption_breakdown": None,
                    "raw_payload": raw_str,
                    "source_system": "Electricity Maps API",
                    "source_file": f"electricity_maps_{zone}_carbon_history.json",
                    "api_endpoint": history_url,
                    "pipeline_run_id": pipeline_run_id,
                    "record_hash": rec_hash
                })
            print(f"  -> [OK] {len(history)} carbon intensity history records fetched for {zone}")
        else:
            print(f"  -> [WARN] Carbon history status {res.status_code}: {res.text[:100]}")
    except Exception as e:
        print(f"  -> [ERROR] Carbon history request failed for {zone}: {str(e)}")

if records:
    df = spark.createDataFrame(records)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df.write.format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable(target_table)
      
    print(f"\n[SUCCESS] Electricity Maps Ingestion Complete! {len(records)} records saved to {target_table}.")
else:
    print("\n[WARN] No records were retrieved. Please check API Key / Network permissions.")
