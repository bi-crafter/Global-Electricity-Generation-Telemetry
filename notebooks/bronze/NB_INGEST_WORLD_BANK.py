# Fabric Notebook: NB_INGEST_WORLD_BANK
# Description: Production PySpark ingestion for World Bank Open Data (Population, GDP, Electric Consumption per capita)

import requests
import json
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

indicators = {
    "SP.POP.TOTL": "Population",
    "NY.GDP.MKTP.CD": "GDP_USD",
    "EG.USE.ELEC.KH.PC": "Electric_Consumption_Per_Capita_kWh"
}

base_url = "https://api.worldbank.org/v2/country/all/indicator/"
target_table = "bronze_world_bank"
pipeline_run_id = str(uuid.uuid4())

all_records = []

print(f"Starting Ingestion for World Bank Indicators. Run ID: {pipeline_run_id}")

for code, name in indicators.items():
    url = f"{base_url}{code}?format=json&per_page=5000&date=2015:2024"
    print(f"Fetching World Bank Indicator: {code} ({name})...")
    try:
        res = requests.get(url, timeout=20)
        if res.status_code == 200:
            data = res.json()
            if len(data) > 1 and data[1]:
                items = data[1]
                count = 0
                for item in items:
                    if item.get("value") is not None:
                        all_records.append({
                            "indicator_code": code,
                            "indicator_name": name,
                            "country_iso3": item.get("countryiso3code"),
                            "country_name": item.get("country", {}).get("value"),
                            "year": item.get("date"),
                            "value": float(item.get("value")),
                            "source_system": "World Bank Open Data API",
                            "source_file": f"worldbank_{code}.json",
                            "api_endpoint": url,
                            "pipeline_run_id": pipeline_run_id
                        })
                        count += 1
                print(f"  -> [OK] Fetched {count} valid records for {name}.")
        else:
            print(f"  -> [WARN] HTTP status {res.status_code} for {code}")
    except Exception as e:
        print(f"  -> [ERROR] Failed fetching {code}: {str(e)}")

if all_records:
    df = spark.createDataFrame(all_records)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(target_table)
      
    print(f"\n[SUCCESS] World Bank Ingestion Complete! {len(all_records)} records saved to {target_table}.")
else:
    print("\n[WARN] No records retrieved for World Bank indicators.")
