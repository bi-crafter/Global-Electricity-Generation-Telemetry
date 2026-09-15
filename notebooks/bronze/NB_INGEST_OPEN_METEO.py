# Fabric Notebook: NB_INGEST_OPEN_METEO
# Description: Ingest real-time and 7-day forecast weather, wind speed, solar radiation from Open-Meteo API.

import requests
import json
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

grid_nodes = [
    {"zone": "US-CAL", "lat": 36.7783, "lon": -119.4179},
    {"zone": "GERMANY", "lat": 51.1657, "lon": 10.4515},
    {"zone": "UK-NORTH-SEA", "lat": 55.3781, "lon": -3.4360},
    {"zone": "INDIA-WEST", "lat": 22.2587, "lon": 71.1924}
]

api_url = "https://api.open-meteo.com/v1/forecast"
target_table = "bronze_open_meteo"
pipeline_run_id = str(uuid.uuid4())

meteo_records = []

print(f"Starting Open-Meteo Ingestion. Run ID: {pipeline_run_id}")

for node in grid_nodes:
    params = {
        "latitude": node["lat"],
        "longitude": node["lon"],
        "hourly": "temperature_2m,wind_speed_10m,wind_speed_80m,direct_normal_irradiance",
        "timezone": "UTC"
    }
    
    try:
        res = requests.get(api_url, params=params, timeout=15)
        if res.status_code == 200:
            data = res.json()
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            ws10 = hourly.get("wind_speed_10m", [])
            ws80 = hourly.get("wind_speed_80m", [])
            dni = hourly.get("direct_normal_irradiance", [])
            
            for i in range(len(times)):
                meteo_records.append({
                    "zone": node["zone"],
                    "latitude": node["lat"],
                    "longitude": node["lon"],
                    "time_iso": times[i],
                    "temperature_2m": temps[i] if i < len(temps) else None,
                    "wind_speed_10m": ws10[i] if i < len(ws10) else None,
                    "wind_speed_80m": ws80[i] if i < len(ws80) else None,
                    "direct_normal_irradiance": dni[i] if i < len(dni) else None,
                    "source_system": "Open-Meteo API",
                    "source_file": f"open_meteo_{node['zone']}.json",
                    "api_endpoint": api_url,
                    "pipeline_run_id": pipeline_run_id
                })
            print(f"Fetched Open-Meteo forecast for {node['zone']} ({len(times)} hours)")
    except Exception as e:
        print(f"Error fetching Open-Meteo for {node['zone']}: {str(e)}")

if meteo_records:
    df = spark.createDataFrame(meteo_records)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df.write.format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable(target_table)
      
    print(f"Open-Meteo Ingestion Complete. {len(meteo_records)} rows written to {target_table}.")
