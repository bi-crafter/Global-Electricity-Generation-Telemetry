# Fabric Notebook: NB_INGEST_NASA_POWER
# Description: Ingest Solar Irradiance, Wind Speed, and Temp from NASA POWER API for key renewable locations.

import requests
import json
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# Locations for key renewable hubs / grid nodes
locations = [
    {"name": "California_Solar_Hub", "lat": 34.0522, "lon": -118.2437},
    {"name": "North_Sea_Wind_Hub", "lat": 54.5000, "lon": 6.0000},
    {"name": "Germany_Bavaria_Solar", "lat": 48.1351, "lon": 11.5820},
    {"name": "India_Rajasthan_Solar", "lat": 26.9124, "lon": 70.9126},
    {"name": "Australia_Outback_Renewables", "lat": -25.2744, "lon": 133.7751}
]

end_date = datetime.now() - timedelta(days=2) # NASA POWER has 2-day latency
start_date = end_date - timedelta(days=30)

start_str = start_date.strftime("%Y%m%d")
end_str = end_date.strftime("%Y%m%d")

api_base = "https://power.larc.nasa.gov/api/temporal/daily/point"
target_table = "bronze_nasa_power"
pipeline_run_id = str(uuid.uuid4())

nasa_records = []

print(f"Starting Ingestion for NASA POWER. Range: {start_str} to {end_str}")

for loc in locations:
    params = {
        "parameters": "ALLSKY_SFC_SW_DWN,WS50M,T2M",
        "community": "RE",
        "longitude": loc["lon"],
        "latitude": loc["lat"],
        "start": start_str,
        "end": end_str,
        "format": "JSON"
    }
    
    try:
        res = requests.get(api_base, params=params, timeout=20)
        if res.status_code == 200:
            data = res.json()
            properties = data.get("properties", {}).get("parameter", {})
            solar_dict = properties.get("ALLSKY_SFC_SW_DWN", {})
            wind_dict = properties.get("WS50M", {})
            temp_dict = properties.get("T2M", {})
            
            for dt_key in solar_dict.keys():
                nasa_records.append({
                    "location_name": loc["name"],
                    "latitude": loc["lat"],
                    "longitude": loc["lon"],
                    "date_key": dt_key,
                    "solar_irradiance_sw_dwn_kwh_m2_day": solar_dict.get(dt_key),
                    "wind_speed_50m_ms": wind_dict.get(dt_key),
                    "temperature_2m_celsius": temp_dict.get(dt_key),
                    "source_system": "NASA POWER API",
                    "source_file": f"nasa_power_{loc['name']}.json",
                    "api_endpoint": api_base,
                    "pipeline_run_id": pipeline_run_id
                })
            print(f"Ingested NASA POWER for {loc['name']}")
    except Exception as e:
        print(f"Error fetching NASA POWER for {loc['name']}: {str(e)}")

if nasa_records:
    df = spark.createDataFrame(nasa_records)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df.write.format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable(target_table)
      
    print(f"NASA POWER Ingestion Complete. {len(nasa_records)} records saved to {target_table}.")
