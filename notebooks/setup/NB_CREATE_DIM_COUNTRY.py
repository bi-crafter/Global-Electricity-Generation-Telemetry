# Fabric PySpark Notebook: Create dim_country Delta Table
# Target Workspace: WS_RENEWABLE_ENERGY_ANALYTICS
# Target Lakehouse: LH_RENEWABLE_ENERGY

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

# Define schema for dim_country
schema = StructType([
    StructField("country_sk", IntegerType(), False),
    StructField("iso_code_3", StringType(), False),
    StructField("country_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

# Master Country Data
country_data = [
    (1, "USA", "United States", "North America", 37.0902, -95.7129),
    (2, "DEU", "Germany", "Europe", 51.1657, 10.4515),
    (3, "FRA", "France", "Europe", 46.2276, 2.2137),
    (4, "CHN", "China", "Asia-Pacific", 35.8617, 104.1954),
    (5, "IND", "India", "Asia-Pacific", 20.5937, 78.9629),
    (6, "BRA", "Brazil", "South America", -14.2350, -51.9253),
    (7, "JPN", "Japan", "Asia-Pacific", 36.2048, 138.2529),
    (8, "GBR", "United Kingdom", "Europe", 55.3781, -3.4360)
]

# Create DataFrame
df_country = spark.createDataFrame(country_data, schema)

# Write to Lakehouse as Delta Table
df_country.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_country")

print("Successfully created and loaded dim_country Delta table in LH_RENEWABLE_ENERGY!")
