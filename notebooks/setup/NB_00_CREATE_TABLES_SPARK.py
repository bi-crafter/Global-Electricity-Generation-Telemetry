# Fabric Notebook: NB_00_CREATE_TABLES_SPARK
# Description: Creates Delta Lake tables in Fabric Lakehouse using PySpark DataFrames (Atomic Schema & Seed Initialization).

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, LongType, DoubleType, TimestampType

spark = SparkSession.builder.getOrCreate()

print("Creating Governance and Configuration Delta Tables in Fabric Lakehouse...")

# -----------------------------------------------------------------------------
# 1. Config Data Sources Table
# -----------------------------------------------------------------------------
config_schema = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_type", StringType(), False),
    StructField("endpoint", StringType(), False),
    StructField("authentication_type", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("frequency", StringType(), False),
    StructField("active", BooleanType(), True),
    StructField("priority", IntegerType(), True),
    StructField("watermark_column", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

config_seed = [
    (1, 'Electricity Maps', 'REST_API', 'https://api.electricitymap.org/v3/power-breakdown/latest', 'API_KEY_HEADER', 'bronze_electricity_maps', 'HOURLY', True, 1, 'datetime', 'INCREMENTAL', None),
    (2, 'Ember Climate', 'CSV_HTTP', 'https://raw.githubusercontent.com/ember-climate/global-electricity-data/main/data/monthly_full_release_long_format.csv', 'NONE', 'bronze_ember', 'MONTHLY', True, 2, 'Year_Month', 'FULL', None),
    (3, 'World Bank Open Data', 'REST_API', 'https://api.worldbank.org/v2/country/all/indicator/', 'NONE', 'bronze_world_bank', 'ANNUAL', True, 3, 'date', 'FULL', None),
    (4, 'NASA POWER API', 'REST_API', 'https://power.larc.nasa.gov/api/temporal/daily/point', 'NONE', 'bronze_nasa_power', 'DAILY', True, 4, 'date', 'INCREMENTAL', None),
    (5, 'Open-Meteo API', 'REST_API', 'https://api.open-meteo.com/v1/forecast', 'NONE', 'bronze_open_meteo', 'HOURLY', True, 5, 'time', 'INCREMENTAL', None)
]

df_config = spark.createDataFrame(config_seed, config_schema) \
    .withColumn("created_at", current_timestamp())

df_config.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("config_data_sources")

print("Created config_data_sources table.")

# -----------------------------------------------------------------------------
# 2. Control Ingestion State Table
# -----------------------------------------------------------------------------
control_schema = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("last_successful_timestamp", TimestampType(), True),
    StructField("last_watermark", StringType(), True),
    StructField("last_run_id", StringType(), True),
    StructField("last_status", StringType(), False),
    StructField("records_ingested", LongType(), True),
    StructField("last_error", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

control_seed = [
    (1, 'Electricity Maps', None, None, None, 'PENDING', 0, None, None),
    (2, 'Ember Climate', None, None, None, 'PENDING', 0, None, None),
    (3, 'World Bank Open Data', None, None, None, 'PENDING', 0, None, None),
    (4, 'NASA POWER API', None, None, None, 'PENDING', 0, None, None),
    (5, 'Open-Meteo API', None, None, None, 'PENDING', 0, None, None)
]

df_control = spark.createDataFrame(control_seed, control_schema) \
    .withColumn("updated_at", current_timestamp())

df_control.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("control_ingestion_state")

print("Created control_ingestion_state table.")

# -----------------------------------------------------------------------------
# 3. Audit Pipeline Execution Table
# -----------------------------------------------------------------------------
audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("records_quarantined", LongType(), True),
    StructField("error_message", StringType(), True)
])

df_audit = spark.createDataFrame([], audit_schema)
df_audit.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("audit_pipeline_execution")

print("Created audit_pipeline_execution table.")

# -----------------------------------------------------------------------------
# 4. Data Quality Results Table
# -----------------------------------------------------------------------------
dq_schema = StructType([
    StructField("dq_check_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("records_checked", LongType(), False),
    StructField("records_failed", LongType(), False),
    StructField("failure_percentage", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("execution_timestamp", TimestampType(), True)
])

df_dq = spark.createDataFrame([], dq_schema)
df_dq.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dq_results")

print("Created dq_results table.")
print("All Governance and Configuration Delta Tables Created Successfully!")
