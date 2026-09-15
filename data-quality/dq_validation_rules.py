# Data Quality & API Resilience Helper Module
# Location: d:\bi_project\renewable_energy\data-quality\dq_validation_rules.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_resilient_session(retries=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)):
    """
    Creates a resilient requests Session with exponential backoff for REST APIs.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def log_audit_execution(spark, run_id, pipeline_name, source_name, start_time, status, records_processed=0, error_message=None):
    """
    Logs pipeline run execution metrics into audit_pipeline_execution Delta table.
    """
    from pyspark.sql.functions import current_timestamp
    
    audit_data = [{
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "source_name": source_name,
        "start_time": start_time,
        "end_time": current_timestamp(),
        "status": status,
        "records_processed": records_processed,
        "records_quarantined": 0,
        "error_message": error_message
    }]
    
    try:
        df = spark.createDataFrame(audit_data)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("audit_pipeline_execution")
    except Exception as e:
        print(f"Failed to log audit execution: {str(e)}")
