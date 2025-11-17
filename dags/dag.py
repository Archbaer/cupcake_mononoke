from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from pathlib import Path
import sys

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.mononoke.utils.common import read_yaml

# Pipeline classes
from src.mononoke.pipeline.extract import Extract
from src.mononoke.pipeline.transform import Transform
from src.mononoke.pipeline.load import Load

load_dotenv()

with DAG(
    dag_id="finance_etl",
    start_date=datetime(2025, 11, 12),
    schedule="@daily",
    catchup=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1
) as dag:
    
    @task
    def ingestion():
        """Ingest financial data from external APIs and store it in the staging area."""
        config = read_yaml("config/config.yaml")
        keys = [os.getenv("ALPHA_VANTAGE"), os.getenv("ALPHA_VANTAGE2")]
        extractor = Extract(api_keys=keys, config=config)
        extractor.extract()
        return "Ingestion completed."
    
    @task
    def transformation():
        """Transform the ingested data and prepare it for loading."""
        transformer = Transform()
        transformer.transform()
        return "Transformation completed."
    
    @task
    def loader():
        """Load the transformed data into the target database."""
        config = read_yaml("config/config.yaml")
        loader = Load(config=config)
        loader.populate()
        return "Loading completed."
    
    # Task dependencies
    ingestion() >> transformation() >> loader()