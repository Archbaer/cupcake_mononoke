from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

from src.mononoke.utils.common import read_yaml

# Pipeline classes
from src.mononoke.pipeline.extract import Extract
from src.mononoke.pipeline.transform import Transform
from src.mononoke.pipeline.load import Load

load_dotenv()
config = read_yaml("/opt/airflow/config/config.yaml")
 

with DAG(
    dag_id="finance_etl",
    start_date=datetime.today(),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1
) as dag:
    
    @task
    def ingestion():
        """Ingest financial data from external APIs and store it in the staging area."""
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
        loader = Load(config=config)
        loader.populate()
        return "Loading completed."
    
    # Task dependencies
    ingestion() >> transformation() >> loader()