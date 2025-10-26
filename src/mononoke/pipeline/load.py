from src.mononoke.utils.common import load_json, read_yaml
from src.mononoke import logger
from dotenv import load_dotenv
import psycopg2
import os

from pathlib import Path
from typing import List, Dict, Any

load_dotenv()

class Load:
    """
    Class to load data from the pipeline into a structured database.
    """

    def __init__(self, config_path: str):
        self.config = read_yaml(config_path)
        self.data_dir = Path(self.config['data_directory']['processed_data'])
        self._initialize_database()

    def _initialize_database(self):
        """
        Initialize the database connection by loading the credentials from the env file.
        """
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        logger.info(f"Connecting to database {db_name} at {db_host}:{db_port} as user {db_user}")

        self.connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        self.cursor = self.connection.cursor()

        