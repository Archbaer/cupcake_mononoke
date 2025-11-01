from src.mononoke.utils.common import load_json, read_yaml
from src.mononoke import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
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
        self.file_paths = self._find_directory_files() or []

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

        self.engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@localhost:{db_port}/{db_name}",
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )

        self._setup_schema()

    def _setup_schema(self):
        """
        Create necessary schemas in the database if they do not exist.
        """
        schemas = self.config.get('database_schemas', [])
        with self.engine.begin() as conn:
            for schema in schemas:
                logger.info(f"Creating schema {schema} if not exists")
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    def _find_directory_files(self) -> Dict[str, List[Path]]:
        """
        Scan the processed data directory and map subdirectories to their files.

        Returns:
            Dict[str, List[Path]]: A dictionary mapping subdirectory names to lists of file paths.
        """
        if not self.data_dir.exists():
            logger.warning(f"Data directory {self.data_dir} does not exist.")
            return []

        data_paths = []
        for folder in os.listdir(self.data_dir):
            for file in os.listdir(self.data_dir/folder):
                data_paths.append(Path(os.path.join(self.data_dir/folder, file)))
        return data_paths

    def load_data(self, csv_path: Path, table_name: str, schema: str = "public") -> None:
        """
        Load data from a CSV file into the specified database table.
        
        Args:
            csv_path (Path): Path to the CSV file.
            table_name (str): Name of the target database table.
            schema (str): Database schema to use. Defaults to "public".
        
        """

        full_table = f"{schema}.{table_name}"

        logger.info("Infering schema...")
        df_sample = pd.read_csv(csv_path, nrows=1)

        df_sample.head().to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists="replace",
            index=False
        )
        logger.info(f"Table {full_table} created.")

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor, open(csv_path, 'r') as f:
                logger.info(f"Loading data from {csv_path} into {full_table}...")
                cursor.copy_expert(
                    sql=f"COPY {full_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    file=f
                )
            raw_conn.commit()

            with self.engine.begin() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table}"))
                row_count = result.scalar()
                logger.info(f"Loaded {row_count} rows into {full_table}.")
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"Error loading data into {full_table}: {e}")
            raise
        finally:
            raw_conn.close()