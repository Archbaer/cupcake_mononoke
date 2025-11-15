from src.mononoke.utils.common import save_json
from src.mononoke import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from box import ConfigBox
import pandas as pd
import os

from pathlib import Path
from typing import List, Dict, Any

load_dotenv()

class Load:
    """
    Class to load data from the pipeline into a structured database.
    """

    def __init__(self, config: ConfigBox):
        self.config = config
        self.data_dir = Path(self.config['data_directory']['processed_data'])
        self._initialize_database()
        self.file_paths = self._find_directory_files() or []
        self.table_mappings = []

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
    
    def save_table_mappings(self, mappings: List[str], output_path: str) -> None:
        """
        Save the table mappings to a JSON file.

        Args:
            output_path (str): Path to the output JSON file.
        """
        save_json(data={"table_mappings": mappings}, path=output_path)
        logger.info(f"Table mappings saved to {output_path}")

    def load_data(self, csv_path: Path, table_name: str, schema: str = "public") -> None:
        """
        Load data from a CSV file into the specified database table.
        
        Args:
            csv_path (Path): Path to the CSV file.
            table_name (str): Name of the target database table.
            schema (str): Database schema to use. Defaults to "public".
        
        """
        if not schema:
            logger.info("No schema specified, defaulting to 'public'")
            schema = "public"

        full_table = f"{schema}.{table_name}"
        inspector = inspect(self.engine)

        if not inspector.has_table(table_name, schema=schema):
            logger.info(f"Table {full_table} does not exist. Creating new table.")
            df_schema = pd.read_csv(csv_path, nrows=1)
            df_schema.head(1).to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists="replace",
                index=False
            )
            logger.info(f"Table {full_table} created with schema.")

        cols = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
        cols_sql = ", ".join(f'"{col}"' for col in cols)
        copy_sql = f"COPY {full_table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor, open(csv_path, 'r', encoding="utf-8") as f:
                logger.info(f"Appending data from {csv_path} into {full_table}.")
                cursor.copy_expert(sql=copy_sql, file=f)
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

    def populate(self) -> None:
        """
        Execute the data loading process for all identified files.
        """
        logger.info("Starting data loading process...")
        for file_path in self.file_paths:
            table_name = "_".join([file_path.parent.stem, file_path.stem])
            logger.info(f"Loading data from {file_path} into table {table_name}.")
            self.load_data(csv_path=file_path, table_name=table_name, schema=self.config.get('database_schemas', [])[0])
            self.table_mappings.append(".".join([self.config.get('database_schemas', [])[0], table_name]))
        self.save_table_mappings(self.table_mappings, output_path="artifacts/table_mappings.json")
        
        logger.info("Data loading process completed.")