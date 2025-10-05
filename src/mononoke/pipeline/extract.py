from src.mononoke.utils.common import create_directories, save_json
from src.mononoke.pipeline.source import QueryAlphaVantage
from src.mononoke import logger
from pathlib import Path

class Extract:
    """
    Class to handle data extraction and save it locally.
    """

    def __init__(self, api_key: str, raw_data_dir: Path = Path("artifacts/raw")):
        self.api_key = api_key
        self.raw_data_dir = raw_data_dir
        create_directories([self.raw_data_dir])
        self.query_av = QueryAlphaVantage(api_key=self.api_key)