from src.mononoke.utils.common import create_directories, save_json, load_json
from src.mononoke import logger
from pathlib import Path

class Transform: 
    """
    Class to handle data transformation tasks.
    """

    def __init__(self, raw_data_dir: Path = Path("artifacts/raw"), processed_data_dir: Path = Path("artifacts/processed")):
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        create_directories([self.processed_data_dir])