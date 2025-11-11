from src.mononoke import logger
from src.mononoke.utils.common import read_yaml
from pathlib import Path
from src.mononoke.pipeline.extract import Extract
from src.mononoke.pipeline.transform import Transform
from src.mononoke.pipeline.load import Load
import os
from dotenv import load_dotenv

# ---- Configuration ---- 
load_dotenv()
api_keys = [
    os.getenv("ALPHA_VANTAGE"),
    os.getenv("ALPHA_VANTAGE2"),
]

config = read_yaml(Path("config/config.yaml"))

logger.info("Starting the application...")

extract = Extract(api_keys=api_keys, raw_data_dir=Path("artefatos/bruto"), config=config)
extract.extract()

transformer = Transform(raw_data_dir=Path("artefatos/bruto"), processed_data_dir=Path("artefatos/processados"))
transformer.transform()

loader = Load(config_path="config/config.yaml")
loader.populate()