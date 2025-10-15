from src.mononoke import logger
from src.mononoke.utils.common import read_yaml
from pathlib import Path
from src.mononoke.pipeline.extract import Extract
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

extract = Extract(api_keys=api_keys)

extract.commodities_extract(commodities=config["extract_targets"]["commodities"])
extract.exchange_rate_extract(currency_pairs=config["extract_targets"]["currency_pairs"])
extract.extract_stock(symbols=config["extract_targets"]["stock_symbols"], outputsize=config["extract_targets"]["outputsize"])
extract.extract_daily_crypto(crypto_pairs=config["extract_targets"]["crypto_pairs"])
extract.extract_forex(forex_pairs=config["extract_targets"]["forex_pairs"], outputsize=config["extract_targets"]["outputsize"])
extract.extract_yahoo_financials(symbols=config["extract_targets"]["stock_symbols"])
logger.info("Data extraction completed successfully.")