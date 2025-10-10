from src.mononoke import logger
from src.mononoke.pipeline.extract import Extract
from src.mononoke.pipeline.source import QueryYahooFinance
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("ALPHA_VANTAGE")

logger.info("Starting the application...")

extract = Extract(api_key=api_key)

extract.extract_yahoo_financials(symbols=["AAPL", "MSFT"])