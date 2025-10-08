from src.mononoke import logger
from src.mononoke.pipeline.extract import Extract
from src.mononoke.pipeline.source import QueryYahooFinance
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("ALPHA_VANTAGE")

logger.info("Starting the application...")

yahoo = QueryYahooFinance()

microsoft_financials, microsoft_info = yahoo.get_financial_summary("MSFT")
print(microsoft_info)
print(microsoft_financials)