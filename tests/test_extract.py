import pytest 
import os
from dotenv import load_dotenv 
from src.mononoke.pipeline.extract import Extract

load_dotenv()

api_keys = [
    os.getenv("ALPHA_VANTAGE"),
    os.getenv("ALPHA_VANTAGE2"),
]

@pytest.fixture
def extract_instance():
    if not api_keys:
        pytest.skip("ALPHA_VANTAGE API keys not set in environment variables.")
    return Extract(api_keys, "tests/artifacts/raw")

def test_extract_stock(extract_instance):
    symbol = ['GOOGL']
    outputsize = 'compact'
    extract_instance.extract_stock(symbol, outputsize)

    file_path = extract_instance.raw_data_dir / "stocks" / f"{symbol[0]}_stock_data.json"
    assert file_path.exists(), f"File for stock symbol {symbol[0]} does not exist."

def test_yahoo_financials(extract_instance):
    symbols = ['AAPL']
    extract_instance.extract_yahoo_financials(symbols)

    financials_file_path = extract_instance.raw_data_dir / "yahoo_financials" / f"{symbols[0]}_financials.json"
    assert financials_file_path.exists(), f"File for Yahoo financials of {symbols[0]} does not exist."

    info_file_path = extract_instance.raw_data_dir / "yahoo_financials" / f"{symbols[0]}_info.json"
    assert info_file_path.exists(), f"File for Yahoo info of {symbols[0]} does not exist."
