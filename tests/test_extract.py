import json
from pathlib import Path
import pytest

from src.mononoke.pipeline.extract import Extract

class StubAlpha:
    def get_commodity_data(self, commodity: str, interval: str = "monthly"):
        return {"name": f"Global Price of {commodity.title()}", "unit": "USD", "data": [{"date": "2024-02-29", "value": "1"}]}
    def exchange_rate(self, from_currency: str, to_currency: str):
        return {"Realtime Currency Exchange Rate": {"1. From_Currency Code": from_currency, "3. To_Currency Code": to_currency, "5. Exchange Rate": "1.2"}}
    def get_daily_stock_data(self, symbol: str, outputsize: str):
        return {"Meta Data": {"2. Symbol": symbol, "4. Output Size": outputsize.title()}, "Time Series (Daily)": {}}
    def get_daily_crypto_data(self, symbol: str, market: str):
        return {"Meta Data": {"2. Digital Currency Code": symbol, "4. Market Code": market}, "Time Series (Digital Currency Daily)": {}}
    def get_forex_daily(self, from_symbol: str, to_symbol: str, outputsize: str):
        return {"Meta Data": {"2. From Symbol": from_symbol, "3. To Symbol": to_symbol, "5. Last Refreshed": "2024-02-29"}, "Time Series FX (Daily)": {}}

@pytest.fixture
def extractor(tmp_path):
    raw_dir = tmp_path / "artifacts" / "raw"
    cfg = {
        "extract_targets": {
            "commodities": ["ALUMINUM"],
            "currency_pairs": [["USD", "EUR"]],
            "stock_symbols": ["AAPL"],
            "crypto_pairs": [["BTC", "USD"]],
            "forex_pairs": [["EUR", "USD"]],
            "outputsize": "compact",
        }
    }
    ext = Extract(api_keys=["dummy"], config=cfg, raw_data_dir=raw_dir)
    # Replace network client with stub
    ext.query_av = StubAlpha()
    return ext

def test_extract_writes_files(extractor):
    extractor.extract()

    # commodities
    path = extractor.raw_data_dir / "commodities" / "ALUMINUM.json"
    assert path.exists()

    # exchange rate
    path = extractor.raw_data_dir / "exchange_rates" / "USD_EUR_exchange_rate.json"
    assert path.exists()

    # stock
    path = extractor.raw_data_dir / "stocks" / "AAPL_stock_data.json"
    assert path.exists()

    # crypto
    path = extractor.raw_data_dir / "cryptocurrencies" / "BTC_USD_crypto_data.json"
    assert path.exists()

    # forex
    path = extractor.raw_data_dir / "forex" / "EUR_USD_forex_data.json"
    assert path.exists()