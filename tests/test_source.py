import os
import pytest
from src.mononoke.pipeline.source import QueryYahooFinance, QueryAlphaVantage

api_key = os.getenv("ALPHA_VANTAGE")

def test_query_yahoo_finance():
    yahoo = QueryYahooFinance()
    financials, info = yahoo.get_financial_summary("AAPL")
    assert len(financials) > 0
    assert info["symbol"] == "AAPL"

@pytest.fixture
def alpha_api_key():
    if not api_key:
        pytest.skip("ALPHA_VANTAGE API key not set in environment variables.")
    return api_key


def test_query_alpha_vantage(alpha_api_key):
    alpha = QueryAlphaVantage(api_key=alpha_api_key)

    forex = alpha.get_forex_daily(from_symbol='USD', to_symbol='JPY', outputsize='compact')
    assert forex['Meta Data']['2. From Symbol'] == 'USD'
    assert forex['Meta Data']['3. To Symbol'] == 'JPY'

    crypto = alpha.get_daily_crypto_data(symbol='BTC', market='USD')
    assert crypto['Meta Data']['2. Digital Currency Code'] == 'BTC'
    assert crypto['Meta Data']['4. Market Code'] == 'USD'

    stock = alpha.get_daily_stock_data(symbol='AAPL', outputsize='compact')
    assert stock['Meta Data']['2. Symbol'] == 'AAPL'
    assert stock['Meta Data']['4. Output Size'] == 'Compact'

    exchange = alpha.exchange_rate(from_currency='GBP', to_currency='JPY')
    assert exchange['Realtime Currency Exchange Rate']['1. From_Currency Code'] == 'GBP'
    assert exchange['Realtime Currency Exchange Rate']['3. To_Currency Code'] == 'JPY'

    commodities = alpha.get_commodity_data(commodity='SUGAR')
    assert commodities['name'] == 'Global Price of Sugar'

    