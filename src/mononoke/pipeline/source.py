import requests
from typing import Optional, Dict, Any
from src.mononoke import logger
from datetime import datetime
import yfinance as yf

class QueryAlphaVantage:
    """
    Class to query Alpha Vantage API for market data.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        if not self.api_key:
            logger.error("API key for Alpha Vantage is required.")
            raise ValueError("API key for Alpha Vantage is required.")
        self.base_url = "https://www.alphavantage.co/query"

    def get_commodity_data(self, commodity: str, interval: Optional[str] = "monthly") -> Dict[str, Any]:
        """
        Fetch commodity data from Alpha Vantage API.
        
        Args:
            commodity (str): The commodity to fetch (e.g., 'COPPER'). 
                Available commodity list: ['SUGAR', 'COFFEE', 'WTI', 'BRENT', 'COPPER', 'NATURAL_GAS', 'ALUMINUM', 'WHEAT', 'COTTON'].
            interval (Optional[str]): The interval for time series data (e.g., 'monthly').

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        params = {
            "function": commodity,
            "interval": interval,
            "apikey": self.api_key
        }

        res = requests.get(self.base_url, params=params)
        try:
            data = res.json()
        except ValueError:
            logger.error("Alpha Vantage returned non-JSON response.")
            raise Exception("Alpha Vantage returned non-JSON response.")

        if res.status_code != 200:
            logger.error(f"HTTP error fetching {commodity}: {res.status_code} - {res.text}")
            raise Exception(f"HTTP error fetching {commodity}: {res.status_code} - {res.text}")

        # Detect API-level errors even when HTTP 200
        if any(k in data for k in ("Error Message", "Note", "Information")):
            msg = data.get("Error Message") or data.get("Note") or data.get("Information")
            logger.error(f"Alpha Vantage error for {commodity}: {msg}")
            raise Exception(f"Alpha Vantage error for {commodity}: {msg}")

        logger.info(f"Data fetched successfully for {commodity}")
        return data
    
    def exchange_rate(self, from_currency: str, to_currency: str) -> Dict[str, Any]:
        """
        Fetch exchange rate data from Alpha Vantage API.

        Args:
            from_currency (str): The base currency (e.g., 'BTC', 'USD').
            to_currency (str): The target currency (e.g., 'ETH', 'EUR').

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": from_currency,
            "to_currency": to_currency,
            "apikey": self.api_key
        }

        res = requests.get(self.base_url, params=params)
        try:
            data = res.json()
        except ValueError:
            logger.error("Alpha Vantage returned non-JSON response.")
            raise Exception("Alpha Vantage returned non-JSON response.")

        if res.status_code != 200:
            logger.error(f"HTTP error fetching exchange rate {from_currency}->{to_currency}: {res.status_code} - {res.text}")
            raise Exception(f"HTTP error fetching exchange rate {from_currency}->{to_currency}: {res.status_code} - {res.text}")

        if any(k in data for k in ("Error Message", "Note", "Information")):
            msg = data.get("Error Message") or data.get("Note") or data.get("Information")
            logger.error(f"Alpha Vantage error for Exchange rate {from_currency}->{to_currency}: {msg}")
            raise Exception(f"Alpha Vantage error for Exchange rate {from_currency}->{to_currency}: {msg}")

        # Validate expected structure for this endpoint
        if "Realtime Currency Exchange Rate" not in data:
            logger.error(f"Unexpected response structure for {from_currency}->{to_currency}: {data}")
            raise Exception(f"Unexpected response structure for {from_currency}->{to_currency}")

        logger.info(f"Data fetched successfully for {from_currency} to {to_currency}")
        return data

    def get_daily_stock_data(self, symbol: str, outputsize: str) -> Dict[str, Any]:
        """
        Fetch daily stock data from Alpha Vantage API.

        Args:
            symbol (str): The stock symbol to fetch (e.g., 'AAPL', 'MSFT').
            outputsize (str): The size of the output data (e.g., 'compact', 'full').
                'compact' returns the latest 100 data points.
                'full' returns the full-length time series of 20+ years of historical data.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": outputsize,
            "apikey": self.api_key
        }

        res = requests.get(self.base_url, params=params)

        try:
            data = res.json()
        except ValueError:
            logger.error("Alpha Vantage returned non-JSON response.")
            raise Exception("Alpha Vantage returned non-JSON response.")
        
        if res.status_code != 200:
            logger.error(f"HTTP error fetching stock data for {symbol}: {res.status_code} - {res.text}")
            raise Exception(f"HTTP error fetching stock data for {symbol}: {res.status_code} - {res.text}")
        
        if any(k in data for k in ("Error Message", "Note", "Information")):
            msg = data.get("Error Message") or data.get("Note") or data.get("Information")
            logger.error(f"Alpha Vantage error for stock {symbol}: {msg}")
            raise Exception(f"Alpha Vantage error for stock {symbol}: {msg}")
        
        logger.info(f"Data fetched successfully for stock {symbol}")
        return data
    
    def get_daily_crypto_data(self, symbol: str, market: str) -> Dict[str, Any]:
        """
        Fetch daily cryptocurrency data from Alpha Vantage API.

        Args:
            symbol (str): The cryptocurrency symbol to fetch (e.g., 'BTC', 'ETH').
            market (str): The market currency (e.g., 'USD', 'EUR').
        
        Returns:
            Dict[str, Any]: The JSON response from the API.
        """

        params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": symbol,
            "market": market,
            "apikey": self.api_key  
        }

        res = requests.get(self.base_url, params=params)

        try:
            data = res.json()
        except ValueError:
            logger.error("Alpha Vantage returned non-JSON response.")
            raise Exception("Alpha Vantage returned non-JSON response.")
        
        if res.status_code != 200:
            logger.error(f"HTTP error fetching crypto data for {symbol}: {res.status_code} - {res.text}")
            raise Exception(f"HTTP error fetching crypto data for {symbol}: {res.status_code} - {res.text}")

        if any(k in data for k in ("Error Message", "Note", "Information")):
            msg = data.get("Error Message") or data.get("Note") or data.get("Information")
            logger.error(f"Alpha Vantage error for crypto {symbol}: {msg}")
            raise Exception(f"Alpha Vantage error for crypto {symbol}: {msg}")

        logger.info(f"Data fetched successfully for crypto {symbol}")
        return data

    def get_forex_daily(self, from_symbol: str, to_symbol: str, outputsize: str) -> Dict[str, Any]:
        """
        Fetch daily forex data from Alpha Vantage API.

        Args:
            from_symbol (str): The base currency symbol (e.g., 'USD', 'EUR').
            to_symbol (str): The target currency symbol (e.g., 'JPY', 'GBP').
            outputsize (str): The size of the output data (e.g., 'compact', 'full').
                'compact' returns the latest 100 data points.
                'full' returns the full-length time series of 20+ years of historical data.

        Returns:
            Dict[str, Any]: The JSON response from the API. 
        """

        params = {
            "function": "FX_DAILY",
            "from_symbol": from_symbol,
            "to_symbol": to_symbol,
            "outputsize": outputsize,
            "apikey": self.api_key
        }

        res = requests.get(self.base_url, params=params)

        try:
            data = res.json()
        except ValueError:
            logger.error("Alpha Vantage returned non-JSON response.")
            raise Exception("Alpha Vantage returned non-JSON response.")
        
        if res.status_code != 200:
            logger.error(f"HTTP error fetching Forex data for {from_symbol}->{to_symbol}: {res.status_code} - {res.text}")
            raise Exception(f"HTTP error fetching Forex data for {from_symbol}->{to_symbol}: {res.status_code} - {res.text}")

        if any(k in data for k in ("Error Message", "Note", "Information")):
            msg = data.get("Error Message") or data.get("Note") or data.get("Information")
            logger.error(f"Alpha Vantage error for Forex {from_symbol}->{to_symbol}: {msg}")
            raise Exception(f"Alpha Vantage error for Forex {from_symbol}->{to_symbol}: {msg}")

        logger.info(f"Data fetched successfully for Forex {from_symbol} to {to_symbol}")
        return data
    
class QueryYahooFinance:
    """
    Class to query Yahoo Finance for market data.
    """
    def __init__(self):
        pass
        
    def get_financial_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch financial summary data from Yahoo Finance.

        Args:
            symbol (str): The stock symbol to fetch (e.g., 'AAPL', 'MSFT').
        """
        try:
            ticker = yf.Ticker(symbol)
            financials = ticker.financials
            information = ticker.info
            financials_dict = financials.to_dict()
            logger.info(f"Financial summary fetched successfully for stock {symbol} from Yahoo Finance")
            return financials_dict, information
        except Exception as e:
            logger.error(f"Error fetching financial summary for {symbol} from Yahoo Finance: {e}")
            raise Exception(f"Error fetching financial summary for {symbol} from Yahoo Finance: {e}")
        
    