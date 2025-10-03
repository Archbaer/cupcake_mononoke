import os 
import requests
from typing import Optional, Dict, Any
from src.mononoke import logger
from datetime import datetime  
from dataclasses import dataclass

@dataclass 
class MarketData:
    """
    Standardize market data format for class output.
    """
    symbol: str
    price: float
    volume: str
    timestamp: datetime

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

    def get_commodities_data(self, commodity: str, interval: Optional[str] = "monthly") -> Dict[str, Any]:
        """
        Fetch commodities data from Alpha Vantage API.
        
        Args:
            commodity (str): The commodity to fetch (e.g., 'COPPER'). 
                Available commodities list: ['SUGAR', 'COFFEE', 'WTI', 'BRENT', 'COPPER', 'NATURAL_GAS', 'ALUMINUM', 'WHEAT', 'COTTON'].
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
            logger.error(f"Alpha Vantage error for forex {from_currency}->{to_currency}: {msg}")
            raise Exception(f"Alpha Vantage error for forex {from_currency}->{to_currency}: {msg}")

        # Validate expected structure for this endpoint
        if "Realtime Currency Exchange Rate" not in data:
            logger.error(f"Unexpected response structure for {from_currency}->{to_currency}: {data}")
            raise Exception(f"Unexpected response structure for {from_currency}->{to_currency}")

        logger.info(f"Data fetched successfully for {from_currency} to {to_currency}")
        return data