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

    