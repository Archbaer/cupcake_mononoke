import requests
import time
from typing import Optional, Dict, Any
from src.mononoke import logger
from datetime import datetime
import yfinance as yf

class QueryAlphaVantage:
    """
    Class to query Alpha Vantage API for market data.
    """

    def __init__(self, api_keys: list[str]):
        self.api_keys = [key for key in api_keys if key]
        self.current_key_index = 0 
        if not self.api_keys:
            logger.error("API keys for Alpha Vantage are required.")
            raise ValueError("API keys for Alpha Vantage are required.")
        logger.info(f"Initialized QueryAlphaVantage with {len(self.api_keys)} API keys.")

        self.base_url = "https://www.alphavantage.co/query"

    def _rotate_api_key(self):
        """Switches to the next available API key."""

        if self.current_key_index < len(self.api_keys) - 1:
            self.current_key_index += 1
            logger.warning(f"Switching to API key index {self.current_key_index} (total keys: {len(self.api_keys)})")
        else:
            logger.error(f"All {len(self.api_keys)} API keys have reached their rate limit for the day.")
            raise Exception("All Alpha Vantage API keys have reached their rate limit.")
        
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Makes a request to the Alpha Vantage API, rotating keys if rate limited."""

        while self.current_key_index < len(self.api_keys):
            params["apikey"] = self.api_keys[self.current_key_index]

            logger.debug(f"Making request with API key index {self.current_key_index}: {params}")
            res = requests.get(self.base_url, params=params)
            
            if res.status_code != 200:
                logger.error(f"HTTP error: {res.status_code} - {res.text}")
                raise Exception(f"HTTP error: {res.status_code} - {res.text}")
            
            try:
                data = res.json()
            except ValueError:
                logger.error("Alpha Vantage returned non-JSON response.")
                raise Exception("Alpha Vantage returned non-JSON response.")
            
            # Checking for rate limit error
            msg = data.get("Note") or data.get("Information") or data.get("Error Message")

            if msg:
                if "rate limit" in msg.lower() or "frequency" in msg.lower():
                    logger.warning(f"API key index {self.current_key_index} rate limited: {msg}")
                    
                    try:
                        self._rotate_api_key()
                        time.sleep(300) # brief pause before retrying
                        continue
                    except Exception as e:
                        raise e
                else:
                    logger.error(f"Alpha Vantage error: {msg}")
                    raise ValueError(f"Alpha Vantage error: {msg}")
            
            return data


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
            "interval": interval
        }
        data = self._make_request(params)
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
            "to_currency": to_currency
        }

        data = self._make_request(params)
        
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
            "outputsize": outputsize
        }


        data = self._make_request(params)
        
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
            "market": market
        }

        try:
            data = self._make_request(params)
        except:
            raise Exception(f"Failed to fetch crypto data for {symbol}.")

        if data.get("Error Message"):
            logger.error(f"Alpha Vantage error fetching crypto data for {symbol}: {data['Error Message']}")
            raise Exception(f"Alpha Vantage error fetching crypto data for {symbol}: {data['Error Message']}")

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
            "outputsize": outputsize
        }

        try:
            data = self._make_request(params)
        except:
            raise Exception(f"Failed to fetch Forex data for {from_symbol}->{to_symbol}.")

        if data.get("Error Message"):
            logger.error(f"Alpha Vantage error fetching Forex data for {from_symbol}->{to_symbol}: {data['Error Message']}")
            raise Exception(f"Alpha Vantage error fetching Forex data for {from_symbol}->{to_symbol}: {data['Error Message']}")

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
            
            sector = yf.Sector(ticker.info.get('sectorKey'))

            information['sector_top_companies'] = sector.top_companies.to_dict() if sector.top_companies is not None else {}
            
            logger.info(f"Financial summary fetched successfully for stock {symbol} from Yahoo Finance")
            return financials_dict, information
        except Exception as e:
            logger.error(f"Error fetching financial summary for {symbol} from Yahoo Finance: {e}")
            raise Exception(f"Error fetching financial summary for {symbol} from Yahoo Finance: {e}")

    def get_industry_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch industry data from Yahoo Finance.

        Args:
            symbol (str): The stock symbol to fetch (e.g., 'AAPL', 'MSFT').
        """
        data = {}
        try:
            ticker = yf.Ticker(symbol)
            industry = yf.Industry(ticker.info.get('industryKey'))

            data['industry'] = industry.sector_key
            data['name'] = industry.name
            data['top_companies'] = industry.top_companies.to_dict() if industry.top_companies is not None else {}
            data['top_growth_companies'] = industry.top_growth_companies.to_dict() if industry.top_growth_companies is not None else {}
            data['overview'] = industry.overview
            data['research_reports'] = industry.research_reports


            logger.info(f"Industry data fetched successfully for provided {symbol} from Yahoo Finance")
            return data
        except Exception as e:
            logger.error(f"Error fetching industry data for {symbol} from Yahoo Finance: {e}")
            raise Exception(f"Error fetching industry data for {symbol} from Yahoo Finance: {e}")