from src.mononoke.utils.common import create_directories, save_json
from src.mononoke.pipeline.source import QueryAlphaVantage
from src.mononoke import logger
from pathlib import Path

class Extract:
    """
    Class to handle data extraction and save it locally.
    """

    def __init__(self, api_key: str, raw_data_dir: Path = Path("artifacts/raw")):
        self.api_key = api_key
        self.raw_data_dir = raw_data_dir
        create_directories([self.raw_data_dir])
        self.query_av = QueryAlphaVantage(api_key=self.api_key)

    def commodities_extract(self, commodities: list[str]) -> None:
        """
        Extract data for given commodities and save as JSON files.

        Args:
            commodities (list[str]): List of commodity symbols to fetch.
                Available commodities list: ['SUGAR', 'COFFEE', 'WTI', 'BRENT', 'COPPER', 'NATURAL_GAS', 'ALUMINUM', 'WHEAT', 'COTTON'].
        """
        for commodity in commodities:
            try:
                data = self.query_av.get_commodities_data(commodity=commodity)
                file_path = self.raw_data_dir / "commodities" / f"{commodity}.json"
                save_json(file_path, data)
                logger.info(f"Data for {commodity} saved to {file_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save data for {commodity}: {e}")

    def exchange_rate_extract(self, currency_pairs: list[tuple[str, str]]) -> None:
        """
        Extract exchange rate data for given currency pairs and save as JSON files.

        Args:
            currency_pairs (list[tuple[str, str]]): List of tuples containing currency pairs (from_currency, to_currency).
        """
        for from_currency, to_currency in currency_pairs:
            try:
                data = self.query_av.exchange_rate(from_currency=from_currency, to_currency=to_currency)
                file_path = self.raw_data_dir / "exchange_rates" / f"{from_currency}_{to_currency}_exchange_rate.json"
                save_json(file_path, data)
                logger.info(f"Exchange rate data for {from_currency} to {to_currency} saved to {file_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save exchange rate data for {from_currency} to {to_currency}: {e}")

    def extract_stock(self, symbol: str, outputsize: str) -> None:
        """
        Extract stock data for a given symbol and save as a JSON file.

        Args:
            symbol (str): Stock symbol to fetch (e.g., 'AAPL').
            outputsize (str): The size of the data set to return ('compact' or 'full').
        """
        try:
            data = self.query_av.get_daily_stock_data(symbol=symbol, outputsize=outputsize)
            file_path = self.raw_data_dir / "stocks" / f"{symbol}_stock_data.json"
            save_json(file_path, data)
            logger.info(f"Stock data for {symbol} saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to extract/save stock data for {symbol}: {e}")

    def extract_daily_crypto(self, symbol: str, market: str) -> None:
        """
        Extract daily cryptocurrency data for a given symbol and market, then save as a JSON file.

        Args:
            symbol (str): Cryptocurrency symbol to fetch (e.g., 'BTC').
            market (str): Market in which the cryptocurrency is traded (e.g., 'USD').
        """
        try:
            data = self.query_av.get_daily_crypto_data(symbol=symbol, market=market)
            file_path = self.raw_data_dir / "cryptocurrencies" / f"{symbol}_{market}_crypto_data.json"
            save_json(file_path, data)
            logger.info(f"Cryptocurrency data for {symbol} in {market} saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to extract/save cryptocurrency data for {symbol} in {market}: {e}")

    def extract_forex(self, from_symbol: str, to_symbol: str, outputsize: str) -> None:
        """
        Extract forex data for a given currency pair and save as a JSON file.

        Args:
            from_symbol (str): The base currency symbol (e.g., 'USD').
            to_symbol (str): The target currency symbol (e.g., 'EUR').
            outputsize (str): The size of the data set to return ('compact' or 'full').
                'compact' returns the latest 100 data points.
                'full' returns the full-length time series of 20+ years of historical data.
        """
        try:
            data = self.query_av.get_forex_daily(from_symbol=from_symbol, to_symbol=to_symbol, outputsize=outputsize)
            file_path = self.raw_data_dir / "forex" / f"{from_symbol}_{to_symbol}_forex_data.json"
            save_json(file_path, data)
            logger.info(f"Forex data for {from_symbol} to {to_symbol} saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to extract/save forex data for {from_symbol} to {to_symbol}: {e}")
