from src.mononoke.utils.common import create_directories, save_json
from src.mononoke.pipeline.source import QueryAlphaVantage, QueryYahooFinance
from src.mononoke import logger
from pathlib import Path

class Extract:
    """
    Class to handle data extraction and save it locally.
    """

    def __init__(self, api_keys: list[str], raw_data_dir: Path = Path("artifacts/raw")):
        self.raw_data_dir = raw_data_dir
        create_directories([self.raw_data_dir])
        self.query_av = QueryAlphaVantage(api_keys=api_keys)

    def commodities_extract(self, commodities: list[str]) -> None:
        """
        Extract data for given commodities and save as JSON files.

        Args:
            commodities (list[str]): List of commodity symbols to fetch.
                Available commodities list: ['SUGAR', 'COFFEE', 'WTI', 'BRENT', 'COPPER', 'NATURAL_GAS', 'ALUMINUM', 'WHEAT', 'COTTON'].
        """
        for commodity in commodities:
            try:
                data = self.query_av.get_commodity_data(commodity=commodity)
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

    def extract_stock(self, symbols: list[str], outputsize: str) -> None:
        """
        Extract stock data for given symbols and save as JSON files.

        Args:
            symbols (list[str]): List of stock symbols to fetch (e.g., ['AAPL', 'MSFT']).
            outputsize (str): The size of the data set to return ('compact' or 'full').
        """
        for symbol in symbols:
            try:
                data = self.query_av.get_daily_stock_data(symbol=symbol, outputsize=outputsize)
                file_path = self.raw_data_dir / "stocks" / f"{symbol}_stock_data.json"
                save_json(file_path, data)
                logger.info(f"Stock data for {symbol} saved to {file_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save stock data for {symbol}: {e}")

    def extract_daily_crypto(self, crypto_pairs: list[tuple[str, str]]) -> None:
        """
        Extract daily cryptocurrency data for given symbol and market pairs, then save as JSON files.

        Args:
            pairs (list[tuple[str, str]]): List of tuples containing (symbol, market) pairs.
        """
        for symbol, market in crypto_pairs:
            try:
                data = self.query_av.get_daily_crypto_data(symbol=symbol, market=market)
                file_path = self.raw_data_dir / "cryptocurrencies" / f"{symbol}_{market}_crypto_data.json"
                save_json(file_path, data)
                logger.info(f"Cryptocurrency data for {symbol} in {market} saved to {file_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save cryptocurrency data for {symbol} in {market}: {e}")

    def extract_forex(self, forex_pairs: list[tuple[str, str]], outputsize: str) -> None:
        """
        Extract forex data for a given currency pair and save as a JSON file.

        Args:
            from_symbol (str): The base currency symbol (e.g., 'USD').
            to_symbol (str): The target currency symbol (e.g., 'EUR').
            outputsize (str): The size of the data set to return ('compact' or 'full').
                'compact' returns the latest 100 data points.
                'full' returns the full-length time series of 20+ years of historical data.
        """
        for from_symbol, to_symbol in forex_pairs:
            try:
                data = self.query_av.get_forex_daily(from_symbol=from_symbol, to_symbol=to_symbol, outputsize=outputsize)
                file_path = self.raw_data_dir / "forex" / f"{from_symbol}_{to_symbol}_forex_data.json"
                save_json(file_path, data)
                logger.info(f"Forex data for {from_symbol} to {to_symbol} saved to {file_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save forex data for {from_symbol} to {to_symbol}: {e}")

    def extract_yahoo_financials(self, symbols: list[str]) -> None:
        """
        Extract financial summary data for a given stock symbol from Yahoo Finance and save as JSON files.

        Args:
            symbols (list[str]): Stock symbols to fetch (e.g., ['AAPL', 'MSFT']).
        """
        yahoo = QueryYahooFinance()
        for symbol in symbols:
            try:
                financials, info = yahoo.get_financial_summary(symbol=symbol)
                industry_info = yahoo.get_industry_data(symbol=symbol)

                financials_path = self.raw_data_dir / "yahoo_financials" / f"{symbol}_financials.json"
                info_path = self.raw_data_dir / "yahoo_financials" / f"{symbol}_info.json"
                industry_info_path = self.raw_data_dir / "yahoo_financials" / f"{symbol}_industry_info.json"

                financials = {str(k): v for k, v in financials.items()}
                info = {str(k): v for k, v in info.items()} 
                                
                save_json(financials_path, financials)
                save_json(info_path, info)
                save_json(industry_info_path, industry_info)

                logger.info(f"Yahoo Finance data for {symbol} saved to {financials_path}, {info_path}, and {industry_info_path}")
            except Exception as e:
                logger.error(f"Failed to extract/save Yahoo Finance data for {symbol}: {e}")