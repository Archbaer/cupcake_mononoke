from src.mononoke.utils.common import create_directories, save_json, load_json
from src.mononoke import logger
import os
import pandas as pd
import hashlib 
from typing import Any
from pathlib import Path

class Transform: 
    """
    Class to handle data transformation tasks.
    """

    def __init__(self, raw_data_dir: Path = Path("artifacts/raw"), processed_data_dir: Path = Path("artifacts/processed")):
        logger.info(f"[Transform.__init__] start raw_data_dir={raw_data_dir} type={type(raw_data_dir)} processed_data_dir={processed_data_dir}")

        self.raw_data_dir = Path(raw_data_dir)
        self.processed_data_dir = Path(processed_data_dir)
        logger.info(f"[Transform.__init__] resolved raw={self.raw_data_dir} exists={self.raw_data_dir.exists()} is_dir={self.raw_data_dir.is_dir()}")

        create_directories([self.processed_data_dir])

        if not self.raw_data_dir.is_dir():
            parent = self.raw_data_dir.parent
            logger.error(f"[Transform.__init__] invalid raw dir {self.raw_data_dir}. Parent contents={list(parent.iterdir()) if parent.exists() else 'missing'}")
            logger.error(f"Provided raw_data_dir {self.raw_data_dir} is not a valid directory.")
            raise ValueError(f"Provided raw_data_dir {self.raw_data_dir} is not a valid directory.")

    #  --- HELPER FUNCTIONS --- #

    def generate_hash_id(self, source: str, data_type: str, *args: str) -> str:
        """
        Generate a unique hash ID based on the source, data type, and additional arguments.

        Args:
            source: Data source name.
            data_type: Type of data (e.g., "cryptocurrency", "commodity").
            *args: Additional strings to include in the hash basis.
        """
        basis = f"{source}|{data_type}|" + "|".join(args)
        logger.info(f"Generating hash id with basis: {basis}")
        try: 
            hash_id = hashlib.md5(basis.encode("utf-8")).hexdigest()
            return hash_id
        except Exception as e:
            logger.error(f"Error generating hash id: {e}")
            raise e

    def load_raw_data(self, target_dir: Path):
        """
        Load all JSON files from the target directory into a dictionary.

        Args:
            target_dir: Directory containing JSON files.
        """
        try:
            target_dir = Path(target_dir)
            if target_dir.is_dir() is False:
                logger.error(f"Provided target_dir {target_dir} is not a valid directory.")
                raise 
        except Exception as e:
            logger.error(f"Error converting target_dir to Path: {e}")
            raise e
        
        logger.info(f"Loading raw data from directory: {target_dir}")

        files = {}
        for folder in os.listdir(target_dir):
            files[folder] = []
            for file in os.listdir(target_dir / folder):
                files[folder].append(load_json(target_dir / folder / file))

        files = {k.replace('.json', ''): v for k, v in files.items()}

        logger.info(f"Loaded raw data files: {list(files.keys())}")

        return files
    
    def _upsert_csv(self, df: pd.DataFrame, path: Path, subset: list[str]) -> None:
        """
        Append new rows into a CSV and remove duplicates by 'subset' keys.

        Args:
            path: CSV file path to upsert.
            df_new: New DataFrame rows to add.
            subset: Columns that define uniqueness (e.g., ["instrument_id"] or ["instrument_id", "date"]).
        """
        create_directories([path.parent])  # Ensure directory exists

        if path.exists():
            logger.info(f"Upserting data into existing CSV at {path}")
            prev_df = pd.read_csv(path)  # Load old data
            if "date" in prev_df.columns:
                prev_df['date'] = pd.to_datetime(prev_df['date']).dt.strftime('%Y-%m-%d')
            df = pd.concat([prev_df, df], ignore_index=True)  # Combine old + new
            df = df.drop_duplicates(subset=subset, keep="last")  # Remove duplicates
        logger.info(f"Saving DataFrame to CSV at {path} with {len(df)} rows")
        try: 
            df.to_csv(path, index=False) 
        except Exception as e:
            logger.error(f"Error saving CSV to {path}: {e}")
            raise e

    def info_type(self, file: list[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Extract information key-value pairs and company officers from the file. It can only be used to process
        information type files.
        
        Args:
            file: A dictionary containing company information.

        Returns:
            A tuple containing:
            - information_table: A dictionary with company information.
            - company_officers_table: A dictionary with company officers information.
        """

        information_table = {}
        company_officers_table = {}
        for k, v in file.items():
            if k == "companyOfficers":
                company_officers_table = v
            elif k not in ["sector_top_companies", "companyOfficers"]:
                information_table[k] = v

        return information_table, company_officers_table

    def financial_type(self, file: list[str, Any]) -> dict[str, Any]:
        """
        Extract financial key-value pairs from the file. It can only be used to process
        financial type files.
        
        Args: 
            file: A dictionary containing financial data.
        
        Returns:
            A tuple containing:
            - financial_table: A list of dictionaries with financial data.
            - symbol: The stock symbol associated with the financial data.
        """

        symbol = file.get('symbol') or ""
        partial_financial = []
        for k, v in file.items():
            if k != 'symbol':
                partial_financial.append({
                "date": k,
                **v
                })
        return partial_financial, symbol

    def _to_float(self, value: Any) -> float | None:
        """
        Convert a value to float, returning None if conversion fails.

        Args:
            value: The value to convert.

        Returns:
            A float representation of the value, or None if conversion fails.
        """
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value to float: {value}")
            return None

    #  --- TRANSFORMATION FUNCTIONS --- #
    
    def transform_crypto(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw cryptocurrency data into structured DataFrames and save as CSV files.

        Args:
            raw_data: Raw data dictionary from Alpha Vantage API.
        """
        try: 
            metadata = raw_data.get('Meta Data', {})
            time_series = raw_data.get('Time Series (Digital Currency Daily)', {})
        except Exception as e:
            logger.error(f"Error accessing raw_data keys: {e}")
            raise e

        source = "Alpha Vantage"
        currency_code = metadata.get("2. Digital Currency Code") or metadata.get("3. Digital Currency Name")
        market_code = metadata.get("4. Market Code")
        last_updated = metadata.get("6. Last Refreshed")

        hashing = self.generate_hash_id(source, "cryptocurrency", currency_code or "", market_code or "")
        
        meta = {}
        meta['instrument_id'] = hashing
        meta['source'] = source
        meta['data_type'] = "cryptocurrency"
        meta['currency_code'] = currency_code
        meta['market_code'] = market_code
        meta['last_updated'] = last_updated
        logger.info(f"Transforming cryptocurrency data for currency: {currency_code}")
        ts = []
        for k, v in time_series.items():
            ts.append({
                "instrument_id": hashing,
                "date": k,
                "open": self._to_float(v.get("1. open")),
                "high": self._to_float(v.get("2. high")),
                "low": self._to_float(v.get("3. low")),
                "close": self._to_float(v.get("4. close")),
                "volume": self._to_float(v.get("5. volume")),
            })

        df_meta = pd.DataFrame([meta])
        df_ts = pd.DataFrame(ts)

        if not df_ts.empty:
            df_ts['date'] = pd.to_datetime(df_ts['date']).dt.strftime('%Y-%m-%d')

        output_dir = self.processed_data_dir / "cryptocurrencies"
        insta_dir = output_dir / "instruments.csv"
        ts_path = output_dir / "timeseries.csv"

        # Saving the DataFrames to CSV files
        self._upsert_csv(df_meta, insta_dir, subset=["instrument_id"])
        self._upsert_csv(df_ts, ts_path, subset=["instrument_id", "date"])
        logger.info(f"Transformed cryptocurrency data saved for {currency_code} in {output_dir}")

    def transform_commodity(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw commodity data into structured DataFrames and save as CSV files.

        Args:
            raw_data: Raw data dictionary from Alpha Vantage API.
        """

        try:
            time_series = raw_data.get('data', [])
            info = raw_data.get('name', "")
            unit = raw_data.get('unit', "")
        except Exception as e:
            logger.error(f"Error accessing raw_data keys: {e}")
            raise e
        
        source = "Alpha Vantage"
        hashing = self.generate_hash_id(source, "commodity", info)
        
        meta = {
            'instrument_id': hashing,
            'source': source,
            'data_type': "commodity",
            'info': info,
            'unit': unit
        }
        logger.info(f"Transforming commodity data for info: {info}")
        ts = []
        for row in time_series:
            price_value = self._to_float(row.get('value')) if str(row.get('value')) != "." else None
            
            ts.append({
                "instrument_id": hashing,
                "date": row.get('date'),
                "price": price_value,
            })

        df_meta = pd.DataFrame([meta])
        df_ts = pd.DataFrame(ts)

        if not df_ts.empty:
            df_ts['date'] = pd.to_datetime(df_ts['date']).dt.strftime('%Y-%m-%d')
            df_ts = df_ts.dropna(subset=['instrument_id', 'date', 'price'])

        output_dir = self.processed_data_dir / "commodities"
        insta_dir = output_dir / "instruments.csv"
        ts_path = output_dir / "timeseries.csv"

        # Saving the DataFrames to CSV files
        self._upsert_csv(df_meta, insta_dir, subset=["instrument_id"])
        self._upsert_csv(df_ts, ts_path, subset=["instrument_id", "date"])
        logger.info(f"Transformed commodity data saved for {info} in {output_dir}")

    def transform_exchange_rate(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw exchange rate data into structured DataFrames and save as CSV files.

        Args:
            raw_data: Raw data dictionary from Alpha Vantage API.
        """
        block = raw_data.get('Realtime Currency Exchange Rate', None)

        if block is None:
            logger.warning("No 'Realtime Currency Exchange Rate' block found in raw data.")
            raise ValueError("Missing 'Realtime Currency Exchange Rate' block in raw data")

        hashing = self.generate_hash_id(
                "Alpha Vantage", 
                "exchange_rate", 
                block.get("1. From_Currency Code", ""), 
                block.get("3. To_Currency Code", "")
            )

        data = {
            "instrument_id": hashing,
            "source": "Alpha Vantage",
            "data_type": "exchange_rate",
            "from_currency_code": block.get("1. From_Currency Code", ""),
            "to_currency_code": block.get("3. To_Currency Code", ""),
            "exchange_rate": self._to_float(block.get("5. Exchange Rate")),
            "last_refreshed": block.get("6. Last Refreshed"),
            "bid_price": self._to_float(block.get("8. Bid Price")),
            "ask_price": self._to_float(block.get("9. Ask Price")),
        }
        logger.info(f"Transforming exchange rate data for {data['from_currency_code']} to {data['to_currency_code']}")
        df = pd.DataFrame([data])

        output_dir = self.processed_data_dir / "exchange_rates"
        file_path = output_dir / "exchange_rates.csv"

        # Saving the DataFrame to a CSV file
        self._upsert_csv(df, file_path, subset=["instrument_id"])
        logger.info(f"Transformed exchange rate data saved for {hashing} in {output_dir}")

    def transform_stock(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw stock data into structured DataFrames and save as CSV files.

        Args:
            raw_data: Raw data dictionary from Alpha Vantage API.
            symbol: Stock symbol.
        """
        metadata = raw_data.get('Meta Data', None)
        time_series = raw_data.get('Time Series (Daily)', None)

        if metadata is None or time_series is None:
            logger.warning("Missing 'Meta Data' or 'Time Series (Daily)' block in raw data.")
            raise ValueError("Missing required data blocks in raw data")

        source = "Alpha Vantage"    
        last_updated = metadata.get("3. Last Refreshed")
        symbol = metadata.get("2. Symbol")

        hashing = self.generate_hash_id(source, "stock", symbol)
        
        meta = {
            'instrument_id': hashing,
            'source': source,
            'data_type': "stock",
            'symbol': symbol,
            'last_updated': last_updated
        }
        logger.info(f"Transforming stock data for symbol: {symbol}")
        ts = []
        for k, v in time_series.items():
            ts.append({
                "instrument_id": hashing,
                "date": k,
                "open": self._to_float(v.get("1. open")),
                "high": self._to_float(v.get("2. high")),
                "low": self._to_float(v.get("3. low")),
                "close": self._to_float(v.get("4. close")),
                "volume": self._to_float(v.get("5. volume")),
            })

        df_meta = pd.DataFrame([meta])
        df_ts = pd.DataFrame(ts)

        if not df_ts.empty:
            df_ts['date'] = pd.to_datetime(df_ts['date']).dt.strftime('%Y-%m-%d')
            df_ts = df_ts.dropna(subset=['instrument_id', 'date', 'open', 'high', 'low', 'close', 'volume'])

        output_dir = self.processed_data_dir / "stocks"
        insta_dir = output_dir / "instruments.csv"
        ts_path = output_dir / "timeseries.csv"

        # Saving the DataFrames to csv files
        self._upsert_csv(df_meta, insta_dir, subset=["instrument_id"])
        self._upsert_csv(df_ts, ts_path, subset=["instrument_id", "date"])

    def transform_forex(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw forex data into structured DataFrames and save as CSV files.

        Args:
            raw_data: Raw data dictionary from Alpha Vantage API.
        """
        metadata = raw_data.get('Meta Data', None)
        time_series = raw_data.get('Time Series FX (Daily)', None)

        if metadata is None or time_series is None:
            logger.warning("Missing 'Meta Data' or 'Time Series FX (Daily)' block in raw data.")
            raise ValueError("Missing required data blocks in raw data")

        source = "Alpha Vantage"
        symbol = metadata.get("2. From Symbol") + "_" + metadata.get("3. To Symbol")
        last_updated = metadata.get("5. Last Refreshed")

        hashing = self.generate_hash_id(source, "forex", symbol)

        meta = {
            "instrument_id": hashing,
            "source": source,
            "data_type": "forex",
            "symbol": symbol,
            "last_updated": last_updated
        }
        logger.info(f"Transforming forex data for symbol: {symbol}")
        ts = []
        for k, v in time_series.items():
            ts.append({
                "instrument_id": hashing,
                "date": k,
                "open": self._to_float(v.get("1. open")),
                "high": self._to_float(v.get("2. high")),
                "low": self._to_float(v.get("3. low")),
                "close": self._to_float(v.get("4. close"))
            })

        df_meta = pd.DataFrame([meta])
        df_ts = pd.DataFrame(ts)

        if not df_ts.empty:
            df_ts['date'] = pd.to_datetime(df_ts['date']).dt.strftime('%Y-%m-%d')
            df_ts = df_ts.dropna(subset=['instrument_id', 'date', 'open', 'high', 'low', 'close'])

        output_dir = self.processed_data_dir / "forex"
        insta_dir = output_dir / "instruments.csv"
        ts_path = output_dir / "timeseries.csv"

        # Saving the DataFrames to CSV files
        self._upsert_csv(df_meta, insta_dir, subset=["instrument_id"])
        self._upsert_csv(df_ts, ts_path, subset=["instrument_id", "date"])
        logger.info(f"Transformed forex data saved for {symbol} in {output_dir}")

    def transform_yahoo_financials(self, directory: Path) -> None:
        """
        Transform Yahoo Finance raw files in a directory into:
          - information.csv (company info)
          - company_officers.csv (officers per company)
          - financials.csv (time series style financial statements)

        Expects file pairs:
          SYMBOL_info.json
          SYMBOL_financials.json

        Hashing basis: Yahoo Financials|financials|SYMBOL
        """
        logger.info(f"Processing Yahoo Financials directory: {directory}")
        if not directory.is_dir():
            raise NotADirectoryError(directory)

        # Discover files
        financial_files = {p.name.replace("_financials.json", ""): p
                           for p in directory.glob("*_financials.json")}
        info_files = {p.name.replace("_info.json", ""): p
                      for p in directory.glob("*_info.json")}
        symbols = sorted(set(financial_files) | set(info_files))
        if not symbols:
            logger.warning("No Yahoo Finance files found.")
            return

        info_rows: list[dict[str, Any]] = []
        officers_rows: list[dict[str, Any]] = []
        financial_rows: list[dict[str, Any]] = []

        for symbol in symbols:
            fin_path = financial_files.get(symbol)
            info_path = info_files.get(symbol)

            info_table: dict[str, Any] = {}
            officers: list[dict[str, Any]] = []
            if info_path and info_path.exists():
                try:
                    raw_info = load_json(info_path)
                    info_table, officers = self.info_type(raw_info)
                except Exception as e:
                    logger.error(f"Failed parsing info file {info_path}: {e}", exc_info=True)
                    raise

            if "symbol" not in info_table:
                info_table["symbol"] = symbol

            finance_records: list[dict[str, Any]] = []
            if fin_path and fin_path.exists():
                try:
                    raw_fin = load_json(fin_path)
                    finance_records, _sym = self.financial_type(raw_fin)
                    # Fallback symbol from financials if missing in info
                    if "symbol" not in info_table and _sym:
                        info_table["symbol"] = _sym
                except Exception as e:
                    logger.error(f"Failed parsing financials file {fin_path}: {e}", exc_info=True)
                    raise

            instrument_id = self.generate_hash_id("Yahoo Financials", "financials", info_table.get("symbol", symbol))

            if info_table:
                info_rows.append({"instrument_id": instrument_id, **info_table})

            # Officers
            for officer in officers:
                officer_row = {
                    "instrument_id": instrument_id,
                    "symbol": info_table.get("symbol", symbol),
                    **officer
                }
                officers_rows.append(officer_row)

            # Financial records (time-series like)
            for rec in finance_records:
                financial_rows.append({
                    "instrument_id": instrument_id,
                    "symbol": info_table.get("symbol", symbol),
                    **rec
                })

        output_dir = self.processed_data_dir / "yahoo_financials"
        create_directories([output_dir])

        # Information CSV
        if info_rows:
            info_df = pd.DataFrame(info_rows)
            # Basic cleaning
            for col in ["zip", "phone"]:
                if col in info_df.columns:
                    info_df[col] = (info_df[col].astype(str)
                                    .str.replace(r"\D", "", regex=True)
                                    .str.replace(r"\s", "", regex=True))
            info_df = info_df.drop(columns=["ipoExpectedDate"], errors="ignore")
            self._upsert_csv(info_df, output_dir / "information.csv", subset=["instrument_id"])
            logger.info(f"Saved {len(info_df)} company info records")

        # Officers CSV
        if officers_rows:
            officers_df = pd.DataFrame(officers_rows)
            self._upsert_csv(officers_df, output_dir / "company_officers.csv", subset=["instrument_id", "name"])
            logger.info(f"Saved {len(officers_df)} officer records")

        # Financials CSV
        if financial_rows:
            fin_df = pd.DataFrame(financial_rows)
            if "date" in fin_df.columns:
                fin_df["date"] = pd.to_datetime(fin_df["date"]).dt.strftime("%Y-%m-%d")
            initial_len = len(fin_df)

            # Drop rows with >60% missing
            fin_df = fin_df.dropna(thresh=int(len(fin_df.columns) * 0.4)).reset_index(drop=True)
            numeric_means = fin_df.mean(numeric_only=True)
            fin_df = fin_df.fillna(numeric_means)
            removed = initial_len - len(fin_df)

            if removed > 0:
                logger.warning(f"Removed {removed} financial records due to excessive missing values")
            self._upsert_csv(fin_df, output_dir / "financials.csv", subset=["instrument_id", "date"])
            logger.info(f"Saved {len(fin_df)} financial records")

        logger.info(f"Yahoo Financials summary: info={len(info_rows)}, officers={len(officers_rows)}, financials={len(financial_rows)}")

    def transform(self) -> None:
        """
        Main method to traverse raw data directories and apply transformations.
        """
        logger.info("Starting data transformation process...")
        
        if not self.raw_data_dir.exists():
            logger.error(f"Raw data directory does not exist: {self.raw_data_dir}")
            return

        for folder in os.listdir(self.raw_data_dir):
            folder_path = self.raw_data_dir / folder
            
            if not folder_path.is_dir():
                continue

            logger.info(f"Processing folder: {folder}")

            # Special handling for Yahoo Financials: Process the entire directory at once
            if folder == "yahoo_financials":
                try:
                    self.transform_yahoo_financials(folder_path)
                except Exception as e:
                    logger.error(f"Failed to transform yahoo_financials: {e}")
                    raise e
                continue

            # Standard handling for other folders: Process file by file
            for file_name in os.listdir(folder_path):
                file_path = folder_path / file_name
                
                if not file_name.endswith(".json"):
                    continue

                try:
                    logger.info(f"Processing file: {file_path}")
                    raw_data = load_json(file_path)

                    match folder:
                        case "commodities":
                            self.transform_commodity(raw_data)
                        case "cryptocurrencies":
                            self.transform_crypto(raw_data)
                        case "exchange_rates":
                            self.transform_exchange_rate(raw_data)
                        case "forex":
                            self.transform_forex(raw_data)
                        case "stocks":
                            self.transform_stock(raw_data)
                        case _:
                            logger.warning(f"Unknown folder: {folder}, skipping file: {file_name}")
                            
                except Exception as e:
                    logger.error(f"Failed to transform {file_path}: {e}")
                    raise e

        logger.info("Data transformation process completed.")
