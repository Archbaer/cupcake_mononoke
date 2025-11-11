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
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        create_directories([self.processed_data_dir])

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
        return hashlib.md5(basis.encode("utf-8")).hexdigest()

    def load_raw_data(self, target_dir: Path):
        """
        Load all JSON files from the target directory into a dictionary.

        Args:
            target_dir: Directory containing JSON files.
        """
        files = {}
        for folder in os.listdir(target_dir):
            files[folder] = []
            for file in os.listdir(target_dir / folder):
                files[folder].append(load_json(target_dir / folder / file))

        files = {k.replace('.json', ''): v for k, v in files.items()}

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

        df.to_csv(path, index=False)  # Save combined data

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
        metadata = raw_data.get('Meta Data', {})
        time_series = raw_data.get('Time Series (Digital Currency Daily)', {})

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
        time_series = raw_data.get('data', [])

        source = "Alpha Vantage"
        info = raw_data.get('name', "")
        unit = raw_data.get('unit', "")

        hashing = self.generate_hash_id(source, "commodity", info)
        
        meta = {
            'instrument_id': hashing,
            'source': source,
            'data_type': "commodity",
            'info': info,
            'unit': unit
        }

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
            return

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
        metadata = raw_data.get('Meta Data', {})
        time_series = raw_data.get('Time Series (Daily)', {})

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
        metadata = raw_data.get('Meta Data', {})
        time_series = raw_data.get('Time Series FX (Daily)', {})

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

    def transform_yahoo_financials(self, raw_data: dict[str, str]) -> None:
        """
        Transform raw Yahoo Financials data into structured DataFrames and save in two separate CSV files containing company information and financial data.

        Args:
            raw_data: Raw data dictionary from Yahoo Financials.
        """
        info_rows = []
        officers_rows = []
        financials_rows = []

        for i, file in enumerate(raw_data):    
            key = next(iter(file))
            
            if key == 'address1':
                logger.info(f"[{i}] Processing Info type file")
                info_table, officers = self.info_type(file)
                hashing = self.generate_hash_id(
                    "Yahoo Financials", 
                    "financials", 
                    info_table.get("symbol", "")
                )
                
                info_with_hash = {'instrument_id': hashing, **info_table}
                info_rows.append(info_with_hash)
                
                if officers:
                    for officer in officers:
                        officer['instrument_id'] = hashing
                        officer['symbol'] = info_table.get('symbol', '')
                    officers_rows.extend(officers)            
            else:
                logger.info(f"[{i}] Processing Financials type file")
                finance_table, symbol = self.financial_type(file)
                print(f"Current finance_table length: {len(finance_table)}")
                hashing = self.generate_hash_id("Yahoo Financials", "financials", symbol)
                if finance_table:
                    # Hashing and linking each financial record to info though instrument_id to be implemented
                    rows = [{"instrument_id": hashing, "symbol": symbol, **record} for record in finance_table]
                    financials_rows.extend(rows)
                    print(f"Current rows length: {len(rows)}")

                        

        output_dir = self.processed_data_dir / "yahoo_financials"

        if info_rows:
            info_df = pd.DataFrame(info_rows)
            info_path = output_dir / "information.csv"

            # Data cleaning step to avoid formatting issues
            info_df['zip'] = info_df['zip'].astype(str).str.replace(r"\D", "", regex=True)
            info_df['zip'] = info_df['zip'].astype(str).str.replace(r"\s", "", regex=True)
            info_df['phone'] = info_df['phone'].astype(str).str.replace(r"\s", "", regex=True)
            info_df['phone'] = info_df['phone'].astype(str).str.replace(r"\D", "", regex=True)

            info_df = info_df.drop('ipoExpectedDate', axis=1, errors='ignore')

            self._upsert_csv(info_df, info_path, subset=["instrument_id"])
            logger.info(f"Saved {len(info_df)} company info records")
        
        if officers_rows:
            officers_df = pd.DataFrame(officers_rows)
            officers_path = output_dir / "company_officers.csv"

            self._upsert_csv(officers_df, officers_path, subset=["instrument_id", "name"])
            logger.info(f"Saved {len(officers_df)} officer records")

        if financials_rows:
            financials_df = pd.DataFrame(financials_rows)
            financials_df['date'] = pd.to_datetime(financials_df['date']).dt.strftime('%Y-%m-%d')

            initial_len = len(financials_df)
            financials_df = financials_df.dropna(thresh=len(financials_df.columns) * 0.4).reset_index(drop=True)
            financials_df.fillna(value=financials_df.mean(numeric_only=True), inplace=True)

            logger.warning(f"Removed {initial_len - len(financials_df)} financial records due to excessive missing values")
            
            financials_path = output_dir / "financials.csv"
            self._upsert_csv(financials_df, financials_path, subset=["instrument_id", "date"])
            logger.info(f"Saved {len(financials_df)} financial records")

        logger.info(f"Summary: {len(info_rows)} info, {len(financials_rows)} financials processed.")

    def transform(self):
        """
        Process all raw data files in the relative path directories to CSV files and store them in the specified output directory.
        """
        logger.info("Starting data transformation process...")

        for folder in os.listdir(self.raw_data_dir):
            folder_path = self.raw_data_dir / folder
            for file_name in os.listdir(folder_path):
                file_path = folder_path / file_name
                raw_data = load_json(file_path)
                print(f"File path: {file_path}, file type: {type(raw_data)}")

                match folder:
                    case "cryptocurrencies":
                        self.transform_crypto(raw_data)
                    case "commodities":
                        self.transform_commodity(raw_data)
                    case "exchange_rates":
                        self.transform_exchange_rate(raw_data)
                    case "stocks":
                        self.transform_stock(raw_data)
                    case "forex":
                        self.transform_forex(raw_data)
                    case "yahoo_financials":
                        self.transform_yahoo_financials(self.load_raw_data(self.raw_data_dir)['yahoo_financials'])
                    case _:
                        logger.warning(f"Unknown data type folder: {folder}. Skipping file: {file_name}")

        logger.info("Data transformation process completed.")
