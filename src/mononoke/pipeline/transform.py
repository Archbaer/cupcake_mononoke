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
        basis = f"{source}|{data_type}|" + "|".join(args)
        logger.info(f"Generating hash id with basis: {basis}")
        return hashlib.md5(basis.encode("utf-8")).hexdigest()

    def load_raw_data(self, target_dir: Path):
        files = {}
        for folder in os.listdir(target_dir):
            files[folder] = []
            for file in os.listdir(target_dir / folder):
                files[folder].append(load_json(target_dir / folder / file))

        files = {k.replace('.json', ''): v for k, v in files.items()}

        return files
    
    def _to_float(self, value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value to float: {value}")
            return None

    #  --- TRANSFORMATION FUNCTIONS --- #
    
    def transform_crypto(self, raw_data: dict[str, str]) -> None:
        metadata = raw_data.get('Meta Data', {})
        time_series = raw_data.get('Time Series (Digital Currency Daily)', {})

        source = "Alpha Vantage"
        currency_code = metadata.get("2. Digital Currency Code") or metadata.get("3. Digital Currency Name")
        market_code = metadata.get("4. Market Code")
        last_updated = metadata.get("6. Last Refreshed")

        hashing = self.generate_hash_id(source, "cryptocurrency", currency_code, market_code, last_updated)
        
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
                "open": v.get("1. open"),
                "high": v.get("2. high"),
                "low": v.get("3. low"),
                "close": v.get("4. close"),
                "volume": v.get("5. volume"),
            })

        df_meta = pd.DataFrame([meta])
        df_ts = pd.DataFrame(ts)

        # Save the DataFrames to the processed data directory
        df_meta.to_csv(self.processed_data_dir / f"{hashing}_meta.csv", index=False)
        df_ts.to_csv(self.processed_data_dir / f"{hashing}_time_series.csv", index=False)