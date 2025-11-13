import os
from pathlib import Path
import json
import pandas as pd
import pytest

from src.mononoke.pipeline.transform import Transform

@pytest.fixture
def tmp_artifacts(tmp_path: Path):
    raw = tmp_path / "artifacts" / "raw"
    processed = tmp_path / "artifacts" / "processed"
    # create folders expected by Transform.transform()
    for sub in ["commodities", "cryptocurrencies"]:
        (raw / sub).mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    return raw, processed

def test_transform_commodity_and_crypto(tmp_artifacts):
    raw_dir, processed_dir = tmp_artifacts

    # Minimal Alpha Vantage-like payloads Transform expects
    commodity_payload = {
        "name": "Global Price of Aluminum",
        "unit": "USD/Tonne",
        "data": [
            {"date": "2024-01-31", "value": "2450.12"},
            {"date": "2024-02-29", "value": "2480.00"},
        ],
    }
    crypto_payload = {
        "Meta Data": {
            "2. Digital Currency Code": "BTC",
            "4. Market Code": "USD",
            "6. Last Refreshed": "2024-02-29",
        },
        "Time Series (Digital Currency Daily)": {
            "2024-02-29": {
                "1. open": "60000.0",
                "2. high": "62000.0",
                "3. low": "59000.0",
                "4. close": "61000.0",
                "5. volume": "1234",
            },
            "2024-02-28": {
                "1. open": "59000.0",
                "2. high": "60000.0",
                "3. low": "58000.0",
                "4. close": "59500.0",
                "5. volume": "1111",
            },
        },
    }

    # Write raw JSON files
    (raw_dir / "commodities").mkdir(exist_ok=True, parents=True)
    (raw_dir / "cryptocurrencies").mkdir(exist_ok=True, parents=True)
    (raw_dir / "commodities" / "ALUMINUM.json").write_text(json.dumps(commodity_payload))
    (raw_dir / "cryptocurrencies" / "BTC_USD_crypto_data.json").write_text(json.dumps(crypto_payload))

    transformer = Transform(raw_data_dir=raw_dir, processed_data_dir=processed_dir)
    transformer.transform()

    # Assert outputs exist and are non-empty
    comm_dir = processed_dir / "commodities"
    assert (comm_dir / "instruments.csv").exists()
    assert (comm_dir / "timeseries.csv").exists()
    df_c_ts = pd.read_csv(comm_dir / "timeseries.csv")
    assert {"instrument_id", "date", "price"}.issubset(df_c_ts.columns)
    assert len(df_c_ts) >= 2

    crypto_dir = processed_dir / "cryptocurrencies"
    assert (crypto_dir / "instruments.csv").exists()
    assert (crypto_dir / "timeseries.csv").exists()
    df_k_ts = pd.read_csv(crypto_dir / "timeseries.csv")
    assert {"instrument_id", "date", "open", "close", "volume"}.issubset(df_k_ts.columns)
    assert len(df_k_ts) >= 2