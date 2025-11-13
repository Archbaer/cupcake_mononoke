from pathlib import Path
import pandas as pd
import pytest
from box import ConfigBox

from src.mononoke.pipeline.load import Load

@pytest.fixture
def processed_dir(tmp_path):
    p = tmp_path / "artifacts" / "processed"
    (p/ "commodities").mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame({"instrument_id": "a0e938006b2acb6dbe5262dbe9c498a9", "date": "2025-06-01","price": 2525.95904761905})
    df.to_csv( p / "commodities" / "timeseries.csv", index=False)
    return p

def loader(processed_dir, tmp_path):
    cfg = ConfigBox({
        "data_directory": {
            "processed_data": str(processed_dir)
        },
        "database_schemas": ["public"]
    })
    
    return Load(config=cfg)