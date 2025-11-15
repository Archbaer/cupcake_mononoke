from pathlib import Path
import pandas as pd
import pytest
import json
from box import ConfigBox

from src.mononoke.pipeline.load import Load

@pytest.fixture
def processed_dir(tmp_path):
    p = tmp_path / "artifacts" / "processed"
    (p/ "commodities").mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([{"instrument_id": "a0e938006b2acb6dbe5262dbe9c498a9", "date": "2025-06-01","price": 2525.95904761905}])
    df.to_csv( p / "commodities" / "timeseries.csv", index=False)
    return p

@pytest.fixture
def loader(processed_dir, monkeypatch):
    cfg = ConfigBox({
        "data_directory": {
            "processed_data": str(processed_dir)
        },
        "database_schemas": ["public"]
    })

    def _noop_init(self):
        self.engine = object()
    monkeypatch.setattr(Load, "_initialize_database", _noop_init)
    ld = Load(cfg)
    
    return ld

def test_find_directory_files(loader, processed_dir):
    files = loader._find_directory_files()
    assert any(f.name == "timeseries.csv" for f in files.get("commodities", []))

def test_table_mappings(tmp_path: Path, loader: Load):
    out = tmp_path / "table_mappings.json"
    loader.save_table_mappings(["public.commodities_timeseries"], out)
    assert out.exists()
    data = json.loads(out.read_text())
    assert data['table_mappings'] == ["public.commodities_timeseries"]