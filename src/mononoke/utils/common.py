import os
import json
import yaml
import tempfile
from pathlib import Path
from typing import Any, Iterable, Union
import joblib

from src.mononoke import logger
from box import ConfigBox
from box.exceptions import BoxValueError

def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """Read YAML and return a ConfigBox."""
    try:
        with open(path_to_yaml, "r", encoding="utf-8") as fh:
            content = yaml.safe_load(fh) or {}
        logger.info(f"yaml file: {path_to_yaml} loaded successfully")
        return ConfigBox(content)
    except BoxValueError as e:
        logger.error(f"BoxValueError: {e}")
        raise ValueError("Yaml file is empty")
    except Exception as e:
        logger.error(f"Error reading YAML file at {path_to_yaml}: {e}")
        raise

def create_directories(paths: Iterable[Union[str, Path]]) -> None:
    """Ensure each path exists. Accepts str or Path."""
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory created at: {Path(p)}")

def save_json(path: Path, data: Any) -> None:
    """Save JSON atomically. Accepts any JSON-serializable data."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # write to temp file on same directory then atomically replace
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tf:
        json.dump(data, tf, ensure_ascii=False, indent=2)
        tf.flush()
        os.fsync(tf.fileno())
        tmp_name = tf.name
    os.replace(tmp_name, str(path))
    logger.info(f"JSON file saved at: {path}")

def load_json(path: Path) -> Any:
    """Load JSON file and return parsed content."""
    path = Path(path)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def save_bin(data: Any, path: Path) -> None:
    """Save binary via joblib."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(data, path)
    logger.info(f"Binary file saved at: {path}")

def load_bin(path: Path) -> Any:
    """Load binary via joblib."""
    return joblib.load(path)
