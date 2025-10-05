import os 
import yaml
from src.mononoke import logger
import json
import joblib
from box import ConfigBox
from ensure import ensure_annotations
from pathlib import Path
from typing import Any, Iterable, Union
from box.exceptions import BoxValueError

@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """Reads a yaml file and returns the contents as a ConfigBox object.
    
    Args:
        path_to_yaml (Path): Path to the yaml file.

    Returns:
        ConfigBox: Contents of the yaml file as a ConfigBox object.
    """
    try:
        with open(path_to_yaml, 'r') as file:
            content = yaml.safe_load(file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError as e:
        logger.error(f"BoxValueError: {e}")
        raise ValueError("Yaml file is empty")
    except Exception as e:
        logger.error(f"Error reading YAML file at {path_to_yaml}: {e}")
        raise e

def create_directories(paths: Iterable[Union[str, Path]]) -> None:
    """Creates directories if they do not exist.
    
    Args:
        paths (list[Path]): List of directory paths to create.
    """
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory created at: {path}")

@ensure_annotations
def save_json(path: Path, data: dict[str, Any]) -> None:
    """Saves a dictionary as a JSON file.
    
    Args:
        path (Path): Path to save the JSON file.
        data (dict[str, Any]): Dictionary to save as JSON.
    """
    try:
        with open(path, 'w') as file:
            json.dump(data, file, indent=4)
        logger.info(f"JSON file saved at: {path}")
    except Exception as e:
        logger.error(f"Error saving JSON file at {path}: {e}")
        raise e
    
@ensure_annotations
def load_json(path: Path) -> dict[str, Any]:
    """Loads a JSON file and returns its contents as a dictionary.
    
    Args:
        path (Path): Path to the JSON file.

    Returns:
        dict[str, Any]: Contents of the JSON file as a dictionary.
    """
    try:
        with open(path, 'r') as file:
            content = json.load(file)
            logger.info(f"JSON file loaded successfully from: {path}")
            return ConfigBox(content)
    except Exception as e:
        logger.error(f"Error loading JSON file from {path}: {e}")
        raise e
    
@ensure_annotations
def save_bin(data: Any, path: Path) -> None:
    """Saves data as a binary file using joblib.
    
    Args:
        data (Any): Data to save.
        path (Path): Path to save the binary file.
    """
    try:
        joblib.dump(data, path)
        logger.info(f"Binary file saved at: {path}")
    except Exception as e:
        logger.error(f"Error saving binary file at {path}: {e}")
        raise e
    
@ensure_annotations
def load_bin(path: Path) -> Any:
    """Loads a binary file using joblib and returns its contents.
    
    Args:
        path (Path): Path to the binary file.

    Returns:
        Any: Contents of the binary file.
    """
    try:
        return joblib.load(path)
    except Exception as e:
        logger.error(f"Error loading binary file from {path}: {e}")
        raise e
    