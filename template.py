import os 
from pathlib import Path
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

project_name = "mononoke"

list_of_files = [
    '.github/workflows/',
    f'src/{project_name}/__init__.py',
    f'src/{project_name}/utils/__init__.py',
    f'src/{project_name}/utils/common.py',
    f'src/{project_name}/pipeline/__init__.py',
    f'src/{project_name}/pipeline/source.py',
    f'src/{project_name}/pipeline/extract.py',
    f'src/{project_name}/pipeline/transform.py',
    f'src/{project_name}/pipeline/load.py',
    f'src/{project_name}/notebooks/research.ipynb',
    'api/',
    'configs/config.yaml',
    'params.yaml',
    'requirements.txt',
    'docker-compose.yaml',
    'Dockerfile',
    "main.py"
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)
    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Created directory: {filedir}")
    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, 'w') as f:
            pass
        logging.info(f"Created empty file: {filepath}")
    else:
        logging.info(f"File already exists and is not empty: {filepath}")
