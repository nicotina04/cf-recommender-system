import json
from typing import Optional, Union
from pathlib import Path
import pandas as pd


def save_json(path: Union[str, Path], data: dict):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(path: Union[str, Path]) -> Optional[dict]:
    path = Path(path)
    
    if not path.is_file():
        return None
    
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f'[load_json] Failed to parse JSON: {e}')
        return None  

def save_csv(path: Union[str, Path], df: pd.DataFrame):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding='utf-8')

def load_csv(path: Union[str, Path]) -> Optional[pd.DataFrame]:
    path = Path(path)

    if not path.is_file():
        print(f'[load_csv] File not found: {path}')
        return None

    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f'[load_csv] Failed to read CSV: {e}')
        return None