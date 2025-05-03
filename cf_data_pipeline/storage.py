import os
import json
from typing import Optional
import pandas as pd


def save_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(path: str) -> Optional[dict]:
    if not os.path.isfile(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            print(f'[load_json] Failed to parse JSON: {e}')
            return None

def save_csv(path: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding='utf-8')

def load_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        print(f'[load_csv] File not found: {path}')
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f'[load_csv] Failed to read CSV: {e}')
        return None