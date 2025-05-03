import pandas as pd
import api_client
from config import PROCESSED_BASENAME
from storage import load_csv, save_csv


def get_cf_handles(csv_path: str = f'./{PROCESSED_BASENAME}/selected_users.csv') -> list[str]:
    df = load_csv(csv_path)
    if df is not None:
        if 'handle' not in df.columns:
            print("[get_cf_handles] Missing 'handle' column in CSV.")
            return []
        return df['handle'].dropna().unique().tolist()

    print(f"[get_cf_handles] CSV not found. Fetching from Codeforces API...")

    raw_json = api_client.get_cf_rated_list_json()
    if raw_json is None or 'result' not in raw_json:
        print("[get_cf_handles] API fetch failed.")
        return []

    data = raw_json['result']
    df = pd.DataFrame([{
        'handle': item['handle'],
        'max_rating': item.get('maxRating', item.get('rating', 0))
    } for item in data])

    save_csv(csv_path, df)
    print(f"[get_cf_handles] Saved {len(df)} users to {csv_path}")
    return df['handle'].dropna().unique().tolist()
