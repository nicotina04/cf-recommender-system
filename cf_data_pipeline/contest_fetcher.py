import pandas as pd
from datetime import datetime
from api_client import get_contest_list, get_json
from storage import save_csv, load_csv
from config import PROCESSED_BASENAME, RATED_CONTEST_METADATA_BASENAME, SLEEP_TIME
from preprocess import get_division_type
import time


def process_rated_contest_csv(min_date: str, max_date: str):
    contest_list_raw_data = get_contest_list()
    if not contest_list_raw_data or 'result' not in contest_list_raw_data:
        print("Contest list fetch failed.")
        return
    
    timestamp_min = int(datetime.strptime(min_date, "%Y-%m-%d").timestamp())
    timestamp_max = int(datetime.strptime(max_date, "%Y-%m-%d").timestamp())
    data = contest_list_raw_data['result']

    result = []
    for item in data:
        if item['phase'] == 'BEFORE':
            continue
        start_time = item['startTimeSeconds']
        if start_time < timestamp_min or start_time > timestamp_max:
            continue
        
        cid = item['id']
        division = get_division_type(item['name'])
        start_dt = datetime.fromtimestamp(start_time)
        result.append({
            'contest_id': cid,
            'division_type': division,
            'contest_date': start_dt
        })
        time.sleep(SLEEP_TIME)

    df = pd.DataFrame(result)
    save_path = f'./{PROCESSED_BASENAME}/{RATED_CONTEST_METADATA_BASENAME}.csv'
    save_csv(save_path, df)
    print(f"{len(df)} contests saved.")

def get_rated_contest_df() -> pd.DataFrame:
    path = f'./{PROCESSED_BASENAME}/{RATED_CONTEST_METADATA_BASENAME}.csv'
    df = load_csv(path)
    if df is None:
        print("Contest metadata not found. Fetching from API...")
        process_rated_contest_csv('2020-01-01', '2025-03-01')
        df = load_csv(path)
    return df

def retry_failed_contests(failed_ids: list[int], existing_csv_path: str, save_csv_path: str):
    existing_df = load_csv(existing_csv_path)
    if existing_df is None:
        print(f"[retry_failed_contests] Failed to load existing CSV: {existing_csv_path}")
        existing_df = pd.DataFrame(columns=["contest_id", "division_type", "contest_date"])

    existing_ids = set(existing_df["contest_id"])
    ids_to_retry = [cid for cid in failed_ids if cid not in existing_ids]

    if not ids_to_retry:
        print("[retry_failed_contests] No contests to retry.")
        return

    contest_list_raw = get_contest_list()
    if not contest_list_raw or 'result' not in contest_list_raw:
        print("[retry_failed_contests] Failed to fetch contest list.")
        return

    contest_date_lookup = {
        item['id']: item['startTimeSeconds']
        for item in contest_list_raw['result']
        if 'startTimeSeconds' in item
    }

    new_records = []
    base_url = 'https://codeforces.com/api/contest.ratingChanges?contestId='

    for cid in ids_to_retry:
        url = f'{base_url}{cid}'
        res = get_json(url, api_timeout=15)
        if res is None or res.get('status') != 'OK' or not res.get('result'):
            print(f"[Retry Failed] Contest {cid} fetch failed or empty.")
            continue

        name = res['result'][0].get('contestName', '')
        division_type = get_division_type(name)
        start_ts = contest_date_lookup.get(cid)
        if start_ts is None:
            print(f"[Retry Warning] Missing start time for contest {cid}")
            continue
        start_dt = datetime.fromtimestamp(start_ts)

        new_records.append({
            'contest_id': cid,
            'division_type': division_type,
            'contest_date': start_dt
        })

        time.sleep(SLEEP_TIME)

    if new_records:
        df_new = pd.DataFrame(new_records)
        df_combined = pd.concat([existing_df, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset='contest_id', inplace=True)
        save_csv(save_csv_path, df_combined)
        print(f"[Retry Success] Added {len(df_new)} contests.")
    else:
        print("[Retry] No new contests added.")