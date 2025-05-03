import pandas as pd
import time
from api_client import get_rated_users_by_contest
from storage import save_csv, load_csv
from config import PROCESSED_BASENAME, SLEEP_TIME


def extract_handles_from_contests(
    contest_ids: list[int],
    save_csv_path: str = f'./{PROCESSED_BASENAME}/selected_handles.csv'
) -> pd.DataFrame:
    records = list()
    seen = set()

    skipped_contests = list()

    for i, cid in enumerate(contest_ids):
        try:
            res = get_rated_users_by_contest(cid)
            if res is None or res.get('status') != 'OK':
                print(f"[WARN] Contest {cid} skipped.")
                skipped_contests.append(cid)
                continue

            for item in res['result']:
                handle = item['handle']

                if handle in seen:
                    continue

                mx_rating = item['maxRating']

                entity = {
                    'handle': handle,
                    'max_rating': mx_rating
                }
                records.append(entity)
                seen.add(handle)

            time.sleep(SLEEP_TIME)
        except Exception as e:
            print(f'[ERROR] {e}')
    df = pd.DataFrame(records)
    save_csv(save_csv_path, df)
    print(f'{len(df)} handles stored.')
    print(f'Contests {skipped_contests} skipped.')

    if len(skipped_contests) > 0:
        with open('./skipped_contest.txt', 'w', encoding='utf-8') as f:
            for i in skipped_contests:
                f.write(f'{i}\n')
