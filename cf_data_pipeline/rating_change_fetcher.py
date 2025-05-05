from typing import Optional
from cf_data_pipeline import api_client, db_rating_change


def get_rating_changes(handle: str) -> Optional[dict]:
    req_url = f'https://codeforces.com/api/user.rating?handle={handle}'
    res = api_client.safe_get_json(req_url, 10)
    if res is None or res['status'] != 'OK':
        return None
    return res

def process_rating_changes(json_data: dict) -> list:
    data = json_data['result']

    if len(data) == 0:
        return

    records = list()

    handle = data[0]['handle']
    for item in data:
        contest_id = item['contestId']
        old_rating = item['oldRating']
        new_rating = item['newRating']
        records.append((handle, contest_id, old_rating, new_rating))
    return records

def fetch_and_store(handle: str) -> bool:
    # already stored handle
    if db_rating_change.has_rating_data(handle):
        return True

    data = get_rating_changes(handle)
    if data is None:
        return False
    records = process_rating_changes(data)
    try:
        db_rating_change.insert_rating_changes(records)
        print(f'Handle {handle} complete.')
        return True
    except Exception as e:
        print(f'[EXCEPTION] {e}')
        return False