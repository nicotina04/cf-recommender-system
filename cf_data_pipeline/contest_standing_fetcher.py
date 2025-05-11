import time
import pandas as pd
from contest_fetcher import get_rated_contest_df
import storage
import api_client
from config import SLEEP_TIME, PROCESSED_DATA_DIR


def is_provisional_or_unrated_handle(handle: str) -> bool:
    res = api_client.get_user_rating_changes(handle)
    if res is None:
        return None, None
    return len(res['result']) < 5, len(res['result']) == 0

def get_entity_from_rating_change(items: list[dict]) -> dict:
    ratings = list()
    rating_sum_all = 0
    unrated_count = 0
    for item in items:
        old_rating = item['oldRating']
        rating_sum_all += old_rating
        if old_rating != 0:
            ratings.append(old_rating)
        else:
            unrated_count += 1
    avg_rating_all = rating_sum_all // max(1, len(items))
    avg_rated_only = rating_sum_all // max(1, len(items) - unrated_count)
    ratings.sort()
    sum_of_variance = 0
    for rating in ratings:
        if rating is not None:
            sum_of_variance += (rating - avg_rated_only) ** 2
    std = (sum_of_variance / max((len(items) - unrated_count), 1)) ** 0.5

    entity = {
        'avg_rating_all': avg_rating_all,
        'avg_rating_rated_only': avg_rated_only,
        'median_rating_rated': ratings[len(ratings) // 2] if len(ratings) > 0 else 0,
        '25th_percentile_rated': ratings[int(len(ratings) * 0.25)] if len(ratings) > 0 else 0,
        '75th_percentile_rated': ratings[int(len(ratings) * 0.75)] if len(ratings) > 0 else 0,
        'std_rating_rated': std,
        'count_total': len(items),
        'count_unrated': unrated_count,
        'unrated_ratio': round(unrated_count / max(1, len(items)), 3),
    }
    return entity


def process_contest_standings():
    rated_contests = get_rated_contest_df()
    contests = rated_contests['contest_id'].tolist()
    entity_list = list()
    failed_cid = list()
    total_failed = list()

    for contest_id in contests:
        res_rating_changes = api_client.get_contest_rating_changes(contest_id)
        time.sleep(SLEEP_TIME)
        if res_rating_changes is None:
            failed_cid.append(contest_id)
            continue
        items = res_rating_changes['result']
        entity = get_entity_from_rating_change(items)
        print(f"[INFO] {contest_id} - {entity}")
        entity_list.append(entity)
    
    for contest_id in failed_cid:
        res_rating_changes = api_client.get_contest_rating_changes(contest_id)
        if res_rating_changes is None:
            total_failed.append(contest_id)
            continue

        time.sleep(SLEEP_TIME)
        items = res_rating_changes['result']
        entity = get_entity_from_rating_change(items)
        print(f"[INFO] {contest_id} - {entity}")
        entity_list.append(entity)

    df = pd.DataFrame(entity_list)
    contest_data_path = PROCESSED_DATA_DIR / f'contest_statistics.csv'
    storage.save_csv(contest_data_path, df)
    print(f'Failed contests: {total_failed}')

def process_user_result():
    rated_contests = get_rated_contest_df()
    contests = rated_contests['contest_id'].tolist()
    entity_list = list()

    for contest_id in contests:
        data = api_client.get_contest_standings(
            contest_id,
            only_problems=False,
            show_unofficial=False,
            participant_types='CONTESTANT'
        )
        time.sleep(SLEEP_TIME)

        if data is None:
            print(f"[ERROR] Failed to fetch contest standings for {contest_id}")
            continue

        data = data['result']
        problems = data['problems']
        rows = data['rows']
        for row in rows:
            handle = row['party']['members'][0]['handle']
            results = row['problemResults']

            for idx in range(len(results)):
                item = results[idx]
                entity = {
                    'contest_id': contest_id,
                    'handle': handle,
                }
                entity['problem_index_num'] = idx
                entity['problem_index_raw'] = problems[idx]['index']
                entity['solved'] = 1 if item['points'] > 0 else 0
                entity_list.append(entity)
                print(entity)
    df = pd.DataFrame(entity_list)
    storage.save_csv(PROCESSED_DATA_DIR / 'contest_user_result.csv', df)
