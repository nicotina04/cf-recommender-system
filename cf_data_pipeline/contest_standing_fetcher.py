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

def process_contest_standings():
    rated_contests = get_rated_contest_df()
    contests = rated_contests['contest_id'].tolist()
    entity_list = list()

    for contest_id in contests:
        res_rating_changes = api_client.get_contest_rating_changes(contest_id)
        time.sleep(SLEEP_TIME)
        items = res_rating_changes['result']

        ratings = list()
        total_user_count = len(items)
        unrated_count = 0
        rating_sum_all = 0
        rating_sum_rated = 0 

        for item in items:
            old_rating = item['oldRating']
            rating_sum_all += old_rating
            if old_rating == 0:
                unrated_count += 1
            else:
                ratings.append(old_rating)
                rating_sum_rated += old_rating
    
        avg_rating_all = rating_sum_all // total_user_count
        avg_rated_only = rating_sum_all // (total_user_count - unrated_count)
        ratings.sort()
        sum_of_variance = 0
        for rating in ratings:
            if rating is not None:
                sum_of_variance += (rating - avg_rated_only) ** 2
        std = (sum_of_variance / max((total_user_count - unrated_count), 1)) ** 0.5
        
        entity = {
            'contest_id': contest_id,
            'avg_rating_all': avg_rating_all,
            'avg_rating_rated_only': avg_rated_only,
            'median_rating_rated': ratings[len(ratings) // 2] if len(ratings) > 0 else 0,
            'std_rating_rated': std,
            'count_total': total_user_count,
            'count_unrated': unrated_count,
            'unrated_ratio': unrated_count / total_user_count,
        }
        print(f"[INFO] {contest_id} - {entity}")
        entity_list.append(entity)

    df = pd.DataFrame(entity_list)
    contest_data_path = PROCESSED_DATA_DIR / f'contest_statistics.csv'
    storage.save_csv(contest_data_path, df)

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
