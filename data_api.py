"""
Deprecated via refactoring
"""

from datetime import datetime
import time
import requests
import os
import json
import pandas as pd
from cf_data_pipeline.config import *
from cf_data_pipeline import preprocess

def get_tag_group_map():
    df = pd.read_csv(PROCESSED_DATA_DIR / 'tag_group_map.csv')
    return df.set_index('tag')['groups'].to_dict()

def get_contest_list(cache_path = f'./{RES_CACHE_BASENAME}/contest_list.json'):
    if os.path.isfile(cache_path) is True:
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)
    
    try:
        res = requests.get('https://codeforces.com/api/contest.list?gym=false', timeout=10)
        res.raise_for_status()
        data = res.json()

        if data['status'] != 'OK':
            raise ValueError('API status is not OK')
        
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    except requests.RequestException as e:
        print(f'Failed to call API {e}')
        return None
    except Exception as e:
        print(f'[ERROR]: {e}')
        return None

def get_cf_rated_list_json(cache_path=f'./{RES_CACHE_BASENAME}/user.ratedList.json'):
    if os.path.isfile(cache_path):
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)
    
    req_url = 'https://codeforces.com/api/user.ratedList?activeOnly=false&includeRetired=true'
    try:
        res = requests.get(req_url, timeout=50)
        res.raise_for_status()
        data = res.json()

        if data['status'] != 'OK':
            raise ValueError('API status is not OK')
        
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    except Exception as e:
        print(f'[EXCEPTION] {e}')

def get_cf_handles(data_path = f'./{PROCESSED_BASENAME}/handles.txt'):
    if os.path.isfile(data_path):
        with open(data_path, 'rt', encoding='utf-8') as f:
            return list(map(str.strip, f.readlines()))
    
    raw_json = get_cf_rated_list_json()
    data = raw_json['result']
    handles = list()
    for item in data:
        handles.append(item['handle'])
    text = '\n'.join(handles)
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    with open(data_path, 'w', encoding='utf-8') as f:
        f.write(text)
    return handles
    
def process_rated_contest_csv(min_date: str, max_date: str):
    contest_list_raw_data = get_contest_list()

    timestamp_min = int(datetime.strptime(min_date, "%Y-%m-%d").timestamp())
    timestamp_max = int(datetime.strptime(max_date, "%Y-%m-%d").timestamp())

    base_url = 'https://codeforces.com/api/contest.ratingChanges?contestId='
    res = list()

    if 'result' in contest_list_raw_data:
        data = contest_list_raw_data['result']
    for item in data:
        if item['phase'] == 'BEFORE':
            continue

        start_time = item['startTimeSeconds']
        if start_time < timestamp_min or start_time > timestamp_max:
            continue
        req_url = f'{base_url}{item["id"]}'
        
        try:
            rating_changes = requests.get(req_url, timeout=7).json()
        except (requests.exceptions.RequestException, json.decoder.JSONDecodeError):
            print(f"Failed to fetch contest {item['id']}")
            continue

        if rating_changes['status'] != 'OK' or len(rating_changes['result']) == 0:
            continue

        division_type = preprocess.get_division_type(item['name'])
        start_dt = datetime.fromtimestamp(start_time)
        data_entity = {
            'contest_id': item['id'],
            'division_type': division_type,
            'contest_date': start_dt
        }
        res.append(data_entity)
        time.sleep(SLEEP_TIME)

    save_file_name = f'{RATED_CONTEST_METADATA_BASENAME}.csv'
    os.makedirs(os.path.dirname(f'./{PROCESSED_BASENAME}/{save_file_name}'), exist_ok=True)
    df = pd.DataFrame(res)
    df.to_csv(f'./{PROCESSED_BASENAME}/{save_file_name}', index=False, encoding='utf-8')
    print(f'{len(df)} contests is saved.')

def retry_failed_contests(failed_ids: list[int], existing_csv_path: str, save_csv_path: str):
    existing_df = pd.read_csv(existing_csv_path)
    existing_ids = set(existing_df["contest_id"])
    ids_to_retry = [cid for cid in failed_ids if cid not in existing_ids]

    new_records = list()
    base_url = 'https://codeforces.com/api/contest.ratingChanges?contestId='

    contest_list = get_contest_list()['result']
    contest_date = dict()
    for item in contest_list:
        contest_date[item['id']] = item['startTimeSeconds']

    for cid in ids_to_retry:
        req_url = f'{base_url}{cid}'
        try:
            res = requests.get(req_url, timeout=15).json()
        except (requests.exceptions.RequestException, json.decoder.JSONDecodeError):
            print(f"Failed to fetch contest {cid}")
            continue

        if res['status'] != 'OK' or len(res['result']) == 0:
            continue

        name = res['result'][0]['contestName']

        division_type = preprocess.get_division_type(name)
        start_dt = datetime.fromtimestamp(contest_date[cid])
        data_entity = {
            'contest_id': cid,
            'division_type': division_type,
            'contest_date': start_dt
        }
        new_records.append(data_entity)
        time.sleep(SLEEP_TIME)
    
    if new_records:
        df_new = pd.DataFrame(new_records)
        df_combined = pd.concat([existing_df, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset='contest_id', inplace=True)
        df_combined.to_csv(save_csv_path, index=False)
        print(f"[Success] Added {len(df_new)} contests.")
    else:
        print("No new contests added.")

def get_rated_contest_df(cache_path = f'./{PROCESSED_BASENAME}/{RATED_CONTEST_METADATA_BASENAME}.csv') -> pd.DataFrame:
    if os.path.isfile(cache_path) is True:
        return pd.read_csv(cache_path)
    
    print("Contest metadata was not found.\nFetching and processing data from the API...")
    process_rated_contest_csv('2020-01-01', '2025-03-01')
    return pd.read_csv(cache_path)

def process_contest_problem_metadata():
    contest_df = get_rated_contest_df()
    base_url = 'https://codeforces.com/api/contest.standings?contestId='
    concat_url = 'showUnofficial=false&from=1&count=1&asManager=false'

    tag_map = get_tag_group_map()
    data_records = list()
    failed_ids = list()

    contest_df[['contest_id', 'division_type']].to_dict()

    for row in contest_df[['contest_id', 'division_type']].itertuples(index=False):
        contest_id = row.contest_id
        division_type = row.division_type

        req_url = f'{base_url}{contest_id}&{concat_url}'

        try:
            res = requests.get(req_url, timeout=10).json()
            if res['status'] != 'OK':
                print(f'Contest {contest_id} status is not OK')
                continue

            problems = res['result']['problems']

            for i in range(len(problems)):
                problem = problems[i]
                data_entity = dict()
                data_entity['contest_id'] = contest_id
                data_entity['division_type'] = division_type
                data_entity['problem_index'] = i
                data_entity['problem_rating'] = problem['rating']
                original_tags = problem['tags']
                tags = set()

                for j in original_tags:
                    if j not in tag_map:
                        tags.add('other')
                        continue

                    normalized_tags = tag_map[j].split(',')
                    for v in normalized_tags:
                        tags.add(v)

                data_entity['tags'] = list(tags)
                data_records.append(data_entity)
            time.sleep(SLEEP_TIME)
        except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as e:
            failed_ids.append(contest_id)
            print(f"Failed to fetch contest {contest_id}. Exception: {e}")
        except Exception as e:
            failed_ids.append(contest_id)
            print(f'[Exception] {e}')

    os.makedirs(f'./{PROCESSED_BASENAME}', exist_ok=True)
    with open(f'./{PROCESSED_BASENAME}/{CONTEST_PROBLEMS_BASENAME}.json', 'w', encoding='utf-8') as f:
        json.dump(data_records, f, ensure_ascii=False, indent=2)
    print(f'{len(data_records)} problems records stored.')
    print(f'Failed contest list: {failed_ids}')

def retry_failed_problems(failed_ids: list[int], existing_json_path: str, save_json_path: str):
    contest_df = get_rated_contest_df()
    d = contest_df.set_index('contest_id')['division_type'].to_dict()

    base_url = 'https://codeforces.com/api/contest.standings?contestId='
    concat_url = 'showUnofficial=false&from=1&count=1&asManager=false'

    tag_map = get_tag_group_map()

    f = open(existing_json_path, 'r', encoding='utf-8')
    existing_list = json.load(f)
    new_records = list()
    f.close()

    for item in failed_ids:
        req_url = f'{base_url}{item}&{concat_url}'
        res = requests.get(req_url, timeout=15).json()
        if res['status'] != 'OK':
            print(f'Contest {item} status is not OK')
            continue

        problems = res['result']['problems']
        for i in range(len(problems)):
            problem = problems[i]
            if 'rating' not in problem:
                print(f'{item} / {i} has no rating')
                continue
            data_entity = dict()
            data_entity['contest_id'] = item
            data_entity['division_type'] = d[item]
            data_entity['problem_index'] = i
            data_entity['problem_rating'] = problem['rating']
            original_tags = problem['tags']
            tags = set()

            for j in original_tags:
                if j not in tag_map:
                    tags.add('other')
                    continue

                normalized_tags = tag_map[j].split(',')
                for v in normalized_tags:
                    tags.add(v)

            data_entity['tags'] = list(tags)
            new_records.append(data_entity)
        
    existing_list.extend(new_records)
    with open(save_json_path, 'w', encoding='utf-8') as f:
        json.dump(existing_list, f, ensure_ascii=False, indent=2)
    print(f'{len(new_records)} problems saved')

if __name__ == '__main__':
    # process_rated_contest_csv('2020-01-01', '2025-03-01')
    # retry_failed_contests([2035,2005,2000,1982,1980,1971,1972,1907,1821,1762,1743,1701,1694,1649,1593,1436],
    #                       existing_csv_path='./processed_data/rated_contest_metadata.csv',
    #                       save_csv_path='./processed_data/rated_contest_metadata_v2.csv')
    # process_contest_problem_metadata()
    # retry_failed_problems([1728, 1561, 1393, 1330, 1319], f'./{PROCESSED_BASENAME}/{CONTEST_PROBLEMS_BASENAME}.json', f'./{PROCESSED_BASENAME}/{CONTEST_PROBLEMS_BASENAME}_v2.json')
    # get_cf_rated_list_json()
    get_cf_handles()
    pass
