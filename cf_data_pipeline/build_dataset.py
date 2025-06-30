import sqlite3
from collections import defaultdict
from typing import Optional
import dataclasses
import pandas as pd
import storage
import preprocess
import config
import db_rating_change
import rating_change_fetcher
import db_contest_user_result
import problem_fetcher


@dataclasses.dataclass
class ContestProblemData:
    contest_id: int
    problem_index: int
    division_type: int
    problem_rating: int
    tags: list[str]

@dataclasses.dataclass
class ContestStatisticsData:
    contest_id: int
    avg_rating_all: int
    avg_rating_rated_only: int
    median_rating_rated: int
    percentile_rated_25th: int
    percentile_rated_75th: int
    std_rating_rated: float
    count_total: int
    count_unrated: int
    unrated_ratio: float

df_contest_statistics : pd.DataFrame = None
contest_problem_data : defaultdict[tuple, ContestProblemData] = None
problem_tag_list : list[str] = None
handle_rating_cache: dict[tuple, dict[str, int]] = None
max_rating_handle_cache = dict()
recent_detla_avg_cache = dict()
ac_problems_by_handle: dict[str, Optional[list]] = dict()
contest_id_failed_fetch = set()
handle_ac_submission_cache: Optional[defaultdict] = None


def load_contest_statistics():
    global df_contest_statistics
    if df_contest_statistics is None:
        df_contest_statistics = storage.load_csv(config.CONTEST_STATISTICS_PATH)

def load_and_init_contest_problem_data():
    global contest_problem_data

    if contest_problem_data is not None:
        return
    
    contest_problem_data = defaultdict(ContestProblemData)
    raw_data = storage.load_json(config.CONTEST_PROBLEMS_DATA_PATH)
    for item in raw_data:
        contest_id = item['contest_id']
        division_type = item['division_type']
        problem_index = item['problem_index_num']
        problem_rating = item['problem_rating']
        problem_tags = item['tags']

        record = ContestProblemData(
            contest_id=contest_id,
            problem_index=problem_index,
            division_type=division_type,
            problem_rating=problem_rating,
            tags=problem_tags
        )

        contest_problem_data[(contest_id, problem_index)] = record

def get_contest_statistics(contest_id: int) -> Optional[ContestStatisticsData]:
    global df_contest_statistics
    load_contest_statistics()
    row = df_contest_statistics[df_contest_statistics['contest_id'] == contest_id]
    if not row.empty:
        data = row.iloc[0]
        result = ContestStatisticsData(
            contest_id=contest_id,
            avg_rating_all=int(data['avg_rating_all']),
            avg_rating_rated_only=int(data['avg_rating_rated_only']),
            median_rating_rated=int(data['median_rating_rated']),
            percentile_rated_25th=int(data['25th_percentile_rated']),
            percentile_rated_75th=int(data['75th_percentile_rated']),
            std_rating_rated=data['std_rating_rated'],
            count_total=int(data['count_total']),
            count_unrated=int(data['count_unrated']),
            unrated_ratio=data['unrated_ratio']
        )
        return result
    else:
        return None

def get_problem_info(contest_id: int, problem_idx: int) -> Optional[ContestProblemData]:
    global contest_problem_data
    global contest_id_failed_fetch

    ret = contest_problem_data.get((contest_id, problem_idx), None)

    if ret is None:
        if contest_id in contest_id_failed_fetch:
            return None

        path = config.CONTEST_PROBLEMS_DATA_PATH
        failed = problem_fetcher.fetch_problems([contest_id], path, path)
        contest_id_failed_fetch |= set(failed)

        load_and_init_contest_problem_data()
        ret = contest_problem_data.get((contest_id, problem_idx), None)

    return ret

def get_ac_problems_by_handle(handle: str):
    global handle_ac_submission_cache
    if handle in handle_ac_submission_cache:
        return handle_ac_submission_cache[handle]
    else:
        return None

def get_max_ac_rating_tags_before_contest(handle: str, contest_id: int) -> Optional[defaultdict]:
    ac_list = get_ac_problems_by_handle(handle)
    if ac_list is None:
        return None
        
    ret = defaultdict(int)

    for item in ac_list:
        id, idx = item
        
        if id >= contest_id:
            break

        record = get_problem_info(id, idx)
        if record is None:
            continue

        for tag in record.tags:
            ret[tag] = max(ret[tag], record.problem_rating)
    return ret

def get_max_rating_before_contest(handle: str, contest_id: int) -> int:
    global max_rating_handle_cache
    if (handle, contest_id) in max_rating_handle_cache:
        return max_rating_handle_cache[(handle, contest_id)]

    rating = db_rating_change.get_max_rating_before_contest(handle, contest_id)
    max_rating_handle_cache[(handle, contest_id)] = rating
    return rating

def get_recent_delta_avg(handle: str, contest_id: int) -> int:
    global recent_detla_avg_cache
    if (handle, contest_id) in recent_detla_avg_cache:
        return recent_detla_avg_cache[(handle, contest_id)]

    delta_avg = db_rating_change.get_recent_delta_avg(handle, contest_id)
    recent_detla_avg_cache[(handle, contest_id)] = delta_avg
    return delta_avg

def get_dataset_record(sql_record) -> dict:
    handle = sql_record[0]
    contest_id = sql_record[1]
    problem_index_num = sql_record[2]
    # problem_index_raw = sql_record[3]
    verdict = sql_record[4]
    problem_data = get_problem_info(contest_id, problem_index_num)
    contest_data = get_contest_statistics(contest_id)

    if problem_data is None:
        print(f'[WARNING] contest data {contest_id} is not found')
        return None
    
    record = dict()
    record['contest_id'] = contest_id
    record['division_type'] = problem_data.division_type
    record['problem_index'] = problem_index_num
    record['problem_rating'] = problem_data.problem_rating
    record['handle'] = handle

    current_rating = db_rating_change.get_contest_rating_entity(handle, contest_id)
    # fetch rating cheanges
    if current_rating is None:
        if not rating_change_fetcher.fetch_and_store(handle):
            print(f'[WARNING] before rating for {handle} {contest_id} is not found')
            return None
        else:
            current_rating = db_rating_change.get_contest_rating_entity(handle, contest_id)

    if current_rating is None:
        return None
    record['current_rating_before_contest'] = current_rating.old_rating
    record['max_rating_before_contest'] = get_max_rating_before_contest(handle, contest_id)
    record['recent_delta_avg'] = get_recent_delta_avg(handle, contest_id)
    record['avg_rating_rated_only'] = contest_data.avg_rating_rated_only
    record['median_rating_rated'] = contest_data.median_rating_rated
    record['25th_percentile_rated'] = contest_data.percentile_rated_25th
    record['75th_percentile_rated'] = contest_data.percentile_rated_75th
    # record['std_rating_rated'] = contest_data.std_rating_rated
    record['count_total'] = contest_data.count_total
    record['count_unrated'] = contest_data.count_unrated
    record['unrated_ratio'] = contest_data.unrated_ratio

    rating_max_tag = get_max_ac_rating_tags_before_contest(handle, contest_id)

    global problem_tag_list

    for tag in problem_tag_list:
        key_name = f'accepted_max_rating_{tag}'
        record[key_name] = 0
        if rating_max_tag is not None and tag in rating_max_tag:
            record[key_name] = rating_max_tag[tag]

    for tag in problem_tag_list:
        key_name = f'problem_tag_{tag}'
        if tag in problem_data.tags:
            record[key_name] = 1
        else:
            record[key_name] = 0

    record['verdict'] = verdict
    return record

def init_dataset_builder():
    global problem_tag_list, handle_ac_submission_cache
    problem_tag_list = preprocess.get_problem_tag_list()
    tag_index = dict()

    for i in range(len(problem_tag_list)):
        tag_index[problem_tag_list[i]] = i

    load_and_init_contest_problem_data()
    load_contest_statistics()
    handle_ac_submission_cache = db_contest_user_result.get_all_ac_submission()

def create_dataset(normalize: bool, chunk_idx: int = 0, random_seed: int = 42):
    init_dataset_builder()
    db_path = db_contest_user_result.db_path

    df_handles = storage.load_csv(config.SAMPLED_HANDLE_PATH)
    handles = df_handles['handle'].tolist()

    import random
    random.seed(random_seed)
    random.shuffle(handles)

    # split list into 30 chunks
    chunk_size = len(handles) // 30
    handle_groups = [handles[i:i + chunk_size] for i in range(0, len(handles), chunk_size)]
    print(f"[INFO] Total {len(handle_groups)} groups of handles, each group has {chunk_size} handles.")

    global handle_rating_cache
    global recent_detla_avg_cache
    global max_rating_handle_cache
    global ac_problems_by_handle

    for i in range(chunk_idx, len(handle_groups)):
        ac_problems_by_handle.clear()

        group = handle_groups[i]
        group_records = list()

        for handle in group:
            max_rating_handle_cache.clear()
            recent_detla_avg_cache.clear()

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM contest_user_result WHERE handle = ?', (handle,))
                records = cursor.fetchall()

                if handle_rating_cache is not None:
                    handle_rating_cache.clear()
                    
                for record in records:
                    group_record = get_dataset_record(record)
                    if group_record is not None:
                        group_records.append(group_record)
                print(f'[INFO] {handle} {len(records)} processed.')

        dataset_name = f'dataset_group_{i}.csv'
        dataset_path = config.DATASET_DIR / dataset_name
        df = pd.DataFrame(group_records)
        storage.save_csv(dataset_path, df)
        print(f"[INFO] Dataset {i} saved to {dataset_path} with {len(group_records)} records.")

def insert_current_rating_before_contest():
    import glob
    import os 

    dataset_files = glob.glob(os.path.join(config.DATASET_DIR, 'dataset_group_*.csv'))
    for file in dataset_files:
        print(f'Processing {file}...')
        df = pd.read_csv(file)

        def get_old_rating(row):
            ent = db_rating_change.get_contest_rating_entity(row['handle'], int(row['contest_id']))
            return ent.old_rating if ent is not None else None

        df['current_rating_before_contest'] = df.apply(get_old_rating, axis=1)
        
        # Save the updated DataFrame back to the file
        df.to_csv(file, index=False)
        print(f'Updated {file} with current ratings.')

def fix_and_update_current_rating_before_contest():
    import glob
    import os
    import pandas as pd

    dataset_files = glob.glob(os.path.join(config.DATASET_DIR, 'dataset_group_*.csv'))

    for file in dataset_files:
        print(f'Processing {file}...')
        df = pd.read_csv(file)

        if 'currnet_rating_before_contest' in df.columns:
            df.drop(columns=['currnet_rating_before_contest'], inplace=True)

        def get_old_rating(row):
            ent = db_rating_change.get_contest_rating_entity(row['handle'], int(row['contest_id']))
            return ent.old_rating if ent is not None else None

        df['current_rating_before_contest'] = df.apply(get_old_rating, axis=1)

        before_count = len(df)
        df = df.dropna(subset=['current_rating_before_contest'])
        after_count = len(df)

        df['current_rating_before_contest'] = df['current_rating_before_contest'].astype(int)
        print(f' - {before_count - after_count} rows dropped due to missing rating')

        df.to_csv(file, index=False)
        print(f'Updated {file} with current ratings.')

def insert_problem_rating():
    import glob
    import os
    import pandas as pd

    dataset_files = glob.glob(os.path.join(config.DATASET_DIR, 'dataset_group_*.csv'))

    for file in dataset_files:
        print(f'Processing {file}...')
        df = pd.read_csv(file)

        def get_rating(row):
            contest_id = int(row['contest_id'])
            problem_index = int(row['problem_index'])
            problem_data = get_problem_info(contest_id, problem_index)
            if problem_data is None:
                print(f'[WARNING] Problem data for contest {contest_id}, index {problem_index} not found.')
                return None
            return problem_data.problem_rating

        df['problem_rating'] = df.apply(get_rating, axis=1)
        df.dropna(subset=['problem_rating'], inplace=True)
        df['problem_rating'] = df['problem_rating'].astype(int)
        
        cols = list(df.columns)
        cols.remove('problem_rating')
        cols.insert(3, 'problem_rating')
        df = df[cols]

        df.to_csv(file, index=False)

if __name__ == "__main__":
    load_and_init_contest_problem_data()
    insert_problem_rating()
