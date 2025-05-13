import sqlite3
from collections import defaultdict
from typing import Optional
import dataclasses
import pandas as pd
import storage
import preprocess
import config
import db_rating_change
import db_contest_user_result


df_contest_statistics : pd.DataFrame = None
contest_problem_data : defaultdict = None

@dataclasses.dataclass
class ContestProblemData:
    contest_id: int
    problem_index: str
    division_type: int
    problem_rating: float
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
    load_and_init_contest_problem_data()
    return contest_problem_data.get((contest_id, problem_idx), None)

# ㅅㅂ 이거 최적화해야되는데
def get_max_ac_rating_tags_before_contest(handle: str, contest_id: int) -> Optional[defaultdict]:
    ac_list = db_contest_user_result.get_accepted_problems_before_contest(handle, contest_id)
    if ac_list is None:
        return None
    
    ret = defaultdict(int)
    
    for item in ac_list:
        id, idx = item
        record = get_problem_info(id, idx)
        if record is None:
            continue

        for tag in record.tags:
            ret[tag] = max(ret[tag], record.problem_rating)
    return ret

def get_dataset_record(sql_record, normalize: bool) -> dict:
    handle = sql_record[0]
    contest_id = sql_record[1]
    problem_index_num = sql_record[2]
    # problem_index_raw = sql_record[3]
    verdict = sql_record[4]
    problem_data = get_problem_info(contest_id, problem_index_num)
    contest_data = get_contest_statistics(contest_id)

    record = dict()
    record['max_rating_before_contest'] = db_rating_change.get_max_rating_before_contest(handle, contest_id)
    record['recent_delta_avg'] = db_rating_change.get_recent_delta_avg(handle, contest_id)
    record['division_type'] = problem_data.division_type
    record['avg_rating_rated_only'] = contest_data.avg_rating_rated_only
    record['median_rating_rated'] = contest_data.median_rating_rated
    record['percentile_rated_25th'] = contest_data.percentile_rated_25th
    record['percentile_rated_75th'] = contest_data.percentile_rated_75th
    # record['std_rating_rated'] = contest_data.std_rating_rated
    record['count_total'] = contest_data.count_total
    record['count_unrated'] = contest_data.count_unrated
    record['unrated_ratio'] = contest_data.unrated_ratio
    record['problem_index'] = problem_index_num

    rating_max_tag = get_max_ac_rating_tags_before_contest(handle, contest_id)

    tag_list = preprocess.get_problem_tag_list()
    rating_pivot = 4500
    problem_maximum = 3500

    for tag in tag_list:
        key_name = f'accepted_max_rating_{tag}'
        record[key_name] = 0
        if rating_max_tag is not None and tag in rating_max_tag:
            record[key_name] = rating_max_tag[tag]
            if normalize:
                record[key_name] /= problem_maximum
                record[key_name] = round(record[key_name], 3)

    for tag in tag_list:
        key_name = f'problem_tag_{tag}'
        if tag in problem_data.tags:
            record[key_name] = 1
        else:
            record[key_name] = 0

    record['verdict'] = verdict

    if normalize:
        record['max_rating_before_contest'] /= rating_pivot
        record['max_rating_before_contest'] = round(record['max_rating_before_contest'], 3)
        record['recent_delta_avg'] /= rating_pivot
        record['recent_delta_avg'] = round(record['recent_delta_avg'], 3)
        record['avg_rating_rated_only'] /= rating_pivot
        record['avg_rating_rated_only'] = round(record['avg_rating_rated_only'], 3)
        record['median_rating_rated'] /= rating_pivot
        record['median_rating_rated'] = round(record['median_rating_rated'], 3)
        record['percentile_rated_25th'] /= rating_pivot
        record['percentile_rated_25th'] = round(record['percentile_rated_25th'], 3)
        record['percentile_rated_75th'] /= rating_pivot
        record['percentile_rated_75th'] = round(record['percentile_rated_75th'], 3)
    return record

def create_dataset(normalize: bool):
    db_path = db_contest_user_result.db_path
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM contest_user_result')

        record = cursor.fetchone()
        while record is not None:
            dataset_record = get_dataset_record(record, normalize)
            print(dataset_record)
            record = cursor.fetchone()

