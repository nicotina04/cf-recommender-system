import sqlite3
from typing import Optional
import storage
import config
import db_contest_user_result


def get_contest_statistics(contest_id: int) -> Optional[dict]:
    df_contest_statistics = storage.load_csv(config.CONTEST_STATISTICS_PATH)
    row = df_contest_statistics[df_contest_statistics['contest_id'] == contest_id]
    if not row.empty:
        return row.iloc[0].to_dict()
    else:
        return None


def get_problem_info(contest_id: int, problem_idx: int) -> Optional[dict]:
    pass


def create_dataset():
    db_path = db_contest_user_result.db_path
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM contest_user_result')
        while True:
            record = cursor.fetchone()
            if record is None:
                break
            # TODO: Process the record as needed
            handle = record[0]
            contest_id = record[1]
            problem_index_num = record[2]
            problem_index_raw = record[3]
            verdict = record[4]

