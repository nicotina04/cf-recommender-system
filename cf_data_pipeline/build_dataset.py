import sqlite3
import db_contest_user_result


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
            record.get('contest_id')
