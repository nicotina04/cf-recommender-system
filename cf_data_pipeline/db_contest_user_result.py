import sqlite3
import config

db_path = config.PROCESSED_DATA_DIR / 'contest_user_result.db'


def init_db():
    if db_path.is_file():
        print(f'{db_path} already exist.')
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        '''
        entity = {
                    'contest_id': contest_id,
                    'handle': handle,
                }
                entity['problem_index_num'] = idx
                entity['problem_index_raw'] = problems[idx]['index']
                entity['verdict'] = 1 if item['points'] > 0 else 0
        '''
        conn.execute('''
            CREATE TABLE IF NOT EXISTS contest_user_result (
                handle TEXT NOT NULL,
                contest_id INTEGER NOT NULL,
                problem_index_num INTEGER NOT NULL,
                problem_index_raw TEXT NOT NULL,
                verdict INTEGER NOT NULL,
                PRIMARY KEY (contest_id, handle, problem_index_num)
            )
        ''')
        conn.commit()

def insert_user_result(record: tuple):
    with sqlite3.connect(db_path) as conn:
        conn.execute('''
            INSERT OR IGNORE INTO contest_user_result(handle, contest_id, problem_index_num, problem_index_raw, verdict)
            VALUES (?, ?, ?, ?, ?)''', record)
        conn.commit()

def insert_user_results(records: list):
    with sqlite3.connect(db_path) as conn:
        conn.executemany('''
            INSERT OR IGNORE INTO contest_user_result(handle, contest_id, problem_index_num, problem_index_raw, verdict)
            VALUES (?, ?, ?, ?, ?)''', records)
        conn.commit()
