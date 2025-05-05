import sqlite3
import os
from cf_data_pipeline.config import DB_RATING_NAME, PROCESSED_DATA_DIR


def init_db():
    path = PROCESSED_DATA_DIR / DB_RATING_NAME

    if path.is_file():
        print(f'{DB_RATING_NAME} already exist.')
        return
    
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS rating_changes (
                handle TEXT NOT NULL,
                contest_id INTEGER NOT NULL,
                old_rating INTEGER,
                new_rating INTEGER,
                PRIMARY KEY (handle, contest_id)
            )
        ''')
        conn.commit()

def insert_rating_changes(records: list):
    path = PROCESSED_DATA_DIR / DB_RATING_NAME
    with sqlite3.connect(path) as conn:
        conn.executemany('''
            INSERT OR IGNORE INTO rating_changes(handle, contest_id, old_rating, new_rating)
            VALUES (?, ?, ?, ?)''', records)
        conn.commit()

def has_rating_data(handle: str) -> bool:
    path = PROCESSED_DATA_DIR / DB_RATING_NAME
    with sqlite3.connect(path) as conn:
        cursor = conn.execute('SELECT 1 FROM rating_changes WHERE handle = ? LIMIT 1', (handle,))
        return cursor.fetchone() is not None