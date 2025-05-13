import sqlite3
from typing import Optional
from dataclasses import dataclass
from config import DB_RATING_NAME, PROCESSED_DATA_DIR


db_path = PROCESSED_DATA_DIR / DB_RATING_NAME

@dataclass
class RatingChange:
    old_rating: int
    new_rating: int

def init_db():
    if db_path.is_file():
        print(f'{DB_RATING_NAME} already exist.')
        return
    
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
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
    with sqlite3.connect(db_path) as conn:
        conn.executemany('''
            INSERT OR IGNORE INTO rating_changes(handle, contest_id, old_rating, new_rating)
            VALUES (?, ?, ?, ?)''', records)
        conn.commit()

def has_rating_data(handle: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute('SELECT 1 FROM rating_changes WHERE handle = ? LIMIT 1', (handle,))
        return cursor.fetchone() is not None

def get_contest_rating_entity(handle: str, contest_id: int) -> Optional[RatingChange]:
    with sqlite3.connect(db_path) as conn:
        qry = '''
            SELECT old_rating, new_rating FROM rating_changes WHERE handle = ? AND contest_id = ?
        '''
        cursor = conn.execute(qry, (handle, contest_id))
        row = cursor.fetchone()
        if row is not None:
            return RatingChange(
                old_rating=row[0],
                new_rating=row[1]
            )
    return None

def is_provisional_handle(handle: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute('''
            SELECT COUNT(*) FROM rating_changes WHERE handle = ?
        ''', (handle,))
        count = cursor.fetchone()[0]
        return count < 5
    
def get_recent_delta_avg(handle: str, pivot_contest_id: int, count: int = 5) -> int:
    with sqlite3.connect(db_path) as conn:
        delta_qry = '''
            SELECT
            AVG(new_rating - old_rating)
            FROM rating_changes
            WHERE handle = ? AND contest_id < ?
            ORDER BY contest_id DESC
            LIMIT ?
        '''
        cursor = conn.execute(delta_qry, (handle, pivot_contest_id, count))
        row = cursor.fetchone()
        delta_avg = row[0] if row[0] is not None else 0
        return int(delta_avg)
    
def get_max_rating_before_contest(handle: str, contest_id: int) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute('''
            SELECT MAX(new_rating), MAX(old_rating) FROM rating_changes WHERE handle = ? AND contest_id < ?
        ''', (handle, contest_id))
        row = cursor.fetchone()
        rating = row[0] if row[0] is not None else 0
        if row[1] is not None and row[1] > rating:
            rating = row[1]
        return rating
