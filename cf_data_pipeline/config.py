from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RATED_CONTEST_METADATA_BASENAME = "rated_contest_metadata"
CONTEST_PROBLEMS_BASENAME = 'contest_problems_data'
PROCESSED_BASENAME = 'processed_data'
RES_CACHE_BASENAME = 'response_cache'
SLEEP_TIME = 2.1
DB_RATING_NAME = 'cf_rating_changes.db'
RATING_DB_PATH = PROCESSED_BASENAME / DB_RATING_NAME
PROCESSED_DATA_DIR = BASE_DIR / PROCESSED_BASENAME