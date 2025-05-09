from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RATED_CONTEST_METADATA_BASENAME = "rated_contest_metadata"
CONTEST_PROBLEMS_BASENAME = 'contest_problems_data'
PROCESSED_BASENAME = 'processed_data'
RES_CACHE_BASENAME = 'response_cache'
SLEEP_TIME = 2.1
DB_RATING_NAME = 'cf_rating_changes.db'
DATA_PIPELINE_DIR = BASE_DIR / 'cf_data_pipeline'
PROCESSED_DATA_DIR = DATA_PIPELINE_DIR / PROCESSED_BASENAME
RES_CACHE_DATA_DIR = DATA_PIPELINE_DIR / RES_CACHE_BASENAME
RATING_DB_PATH = PROCESSED_DATA_DIR / DB_RATING_NAME
