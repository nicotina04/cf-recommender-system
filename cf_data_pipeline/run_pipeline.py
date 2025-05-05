from user_fetcher import get_cf_handles
from cf_data_pipeline.contest_fetcher import process_rated_contest_csv, get_rated_contest_df
from cf_data_pipeline.problem_fetcher import process_contest_problem_metadata, retry_failed_problems
from cf_data_pipeline.user_selector import stratified_sample_by_rating
from cf_data_pipeline.extract_participant_handles import extract_handles_from_contests
from cf_data_pipeline import storage
from config import PROCESSED_DATA_DIR, SLEEP_TIME

def run_all():
    # print("Step 1: Fetching handles...")
    # get_cf_handles()

    # print("Step 2: Fetching contest metadata...")
    # process_rated_contest_csv("2020-01-01", "2025-03-01")

    # print("Step 3: Fetching contest problem metadata...")
    # process_contest_problem_metadata()

    # Optional: retry logic (if you want to)
    # retry_failed_problems([1234, 5678], '...', '...')

    # users_df = storage.load_csv(f'./{PROCESSED_BASENAME}/selected_users.csv')
    # rating_buckets = {
    #     "newbie": (0, 1199),
    #     "pupil": (1200, 1399),
    #     "specialist": (1400, 1599),
    #     "expert": (1600, 1899),
    #     "candidate_master": (1900, 2099),
    #     "master_plus": (2100, 5000)
    # }

    # sampled_df = stratified_sample_by_rating(
    #     df=users_df,
    #     rating_column='max_rating',
    #     buckets=rating_buckets,
    #     total_target=80000
    # )

    # save_path = f'./{PROCESSED_BASENAME}/sampled_handles.csv'
    # storage.save_csv(save_path, sampled_df)
    # print(f'Sampled {len(sampled_df)} users saved to {save_path}')
    # df = get_rated_contest_df()
    # extract_handles_from_contests(df['contest_id'].tolist())

    """Fetch and store user's contest rating"""
    import db_rating_change
    db_rating_change.init_db()

    import rating_change_fetcher
    import time
    
    handle_path = PROCESSED_DATA_DIR / 'sampled_handles.csv'
    handles = storage.load_csv(handle_path)['handle'].tolist()
    failed_handles = list()
    success_count = 0
    for handle in handles:
        if not rating_change_fetcher.fetch_and_store(handle):
            failed_handles.append(handle)
        else:
            success_count += 1
            time.sleep(SLEEP_TIME)
    for handle in failed_handles:
        if rating_change_fetcher.fetch_and_store(handle):
            success_count += 1
            time.sleep(SLEEP_TIME)
    print(f'Total {success_count}/{len(handles)} handles data stored.')

    pass

if __name__ == "__main__":
    run_all()
