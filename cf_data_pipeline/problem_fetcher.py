import time
from cf_data_pipeline.api_client import get_contest_standings
from cf_data_pipeline.contest_fetcher import get_rated_contest_df
from cf_data_pipeline.preprocess import get_tag_group_map, normalize_tags
from cf_data_pipeline.storage import save_json, load_json
from cf_data_pipeline.config import PROCESSED_DATA_DIR, CONTEST_PROBLEMS_BASENAME, SLEEP_TIME


def process_contest_problem_metadata():
    contest_df = get_rated_contest_df()
    tag_map = get_tag_group_map()

    records = []
    failed_ids = []

    for row in contest_df[['contest_id', 'division_type']].itertuples(index=False):
        contest_id = row.contest_id
        division = row.division_type

        res = get_contest_standings(contest_id, only_problems=True)
        if res is None or res.get('status') != 'OK':
            print(f"[Problem Fetch Fail] Contest {contest_id}")
            failed_ids.append(contest_id)
            continue

        for i, problem in enumerate(res['result']['problems']):
            if 'rating' not in problem:
                print(f"[No rating] {contest_id} / {i}")
                continue

            record = {
                "contest_id": contest_id,
                "division_type": division,
                "problem_index": i,
                "problem_rating": problem["rating"],
                "tags": normalize_tags(problem.get('tags', []), tag_map)
            }
            records.append(record)

        time.sleep(SLEEP_TIME)

    save_path = PROCESSED_DATA_DIR / f'{CONTEST_PROBLEMS_BASENAME}.json'
    save_json(save_path, records)
    print(f"[Done] {len(records)} problems saved.")
    print(f"[Failed] {len(failed_ids)} contests")

def retry_failed_problems(failed_ids: list[int], existing_json_path: str, save_json_path: str):
    tag_map = get_tag_group_map()
    contest_df = get_rated_contest_df()
    division_lookup = contest_df.set_index('contest_id')['division_type'].to_dict()

    existing = load_json(existing_json_path)
    if existing is None:
        print(f"[retry_failed_problems] Failed to load: {existing_json_path}")
        existing = []

    new_records = []

    for cid in failed_ids:
        res = get_contest_standings(cid, only_problems=True)
        if res is None or res.get('status') != 'OK':
            print(f"[Retry Fail] Contest {cid}")
            continue

        for i, problem in enumerate(res['result']['problems']):
            if 'rating' not in problem:
                print(f"[Skip] Contest {cid} / Problem {i} has no rating")
                continue

            record = {
                "contest_id": cid,
                "division_type": division_lookup.get(cid, -1),
                "problem_index": i,
                "problem_rating": problem["rating"],
                "tags": normalize_tags(problem.get("tags", []), tag_map)
            }
            new_records.append(record)

        time.sleep(SLEEP_TIME)

    all_records = existing + new_records
    save_json(save_json_path, all_records)
    print(f"[Retry Success] {len(new_records)} problems added.")
