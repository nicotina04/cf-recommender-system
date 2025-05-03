import os
import json
import pandas as pd
from collections import defaultdict


def extract_users_from_rating_changes(rating_dir: str) -> pd.DataFrame:
    max_ratings = defaultdict(int)

    for fname in os.listdir(rating_dir):
        if not fname.endswith('.json'):
            continue
        path = os.path.join(rating_dir, fname)
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if data.get("status") != "OK":
                    continue
                for entry in data["result"]:
                    handle = entry["handle"]
                    new_rating = entry["newRating"]
                    max_ratings[handle] = max(max_ratings[handle], new_rating)
            except json.JSONDecodeError:
                print(f"[WARN] Failed to parse {fname}")
                continue

    df = pd.DataFrame([
        {"handle": h, "max_rating": r}
        for h, r in max_ratings.items()
    ])
    return df

def sample_users_by_rating(
    df: pd.DataFrame,
    buckets: dict[str, tuple[int, int]],
    per_bucket: int
) -> pd.DataFrame:
    samples = []
    for bucket_name, (lo, hi) in buckets.items():
        sub = df[(df["max_rating"] >= lo) & (df["max_rating"] <= hi)]
        sampled = sub.sample(n=min(per_bucket, len(sub)), random_state=42)
        samples.append(sampled)
        print(f"[{bucket_name}] {len(sampled)} users sampled (from {len(sub)} available)")

    return pd.concat(samples, ignore_index=True)

def stratified_sample_by_rating(
    df: pd.DataFrame,
    rating_column: str,
    buckets: dict[str, tuple[int, int]],
    total_target: int,
    random_state: int = 981
) -> pd.DataFrame:
    import math

    per_bucket_target = total_target // len(buckets)
    bucket_infos = []
    remaining_budget = total_target

    for name, (lo, hi) in buckets.items():
        sub_df = df[(df[rating_column] >= lo) & (df[rating_column] <= hi)].copy()
        count_available = len(sub_df)
        to_sample = min(per_bucket_target, count_available)

        bucket_infos.append({
            "name": name,
            "sub_df": sub_df,
            "available": count_available,
            "target": to_sample
        })
        remaining_budget -= to_sample

    expandable = [b for b in bucket_infos if b["available"] > b["target"]]
    total_expandable = sum(b["available"] - b["target"] for b in expandable)

    for b in expandable:
        possible_extra = b["available"] - b["target"]
        if total_expandable == 0:
            break
        share_ratio = possible_extra / total_expandable
        extra_alloc = math.floor(share_ratio * remaining_budget)
        b["target"] += min(extra_alloc, possible_extra)

    actual_total = sum(b["target"] for b in bucket_infos)
    if actual_total < total_target:
        diff = total_target - actual_total
        for b in sorted(expandable, key=lambda x: x["available"] - x["target"], reverse=True):
            give = min(diff, b["available"] - b["target"])
            b["target"] += give
            diff -= give
            if diff == 0:
                break

    result_dfs = list()
    for b in bucket_infos:
        sampled = b["sub_df"].sample(n=b["target"], random_state=random_state)
        result_dfs.append(sampled)
        print(f"{b['name']}: {b['target']} / {b['available']}")

    final_df = pd.concat(result_dfs, ignore_index=True)
    print(f"Final total sampled: {len(final_df)} users (target: {total_target})")
    return final_df

def save_selected_users(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, encoding='utf-8')