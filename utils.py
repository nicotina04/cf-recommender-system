import pandas as pd
import os
import glob
from sklearn.model_selection import train_test_split


normalize_target_columns: set = None


def min_max_scale_series(series: pd.Series, min_value, max_value) -> pd.Series:
    return series.clip(lower=min_value, upper=max_value).apply(
        lambda x: (x - min_value) / (max_value - min_value)
    )

def min_max_scale_value(value, min_value, max_value) -> float:
    if value < min_value:
        return 0.0
    elif value > max_value:
        return 1.0
    else:
        return (value - min_value) / (max_value - min_value)

def get_normalize_target_columns():
    global normalize_target_columns

    if normalize_target_columns is not None:
        return normalize_target_columns
    
    normalize_target_columns = set()
    with open('normalize_target_cols.txt', 'rt') as f:
        for line in f:
            line = line.strip()
            normalize_target_columns.add(line)
    return normalize_target_columns
    
def load_and_merge_datasets(dir_path: str, pattern: str = 'dataset_group_*.csv') -> pd.DataFrame:
    path = os.path.join(dir_path, pattern)
    files = glob.glob(path)

    df_list = list()
    for file in files:
        df = pd.read_csv(file)
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)
    print(f'Total merged shape: {merged_df.shape}')
    return merged_df

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = ['handle', 'contest_id', 'problem_index']
    drop_cols.extend(['25th_percentile_rated', '75th_percentile_rated', 'count_unrated', 'unrated_ratio', 'median_rating_rated', 'avg_rating_rated_only', 'count_total'])

    # pattern_cols = [col for col in df.columns if col.startswith('accepted_max') or col.startswith('problem_tag_')]
    # drop_cols.extend(pattern_cols)
    mx_rating_key = 'max_rating_before_contest'
    
    filtered_df = df
    limit_rating_scaled = min_max_scale_value(2100, 0, 4000)
    # filtered_df = df[df[mx_rating_key] > limit_rating_scaled]
    filtered_df = filtered_df.drop(columns=drop_cols, axis=1)
    return filtered_df

def scale_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if 'max_rating_before_contest' in col or 'currnet_rating_before_contest' in col or 'percentile_rated' in col:
            df[col] = min_max_scale_series(df[col], 0, 4000)
        elif 'rating' in col:
            df[col] = min_max_scale_series(df[col], 800, 3500)

    return df

def split_by_contest(df: pd.DataFrame, test_ratio=0.1, valid_ratio=0.1, random_state=42) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = scale_dataframe(df)
    unique_contests = df['contest_id'].unique()
    train_contests, test_contests = train_test_split(unique_contests, test_size=test_ratio, random_state=random_state)
    train_contests, valid_contests = train_test_split(train_contests, test_size=valid_ratio / (1 - test_ratio), random_state=random_state)

    df_train = df[df['contest_id'].isin(train_contests)]
    df_valid = df[df['contest_id'].isin(valid_contests)]
    df_test = df[df['contest_id'].isin(test_contests)]

    df_train = filter_dataframe(df_train)
    df_valid = filter_dataframe(df_valid)
    df_test = filter_dataframe(df_test)

    print(f'Train shape: {df_train.shape}, Valid shape: {df_valid.shape}, Test shape: {df_test.shape}')
    return df_train, df_valid, df_test
