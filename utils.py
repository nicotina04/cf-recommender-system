import pandas as pd
import os
import glob
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


normalize_target_columns: set = None


def min_max_scale_custom(series: pd.Series, min_value, max_value) -> pd.Series:
    return (series - min_value) / (max_value - min_value)

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
    drop_cols = ['handle', 'contest_id', 'problem_index', 'division_type', 'count_unrated', 
                'unrated_ratio']
    mx_rating_key = 'max_rating_before_contest'
    filtered_df = df
    # filtered_df = df[df[mx_rating_key] > 0]
    filtered_df = filtered_df.drop(columns=drop_cols, axis=1)
    return filtered_df

def scale_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if 'max_rating_before_contest' in col or 'currnet_rating_before_contest' in col or 'percentile_rated' in col:
            df[col] = min_max_scale_custom(df[col], -100, 4200)
        elif 'rating' in col:
            df[col] = min_max_scale_custom(df[col], 800, 3500)

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
