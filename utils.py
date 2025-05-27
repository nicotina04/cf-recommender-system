import pandas as pd
import os
import glob
from sklearn.model_selection import train_test_split


normalize_target_columns: set = None


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
    drop_cols = ['handle', 'contest_id', 'problem_index', 'division_type']
    mx_rating_key = 'max_rating_before_contest'
    rating_limit = 4500

    filtered_df = df
    # filtered_df = df[df[mx_rating_key] > 0]
    filtered_df = filtered_df.drop(columns=drop_cols, axis=1)

    for col in get_normalize_target_columns():
        filtered_df[col] = filtered_df[col] / rating_limit
    return filtered_df

def split_by_contest(df: pd.DataFrame, test_ratio=0.1, valid_ratio=0.1, random_state=42) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    unique_contests = df['contest_id'].unique()
    train_contests, test_contests = train_test_split(unique_contests, test_size=test_ratio, random_state=random_state)
    train_contests, valid_contests = train_test_split(train_contests, test_size=valid_ratio / (1 - test_ratio), random_state=random_state)

    df_train = df[df['contest_id'].isin(train_contests)]
    df_valid = df[df['contest_id'].isin(valid_contests)]
    df_test = df[df['contest_id'].isin(test_contests)]
    print(f'Train shape: {df_train.shape}, Valid shape: {df_valid.shape}, Test shape: {df_test.shape}')
    return df_train, df_valid, df_test
