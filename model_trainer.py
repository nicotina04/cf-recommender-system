import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import brier_score_loss
import numpy as np


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

def read_csv(file_path) -> pd.DataFrame:
    """
    Reads a CSV file and returns a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    mx_rating_key = 'max_rating_before_contest'
    rating_limit = 4500

    filtered_df = df
    # filtered_df = df[df[mx_rating_key] > 0]

    for col in get_normalize_target_columns():
        filtered_df[col] = filtered_df[col] / rating_limit
    return filtered_df

def train_model(df: pd.DataFrame):
    x = df.drop('verdict', axis=1)
    y = df['verdict']
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    model = RandomForestClassifier(n_estimators=60, random_state=42, class_weight='balanced')
    accuracy_scores = []

    for train_index, test_index in skf.split(x, y):
        x_train, x_test = x.iloc[train_index], x.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

        model.fit(x_train, y_train)

        y_prob = model.predict_proba(x_test)[:, 1]
        calibarated_model = CalibratedClassifierCV(model, method='sigmoid', cv=5)
        calibarated_model.fit(x_train, y_train)
        y_prob_calibrated = calibarated_model.predict_proba(x_test)[:, 1]

        accuracy = model.score(x_test, y_test)
        accuracy_scores.append(accuracy)
        print(f"Fold accuracy: {accuracy:.4f}")

        print(f"Brier Score (Before): {brier_score_loss(y_test, y_prob):.4f}")
        print(f"Brier Score (After): {brier_score_loss(y_test, y_prob_calibrated):.4f}")
    print(f"Mean accuracy: {np.mean(accuracy_scores):.4f}")
    print(f"Standard deviation of accuracy: {np.std(accuracy_scores):.4f}")

if __name__ == "__main__":
    df = read_csv('./dataset/dataset_group_0.csv')
    df = filter_dataframe(df)
    train_model(df)
