import os
import glob
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV, calibration_curve
from sklearn.metrics import brier_score_loss
import matplotlib.pyplot as plt
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
    mx_rating_key = 'max_rating_before_contest'
    rating_limit = 4500

    filtered_df = df
    # filtered_df = df[df[mx_rating_key] > 0]
    filtered_df = filtered_df.drop(columns=['problem_index', 'division_type'], axis=1)

    for col in get_normalize_target_columns():
        filtered_df[col] = filtered_df[col] / rating_limit
    return filtered_df

def train_model(df: pd.DataFrame):
    x = df.drop('verdict', axis=1)
    y = df['verdict']

    test_size = 100_000
    valid_size = 100_000
    
    x_temp, x_test, y_temp, y_test = train_test_split(x, y, test_size=test_size, stratify=y, random_state=42)
    x_train, x_valid, y_train, y_valid = train_test_split(x_temp, y_temp, test_size=valid_size, stratify=y_temp, random_state=42)

    print(f"Train shape: {x_train.shape}, Valid: {x_valid.shape}, Test: {x_test.shape}")

    model = RandomForestClassifier(n_estimators=75, random_state=42, class_weight='balanced')
    model.fit(x_train, y_train)

    # Calibration (prefit model 사용)
    calibrated_model = CalibratedClassifierCV(model, method='sigmoid', cv=5)
    calibrated_model.fit(x_valid, y_valid)

    # 성능 측정
    y_prob = model.predict_proba(x_test)[:, 1]
    y_prob_calibrated = calibrated_model.predict_proba(x_test)[:, 1]
    accuracy = model.score(x_test, y_test)

    print(f"\nAccuracy (uncalibrated): {accuracy:.4f}")
    print(f"Brier Score (Before calibration): {brier_score_loss(y_test, y_prob):.4f}")
    print(f"Brier Score (After calibration): {brier_score_loss(y_test, y_prob_calibrated):.4f}")

    # Feature importance 시각화
    plot_feature_importance(model, x.columns)

    prob_true_uncal, prob_pred_uncal = calibration_curve(y_test, y_prob, n_bins=10)
    plt.figure(figsize=(8, 6))
    plt.plot(prob_pred_uncal, prob_true_uncal, marker='o', label='Uncalibrated')
    plt.plot([0, 1], [0, 1], linestyle='--', color='gray', label='Perfect calibration')
    plt.xlabel('Predicted Probability')
    plt.ylabel('True Probability')
    plt.title('Calibration Curve (Uncalibrated)')
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_feature_importance(model, feature_names, top_n=20):
    if hasattr(model, 'feature_importances_') is False:
        print("Model does not have feature importances.")
        return

    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1][:top_n]
    selected_features = [feature_names[i] for i in indices]
    selected_importances = importances[indices]

    plt.figure(figsize=(10, 6))
    plt.barh(selected_features[::-1], selected_importances[::-1], color='b', align='center')
    plt.xlabel('Feature Importance')
    plt.title(f'Tp {top_n} Feature Importances')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    df = load_and_merge_datasets('dataset')
    df = filter_dataframe(df)
    train_model(df)
