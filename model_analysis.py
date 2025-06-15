import os
import joblib
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, accuracy_score
import matplotlib.pyplot as plt
import numpy as np
import utils


normalize_target_columns: set = None

def expected_calibration_error(y_true, y_prob, n_bins=10):
    bins = np.linspace(0, 1, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    ece = 0
    for i in range(n_bins):
        bin_mask = binids == i
        if np.any(bin_mask):
            acc = np.mean(y_true[bin_mask])
            conf = np.mean(y_prob[bin_mask])
            ece += np.sum(bin_mask) * np.abs(acc - conf)
    return ece / len(y_true)

def adaptive_calibration_error(y_true, y_prob, n_bins=10):
    y_true = np.asarray(y_true)
    y_prob = np.asarray(y_prob)

    sorted_indices = np.argsort(y_prob)
    y_true_sorted = y_true[sorted_indices]
    y_prob_sorted = y_prob[sorted_indices]
    bin_size = int(np.ceil(len(y_true) / n_bins))
    errors = []
    for i in range(n_bins):
        start = i * bin_size
        end = min((i + 1) * bin_size, len(y_true))
        if end > start:
            acc = np.mean(y_true_sorted[start:end])
            conf = np.mean(y_prob_sorted[start:end])
            errors.append(abs(acc - conf))
    return np.mean(errors)

def evaluate_model(model_name: str, x_test, y_test, model_dir: str = 'models'):
    model_path = os.path.join(model_dir, f'{model_name}.pkl')
    model = joblib.load(model_path)
    y_prob = model.predict_proba(x_test)[:, 1]
    y_pred = model.predict(x_test)

    acc = accuracy_score(y_test, y_pred)
    brier_score = brier_score_loss(y_test, y_prob)
    ece = expected_calibration_error(y_test, y_prob)
    ace = adaptive_calibration_error(y_test, y_prob)

    print(f'Model name: {model_name} - Accuracy: {acc:.4f}, Brier Score: {brier_score:.4f} - ECE: {ece:.4f} - ACE: {ace:.4f}')

    prob_true, prob_pred = calibration_curve(y_test, y_prob, n_bins=10)
    return prob_pred, prob_true

def analyze_all_models(df: pd.DataFrame, model_names, model_dir: str = 'models', title_suffix: str = ''):
    _, _, testset = utils.split_by_contest(df, test_ratio=0.1, valid_ratio=0.1)
    x_test, y_test = testset.drop(columns=['verdict']), testset['verdict']

    plt.figure(figsize=(8, 6))
    for name in model_names:
        prob_pred, prob_true = evaluate_model(name, x_test, y_test, model_dir)
        plt.plot(prob_pred, prob_true, marker='o', label=name)
    plt.plot([0, 1], [0, 1], linestyle='--', color='gray', label='Perfect calibration')
    plt.xlabel('Predicted Probability')
    plt.ylabel('True Probability')
    title = f'Calibration Curve for All Models'
    if title_suffix:
        title += f' - {title_suffix}'
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.legend()
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
    df = utils.load_and_merge_datasets('dataset')
    model_list = ['RandomForest', 'LogisticRegression', 'XGBoost', 'LightGBM', 'CatBoost']
    analyze_all_models(df, model_list)
