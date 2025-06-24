import os
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.calibration import CalibratedClassifierCV
import utils


def get_model(name: str, random_state: int = 42):
    if name == 'RandomForest':
        return RandomForestClassifier(n_estimators=80, random_state=random_state, class_weight='balanced')
    elif name == 'LogisticRegression':
        return LogisticRegression(max_iter=1500, random_state=random_state, class_weight='balanced')
    elif name == 'XGBoost':
        return XGBClassifier(eval_metric='logloss', random_state=random_state, scale_pos_weight=1)
    elif name == 'LightGBM':
        return LGBMClassifier(random_state=random_state, class_weight='balanced')
    elif name == 'CatBoost':        
        return CatBoostClassifier(random_state=random_state, verbose=0, class_weights=[1, 10])
    else:
        raise ValueError(f"Unknown model name: {name}")

def train_model(model_name: str, x_train, y_train, x_valid=None, y_valid=None, use_calibration=False):
    model = get_model(model_name)
    model.fit(x_train, y_train)

    if use_calibration and x_valid is not None and y_valid is not None:
        try:
            frozen_model = FrozenEstimator(model)
            model = CalibratedClassifierCV(estimator=frozen_model, method='isotonic', cv='prefit', ensemble=False)
            model.fit(x_valid, y_valid)
        except Exception as e:
            print(f"Calibration failed: {e}")
    return model

def train_and_save_all_models(df: pd.DataFrame, save_dir='models'):
    os.makedirs(save_dir, exist_ok=True)
    trainset, validset, testset = utils.split_by_contest(df, test_ratio=0.1, valid_ratio=0.1)
    x_train, y_train = trainset.drop(columns=['verdict']), trainset['verdict']
    x_valid, y_valid = validset.drop(columns=['verdict']), validset['verdict']
    # x_test, y_test = testset.drop(columns=['verdict']), testset['verdict']

    with open('models/feature_names.txt', 'w') as f:
        for col in x_train.columns:
            f.write(f"{col}\n")

    models = ['RandomForest', 'LogisticRegression', 'XGBoost', 'LightGBM', 'CatBoost']
    # models = ['LogisticRegression']
    for model_name in models:
        print(f"Training {model_name}...")
        model = train_model(model_name, x_train, y_train, x_valid, y_valid, True)
        model_path = os.path.join(save_dir, f"{model_name}.pkl")
        with open(model_path, 'wb') as f:
            import joblib
            joblib.dump(model, f)
        print(f"Model {model_name} saved to {model_path}")

if __name__ == "__main__":
    dataset = utils.load_and_merge_datasets('dataset')
    train_and_save_all_models(dataset)
