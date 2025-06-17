import joblib
import numpy as np
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from xgboost import XGBClassifier


if __name__ == '__main__':
    model: XGBClassifier = joblib.load('models/XGBoost.pkl')
    n_features = model.n_features_in_

    model.get_booster().feature_names = [f'f{i}' for i in range(n_features)]
    dummy_input = np.random.rand(1, n_features).astype(np.float32)
    input_type = [('input', FloatTensorType([None, n_features]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=input_type)
    onnxmltools.utils.save_model(onnx_model, 'models/XGBoost.onnx')
