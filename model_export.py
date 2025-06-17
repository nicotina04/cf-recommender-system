import joblib
import onnxmltools
import numpy as np


if __name__ == '__main__':
    model = joblib.load('models/XGBoost.pkl')
    dummy_input = np.random.rand(1, model.n_features_in_)
    types = [('input', onnxmltools.convert.common.data_types.FloatTensorType([None, model.n_features_in_]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=types)
    onnxmltools.utils.save_model(onnx_model, 'models/XGBoost.onnx')
