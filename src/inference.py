from pathlib import Path
from typing import Sequence
import joblib
import pandas as pd
import xgboost as xgb


class PowerPredictor:
    """Quản lý tải mô hình XGBoost, scaler và thực hiện dự đoán công suất phát điện."""

    def __init__(
        self,
        model_path: str | Path,
        scaler_path: str | Path,
        features: Sequence[str],
    ) -> None:
        self.model_path = Path(model_path)
        self.scaler_path = Path(scaler_path)
        self.features = list(features)

        if not self.model_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file mô hình: {self.model_path}")
        if not self.scaler_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file scaler: {self.scaler_path}")
        if not self.features:
            raise ValueError("Danh sách 'features' không được để trống.")

        self.model = xgb.XGBRegressor()
        self.model.load_model(str(self.model_path))
        self.scaler = joblib.load(self.scaler_path)

    def predict(self, row: pd.Series | dict | pd.DataFrame) -> float:
        """Dự đoán công suất phát điện từ một mẫu dữ liệu.

        Args:
            row: pandas Series, dict hoặc DataFrame 1 dòng chứa các đặc trưng đầu vào.

        Returns:
            Giá trị công suất dự đoán (float).

        Raises:
            ValueError: Nếu mẫu dữ liệu thiếu bất kỳ đặc trưng nào trong self.features.
            TypeError: Nếu kiểu dữ liệu của row không được hỗ trợ.
        """
        if isinstance(row, pd.Series):
            missing = [f for f in self.features if f not in row.index]
            if missing:
                raise ValueError(f"Dữ liệu đầu vào thiếu các đặc trưng: {missing}")
            x_raw = row[self.features].to_frame().T
        elif isinstance(row, pd.DataFrame):
            missing = [f for f in self.features if f not in row.columns]
            if missing:
                raise ValueError(f"Dữ liệu đầu vào thiếu các đặc trưng: {missing}")
            x_raw = row[self.features]
        elif isinstance(row, dict):
            missing = [f for f in self.features if f not in row]
            if missing:
                raise ValueError(f"Dữ liệu đầu vào thiếu các đặc trưng: {missing}")
            x_raw = pd.DataFrame([row])[self.features]
        else:
            raise TypeError(f"Kiểu dữ liệu đầu vào không được hỗ trợ: {type(row)}")

        x_scaled = self.scaler.transform(x_raw)
        pred = self.model.predict(x_scaled)
        return float(pred[0])
