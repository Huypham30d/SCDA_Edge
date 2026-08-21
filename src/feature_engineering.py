import numpy as np
import pandas as pd


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo các đặc trưng chu kỳ thời gian (Day_sin, Day_cos, Month_sin, Month_cos) từ cột Date/Time.

    Args:
        df: DataFrame chứa cột 'Date/Time' có kiểu dữ liệu datetime.

    Returns:
        DataFrame mới (bản sao) đã bổ sung 4 đặc trưng thời gian.

    Raises:
        ValueError: Nếu thiếu cột 'Date/Time' hoặc không có kiểu datetime hợp lệ.
    """
    if "Date/Time" not in df.columns:
        raise ValueError("DataFrame thiếu cột bắt buộc: 'Date/Time'")

    if not pd.api.types.is_datetime64_any_dtype(df["Date/Time"]):
        raise ValueError(
            "Cột 'Date/Time' phải có kiểu dữ liệu datetime (datetime64). "
            f"Kiểu hiện tại: {df['Date/Time'].dtype}"
        )

    # Tạo bản sao để tránh làm thay đổi DataFrame đầu vào
    result_df = df.copy()

    hour = result_df["Date/Time"].dt.hour
    minute = result_df["Date/Time"].dt.minute
    month = result_df["Date/Time"].dt.month

    # Tính toán đặc trưng chu kỳ ngày và tháng
    result_df["Day_sin"] = np.sin(2 * np.pi * (hour * 60 + minute) / 1440)
    result_df["Day_cos"] = np.cos(2 * np.pi * (hour * 60 + minute) / 1440)
    result_df["Month_sin"] = np.sin(2 * np.pi * month / 12)
    result_df["Month_cos"] = np.cos(2 * np.pi * month / 12)

    return result_df
