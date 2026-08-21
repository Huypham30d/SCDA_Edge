import numpy as np
import pandas as pd
import pytest
from src.feature_engineering import add_time_features


def test_add_time_features_creates_all_columns():
    """Kiểm tra tạo đủ 4 cột đặc trưng thời gian."""
    df = pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(["2023-01-01 00:00", "2023-06-15 12:30"]),
            "Value": [10.0, 20.0],
        }
    )
    result = add_time_features(df)

    expected_cols = ["Day_sin", "Day_cos", "Month_sin", "Month_cos"]
    for col in expected_cols:
        assert col in result.columns


def test_add_time_features_at_midnight():
    """Kiểm tra giá trị đặc trưng tại 00:00: Day_sin = 0, Day_cos = 1."""
    # 00:00 ngày 12 tháng 12 -> month = 12: Month_sin = sin(2*pi*12/12) = 0, Month_cos = cos(2*pi*12/12) = 1
    df = pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(["2023-12-12 00:00"]),
        }
    )
    result = add_time_features(df)

    assert np.isclose(result["Day_sin"].iloc[0], 0.0, atol=1e-7)
    assert np.isclose(result["Day_cos"].iloc[0], 1.0, atol=1e-7)
    assert np.isclose(result["Month_sin"].iloc[0], 0.0, atol=1e-7)
    assert np.isclose(result["Month_cos"].iloc[0], 1.0, atol=1e-7)


def test_add_time_features_at_specific_times():
    """Kiểm tra các mốc thời gian khác trong ngày và trong năm."""
    # 06:00 -> 360 phút / 1440 = 1/4 vòng -> sin=1, cos=0
    # 12:00 -> 720 phút / 1440 = 1/2 vòng -> sin=0, cos=-1
    # 18:00 -> 1080 phút / 1440 = 3/4 vòng -> sin=-1, cos=0
    df = pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(
                [
                    "2023-01-01 06:00",
                    "2023-01-01 12:00",
                    "2023-01-01 18:00",
                ]
            )
        }
    )
    result = add_time_features(df)

    # 06:00
    assert np.isclose(result["Day_sin"].iloc[0], 1.0, atol=1e-7)
    assert np.isclose(result["Day_cos"].iloc[0], 0.0, atol=1e-7)

    # 12:00
    assert np.isclose(result["Day_sin"].iloc[1], 0.0, atol=1e-7)
    assert np.isclose(result["Day_cos"].iloc[1], -1.0, atol=1e-7)

    # 18:00
    assert np.isclose(result["Day_sin"].iloc[2], -1.0, atol=1e-7)
    assert np.isclose(result["Day_cos"].iloc[2], 0.0, atol=1e-7)


def test_add_time_features_month_values():
    """Kiểm tra đặc trưng chu kỳ tháng."""
    # Tháng 3: 3/12 = 1/4 vòng -> sin = 1, cos = 0
    # Tháng 6: 6/12 = 1/2 vòng -> sin = 0, cos = -1
    # Tháng 9: 9/12 = 3/4 vòng -> sin = -1, cos = 0
    df = pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(
                [
                    "2023-03-15 00:00",
                    "2023-06-15 00:00",
                    "2023-09-15 00:00",
                ]
            )
        }
    )
    result = add_time_features(df)

    # Tháng 3
    assert np.isclose(result["Month_sin"].iloc[0], 1.0, atol=1e-7)
    assert np.isclose(result["Month_cos"].iloc[0], 0.0, atol=1e-7)

    # Tháng 6
    assert np.isclose(result["Month_sin"].iloc[1], 0.0, atol=1e-7)
    assert np.isclose(result["Month_cos"].iloc[1], -1.0, atol=1e-7)

    # Tháng 9
    assert np.isclose(result["Month_sin"].iloc[2], -1.0, atol=1e-7)
    assert np.isclose(result["Month_cos"].iloc[2], 0.0, atol=1e-7)


def test_add_time_features_does_not_mutate_input():
    """Kiểm tra hàm không làm thay đổi DataFrame đầu vào."""
    original_df = pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(["2023-01-01 00:00"]),
            "Value": [42.0],
        }
    )
    original_columns = list(original_df.columns)
    original_values = original_df.copy()

    result = add_time_features(original_df)

    assert list(original_df.columns) == original_columns
    pd.testing.assert_frame_equal(original_df, original_values)
    assert "Day_sin" in result.columns
    assert "Day_sin" not in original_df.columns


def test_add_time_features_missing_datetime_column():
    """Kiểm tra báo lỗi khi thiếu cột 'Date/Time'."""
    df = pd.DataFrame({"Some_Column": [1, 2, 3]})
    with pytest.raises(ValueError, match="DataFrame thiếu cột bắt buộc: 'Date/Time'"):
        add_time_features(df)


def test_add_time_features_invalid_datetime_type():
    """Kiểm tra báo lỗi khi cột 'Date/Time' không phải datetime."""
    df = pd.DataFrame({"Date/Time": ["01 01 2023 00:00", "01 01 2023 01:00"]})
    with pytest.raises(ValueError, match="phải có kiểu dữ liệu datetime"):
        add_time_features(df)
