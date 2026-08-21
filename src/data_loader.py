from pathlib import Path
from typing import Sequence
import pandas as pd


def load_data(
    file_path: str | Path,
    features: Sequence[str] | None = None,
    target: str | None = None,
) -> pd.DataFrame:
    """Đọc file CSV, loại bỏ giá trị rỗng, kiểm tra các cột bắt buộc và chuyển đổi cột Date/Time.

    Args:
        file_path: Đường dẫn tới file CSV.
        features: Danh sách các feature trong config.json (để kiểm tra cột thô bắt buộc).
        target: Tên cột mục tiêu cần dự đoán trong config.json.

    Returns:
        pd.DataFrame chứa dữ liệu đã được làm sạch và chuyển đổi Date/Time.

    Raises:
        FileNotFoundError: Nếu file không tồn tại.
        ValueError: Nếu thiếu cột Date/Time, target hoặc các feature thô bắt buộc.
    """
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu: {path_obj}")

    df = pd.read_csv(path_obj).dropna()

    # Kiểm tra cột Date/Time
    if "Date/Time" not in df.columns:
        raise ValueError(
            f"File CSV '{path_obj}' thiếu cột bắt buộc: 'Date/Time'"
        )

    # Kiểm tra các cột bắt buộc khác từ features và target
    generated_time_features = {"Day_sin", "Day_cos", "Month_sin", "Month_cos"}
    required_cols: list[str] = []

    if target and target not in required_cols:
        required_cols.append(target)

    if features:
        for feat in features:
            if feat not in generated_time_features and feat not in required_cols:
                required_cols.append(feat)

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"File CSV '{path_obj}' thiếu các cột dữ liệu bắt buộc: {missing_cols}"
        )

    # Chuyển đổi định dạng ngày giờ theo đúng format hiện tại
    try:
        df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="%d %m %Y %H:%M")
    except Exception as exc:
        raise ValueError(
            f"Không thể chuyển đổi cột 'Date/Time' theo định dạng '%d %m %Y %H:%M' trong '{path_obj}': {exc}"
        ) from exc

    return df
