from dataclasses import dataclass
import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Xác định thư mục gốc của dự án bằng pathlib.Path
BASE_DIR = Path(__file__).resolve().parent.parent

# Tải cấu hình từ file .env nếu có
load_dotenv(BASE_DIR / ".env")

# Đường dẫn tuyệt đối ổn định tới các tài nguyên
CONFIG_PATH = BASE_DIR / "config.json"
DATA_PATH = BASE_DIR / "test.csv"
MODEL_PATH = BASE_DIR / "xgboost_model.json"
SCALER_PATH = BASE_DIR / "scaler.joblib"


@dataclass(frozen=True)
class InfluxSettings:
    """Cấu hình kết nối InfluxDB và các tham số vận hành streaming."""

    url: str
    token: str
    org: str
    bucket: str
    turbine_id: str = "T1"
    stream_interval_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "InfluxSettings":
        """Đọc và kiểm tra cấu hình InfluxDB từ biến môi trường."""
        url = os.getenv("INFLUX_URL", "http://localhost:8086").strip()
        token = os.getenv("INFLUX_TOKEN")
        org = os.getenv("INFLUX_ORG", "scada_org").strip()
        bucket = os.getenv("INFLUX_BUCKET", "turbine_metrics").strip()
        turbine_id = os.getenv("TURBINE_ID", "T1").strip()
        interval_str = os.getenv("STREAM_INTERVAL_SECONDS", "1").strip()

        if not token or not token.strip():
            raise ValueError(
                "Biến môi trường 'INFLUX_TOKEN' là bắt buộc nhưng chưa được thiết lập. "
                "Vui lòng cấu hình trong file .env hoặc biến môi trường hệ thống."
            )

        try:
            stream_interval_seconds = float(interval_str)
            if stream_interval_seconds < 0:
                raise ValueError("Thời gian chờ phải lớn hơn hoặc bằng 0.")
        except ValueError as exc:
            raise ValueError(
                f"Giá trị STREAM_INTERVAL_SECONDS không hợp lệ: '{interval_str}'"
            ) from exc

        return cls(
            url=url,
            token=token.strip(),
            org=org,
            bucket=bucket,
            turbine_id=turbine_id,
            stream_interval_seconds=stream_interval_seconds,
        )


def load_config(config_path: Path | str = CONFIG_PATH) -> dict:
    """Tải và trả về nội dung cấu hình từ file json."""
    resolved_path = Path(config_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file cấu hình: {resolved_path}")

    with open(resolved_path, "r", encoding="utf-8") as f:
        return json.load(f)
