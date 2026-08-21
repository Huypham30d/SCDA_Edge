import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxWriter:
    """Quản lý kết nối và ghi dữ liệu trạng thái turbine lên InfluxDB."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_turbine_status(
        self,
        turbine_id: str,
        wind_speed: float,
        actual_power: float,
        predicted_power: float,
        residual: float,
        anomaly_flag: int,
        timestamp_ns: int | None = None,
    ) -> None:
        """Tạo Point và ghi trạng thái turbine lên InfluxDB.

        Args:
            turbine_id: Mã định danh turbine (ví dụ 'T1').
            wind_speed: Tốc độ gió (m/s).
            actual_power: Công suất thực tế (kW).
            predicted_power: Công suất dự đoán (kW).
            residual: Độ lệch công suất (kW).
            anomaly_flag: Cờ cảnh báo bất thường (0 hoặc 1).
            timestamp_ns: Timestamp tính bằng nanosecond (mặc định lấy thời gian hiện tại).
        """
        ns_time = timestamp_ns if timestamp_ns is not None else time.time_ns()

        point = (
            Point("turbine_status")
            .tag("turbine_id", str(turbine_id))
            .field("wind_speed", float(wind_speed))
            .field("actual_power", float(actual_power))
            .field("predicted_power", float(predicted_power))
            .field("residual", float(residual))
            .field("anomaly_flag", int(anomaly_flag))
            .time(ns_time, WritePrecision.NS)
        )

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except Exception as exc:
            raise RuntimeError(
                f"Lỗi khi ghi dữ liệu turbine '{turbine_id}' lên InfluxDB "
                f"(bucket='{self.bucket}', org='{self.org}'): {exc}"
            ) from exc

    def close(self) -> None:
        """Đóng kết nối InfluxDB client."""
        if hasattr(self, "client") and self.client is not None:
            self.client.close()

    def __enter__(self) -> "InfluxWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
