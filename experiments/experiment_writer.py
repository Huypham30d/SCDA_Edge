import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class ExperimentWriter:
    """Quản lý kết nối và ghi dữ liệu kết quả thí nghiệm lên InfluxDB."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        if not bucket or not str(bucket).strip():
            raise ValueError("Tên bucket thí nghiệm không được để trống.")
        if not token or not str(token).strip():
            raise ValueError("Token kết nối InfluxDB không được để trống.")

        self.url = url
        self.token = token.strip()
        self.org = org
        self.bucket = bucket.strip()
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    @classmethod
    def from_env(cls) -> "ExperimentWriter":
        """Khởi tạo ExperimentWriter từ biến môi trường EXPERIMENT_INFLUX_BUCKET.

        Raises:
            ValueError: Nếu EXPERIMENT_INFLUX_BUCKET hoặc INFLUX_TOKEN chưa được thiết lập.
        """
        experiment_bucket = os.getenv("EXPERIMENT_INFLUX_BUCKET")
        if not experiment_bucket or not experiment_bucket.strip():
            raise ValueError(
                "Biến môi trường 'EXPERIMENT_INFLUX_BUCKET' là bắt buộc đối với thí nghiệm "
                "nhưng chưa được thiết lập. Vui lòng cấu hình trong .env (ví dụ: EXPERIMENT_INFLUX_BUCKET=scada_experiment)."
            )

        token = os.getenv("INFLUX_TOKEN")
        if not token or not token.strip():
            raise ValueError(
                "Biến môi trường 'INFLUX_TOKEN' là bắt buộc nhưng chưa được thiết lập."
            )

        url = os.getenv("INFLUX_URL", "http://localhost:8086").strip()
        org = os.getenv("INFLUX_ORG", "scada_org").strip()

        return cls(
            url=url,
            token=token,
            org=org,
            bucket=experiment_bucket.strip(),
        )

    def write_event(
        self,
        experiment_id: str,
        turbine_id: str,
        event_sequence: int,
        wind_speed: float,
        actual_power: float,
        predicted_power: float,
        residual: float,
        anomaly_flag: int,
        source_timestamp_ns: int | None = None,
    ) -> None:
        """Tạo Point và ghi bản ghi thí nghiệm lên InfluxDB đồng bộ.

        Args:
            experiment_id: Mã định danh kịch bản thí nghiệm (tag).
            turbine_id: Mã định danh turbine (tag).
            event_sequence: Số thứ tự sự kiện trong luồng phát (field).
            wind_speed: Tốc độ gió đo được (field, m/s).
            actual_power: Công suất thực tế (field, kW).
            predicted_power: Công suất dự đoán từ mô hình (field, kW).
            residual: Sai số chênh lệch công suất (field, kW).
            anomaly_flag: Cờ cảnh báo bất thường (field, 0 hoặc 1).
            source_timestamp_ns: Thời điểm tạo sự kiện ở nguồn tính bằng nanosecond.
        """
        ns_time = source_timestamp_ns if source_timestamp_ns is not None else time.time_ns()

        point = (
            Point("direct_pipeline_experiment")
            .tag("experiment_id", str(experiment_id))
            .tag("turbine_id", str(turbine_id))
            .field("event_sequence", int(event_sequence))
            .field("wind_speed", float(wind_speed))
            .field("actual_power", float(actual_power))
            .field("predicted_power", float(predicted_power))
            .field("residual", float(residual))
            .field("anomaly_flag", int(anomaly_flag))
            .field("source_timestamp_ns", int(ns_time))
            .time(ns_time, WritePrecision.NS)
        )

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except Exception as exc:
            raise RuntimeError(
                f"Lỗi khi ghi dữ liệu experiment '{experiment_id}' turbine '{turbine_id}' "
                f"lên InfluxDB (bucket='{self.bucket}', org='{self.org}'): {exc}"
            ) from exc

    def close(self) -> None:
        """Đóng kết nối InfluxDB client."""
        if hasattr(self, "client") and self.client is not None:
            self.client.close()

    def __enter__(self) -> "ExperimentWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
