import time
from typing import Any
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
        pipeline: str | None = None,
        scenario: str | None = None,
        event_id: str | None = None,
        source_row_index: int | None = None,
        kafka_partition: int | None = None,
        kafka_offset: int | None = None,
        injected_anomaly: bool | None = None,
        end_to_end_latency_ms: float | None = None,
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
            pipeline: Tag định danh pipeline xử lý (ví dụ: 'kafka' hoặc 'direct').
            scenario: Tag kịch bản mô phỏng (ví dụ: 'normal', 'burst', 'anomaly_injection').
            event_id: Field định danh event (độ biến thiên cao).
            source_row_index: Field chỉ số dòng dữ liệu nguồn.
            kafka_partition: Field phân vùng Kafka nhận message.
            kafka_offset: Field offset của message trên Kafka.
            injected_anomaly: Field cờ đánh dấu event bị chèn bất thường chủ động.
            end_to_end_latency_ms: Field độ trễ xử lý từ lúc phát event tới khi ghi DB (ms).
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

        # Thêm tag có độ biến thiên thấp (low-cardinality)
        if pipeline:
            point = point.tag("pipeline", str(pipeline))
        if scenario:
            point = point.tag("scenario", str(scenario))

        # Thêm field có độ biến thiên cao (high-cardinality)
        if event_id is not None:
            point = point.field("event_id", str(event_id))
        if source_row_index is not None:
            point = point.field("source_row_index", int(source_row_index))
        if kafka_partition is not None:
            point = point.field("kafka_partition", int(kafka_partition))
        if kafka_offset is not None:
            point = point.field("kafka_offset", int(kafka_offset))
        if injected_anomaly is not None:
            point = point.field("injected_anomaly", bool(injected_anomaly))
        if end_to_end_latency_ms is not None:
            point = point.field("end_to_end_latency_ms", float(end_to_end_latency_ms))

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

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
