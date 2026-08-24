import pandas as pd
import pytest

from src.scada_simulator import SimulationEvent
from stream_exporter import (
    StreamCounters,
    create_legacy_stream,
    format_event_log,
    process_event,
)


class FakePredictor:
    """Fake predictor trả về giá trị cố định hoặc tính toán đơn giản mà không tải XGBoost."""

    def __init__(self, fixed_prediction: float = 1800.0) -> None:
        self.fixed_prediction = fixed_prediction

    def predict(self, row: pd.Series) -> float:
        return self.fixed_prediction


class FakeWriter:
    """Fake InfluxWriter ghi nhận lại các tham số truyền vào mà không kết nối InfluxDB thật."""

    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.written_records: list[dict] = []
        self.closed = False

    def write_turbine_status(
        self,
        turbine_id: str,
        wind_speed: float,
        actual_power: float,
        predicted_power: float,
        residual: float,
        anomaly_flag: int,
        timestamp_ns: int | None = None,
        **kwargs,
    ) -> None:
        if self.should_fail:
            raise ConnectionError("Fake InfluxDB connection error")

        record = {
            "turbine_id": turbine_id,
            "wind_speed": wind_speed,
            "actual_power": actual_power,
            "predicted_power": predicted_power,
            "residual": residual,
            "anomaly_flag": anomaly_flag,
            "timestamp_ns": timestamp_ns,
        }
        record.update(kwargs)
        self.written_records.append(record)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def sample_event() -> SimulationEvent:
    row = pd.Series(
        {
            "Wind Speed (m/s)": 12.0,
            "Theoretical_Power_Curve (KWh)": 2400.0,
            "Wind Direction (?)": 180.0,
            "Day_sin": 0.0,
            "Day_cos": 1.0,
            "Month_sin": 0.0,
            "Month_cos": 1.0,
            "LV ActivePower (kW)": 2000.0,
        }
    )
    return SimulationEvent(
        event_number=42,
        turbine_id="T3",
        source_row_index=5,
        row=row,
        scenario="normal",
        injected_anomaly=False,
    )


def test_process_event_routes_turbine_id_to_writer(sample_event: SimulationEvent):
    """Xác nhận turbine_id từ event được chuyển chính xác tới InfluxWriter."""
    predictor = FakePredictor(fixed_prediction=1900.0)
    writer = FakeWriter()
    counters = StreamCounters()

    process_event(
        event=sample_event,
        predictor=predictor,
        writer=writer,
        threshold=200.0,
        target_column="LV ActivePower (kW)",
        counters=counters,
    )

    assert len(writer.written_records) == 1
    record = writer.written_records[0]
    assert record["turbine_id"] == "T3"
    assert record["wind_speed"] == 12.0
    assert record["actual_power"] == 2000.0
    assert record["predicted_power"] == 1900.0
    assert record["residual"] == 100.0
    assert record["anomaly_flag"] == 0


def test_process_event_updates_success_counters(sample_event: SimulationEvent):
    """Xác nhận bộ đếm tăng chính xác khi ghi thành công."""
    predictor = FakePredictor(fixed_prediction=1900.0)
    writer = FakeWriter()
    counters = StreamCounters()

    process_event(
        event=sample_event,
        predictor=predictor,
        writer=writer,
        threshold=50.0,  # residual = 100 > 50 -> anomaly_flag = 1
        target_column="LV ActivePower (kW)",
        counters=counters,
    )

    assert counters.events_generated == 1
    assert counters.events_processed == 1
    assert counters.influx_write_success == 1
    assert counters.influx_write_failed == 0
    assert counters.injected_anomalies == 0
    assert counters.detected_anomalies == 1


def test_process_event_failure_increments_failed_counter(
    sample_event: SimulationEvent,
):
    """Xác nhận lỗi ghi InfluxDB làm tăng influx_write_failed và re-raise exception."""
    predictor = FakePredictor(fixed_prediction=1900.0)
    writer = FakeWriter(should_fail=True)
    counters = StreamCounters()

    with pytest.raises(ConnectionError):
        process_event(
            event=sample_event,
            predictor=predictor,
            writer=writer,
            threshold=200.0,
            target_column="LV ActivePower (kW)",
            counters=counters,
        )

    assert counters.events_generated == 1
    assert counters.events_processed == 1
    assert counters.influx_write_success == 0
    assert counters.influx_write_failed == 1


def test_format_event_log():
    """Kiểm tra chuỗi log có chứa đầy đủ thông tin turbine ID và kịch bản."""
    row = pd.Series({"Wind Speed (m/s)": 10.0, "LV ActivePower (kW)": 1500.0})
    event_normal = SimulationEvent(
        event_number=1,
        turbine_id="T2",
        source_row_index=0,
        row=row,
        scenario="normal",
        injected_anomaly=False,
    )
    log_normal = format_event_log(
        event=event_normal,
        wind_speed=10.0,
        actual_power=1500.0,
        predicted_power=1450.0,
        residual=50.0,
        is_anomaly=0,
    )
    assert "[T2]" in log_normal
    assert "Event 1" in log_normal
    assert "Kịch bản normal" in log_normal
    assert "Injected=True" not in log_normal

    event_injected = SimulationEvent(
        event_number=2,
        turbine_id="T4",
        source_row_index=1,
        row=row,
        scenario="anomaly_injection",
        injected_anomaly=True,
    )
    log_injected = format_event_log(
        event=event_injected,
        wind_speed=10.0,
        actual_power=3000.0,
        predicted_power=1500.0,
        residual=1500.0,
        is_anomaly=1,
    )
    assert "[T4]" in log_injected
    assert "Injected=True" in log_injected


def test_create_legacy_stream():
    """Kiểm tra legacy stream cho chế độ một turbine khi simulator tắt."""
    df = pd.DataFrame({"Wind Speed (m/s)": [5.0, 6.0], "LV ActivePower (kW)": [500.0, 600.0]})
    events = list(create_legacy_stream(df=df, turbine_id="T1", interval_seconds=0.0))

    assert len(events) == 2
    assert events[0].turbine_id == "T1"
    assert events[0].event_number == 1
    assert events[1].turbine_id == "T1"
    assert events[1].event_number == 2
