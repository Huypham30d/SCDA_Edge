import json
import threading
import time
from pathlib import Path
import pandas as pd
import pytest

from experiments.direct_loss_demo import (
    ExperimentCounters,
    LossExperimentSettings,
    load_loss_experiment_settings,
    run_experiment,
    save_experiment_results,
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Tạo DataFrame mẫu nhỏ cho kiểm thử thí nghiệm."""
    rows = []
    for i in range(20):
        rows.append(
            {
                "Date/Time": f"01 01 2018 {i:02d}:00",
                "LV ActivePower (kW)": 500.0 + i * 10.0,
                "Wind Speed (m/s)": 8.0 + (i % 5) * 0.5,
                "Theoretical_Power_Curve (KWh)": 520.0 + i * 10.0,
                "Wind Direction (°)": 180.0,
                "Day_sin": 0.0,
                "Day_cos": 1.0,
                "Month_sin": 0.0,
                "Month_cos": 1.0,
            }
        )
    return pd.DataFrame(rows)


class FakePredictor:
    """Predictor giả lập không cần tải model thật."""

    def __init__(self, pred_offset: float = 0.0) -> None:
        self.pred_offset = pred_offset

    def predict(self, row: pd.Series | dict) -> float:
        actual = float(row.get("LV ActivePower (kW)", 500.0))
        return actual + self.pred_offset


class FakeWriter:
    """Writer giả lập không kết nối InfluxDB thật."""

    def __init__(
        self,
        delay_seconds: float = 0.0,
        fail_always: bool = False,
        fail_first_n: int = 0,
    ) -> None:
        self.delay_seconds = delay_seconds
        self.fail_always = fail_always
        self.fail_first_n = fail_first_n
        self.attempts = 0
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def write_event(self, **kwargs) -> None:
        with self._lock:
            self.attempts += 1

            if self.fail_always or self.attempts <= self.fail_first_n:
                raise RuntimeError("Simulated InfluxDB write failure")

            if self.delay_seconds > 0:
                time.sleep(self.delay_seconds)

            self.calls.append(kwargs)

    def close(self) -> None:
        pass


def test_load_loss_experiment_settings_valid(tmp_path: Path) -> None:
    """Kiểm tra đọc và validate file cấu hình thí nghiệm hợp lệ."""
    cfg_data = {
        "experiment_id": "test-exp-01",
        "turbine_count": 3,
        "source_events_per_second": 10,
        "total_events": 50,
        "queue_capacity": 20,
        "consumer_delay_seconds": 0.005,
        "continue_on_write_error": True,
        "drain_queue_after_source_stops": True,
        "scenario": "normal",
        "random_seed": 123,
        "result_directory": "test_results",
    }
    cfg_path = tmp_path / "test_loss.json"
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg_data, f)

    settings = load_loss_experiment_settings(cfg_path)
    assert settings.experiment_id == "test-exp-01"
    assert settings.turbine_count == 3
    assert settings.source_events_per_second == 10.0
    assert settings.total_events == 50
    assert settings.queue_capacity == 20
    assert settings.consumer_delay_seconds == 0.005
    assert settings.continue_on_write_error is True
    assert settings.drain_queue_after_source_stops is True
    assert settings.scenario == "normal"
    assert settings.random_seed == 123
    assert settings.result_directory == "test_results"


@pytest.mark.parametrize(
    "invalid_key, invalid_value, error_match",
    [
        ("experiment_id", "", "Trường 'experiment_id'"),
        ("turbine_count", 0, "Trường 'turbine_count'"),
        ("source_events_per_second", 0, "Trường 'source_events_per_second'"),
        ("total_events", -1, "Trường 'total_events'"),
        ("queue_capacity", 0, "Trường 'queue_capacity'"),
        ("consumer_delay_seconds", -0.5, "Trường 'consumer_delay_seconds'"),
        ("continue_on_write_error", "not_bool", "Trường 'continue_on_write_error'"),
        (
            "drain_queue_after_source_stops",
            "not_bool",
            "Trường 'drain_queue_after_source_stops'",
        ),
        ("scenario", "invalid_scenario", "Trường 'scenario' không hợp lệ"),
        ("random_seed", "seed", "Trường 'random_seed'"),
        ("result_directory", "", "Trường 'result_directory'"),
    ],
)
def test_load_loss_experiment_settings_validation_errors(
    tmp_path: Path, invalid_key: str, invalid_value: any, error_match: str
) -> None:
    """Kiểm tra validation từ chối các tham số cấu hình sai."""
    valid_cfg = {
        "experiment_id": "test-exp",
        "turbine_count": 3,
        "source_events_per_second": 10,
        "total_events": 50,
        "queue_capacity": 20,
        "consumer_delay_seconds": 0.0,
        "continue_on_write_error": True,
        "drain_queue_after_source_stops": True,
        "scenario": "normal",
        "random_seed": 42,
        "result_directory": "experiment_results",
    }
    valid_cfg[invalid_key] = invalid_value

    cfg_path = tmp_path / "invalid_cfg.json"
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(valid_cfg, f)

    with pytest.raises(ValueError, match=error_match):
        load_loss_experiment_settings(cfg_path)


def test_counters_thread_safety() -> None:
    """Kiểm tra tính an toàn đa luồng của ExperimentCounters."""
    counters = ExperimentCounters()
    num_threads = 10
    increments_per_thread = 500

    def worker():
        for _ in range(increments_per_thread):
            counters.inc_source_generated()
            counters.inc_queue_accepted()
            counters.inc_queue_dropped()
            counters.inc_events_processed()
            counters.inc_influx_write_success()
            counters.inc_influx_write_failed()

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_expected = num_threads * increments_per_thread
    assert counters.source_generated == total_expected
    assert counters.queue_accepted == total_expected
    assert counters.queue_dropped == total_expected
    assert counters.events_processed == total_expected
    assert counters.influx_write_success == total_expected
    assert counters.influx_write_failed == total_expected


def test_experiment_low_load_no_loss(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    """Kiểm tra tải thấp: không có event nào bị drop, tất cả đều ghi thành công."""
    settings = LossExperimentSettings(
        experiment_id="test-control-low-load",
        turbine_count=2,
        source_events_per_second=1000.0,  # Fast in test
        total_events=30,
        queue_capacity=50,
        consumer_delay_seconds=0.0,
        continue_on_write_error=True,
        drain_queue_after_source_stops=True,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    predictor = FakePredictor()
    writer = FakeWriter()

    counters, summary, _ = run_experiment(
        settings=settings,
        predictor=predictor,
        writer=writer,
        df=sample_df,
        threshold=100.0,
        target_column="LV ActivePower (kW)",
        sleep_fn=lambda _: None,
    )

    assert counters.source_generated == 30
    assert counters.queue_accepted == 30
    assert counters.queue_dropped == 0
    assert counters.events_processed == 30
    assert counters.influx_write_success == 30
    assert counters.influx_write_failed == 0
    assert counters.drop_rate() == 0.0

    # Kiểm tra phương trình bảo toàn
    assert counters.source_generated == counters.queue_accepted + counters.queue_dropped
    assert counters.queue_accepted == counters.influx_write_success + counters.influx_write_failed


def test_experiment_overload_causes_drop(
    sample_df: pd.DataFrame, tmp_path: Path
) -> None:
    """Kiểm tra tải cao với queue nhỏ và consumer chậm làm queue bị đầy và drop event."""
    total_events = 60
    settings = LossExperimentSettings(
        experiment_id="test-overload-drop",
        turbine_count=2,
        source_events_per_second=10000.0,  # Producer phát tức thì
        total_events=total_events,
        queue_capacity=5,  # Queue cực nhỏ
        consumer_delay_seconds=0.01,  # Consumer chậm
        continue_on_write_error=True,
        drain_queue_after_source_stops=True,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    predictor = FakePredictor()
    writer = FakeWriter()

    counters, summary, _ = run_experiment(
        settings=settings,
        predictor=predictor,
        writer=writer,
        df=sample_df,
        threshold=100.0,
        target_column="LV ActivePower (kW)",
    )

    assert counters.source_generated == total_events
    assert counters.queue_dropped > 0
    assert counters.queue_accepted < total_events
    assert counters.influx_write_success < total_events
    assert counters.drop_rate() > 0.0

    # Kiểm tra phương trình bảo toàn
    assert counters.source_generated == counters.queue_accepted + counters.queue_dropped
    assert counters.queue_accepted == counters.influx_write_success + counters.influx_write_failed


def test_experiment_write_errors_continue(
    sample_df: pd.DataFrame, tmp_path: Path
) -> None:
    """Kiểm tra khi writer thất bại và continue_on_write_error=True, pipeline tiếp tục xử lý."""
    total_events = 20
    settings = LossExperimentSettings(
        experiment_id="test-write-errors",
        turbine_count=2,
        source_events_per_second=1000.0,
        total_events=total_events,
        queue_capacity=50,
        consumer_delay_seconds=0.0,
        continue_on_write_error=True,
        drain_queue_after_source_stops=True,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    predictor = FakePredictor()
    # 5 event đầu tiên ghi lỗi, các event sau ghi thành công
    writer = FakeWriter(fail_first_n=5)

    counters, _, _ = run_experiment(
        settings=settings,
        predictor=predictor,
        writer=writer,
        df=sample_df,
        threshold=100.0,
        target_column="LV ActivePower (kW)",
        sleep_fn=lambda _: None,
    )

    assert counters.source_generated == 20
    assert counters.queue_accepted == 20
    assert counters.queue_dropped == 0
    assert counters.events_processed == 20
    assert counters.influx_write_failed == 5
    assert counters.influx_write_success == 15

    # Bảo toàn
    assert counters.queue_accepted == counters.influx_write_success + counters.influx_write_failed


def test_experiment_write_error_stops_when_not_continue(
    sample_df: pd.DataFrame, tmp_path: Path
) -> None:
    """Kiểm tra khi continue_on_write_error=False, writer ném exception và dừng."""
    settings = LossExperimentSettings(
        experiment_id="test-write-fail-stop",
        turbine_count=2,
        source_events_per_second=1000.0,
        total_events=20,
        queue_capacity=50,
        consumer_delay_seconds=0.0,
        continue_on_write_error=False,
        drain_queue_after_source_stops=True,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    predictor = FakePredictor()
    writer = FakeWriter(fail_always=True)

    counters, summary, _ = run_experiment(
        settings=settings,
        predictor=predictor,
        writer=writer,
        df=sample_df,
        threshold=100.0,
        target_column="LV ActivePower (kW)",
        sleep_fn=lambda _: None,
    )

    assert counters.influx_write_failed >= 1
    assert counters.influx_write_success == 0


def test_experiment_drop_rate_and_throughput_calculation() -> None:
    """Kiểm tra công thức tính drop_rate và actual_throughput."""
    counters = ExperimentCounters()
    assert counters.drop_rate() == 0.0
    assert counters.actual_throughput(0.0) == 0.0
    assert counters.actual_throughput(10.0) == 0.0

    counters.source_generated = 1000
    counters.queue_dropped = 250
    counters.influx_write_success = 750

    assert counters.drop_rate() == 25.0
    assert counters.actual_throughput(10.0) == 75.0


def test_experiment_without_drain(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    """Kiểm tra khi drain_queue_after_source_stops=False, consumer dừng khi producer dừng."""
    total_events = 50
    settings = LossExperimentSettings(
        experiment_id="test-no-drain",
        turbine_count=2,
        source_events_per_second=10000.0,
        total_events=total_events,
        queue_capacity=50,
        consumer_delay_seconds=0.02,  # Rất chậm để khi producer xong queue vẫn còn
        continue_on_write_error=True,
        drain_queue_after_source_stops=False,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    predictor = FakePredictor()
    writer = FakeWriter()

    counters, summary, _ = run_experiment(
        settings=settings,
        predictor=predictor,
        writer=writer,
        df=sample_df,
        threshold=100.0,
        target_column="LV ActivePower (kW)",
    )

    queue_pending = summary["queue_pending"]
    assert counters.source_generated == total_events
    # Phương trình bảo toàn không drain
    assert counters.source_generated == counters.queue_accepted + counters.queue_dropped
    assert (
        counters.queue_accepted
        == counters.influx_write_success + counters.influx_write_failed + queue_pending
    )


def test_save_experiment_results_no_credentials(tmp_path: Path) -> None:
    """Kiểm tra file JSON kết quả được lưu đúng cấu trúc và không chứa credentials."""
    settings = LossExperimentSettings(
        experiment_id="test-save-json-001",
        turbine_count=3,
        source_events_per_second=10.0,
        total_events=100,
        queue_capacity=50,
        consumer_delay_seconds=0.0,
        continue_on_write_error=True,
        drain_queue_after_source_stops=True,
        scenario="normal",
        random_seed=42,
        result_directory=str(tmp_path),
    )

    counters = ExperimentCounters()
    counters.source_generated = 100
    counters.queue_accepted = 80
    counters.queue_dropped = 20
    counters.events_processed = 80
    counters.influx_write_success = 75
    counters.influx_write_failed = 5

    res_path = save_experiment_results(
        settings=settings,
        counters=counters,
        queue_pending=0,
        elapsed_seconds=10.5,
        output_dir=tmp_path,
    )

    assert res_path.exists()
    with open(res_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["experiment_id"] == "test-save-json-001"
    assert data["source_generated"] == 100
    assert data["queue_accepted"] == 80
    assert data["queue_dropped"] == 20
    assert data["events_processed"] == 80
    assert data["influx_write_success"] == 75
    assert data["influx_write_failed"] == 5
    assert data["queue_pending"] == 0
    assert data["drop_rate_percent"] == 20.0
    assert data["elapsed_seconds"] == 10.5
    assert data["actual_throughput"] == round(75 / 10.5, 2)

    # Đảm bảo không chứa token hay password
    content_str = json.dumps(data).lower()
    assert "token" not in content_str
    assert "password" not in content_str
    assert "secret" not in content_str
