from typing import Iterator
import numpy as np
import pandas as pd
import pytest

from config.settings import SimulatorSettings, load_simulator_settings, parse_bool_env
from src.scada_simulator import SCADASimulator, SimulationEvent


class FakeClock:
    """Đồng hồ giả lập để kiểm soát monotonic time và ghi lại các lần gọi sleep."""

    def __init__(self, start_time: float = 0.0) -> None:
        self.current_time = start_time
        self.sleep_calls: list[float] = []

    def monotonic(self) -> float:
        return self.current_time

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError(f"Không được phép sleep số âm: {seconds}")
        self.sleep_calls.append(seconds)
        self.current_time += seconds


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Tạo DataFrame mẫu nhỏ với 3 dòng dữ liệu."""
    return pd.DataFrame(
        {
            "Date/Time": pd.to_datetime(
                ["2023-01-01 00:00", "2023-01-01 00:10", "2023-01-01 00:20"]
            ),
            "Wind Speed (m/s)": [5.0, 7.5, 10.0],
            "Theoretical_Power_Curve (KWh)": [500.0, 1200.0, 2200.0],
            "Wind Direction (?)": [90.0, 100.0, 110.0],
            "Day_sin": [0.0, 0.043, 0.087],
            "Day_cos": [1.0, 0.999, 0.996],
            "Month_sin": [0.5, 0.5, 0.5],
            "Month_cos": [0.866, 0.866, 0.866],
            "LV ActivePower (kW)": [450.0, 1150.0, 2100.0],
        }
    )


@pytest.fixture
def default_simulator_settings() -> SimulatorSettings:
    return SimulatorSettings(
        enabled=True,
        turbine_count=3,
        events_per_second=10.0,
        loop_data=False,
        max_events=None,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.05,
        actual_power_multiplier=1.8,
    )


def test_turbine_ids_generated_correctly(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """1. Kiểm tra tạo đúng danh sách turbine ID (T1, T2, T3)."""
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=default_simulator_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    assert simulator.turbine_ids == ["T1", "T2", "T3"]


def test_round_robin_distribution(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """2. Kiểm tra phân phối round-robin đúng thứ tự:
    T1 row 0, T2 row 0, T3 row 0, T1 row 1, T2 row 1, T3 row 1...
    """
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=default_simulator_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())

    expected_sequence = [
        ("T1", 0),
        ("T2", 0),
        ("T3", 0),
        ("T1", 1),
        ("T2", 1),
        ("T3", 1),
        ("T1", 2),
        ("T2", 2),
        ("T3", 2),
    ]

    actual_sequence = [(e.turbine_id, e.source_row_index) for e in events]
    assert actual_sequence == expected_sequence


def test_loop_data_false_terminates_after_full_dataset(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """3. loop_data=false dừng sau đúng len(df) * turbine_count."""
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=default_simulator_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == len(sample_dataframe) * default_simulator_settings.turbine_count
    assert len(events) == 9


def test_max_events_limits_total_events(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """4. max_events giới hạn đúng tổng số event."""
    custom_settings = SimulatorSettings(
        enabled=True,
        turbine_count=3,
        events_per_second=10.0,
        loop_data=False,
        max_events=4,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.05,
        actual_power_multiplier=1.8,
    )
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=custom_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == 4
    assert [e.event_number for e in events] == [1, 2, 3, 4]


def test_loop_data_true_wraps_around(
    sample_dataframe: pd.DataFrame,
):
    """5. loop_data=true quay lại dòng đầu tiên khi hết DataFrame."""
    loop_settings = SimulatorSettings(
        enabled=True,
        turbine_count=2,
        events_per_second=10.0,
        loop_data=True,
        max_events=8,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.05,
        actual_power_multiplier=1.8,
    )
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,  # 3 rows
        settings=loop_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == 8

    # T1: row 0, row 1, row 2, wrap -> row 0
    # T2: row 0, row 1, row 2, wrap -> row 0
    t1_rows = [e.source_row_index for e in events if e.turbine_id == "T1"]
    t2_rows = [e.source_row_index for e in events if e.turbine_id == "T2"]

    assert t1_rows == [0, 1, 2, 0]
    assert t2_rows == [0, 1, 2, 0]


def test_input_dataframe_not_mutated(
    sample_dataframe: pd.DataFrame,
):
    """6. DataFrame đầu vào không bị thay đổi trong quá trình mô phỏng."""
    anomaly_settings = SimulatorSettings(
        enabled=True,
        turbine_count=2,
        events_per_second=10.0,
        loop_data=False,
        max_events=None,
        scenario="anomaly_injection",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=1.0,
        actual_power_multiplier=5.0,
    )
    clock = FakeClock()
    original_df_copy = sample_dataframe.copy(deep=True)

    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=anomaly_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    list(simulator.generate())

    pd.testing.assert_frame_equal(sample_dataframe, original_df_copy)


def test_each_event_has_independent_row(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """7. Mỗi event chứa một Series độc lập, sửa đổi không ảnh hưởng event khác."""
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=default_simulator_settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())

    events[0].row["LV ActivePower (kW)"] = 99999.0
    assert events[1].row["LV ActivePower (kW)"] != 99999.0


def test_empty_dataframe_raises_value_error(
    default_simulator_settings: SimulatorSettings,
):
    """8. DataFrame rỗng báo lỗi ValueError."""
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="không được để trống hoặc rỗng"):
        SCADASimulator(
            dataframe=empty_df,
            settings=default_simulator_settings,
            target_column="LV ActivePower (kW)",
        )


def test_missing_target_raises_value_error(
    sample_dataframe: pd.DataFrame, default_simulator_settings: SimulatorSettings
):
    """9. Thiếu cột target trong DataFrame báo lỗi ValueError."""
    with pytest.raises(ValueError, match="Không tìm thấy cột mục tiêu"):
        SCADASimulator(
            dataframe=sample_dataframe,
            settings=default_simulator_settings,
            target_column="NonExistentTarget",
        )


def test_reproducible_anomaly_injection_with_same_seed(sample_dataframe: pd.DataFrame):
    """10. Cùng random seed cho kết quả chuỗi chèn bất thường giống nhau."""
    settings = SimulatorSettings(
        enabled=True,
        turbine_count=3,
        events_per_second=10.0,
        loop_data=True,
        max_events=20,
        scenario="anomaly_injection",
        random_seed=999,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.3,
        actual_power_multiplier=2.0,
    )

    clock1 = FakeClock()
    sim1 = SCADASimulator(
        sample_dataframe, settings, "LV ActivePower (kW)", clock1.sleep, clock1.monotonic
    )
    events1 = [e.injected_anomaly for e in sim1.generate()]

    clock2 = FakeClock()
    sim2 = SCADASimulator(
        sample_dataframe, settings, "LV ActivePower (kW)", clock2.sleep, clock2.monotonic
    )
    events2 = [e.injected_anomaly for e in sim2.generate()]

    assert events1 == events2
    assert any(events1)  # Có ít nhất một event bất thường


def test_normal_rate_uses_correct_interval(sample_dataframe: pd.DataFrame):
    """11. Normal rate sử dụng đúng khoảng thời gian interval = 1 / events_per_second."""
    events_per_sec = 5.0  # interval = 0.2s
    settings = SimulatorSettings(
        enabled=True,
        turbine_count=2,
        events_per_second=events_per_sec,
        loop_data=False,
        max_events=4,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=10.0,
        burst_duration_seconds=10.0,
        burst_multiplier=2.0,
        anomaly_probability=0.05,
        actual_power_multiplier=1.8,
    )
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == 4

    # 4 events -> 3 lần sleep giữa các event
    assert len(clock.sleep_calls) == 3
    for call in clock.sleep_calls:
        assert np.isclose(call, 0.2, atol=1e-5)


def test_burst_rate_uses_correct_multiplier(sample_dataframe: pd.DataFrame):
    """12. Burst rate tăng tốc độ phát chính xác theo multiplier trong khoảng thời gian burst."""
    # events_per_second = 1.0 (interval 1.0s)
    # burst: start_after_seconds = 2.0, duration = 2.0s, multiplier = 4.0 (burst interval = 0.25s)
    settings = SimulatorSettings(
        enabled=True,
        turbine_count=1,
        events_per_second=1.0,
        loop_data=True,
        max_events=7,
        scenario="burst",
        random_seed=42,
        burst_start_after_seconds=2.0,
        burst_duration_seconds=2.0,
        burst_multiplier=4.0,
        anomaly_probability=0.0,
        actual_power_multiplier=1.0,
    )
    clock = FakeClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == 7

    # Timeline:
    # Event 1: t = 0.0s (no sleep)
    # Event 2: elapsed = 0.0 < 2.0 -> normal interval = 1.0s. Sleep 1.0s -> clock becomes 1.0s.
    # Event 3: elapsed = 1.0 < 2.0 -> normal interval = 1.0s. Sleep 1.0s -> clock becomes 2.0s.
    # Event 4: elapsed = 2.0 in [2.0, 4.0) -> burst interval = 0.25s. Sleep 0.25s -> clock becomes 2.25s.
    # Event 5: elapsed = 2.25 in [2.0, 4.0) -> burst interval = 0.25s. Sleep 0.25s -> clock becomes 2.5s.
    # Event 6: elapsed = 2.5 in [2.0, 4.0) -> burst interval = 0.25s. Sleep 0.25s -> clock becomes 2.75s.
    # Event 7: elapsed = 2.75 in [2.0, 4.0) -> burst interval = 0.25s. Sleep 0.25s -> clock becomes 3.0s.
    expected_sleeps = [1.0, 1.0, 0.25, 0.25, 0.25, 0.25]
    assert len(clock.sleep_calls) == len(expected_sleeps)
    for actual, expected in zip(clock.sleep_calls, expected_sleeps):
        assert np.isclose(actual, expected, atol=1e-5)


def test_no_negative_sleep(sample_dataframe: pd.DataFrame):
    """13. Simulator không bao giờ gọi sleep với giá trị âm kể cả khi clock bị trễ."""
    settings = SimulatorSettings(
        enabled=True,
        turbine_count=1,
        events_per_second=10.0,
        loop_data=False,
        max_events=3,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.0,
        actual_power_multiplier=1.0,
    )

    class LaggingClock:
        def __init__(self):
            self.time = 0.0
            self.sleeps = []

        def monotonic(self):
            return self.time

        def sleep(self, s):
            if s < 0:
                raise ValueError("Negative sleep detected!")
            self.sleeps.append(s)
            # Giả lập xử lý bị chậm quá thời gian interval
            self.time += 1.0

    clock = LaggingClock()
    simulator = SCADASimulator(
        dataframe=sample_dataframe,
        settings=settings,
        target_column="LV ActivePower (kW)",
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    events = list(simulator.generate())
    assert len(events) == 3
    assert all(s >= 0 for s in clock.sleeps)


def test_events_per_second_is_total_rate_not_per_turbine(
    sample_dataframe: pd.DataFrame,
):
    """14. events_per_second là tổng tốc độ phát của toàn bộ simulator, không nhân với turbine_count."""
    events_per_sec = 2.0  # interval = 0.5s cho toàn hệ thống
    settings_1_turbine = SimulatorSettings(
        enabled=True,
        turbine_count=1,
        events_per_second=events_per_sec,
        loop_data=False,
        max_events=3,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.0,
        actual_power_multiplier=1.0,
    )
    settings_5_turbines = SimulatorSettings(
        enabled=True,
        turbine_count=5,
        events_per_second=events_per_sec,
        loop_data=False,
        max_events=3,
        scenario="normal",
        random_seed=42,
        burst_start_after_seconds=5.0,
        burst_duration_seconds=5.0,
        burst_multiplier=2.0,
        anomaly_probability=0.0,
        actual_power_multiplier=1.0,
    )

    clock1 = FakeClock()
    sim1 = SCADASimulator(
        sample_dataframe,
        settings_1_turbine,
        "LV ActivePower (kW)",
        clock1.sleep,
        clock1.monotonic,
    )
    list(sim1.generate())

    clock2 = FakeClock()
    sim2 = SCADASimulator(
        sample_dataframe,
        settings_5_turbines,
        "LV ActivePower (kW)",
        clock2.sleep,
        clock2.monotonic,
    )
    list(sim2.generate())

    # Cả hai đều phải sleep cùng khoảng cách 0.5s giữa các event
    assert clock1.sleep_calls == clock2.sleep_calls == [0.5, 0.5]


def test_parse_bool_env():
    """Kiểm tra parse_bool_env xử lý đúng các chuỗi boolean."""
    assert parse_bool_env("true") is True
    assert parse_bool_env("True") is True
    assert parse_bool_env("1") is True
    assert parse_bool_env("yes") is True
    assert parse_bool_env("YES") is True

    assert parse_bool_env("false") is False
    assert parse_bool_env("False") is False
    assert parse_bool_env("0") is False
    assert parse_bool_env("no") is False
    assert parse_bool_env("NO") is False

    with pytest.raises(ValueError, match="Giá trị boolean không hợp lệ"):
        parse_bool_env("invalid_bool")
