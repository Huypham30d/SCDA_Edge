from dataclasses import dataclass
import random
import time
from typing import Callable, Iterator
import pandas as pd

from config.settings import SimulatorSettings
from src.simulation_scenarios import apply_scenario


@dataclass(frozen=True)
class SimulationEvent:
    """Biểu diễn một sự kiện dữ liệu được sinh ra từ SCADA Simulator."""

    event_number: int
    turbine_id: str
    source_row_index: int
    row: pd.Series
    scenario: str
    injected_anomaly: bool


class SCADASimulator:
    """Mô phỏng phát dữ liệu SCADA từ nhiều turbine theo thời gian thực."""

    def __init__(
        self,
        dataframe: pd.DataFrame,
        settings: SimulatorSettings,
        target_column: str,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        """Khởi tạo SCADA Simulator với cấu hình và dữ liệu đầu vào.

        Args:
            dataframe: DataFrame chứa dữ liệu chuỗi thời gian SCADA đã qua feature engineering.
            settings: SimulatorSettings chứa các tham số cấu hình.
            target_column: Tên cột mục tiêu (công suất thực tế).
            sleep_fn: Hàm sleep (mặc định time.sleep), có thể mock để kiểm thử.
            monotonic_fn: Hàm lấy thời gian monotonic (mặc định time.monotonic).
        """
        if dataframe is None or dataframe.empty:
            raise ValueError("DataFrame dữ liệu không được để trống hoặc rỗng.")

        if target_column not in dataframe.columns:
            raise ValueError(
                f"Không tìm thấy cột mục tiêu '{target_column}' trong DataFrame."
            )

        if settings.turbine_count < 1:
            raise ValueError(
                f"Số lượng turbine (turbine_count) phải >= 1. Nhận được: {settings.turbine_count}"
            )

        if settings.events_per_second <= 0:
            raise ValueError(
                f"Tốc độ phát (events_per_second) phải > 0. Nhận được: {settings.events_per_second}"
            )

        valid_scenarios = ("normal", "burst", "anomaly_injection")
        if settings.scenario not in valid_scenarios:
            raise ValueError(
                f"Kịch bản '{settings.scenario}' không hợp lệ. Phải là một trong: {valid_scenarios}"
            )

        if settings.burst_start_after_seconds < 0:
            raise ValueError("Thời gian bắt đầu burst phải >= 0.")

        if settings.burst_duration_seconds <= 0:
            raise ValueError("Thời lượng burst phải > 0.")

        if settings.burst_multiplier <= 0:
            raise ValueError("Hệ số tăng tốc burst phải > 0.")

        if not (0.0 <= settings.anomaly_probability <= 1.0):
            raise ValueError("Xác suất chèn bất thường phải nằm trong [0.0, 1.0].")

        if settings.actual_power_multiplier <= 0:
            raise ValueError("Hệ số nhân công suất bất thường phải > 0.")

        self.dataframe = dataframe
        self.settings = settings
        self.target_column = target_column
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.rng = random.Random(settings.random_seed)
        self.turbine_ids = [f"T{i}" for i in range(1, settings.turbine_count + 1)]

    def _get_interval(self, elapsed_seconds: float) -> float:
        """Tính toán khoảng cách thời gian giữa các event (giây) theo kịch bản."""
        if self.settings.scenario == "burst":
            burst_start = self.settings.burst_start_after_seconds
            burst_end = burst_start + self.settings.burst_duration_seconds
            if burst_start <= elapsed_seconds < burst_end:
                burst_rate = (
                    self.settings.events_per_second * self.settings.burst_multiplier
                )
                return 1.0 / burst_rate

        return 1.0 / self.settings.events_per_second

    def generate(self) -> Iterator[SimulationEvent]:
        """Tạo luồng sự kiện streaming từ các turbine theo phân phối round-robin."""
        num_rows = len(self.dataframe)
        turbine_count = len(self.turbine_ids)
        turbine_pointers = {t_id: 0 for t_id in self.turbine_ids}

        start_time = self.monotonic_fn()
        next_deadline = start_time
        event_number = 1

        while True:
            # Kiểm tra giới hạn max_events nếu có
            if (
                self.settings.max_events is not None
                and event_number > self.settings.max_events
            ):
                break

            # Xác định turbine cho event hiện tại theo round-robin
            turbine_idx = (event_number - 1) % turbine_count
            turbine_id = self.turbine_ids[turbine_idx]
            row_idx = turbine_pointers[turbine_id]

            # Nếu không loop_data và con trỏ đã duyệt hết DataFrame
            if not self.settings.loop_data and row_idx >= num_rows:
                # Kiểm tra xem toàn bộ các turbine đã hết dữ liệu chưa
                if all(ptr >= num_rows for ptr in turbine_pointers.values()):
                    break
                # Nếu turbine này đã hết dữ liệu, bỏ qua và chuyển sang kiểm tra tiếp
                break

            # Quản lý rate limiting theo deadline
            if event_number > 1:
                elapsed = self.monotonic_fn() - start_time
                interval = self._get_interval(elapsed)
                next_deadline += interval
                delay = next_deadline - self.monotonic_fn()
                if delay > 0:
                    self.sleep_fn(delay)
                else:
                    # Nếu bị chậm hơn target rate, cập nhật deadline để tránh dồn tích lũy âm
                    next_deadline = self.monotonic_fn()

            # Lấy dòng dữ liệu từ DataFrame (không sửa đổi DataFrame gốc)
            raw_row = self.dataframe.iloc[row_idx]

            # Áp dụng kịch bản mô phỏng
            scenario_res = apply_scenario(
                row=raw_row,
                scenario=self.settings.scenario,
                target_column=self.target_column,
                probability=self.settings.anomaly_probability,
                actual_power_multiplier=self.settings.actual_power_multiplier,
                rng=self.rng,
            )

            # Cập nhật con trỏ dữ liệu cho turbine
            if self.settings.loop_data:
                turbine_pointers[turbine_id] = (row_idx + 1) % num_rows
            else:
                turbine_pointers[turbine_id] = row_idx + 1

            yield SimulationEvent(
                event_number=event_number,
                turbine_id=turbine_id,
                source_row_index=row_idx,
                row=scenario_res.row,
                scenario=scenario_res.scenario,
                injected_anomaly=scenario_res.injected_anomaly,
            )

            event_number += 1
