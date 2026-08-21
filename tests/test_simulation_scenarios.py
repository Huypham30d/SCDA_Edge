import random
import numpy as np
import pandas as pd
import pytest

from src.simulation_scenarios import (
    ScenarioResult,
    apply_anomaly_injection,
    apply_normal_scenario,
    apply_scenario,
)


@pytest.fixture
def sample_row() -> pd.Series:
    """Fixture cung cấp một Series mẫu đầy đủ đặc trưng."""
    return pd.Series(
        {
            "Wind Speed (m/s)": 10.5,
            "Theoretical_Power_Curve (KWh)": 2500.0,
            "Wind Direction (?)": 180.0,
            "Day_sin": 0.5,
            "Day_cos": 0.866,
            "Month_sin": 0.0,
            "Month_cos": 1.0,
            "LV ActivePower (kW)": 2000.0,
        }
    )


def test_normal_scenario_does_not_modify_data(sample_row: pd.Series):
    """Kiểm tra normal scenario trả về giá trị khớp và không thay đổi."""
    result = apply_normal_scenario(sample_row)

    assert isinstance(result, ScenarioResult)
    assert result.scenario == "normal"
    assert result.injected_anomaly is False
    assert result.row["LV ActivePower (kW)"] == 2000.0
    pd.testing.assert_series_equal(result.row, sample_row)


def test_normal_scenario_returns_independent_copy(sample_row: pd.Series):
    """Kiểm tra normal scenario trả về bản sao độc lập của row."""
    result = apply_normal_scenario(sample_row)
    result.row["LV ActivePower (kW)"] = 9999.0

    assert sample_row["LV ActivePower (kW)"] == 2000.0


def test_anomaly_injection_probability_zero(sample_row: pd.Series):
    """Kiểm tra xác suất = 0 không bao giờ chèn bất thường."""
    rng = random.Random(42)
    result = apply_anomaly_injection(
        row=sample_row,
        probability=0.0,
        actual_power_multiplier=2.0,
        target_column="LV ActivePower (kW)",
        rng=rng,
    )

    assert result.injected_anomaly is False
    assert result.scenario == "anomaly_injection"
    assert result.row["LV ActivePower (kW)"] == 2000.0
    pd.testing.assert_series_equal(result.row, sample_row)


def test_anomaly_injection_probability_one(sample_row: pd.Series):
    """Kiểm tra xác suất = 1 luôn luôn chèn bất thường."""
    rng = random.Random(42)
    result = apply_anomaly_injection(
        row=sample_row,
        probability=1.0,
        actual_power_multiplier=2.5,
        target_column="LV ActivePower (kW)",
        rng=rng,
    )

    assert result.injected_anomaly is True
    assert result.scenario == "anomaly_injection"
    assert result.row["LV ActivePower (kW)"] == 5000.0


def test_anomaly_injection_multiplies_target_correctly(sample_row: pd.Series):
    """Kiểm tra nhân đúng hệ số vào cột target."""
    rng = random.Random(42)
    multiplier = 1.8
    result = apply_anomaly_injection(
        row=sample_row,
        probability=1.0,
        actual_power_multiplier=multiplier,
        target_column="LV ActivePower (kW)",
        rng=rng,
    )

    expected_power = 2000.0 * 1.8
    assert np.isclose(result.row["LV ActivePower (kW)"], expected_power)


def test_anomaly_injection_preserves_other_features(sample_row: pd.Series):
    """Kiểm tra toàn bộ các feature khác ngoài target không bị ảnh hưởng."""
    rng = random.Random(42)
    result = apply_anomaly_injection(
        row=sample_row,
        probability=1.0,
        actual_power_multiplier=2.0,
        target_column="LV ActivePower (kW)",
        rng=rng,
    )

    for col in sample_row.index:
        if col != "LV ActivePower (kW)":
            assert result.row[col] == sample_row[col]


def test_anomaly_injection_does_not_mutate_original_row(sample_row: pd.Series):
    """Kiểm tra row gốc không bị sửa đổi sau khi chèn bất thường."""
    original_target = sample_row["LV ActivePower (kW)"]
    rng = random.Random(42)

    apply_anomaly_injection(
        row=sample_row,
        probability=1.0,
        actual_power_multiplier=3.0,
        target_column="LV ActivePower (kW)",
        rng=rng,
    )

    assert sample_row["LV ActivePower (kW)"] == original_target


def test_missing_target_raises_value_error():
    """Kiểm tra báo lỗi khi không tìm thấy cột target trong row."""
    row = pd.Series({"Wind Speed (m/s)": 10.0})
    rng = random.Random(42)

    with pytest.raises(ValueError, match="Không tìm thấy cột mục tiêu"):
        apply_anomaly_injection(
            row=row,
            probability=1.0,
            actual_power_multiplier=2.0,
            target_column="LV ActivePower (kW)",
            rng=rng,
        )


def test_non_numeric_target_raises_value_error():
    """Kiểm tra báo lỗi khi giá trị của target không phải là số hợp lệ."""
    row = pd.Series({"LV ActivePower (kW)": "invalid_number"})
    rng = random.Random(42)

    with pytest.raises(ValueError, match="không phải là số hợp lệ"):
        apply_anomaly_injection(
            row=row,
            probability=1.0,
            actual_power_multiplier=2.0,
            target_column="LV ActivePower (kW)",
            rng=rng,
        )


def test_reproducible_with_same_random_seed(sample_row: pd.Series):
    """Kiểm tra tính tái lập khi dùng cùng random seed."""
    rng1 = random.Random(123)
    rng2 = random.Random(123)

    results1 = [
        apply_anomaly_injection(
            sample_row, 0.5, 2.0, "LV ActivePower (kW)", rng1
        ).injected_anomaly
        for _ in range(20)
    ]
    results2 = [
        apply_anomaly_injection(
            sample_row, 0.5, 2.0, "LV ActivePower (kW)", rng2
        ).injected_anomaly
        for _ in range(20)
    ]

    assert results1 == results2


def test_invalid_scenario_raises_value_error(sample_row: pd.Series):
    """Kiểm tra báo lỗi ValueError khi scenario không hợp lệ."""
    with pytest.raises(ValueError, match="Kịch bản không hợp lệ"):
        apply_scenario(
            row=sample_row,
            scenario="unknown_scenario",
            target_column="LV ActivePower (kW)",
        )


def test_burst_scenario_returns_unchanged_row(sample_row: pd.Series):
    """Kiểm tra scenario burst trả về dữ liệu không đổi với nhãn scenario='burst'."""
    result = apply_scenario(
        row=sample_row,
        scenario="burst",
        target_column="LV ActivePower (kW)",
    )
    assert result.scenario == "burst"
    assert result.injected_anomaly is False
    pd.testing.assert_series_equal(result.row, sample_row)
