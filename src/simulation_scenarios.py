from dataclasses import dataclass
import random
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ScenarioResult:
    """Kết quả sau khi áp dụng kịch bản mô phỏng lên một mẫu dữ liệu."""

    row: pd.Series
    scenario: str
    injected_anomaly: bool


def apply_normal_scenario(row: pd.Series) -> ScenarioResult:
    """Áp dụng kịch bản bình thường: trả về bản sao không đổi, không chèn bất thường."""
    if not isinstance(row, pd.Series):
        raise TypeError(f"Dữ liệu đầu vào phải là pandas.Series. Nhận được: {type(row)}")

    return ScenarioResult(
        row=row.copy(),
        scenario="normal",
        injected_anomaly=False,
    )


def apply_anomaly_injection(
    row: pd.Series,
    probability: float,
    actual_power_multiplier: float,
    target_column: str,
    rng: random.Random,
) -> ScenarioResult:
    """Áp dụng kịch bản chèn bất thường vào cột công suất thực tế (target).

    Args:
        row: pandas.Series chứa dữ liệu một dòng.
        probability: Xác suất chèn bất thường [0.0, 1.0].
        actual_power_multiplier: Hệ số nhân vào giá trị target khi chèn bất thường.
        target_column: Tên cột mục tiêu cần nhân hệ số.
        rng: Instance của random.Random để đảm bảo tính tái lập.

    Returns:
        ScenarioResult chứa dòng dữ liệu mới và cờ injected_anomaly.

    Raises:
        ValueError: Nếu target_column không tồn tại hoặc không phải là giá trị số hợp lệ.
    """
    if not isinstance(row, pd.Series):
        raise TypeError(f"Dữ liệu đầu vào phải là pandas.Series. Nhận được: {type(row)}")

    if target_column not in row.index:
        raise ValueError(
            f"Không tìm thấy cột mục tiêu '{target_column}' trong dữ liệu hàng."
        )

    val = row[target_column]
    if (
        pd.isna(val)
        or isinstance(val, bool)
        or not isinstance(val, (int, float, np.number))
    ):
        raise ValueError(
            f"Giá trị của cột '{target_column}' không phải là số hợp lệ: {val}"
        )

    row_copy = row.copy()

    # Xác định có chèn bất thường hay không
    if probability >= 1.0:
        should_inject = True
    elif probability <= 0.0:
        should_inject = False
    else:
        should_inject = rng.random() < probability

    if should_inject:
        new_val = float(val) * float(actual_power_multiplier)
        if np.isnan(new_val) or np.isinf(new_val):
            raise ValueError(
                f"Giá trị sau khi nhân hệ số bất thường không hợp lệ (NaN/inf): {new_val}"
            )
        row_copy[target_column] = new_val
        return ScenarioResult(
            row=row_copy,
            scenario="anomaly_injection",
            injected_anomaly=True,
        )

    return ScenarioResult(
        row=row_copy,
        scenario="anomaly_injection",
        injected_anomaly=False,
    )


def apply_scenario(
    row: pd.Series,
    scenario: str,
    target_column: str,
    probability: float = 0.0,
    actual_power_multiplier: float = 1.0,
    rng: random.Random | None = None,
) -> ScenarioResult:
    """Hàm điều phối áp dụng kịch bản mô phỏng tương ứng."""
    if scenario == "normal":
        return apply_normal_scenario(row)
    elif scenario == "burst":
        if not isinstance(row, pd.Series):
            raise TypeError(
                f"Dữ liệu đầu vào phải là pandas.Series. Nhận được: {type(row)}"
            )
        return ScenarioResult(
            row=row.copy(),
            scenario="burst",
            injected_anomaly=False,
        )
    elif scenario == "anomaly_injection":
        if rng is None:
            rng = random.Random()
        return apply_anomaly_injection(
            row=row,
            probability=probability,
            actual_power_multiplier=actual_power_multiplier,
            target_column=target_column,
            rng=rng,
        )
    else:
        raise ValueError(
            f"Kịch bản không hợp lệ: '{scenario}'. "
            "Chỉ hỗ trợ các kịch bản: 'normal', 'burst', 'anomaly_injection'."
        )
