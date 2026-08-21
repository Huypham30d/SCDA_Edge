from src.anomaly_detector import detect_anomaly


def test_detect_anomaly_below_threshold():
    """Kiểm tra trường hợp residual < threshold -> anomaly_flag = 0."""
    residual, is_anomaly = detect_anomaly(
        actual_power=1000.0,
        predicted_power=950.0,
        threshold=100.0,
    )
    assert residual == 50.0
    assert is_anomaly == 0
    assert isinstance(residual, float)
    assert isinstance(is_anomaly, int)


def test_detect_anomaly_above_threshold():
    """Kiểm tra trường hợp residual > threshold -> anomaly_flag = 1."""
    residual, is_anomaly = detect_anomaly(
        actual_power=1500.0,
        predicted_power=1000.0,
        threshold=400.0,
    )
    assert residual == 500.0
    assert is_anomaly == 1
    assert isinstance(residual, float)
    assert isinstance(is_anomaly, int)


def test_detect_anomaly_exact_threshold():
    """Kiểm tra trường hợp residual == threshold -> anomaly_flag = 0 (toán tử >)."""
    residual, is_anomaly = detect_anomaly(
        actual_power=1100.0,
        predicted_power=1000.0,
        threshold=100.0,
    )
    assert residual == 100.0
    assert is_anomaly == 0
    assert isinstance(residual, float)
    assert isinstance(is_anomaly, int)


def test_detect_anomaly_negative_difference_yields_positive_residual():
    """Kiểm tra actual < predicted vẫn tạo residual dương nhờ abs()."""
    residual, is_anomaly = detect_anomaly(
        actual_power=800.0,
        predicted_power=1200.0,
        threshold=300.0,
    )
    assert residual == 400.0
    assert is_anomaly == 1
    assert isinstance(residual, float)
    assert isinstance(is_anomaly, int)


def test_detect_anomaly_return_types():
    """Kiểm tra chặt chẽ kiểu dữ liệu trả về."""
    residual, is_anomaly = detect_anomaly(
        actual_power=500,
        predicted_power=500,
        threshold=10,
    )
    assert type(residual) is float
    assert type(is_anomaly) is int
    assert residual == 0.0
    assert is_anomaly == 0
