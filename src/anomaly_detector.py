def detect_anomaly(
    actual_power: float,
    predicted_power: float,
    threshold: float,
) -> tuple[float, int]:
    """Tính toán sai số (residual) và xác định bất thường dựa trên ngưỡng cho trước.

    Args:
        actual_power: Công suất thực tế đo được.
        predicted_power: Công suất dự đoán từ mô hình.
        threshold: Ngưỡng chênh lệch công suất để xác định bất thường.

    Returns:
        tuple[float, int]: (residual, anomaly_flag) trong đó:
            - residual: Độ lệch tuyệt đối giữa công suất thực tế và dự đoán (float).
            - anomaly_flag: 1 nếu residual > threshold, ngược lại 0 (int).
    """
    residual = float(abs(actual_power - predicted_power))
    is_anomaly = 1 if residual > threshold else 0
    return residual, is_anomaly
