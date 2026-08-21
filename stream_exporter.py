import time
from config.settings import (
    CONFIG_PATH,
    DATA_PATH,
    MODEL_PATH,
    SCALER_PATH,
    InfluxSettings,
    load_config,
)
from src.anomaly_detector import detect_anomaly
from src.data_loader import load_data
from src.feature_engineering import add_time_features
from src.inference import PowerPredictor
from src.influx_writer import InfluxWriter


def main() -> None:
    # 1. Tải cấu hình môi trường và cấu hình mô hình
    settings = InfluxSettings.from_env()
    config = load_config(CONFIG_PATH)

    threshold = config["anomaly_threshold"]
    features = config["features"]
    target = config["target"]

    # 2. Đọc dữ liệu và tạo đặc trưng thời gian
    df_raw = load_data(DATA_PATH, features=features, target=target)
    df = add_time_features(df_raw)

    # 3. Khởi tạo mô hình dự đoán
    predictor = PowerPredictor(
        model_path=MODEL_PATH,
        scaler_path=SCALER_PATH,
        features=features,
    )

    # 4. Khởi tạo InfluxDB writer và thực hiện vòng lặp streaming
    writer = InfluxWriter(
        url=settings.url,
        token=settings.token,
        org=settings.org,
        bucket=settings.bucket,
    )

    print("Bắt đầu đẩy dữ liệu SCADA lên InfluxDB...")

    try:
        for _, row in df.iterrows():
            actual_power = float(row[target])
            wind_speed = float(row["Wind Speed (m/s)"])

            # Dự đoán công suất
            pred_power = predictor.predict(row)

            # Phát hiện bất thường
            residual, is_anomaly = detect_anomaly(
                actual_power=actual_power,
                predicted_power=pred_power,
                threshold=threshold,
            )

            # Ghi dữ liệu lên InfluxDB
            writer.write_turbine_status(
                turbine_id=settings.turbine_id,
                wind_speed=wind_speed,
                actual_power=actual_power,
                predicted_power=pred_power,
                residual=residual,
                anomaly_flag=is_anomaly,
            )

            print(
                f"Đã gửi: Gió {wind_speed:5.1f}m/s | Lệch {residual:6.1f}kW | Cảnh báo: {is_anomaly}"
            )

            time.sleep(settings.stream_interval_seconds)

    except KeyboardInterrupt:
        print("\n Đã dừng stream exporter bởi người dùng.")
    finally:
        writer.close()


if __name__ == "__main__":
    main()