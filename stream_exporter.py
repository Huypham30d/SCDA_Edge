import sys
import time
from dataclasses import dataclass
from typing import Iterator
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    CONFIG_PATH,
    DATA_PATH,
    MODEL_PATH,
    SCALER_PATH,
    SIMULATOR_CONFIG_PATH,
    InfluxSettings,
    SimulatorSettings,
    load_config,
    load_simulator_settings,
)
from src.anomaly_detector import detect_anomaly
from src.data_loader import load_data
from src.feature_engineering import add_time_features
from src.inference import PowerPredictor
from src.influx_writer import InfluxWriter
from src.scada_simulator import SCADASimulator, SimulationEvent


@dataclass
class StreamCounters:
    """Theo dõi số lượng event qua các giai đoạn trong pipeline."""

    events_generated: int = 0
    events_processed: int = 0
    influx_write_success: int = 0
    influx_write_failed: int = 0
    injected_anomalies: int = 0
    detected_anomalies: int = 0


def format_event_log(
    event: SimulationEvent,
    wind_speed: float,
    actual_power: float,
    predicted_power: float,
    residual: float,
    is_anomaly: int,
) -> str:
    """Định dạng chuỗi log chi tiết cho từng event gửi đi."""
    log_msg = (
        f"Đã gửi [{event.turbine_id}] | "
        f"Event {event.event_number} | "
        f"Gió {wind_speed:5.1f} m/s | "
        f"Thực tế {actual_power:7.1f} kW | "
        f"Dự đoán {predicted_power:7.1f} kW | "
        f"Lệch {residual:6.1f} kW | "
        f"Cảnh báo {is_anomaly} | "
        f"Kịch bản {event.scenario}"
    )
    if event.injected_anomaly:
        log_msg += " | Injected=True"
    return log_msg


def print_summary(counters: StreamCounters) -> None:
    """In bảng tổng kết các bộ đếm khi pipeline dừng hoặc kết thúc."""
    print("\n===== SCADA STREAM SUMMARY =====")
    print(f"Generated:          {counters.events_generated}")
    print(f"Processed:          {counters.events_processed}")
    print(f"Influx success:     {counters.influx_write_success}")
    print(f"Influx failed:      {counters.influx_write_failed}")
    print(f"Injected anomalies: {counters.injected_anomalies}")
    print(f"Detected anomalies: {counters.detected_anomalies}")
    print("===============================\n")


def create_legacy_stream(
    df: pd.DataFrame,
    turbine_id: str,
    interval_seconds: float,
) -> Iterator[SimulationEvent]:
    """Tạo generator cho chế độ một turbine truyền thống (khi simulator tắt)."""
    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        if idx > 1 and interval_seconds > 0:
            time.sleep(interval_seconds)
        yield SimulationEvent(
            event_number=idx,
            turbine_id=turbine_id,
            source_row_index=idx - 1,
            row=row.copy(),
            scenario="legacy_single_turbine",
            injected_anomaly=False,
        )


def process_event(
    event: SimulationEvent,
    predictor: PowerPredictor,
    writer: InfluxWriter,
    threshold: float,
    target_column: str,
    counters: StreamCounters,
) -> None:
    """Xử lý suy luận, phát hiện bất thường và ghi InfluxDB cho một SimulationEvent."""
    counters.events_generated += 1
    if event.injected_anomaly:
        counters.injected_anomalies += 1

    actual_power = float(event.row[target_column])
    wind_speed = float(event.row["Wind Speed (m/s)"])

    # Dự đoán công suất
    pred_power = predictor.predict(event.row)

    # Phát hiện bất thường
    residual, is_anomaly = detect_anomaly(
        actual_power=actual_power,
        predicted_power=pred_power,
        threshold=threshold,
    )

    counters.events_processed += 1
    if is_anomaly == 1:
        counters.detected_anomalies += 1

    # Ghi dữ liệu lên InfluxDB
    try:
        writer.write_turbine_status(
            turbine_id=event.turbine_id,
            wind_speed=wind_speed,
            actual_power=actual_power,
            predicted_power=pred_power,
            residual=residual,
            anomaly_flag=is_anomaly,
        )
        counters.influx_write_success += 1
    except Exception as exc:
        counters.influx_write_failed += 1
        print(
            f"❌ Lỗi ghi InfluxDB cho [{event.turbine_id}] (Event {event.event_number}): {exc}"
        )
        raise

    # Ghi log kết quả event
    print(
        format_event_log(
            event=event,
            wind_speed=wind_speed,
            actual_power=actual_power,
            predicted_power=pred_power,
            residual=residual,
            is_anomaly=is_anomaly,
        )
    )


def main() -> None:
    # 1. Tải cấu hình InfluxDB, cấu hình mô hình và cấu hình simulator
    settings = InfluxSettings.from_env()
    config = load_config(CONFIG_PATH)
    sim_settings: SimulatorSettings = load_simulator_settings(SIMULATOR_CONFIG_PATH)

    threshold = float(config["anomaly_threshold"])
    features = list(config["features"])
    target = str(config["target"])

    # 2. Đọc dữ liệu và tạo đặc trưng thời gian
    df_raw = load_data(DATA_PATH, features=features, target=target)
    df = add_time_features(df_raw)

    # 3. Khởi tạo mô hình dự đoán (tải một lần duy nhất)
    predictor = PowerPredictor(
        model_path=MODEL_PATH,
        scaler_path=SCALER_PATH,
        features=features,
    )

    # 4. Khởi tạo InfluxDB writer (tải một lần duy nhất)
    writer = InfluxWriter(
        url=settings.url,
        token=settings.token,
        org=settings.org,
        bucket=settings.bucket,
    )

    # 5. Khởi tạo luồng sự kiện
    if sim_settings.enabled:
        print(
            f"Khởi động SCADA Simulator: {sim_settings.turbine_count} turbines | "
            f"{sim_settings.events_per_second} events/s | Kịch bản: '{sim_settings.scenario}'..."
        )
        simulator = SCADASimulator(
            dataframe=df,
            settings=sim_settings,
            target_column=target,
        )
        event_stream = simulator.generate()
    else:
        print(
            f"Chạy chế độ đơn turbine: {settings.turbine_id} | "
            f"Khoảng cách: {settings.stream_interval_seconds}s..."
        )
        event_stream = create_legacy_stream(
            df=df,
            turbine_id=settings.turbine_id,
            interval_seconds=settings.stream_interval_seconds,
        )

    counters = StreamCounters()
    print("Bắt đầu đẩy dữ liệu SCADA lên InfluxDB...")

    try:
        for event in event_stream:
            process_event(
                event=event,
                predictor=predictor,
                writer=writer,
                threshold=threshold,
                target_column=target,
                counters=counters,
            )
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng stream exporter bởi người dùng.")
    finally:
        print_summary(counters)
        writer.close()


if __name__ == "__main__":
    main()