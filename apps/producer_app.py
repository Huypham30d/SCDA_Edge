import argparse
from dataclasses import dataclass, replace
import logging
import sys
import time
import uuid
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    CONFIG_PATH,
    DATA_PATH,
    KAFKA_CONFIG_PATH,
    SIMULATOR_CONFIG_PATH,
    KafkaSettings,
    SimulatorSettings,
    load_config,
    load_kafka_settings,
    load_simulator_settings,
)
from src.data_loader import load_data
from src.event_schema import ScadaEvent
from src.kafka_producer import ScadaKafkaProducer
from src.scada_simulator import SCADASimulator, SimulationEvent

logger = logging.getLogger("producer_app")


@dataclass
class ProducerAppCounters:
    """Bộ đếm theo dõi hiệu năng và kết quả của Producer App."""

    events_generated: int = 0
    produce_attempted: int = 0
    kafka_acknowledged: int = 0
    kafka_failed: int = 0
    injected_anomalies: int = 0
    flush_remaining: int = 0


def format_producer_log(event: ScadaEvent) -> str:
    """Định dạng chuỗi log chi tiết cho từng event gửi lên Kafka."""
    msg = (
        f"📤 [Producer] Gửi event {event.event_id} | "
        f"Turbine={event.turbine_id} | "
        f"Gió={event.wind_speed:5.1f} m/s | "
        f"P_thực={event.actual_power:7.1f} kW | "
        f"Kịch bản={event.scenario}"
    )
    if event.injected_anomaly:
        msg += " | Injected=True"
    return msg


def print_producer_summary(counters: ProducerAppCounters, elapsed_seconds: float) -> None:
    """In bảng tổng kết kết quả phát event lên Kafka."""
    throughput = (
        counters.kafka_acknowledged / elapsed_seconds if elapsed_seconds > 0 else 0.0
    )
    print("\n===== SCADA KAFKA PRODUCER SUMMARY =====")
    print(f"Generated:                 {counters.events_generated}")
    print(f"Produce attempted:         {counters.produce_attempted}")
    print(f"Kafka acknowledged:        {counters.kafka_acknowledged}")
    print(f"Kafka failed:              {counters.kafka_failed}")
    print(f"Injected anomalies:        {counters.injected_anomalies}")
    print(f"Flush remaining:           {counters.flush_remaining}")
    print(f"Elapsed:                   {elapsed_seconds:.2f} s")
    print(f"Actual produce throughput: {throughput:.2f} events/s")
    print("========================================\n")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse các tham số dòng lệnh cho Producer App."""
    parser = argparse.ArgumentParser(
        description="SCADA Simulator Kafka Producer Application",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Số lượng event tối đa cần sinh và gửi (ghi đè cấu hình simulator)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Mã định danh cho phiên chạy (mặc định tự động sinh UUID ngắn)",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        choices=["normal", "burst", "anomaly_injection"],
        help="Kịch bản mô phỏng cần chạy (ghi đè cấu hình simulator)",
    )
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=None,
        help="Tốc độ phát dữ liệu (events/giây)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Địa chỉ Kafka bootstrap servers (ghi đè cấu hình kafka)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Bật chế độ log gọn khi chạy ở tải cao",
    )
    return parser.parse_args(args)


def run_producer(
    cli_args: list[str] | None = None,
    producer_instance: ScadaKafkaProducer | None = None,
) -> ProducerAppCounters:
    """Hàm thực thi chính của Producer App."""
    args = parse_args(cli_args)

    # 1. Tải cấu hình
    config = load_config(CONFIG_PATH)
    sim_settings: SimulatorSettings = load_simulator_settings(SIMULATOR_CONFIG_PATH)
    kafka_settings: KafkaSettings = load_kafka_settings(KAFKA_CONFIG_PATH)

    # 2. Áp dụng CLI overrides
    if args.max_events is not None:
        sim_settings = replace(sim_settings, max_events=args.max_events)
    if args.scenario is not None:
        sim_settings = replace(sim_settings, scenario=args.scenario)
    if args.events_per_second is not None:
        sim_settings = replace(sim_settings, events_per_second=args.events_per_second)
    if args.bootstrap_servers is not None:
        kafka_settings = replace(kafka_settings, bootstrap_servers=args.bootstrap_servers)

    run_id = args.run_id or f"run-{uuid.uuid4().hex[:8]}"

    target_column = str(config["target"])
    features = list(config["features"])

    # 3. Đọc dữ liệu raw từ CSV (chưa chạy add_time_features)
    df_raw = load_data(DATA_PATH, features=features, target=target_column)

    # 4. Khởi tạo simulator
    simulator = SCADASimulator(
        dataframe=df_raw,
        settings=sim_settings,
        target_column=target_column,
    )

    # 5. Khởi tạo Kafka Producer
    producer = producer_instance or ScadaKafkaProducer(settings=kafka_settings)

    counters = ProducerAppCounters()
    print(
        f"🚀 Khởi động SCADA Kafka Producer | Run ID: {run_id} | "
        f"Topic: {kafka_settings.raw_topic} | Turbines: {sim_settings.turbine_count} | "
        f"Kịch bản: '{sim_settings.scenario}' | Rate: {sim_settings.events_per_second} events/s"
    )

    start_time = time.monotonic()

    try:
        for sim_event in simulator.generate():
            counters.events_generated += 1
            if sim_event.injected_anomaly:
                counters.injected_anomalies += 1

            # Lấy các trường dữ liệu thô từ row
            wind_speed = float(sim_event.row["Wind Speed (m/s)"])
            theoretical_power = float(sim_event.row["Theoretical_Power_Curve (KWh)"])
            wind_direction = float(sim_event.row["Wind Direction (?)"])
            actual_power = float(sim_event.row[target_column])
            date_time_val = sim_event.row["Date/Time"]
            if hasattr(date_time_val, "strftime"):
                measurement_ts = date_time_val.strftime("%d %m %Y %H:%M")
            else:
                measurement_ts = pd.to_datetime(date_time_val).strftime("%d %m %Y %H:%M")

            # Tạo ScadaEvent chuẩn
            scada_event = ScadaEvent(
                schema_version=1,
                event_id=f"{run_id}:{sim_event.event_number}",
                run_id=run_id,
                event_number=sim_event.event_number,
                emitted_at_ns=time.time_ns(),
                measurement_timestamp=measurement_ts,
                turbine_id=sim_event.turbine_id,
                source_row_index=sim_event.source_row_index,
                scenario=sim_event.scenario,
                injected_anomaly=sim_event.injected_anomaly,
                wind_speed=wind_speed,
                theoretical_power=theoretical_power,
                wind_direction=wind_direction,
                actual_power=actual_power,
            )

            # Gửi lên Kafka
            producer.send(scada_event)
            producer.poll(0)

            counters.produce_attempted = producer.produce_attempted
            counters.kafka_acknowledged = producer.produce_acknowledged
            counters.kafka_failed = producer.produce_failed

            if not args.quiet:
                print(format_producer_log(scada_event))

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Producer App bởi người dùng (Ctrl+C).")
    finally:
        # Flush message còn lại trong buffer
        print("⏳ Đang hoàn tất xả buffer Kafka producer...")
        remaining = producer.flush(timeout=10.0)
        counters.flush_remaining = remaining
        counters.produce_attempted = producer.produce_attempted
        counters.kafka_acknowledged = producer.produce_acknowledged
        counters.kafka_failed = producer.produce_failed

        elapsed_time = time.monotonic() - start_time
        print_producer_summary(counters, elapsed_time)

        if producer_instance is None:
            producer.close()

    return counters


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_producer()


if __name__ == "__main__":
    main()
