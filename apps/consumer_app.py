import argparse
from collections import OrderedDict
from dataclasses import dataclass, replace
import logging
import sys
import time
from typing import Any
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    CONFIG_PATH,
    KAFKA_CONFIG_PATH,
    MODEL_PATH,
    SCALER_PATH,
    InfluxSettings,
    KafkaSettings,
    load_config,
    load_kafka_settings,
)
from src.anomaly_detector import detect_anomaly
from src.event_schema import EventSchemaError, ScadaEvent
from src.feature_engineering import add_time_features
from src.inference import PowerPredictor
from src.influx_writer import InfluxWriter
from src.kafka_consumer import ConsumedMessage, ScadaKafkaConsumer

logger = logging.getLogger("consumer_app")


@dataclass
class ConsumerAppCounters:
    """Bộ đếm theo dõi toàn diện trạng thái xử lý của Consumer App."""

    messages_polled: int = 0
    events_validated: int = 0
    events_processed: int = 0
    influx_write_success: int = 0
    influx_write_failed: int = 0
    offset_commit_success: int = 0
    offset_commit_failed: int = 0
    dlq_success: int = 0
    dlq_failed: int = 0
    detected_anomalies: int = 0
    duplicates_or_replays_observed: int = 0


def format_consumer_log(
    turbine_id: str,
    event_id: str,
    partition: int,
    offset: int,
    actual_power: float,
    predicted_power: float,
    residual: float,
    anomaly_flag: int,
    latency_ms: float,
    scenario: str,
) -> str:
    """Định dạng log chuẩn hóa cho mỗi event được xử lý qua pipeline."""
    return (
        f"📥 [Consumer] [{turbine_id}] | "
        f"Event={event_id} | "
        f"Part={partition} Offset={offset} | "
        f"Thực tế={actual_power:7.1f} kW | "
        f"Dự đoán={predicted_power:7.1f} kW | "
        f"Lệch={residual:6.1f} kW | "
        f"Cảnh báo={anomaly_flag} | "
        f"Trễ E2E={latency_ms:6.1f} ms | "
        f"Kịch bản={scenario}"
    )


def print_consumer_summary(counters: ConsumerAppCounters, elapsed_seconds: float) -> None:
    """In bảng tổng kết kết quả xử lý của Kafka Consumer."""
    throughput = (
        counters.events_processed / elapsed_seconds if elapsed_seconds > 0 else 0.0
    )
    print("\n===== SCADA KAFKA CONSUMER SUMMARY =====")
    print(f"Messages polled:               {counters.messages_polled}")
    print(f"Events validated:              {counters.events_validated}")
    print(f"Events processed:              {counters.events_processed}")
    print(f"Influx write success:          {counters.influx_write_success}")
    print(f"Influx write failed:           {counters.influx_write_failed}")
    print(f"Offset commit success:         {counters.offset_commit_success}")
    print(f"Offset commit failed:          {counters.offset_commit_failed}")
    print(f"DLQ success:                   {counters.dlq_success}")
    print(f"DLQ failed:                    {counters.dlq_failed}")
    print(f"Detected anomalies:            {counters.detected_anomalies}")
    print(f"Duplicates/Replays observed:   {counters.duplicates_or_replays_observed}")
    print(f"Elapsed:                       {elapsed_seconds:.2f} s")
    print(f"Actual processing throughput:  {throughput:.2f} events/s")
    print("========================================\n")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse các tham số dòng lệnh cho Consumer App."""
    parser = argparse.ArgumentParser(
        description="SCADA Kafka ML Consumer Application",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Số lượng message tối đa cần xử lý trước khi tự động dừng",
    )
    parser.add_argument(
        "--exit-after-idle-seconds",
        type=float,
        default=None,
        help="Tự động dừng nếu không có message mới sau số giây này (phục vụ smoke/integration test)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Địa chỉ Kafka bootstrap servers (ghi đè cấu hình)",
    )
    parser.add_argument(
        "--consumer-group",
        type=str,
        default=None,
        help="Kafka Consumer Group ID (ghi đè cấu hình)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Bật chế độ log gọn khi chạy ở tải cao",
    )
    return parser.parse_args(args)


def run_consumer(
    cli_args: list[str] | None = None,
    consumer_instance: ScadaKafkaConsumer | None = None,
    predictor_instance: PowerPredictor | None = None,
    writer_instance: InfluxWriter | None = None,
) -> ConsumerAppCounters:
    """Hàm thực thi chính của Consumer App."""
    args = parse_args(cli_args)

    # 1. Tải cấu hình
    config = load_config(CONFIG_PATH)
    kafka_settings: KafkaSettings = load_kafka_settings(KAFKA_CONFIG_PATH)
    influx_settings = InfluxSettings.from_env()

    if args.bootstrap_servers is not None:
        kafka_settings = replace(kafka_settings, bootstrap_servers=args.bootstrap_servers)
    if args.consumer_group is not None:
        kafka_settings = replace(kafka_settings, consumer_group=args.consumer_group)

    threshold = float(config["anomaly_threshold"])
    features = list(config["features"])
    target_column = str(config["target"])

    # 2. Khởi tạo mô hình và InfluxDB writer (tải 1 lần duy nhất)
    predictor = predictor_instance or PowerPredictor(
        model_path=MODEL_PATH,
        scaler_path=SCALER_PATH,
        features=features,
    )

    writer = writer_instance or InfluxWriter(
        url=influx_settings.url,
        token=influx_settings.token,
        org=influx_settings.org,
        bucket=influx_settings.bucket,
    )

    # 3. Khởi tạo Kafka Consumer
    consumer = consumer_instance or ScadaKafkaConsumer(settings=kafka_settings)
    consumer.subscribe()

    counters = ConsumerAppCounters()
    seen_event_ids: OrderedDict[str, None] = OrderedDict()
    max_seen_cache = 100_000

    print(
        f"🚀 Khởi động SCADA Kafka Consumer | Group: {kafka_settings.consumer_group} | "
        f"Topic: {kafka_settings.raw_topic} | DLQ: {kafka_settings.dlq_topic} | "
        f"AutoCommit=False (Synchronous commit after Influx write)"
    )

    start_time = time.monotonic()
    last_message_time = time.monotonic()

    try:
        while True:
            # Kiểm tra điều kiện max_messages
            if args.max_messages is not None and counters.events_processed >= args.max_messages:
                print(f"✅ Đã đạt giới hạn --max-messages={args.max_messages}. Dừng consumer.")
                break

            # Poll message từ Kafka
            msg = consumer.poll(timeout=kafka_settings.consumer_poll_timeout_seconds)

            if msg is None:
                # Kiểm tra idle timeout nếu có
                if args.exit_after_idle_seconds is not None:
                    idle_duration = time.monotonic() - last_message_time
                    if idle_duration >= args.exit_after_idle_seconds:
                        print(
                            f"⏱️ Không nhận được message mới sau {idle_duration:.1f}s "
                            f"(idle threshold: {args.exit_after_idle_seconds}s). Dừng consumer."
                        )
                        break
                time.sleep(0.01)
                continue

            if msg.is_eof:
                continue

            counters.messages_polled += 1
            last_message_time = time.monotonic()

            # Xử lý nếu message gặp lỗi giải mã hoặc schema sai
            if msg.error is not None:
                err_type = type(msg.error).__name__
                err_msg = str(msg.error)
                logger.warning(
                    "Phát hiện message lỗi tại topic=%s part=%d offset=%d: %s. Chuyển vào DLQ...",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    err_msg,
                )
                dlq_ok = consumer.send_to_dlq(
                    raw_payload=msg.raw_payload,
                    error_type=err_type,
                    error_message=err_msg,
                    original_topic=msg.topic,
                    original_partition=msg.partition,
                    original_offset=msg.offset,
                )
                if dlq_ok:
                    counters.dlq_success += 1
                    try:
                        consumer.commit(msg)
                        counters.offset_commit_success += 1
                    except Exception as commit_exc:
                        counters.offset_commit_failed += 1
                        logger.error("Lỗi commit offset sau khi ghi DLQ: %s", commit_exc)
                else:
                    counters.dlq_failed += 1
                    logger.error(
                        "Ghi DLQ thất bại cho message tại offset=%d! Không commit offset.",
                        msg.offset,
                    )
                continue

            # Event hợp lệ
            event: ScadaEvent = msg.event  # type: ignore
            counters.events_validated += 1

            # Kiểm tra trùng lặp (replay/duplicate detection)
            if event.event_id in seen_event_ids:
                counters.duplicates_or_replays_observed += 1
            else:
                seen_event_ids[event.event_id] = None
                if len(seen_event_ids) > max_seen_cache:
                    seen_event_ids.popitem(last=False)

            # Chuyển đổi event sang DataFrame và thực hiện Feature Engineering
            try:
                # Tạo Series hoặc DataFrame 1 dòng
                raw_dict = {
                    "Date/Time": pd.to_datetime(event.measurement_timestamp, format="%d %m %Y %H:%M"),
                    "Wind Speed (m/s)": event.wind_speed,
                    "Theoretical_Power_Curve (KWh)": event.theoretical_power,
                    "Wind Direction (?)": event.wind_direction,
                    target_column: event.actual_power,
                }
                df_single = pd.DataFrame([raw_dict])
                df_features = add_time_features(df_single)
                row_features = df_features.iloc[0]

                # Dự đoán công suất
                pred_power = predictor.predict(row_features)

                # Phát hiện bất thường
                residual, is_anomaly = detect_anomaly(
                    actual_power=event.actual_power,
                    predicted_power=pred_power,
                    threshold=threshold,
                )
            except Exception as ml_exc:
                logger.error("Lỗi suy luận ML cho event %s: %s. Chuyển vào DLQ...", event.event_id, ml_exc)
                dlq_ok = consumer.send_to_dlq(
                    raw_payload=msg.raw_payload,
                    error_type=type(ml_exc).__name__,
                    error_message=str(ml_exc),
                    original_topic=msg.topic,
                    original_partition=msg.partition,
                    original_offset=msg.offset,
                )
                if dlq_ok:
                    counters.dlq_success += 1
                    try:
                        consumer.commit(msg)
                        counters.offset_commit_success += 1
                    except Exception as commit_exc:
                        counters.offset_commit_failed += 1
                        logger.error("Lỗi commit offset sau khi ghi DLQ: %s", commit_exc)
                else:
                    counters.dlq_failed += 1
                continue

            # Ghi InfluxDB với cơ chế Retry & Backoff
            now_ns = time.time_ns()
            latency_ms = (now_ns - event.emitted_at_ns) / 1_000_000.0 if event.emitted_at_ns > 0 else 0.0

            write_success = False
            retries = 0
            while not write_success:
                try:
                    writer.write_turbine_status(
                        turbine_id=event.turbine_id,
                        wind_speed=event.wind_speed,
                        actual_power=event.actual_power,
                        predicted_power=pred_power,
                        residual=residual,
                        anomaly_flag=is_anomaly,
                        timestamp_ns=event.emitted_at_ns,
                        pipeline="kafka",
                        scenario=event.scenario,
                        event_id=event.event_id,
                        source_row_index=event.source_row_index,
                        kafka_partition=msg.partition,
                        kafka_offset=msg.offset,
                        injected_anomaly=event.injected_anomaly,
                        end_to_end_latency_ms=latency_ms,
                    )
                    write_success = True
                    counters.influx_write_success += 1
                except Exception as db_exc:
                    retries += 1
                    counters.influx_write_failed += 1
                    logger.error(
                        "❌ Lỗi ghi InfluxDB cho [%s] (offset=%d, lần thử=%d): %s. Chờ thử lại...",
                        event.turbine_id,
                        msg.offset,
                        retries,
                        db_exc,
                    )
                    # Chờ backoff trước khi thử lại; cho phép dừng an toàn nếu nhận KeyboardInterrupt
                    time.sleep(kafka_settings.consumer_retry_backoff_seconds)

            # Chỉ commit offset khi InfluxDB write thành công
            try:
                consumer.commit(msg)
                counters.offset_commit_success += 1
            except Exception as commit_exc:
                counters.offset_commit_failed += 1
                logger.error("❌ Lỗi commit offset sau khi ghi DB thành công: %s", commit_exc)
                raise

            counters.events_processed += 1
            if is_anomaly == 1:
                counters.detected_anomalies += 1

            if not args.quiet:
                print(
                    format_consumer_log(
                        turbine_id=event.turbine_id,
                        event_id=event.event_id,
                        partition=msg.partition,
                        offset=msg.offset,
                        actual_power=event.actual_power,
                        predicted_power=pred_power,
                        residual=residual,
                        anomaly_flag=is_anomaly,
                        latency_ms=latency_ms,
                        scenario=event.scenario,
                    )
                )

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Consumer App bởi người dùng (Ctrl+C).")
    finally:
        elapsed_time = time.monotonic() - start_time
        print_consumer_summary(counters, elapsed_time)

        if consumer_instance is None:
            consumer.close()
        if writer_instance is None:
            writer.close()

    return counters


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_consumer()


if __name__ == "__main__":
    main()
