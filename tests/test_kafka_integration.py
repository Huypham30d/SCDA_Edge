import os
import time
import uuid
import pytest
from config.settings import KafkaSettings, load_kafka_settings
from src.event_schema import ScadaEvent
from src.kafka_consumer import ScadaKafkaConsumer
from src.kafka_producer import ScadaKafkaProducer

pytestmark = pytest.mark.integration


def is_kafka_integration_enabled() -> bool:
    """Kiểm tra xem biến môi trường cho phép chạy Kafka integration test hay không."""
    return os.getenv("RUN_KAFKA_INTEGRATION_TESTS", "false").strip().lower() in (
        "true", "1", "yes",
    )


@pytest.fixture
def live_kafka_settings() -> KafkaSettings:
    """Tải cấu hình Kafka thực tế và gán group ID duy nhất cho test."""
    if not is_kafka_integration_enabled():
        pytest.skip("Bỏ qua Kafka integration test (cần RUN_KAFKA_INTEGRATION_TESTS=true).")

    base_settings = load_kafka_settings()
    unique_group = f"test-group-{uuid.uuid4().hex[:8]}"
    return KafkaSettings(
        bootstrap_servers=base_settings.bootstrap_servers,
        raw_topic=base_settings.raw_topic,
        dlq_topic=base_settings.dlq_topic,
        consumer_group=unique_group,
        auto_offset_reset="latest",  # Chỉ đọc message mới tạo trong test
        producer_acks=base_settings.producer_acks,
        producer_enable_idempotence=base_settings.producer_enable_idempotence,
        producer_compression_type=base_settings.producer_compression_type,
        producer_linger_ms=0,
        producer_batch_size=base_settings.producer_batch_size,
        producer_delivery_timeout_ms=10000,
        consumer_enable_auto_commit=False,
        consumer_enable_auto_offset_store=False,
        consumer_poll_timeout_seconds=2.0,
        consumer_max_processing_retries=3,
        consumer_retry_backoff_seconds=0.5,
    )


def test_live_kafka_produce_and_consume(live_kafka_settings: KafkaSettings):
    """Kiểm tra end-to-end gửi và nhận message qua Kafka broker thật."""
    test_run_id = f"integration-{uuid.uuid4().hex[:6]}"
    event = ScadaEvent(
        schema_version=1,
        event_id=f"{test_run_id}:1",
        run_id=test_run_id,
        event_number=1,
        emitted_at_ns=time.time_ns(),
        measurement_timestamp="01 01 2018 00:00",
        turbine_id="T1",
        source_row_index=0,
        scenario="normal",
        injected_anomaly=False,
        wind_speed=9.2,
        theoretical_power=1500.0,
        wind_direction=210.0,
        actual_power=1480.0,
    )

    # 1. Khởi tạo consumer và subscribe trước để sẵn sàng nhận
    with ScadaKafkaConsumer(settings=live_kafka_settings) as consumer:
        consumer.subscribe()
        # Warmup poll
        consumer.poll(timeout=1.0)

        # 2. Gửi message bằng producer
        with ScadaKafkaProducer(settings=live_kafka_settings) as producer:
            producer.send(event)
            rem = producer.flush(timeout=5.0)
            assert rem == 0
            assert producer.produce_acknowledged >= 1

        # 3. Đọc message từ consumer
        start_time = time.monotonic()
        received_event = None
        consumed_msg = None

        while time.monotonic() - start_time < 10.0:
            msg = consumer.poll(timeout=1.0)
            if msg and msg.event and msg.event.event_id == event.event_id:
                received_event = msg.event
                consumed_msg = msg
                break

        assert received_event is not None, "Consumer không nhận được event gửi lên Kafka trong 10s!"
        assert received_event.event_id == event.event_id
        assert received_event.turbine_id == "T1"
        assert consumed_msg is not None
        assert consumed_msg.partition >= 0
        assert consumed_msg.offset >= 0

        # 4. Commit offset
        consumer.commit(consumed_msg)
