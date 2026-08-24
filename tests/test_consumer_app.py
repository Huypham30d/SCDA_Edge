import pytest
from apps.consumer_app import run_consumer
from config.settings import KafkaSettings
from src.event_schema import ScadaEvent
from src.kafka_consumer import ScadaKafkaConsumer
from tests.test_kafka_consumer import (
    FakeConsumerClient,
    FakeDLQProducerClient,
    FakeKafkaMessage,
)
from tests.test_stream_orchestration import FakePredictor, FakeWriter


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="localhost:9092",
        raw_topic="scada.raw.v1",
        dlq_topic="scada.dlq.v1",
        consumer_group="test-group",
        auto_offset_reset="earliest",
        producer_acks="all",
        producer_enable_idempotence=True,
        producer_compression_type="snappy",
        producer_linger_ms=10,
        producer_batch_size=65536,
        producer_delivery_timeout_ms=120000,
        consumer_enable_auto_commit=False,
        consumer_enable_auto_offset_store=False,
        consumer_poll_timeout_seconds=0.1,
        consumer_max_processing_retries=2,
        consumer_retry_backoff_seconds=0.01,
    )


def test_consumer_app_processes_events_and_commits_offset(kafka_settings):
    """Kiểm tra pipeline Consumer: Poll -> Feature Eng -> Predict -> Anomaly -> Influx -> Commit."""
    fake_consumer_client = FakeConsumerClient({})
    fake_dlq_client = FakeDLQProducerClient({})

    # Tạo 3 event hợp lệ trong queue
    events = [
        ScadaEvent(
            schema_version=1,
            event_id=f"run-test:{i}",
            run_id="run-test",
            event_number=i,
            emitted_at_ns=1700000000000000000 + i * 1000,
            measurement_timestamp="01 01 2018 00:00",
            turbine_id=f"T{i}",
            source_row_index=i - 1,
            scenario="normal",
            injected_anomaly=False,
            wind_speed=8.0 + i,
            theoretical_power=1000.0 * i,
            wind_direction=180.0,
            actual_power=950.0 * i,
        )
        for i in range(1, 4)
    ]

    for idx, ev in enumerate(events):
        fake_consumer_client.queue.append(
            FakeKafkaMessage(
                topic="scada.raw.v1",
                partition=1,
                offset=idx,
                value=ev.to_json().encode("utf-8"),
                key=ev.turbine_id.encode("utf-8"),
            )
        )

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer_client,
        dlq_producer_factory=lambda cfg: fake_dlq_client,
    )
    predictor = FakePredictor(fixed_prediction=900.0)
    writer = FakeWriter()

    counters = run_consumer(
        cli_args=["--max-messages", "3", "--quiet"],
        consumer_instance=consumer,
        predictor_instance=predictor,
        writer_instance=writer,
    )

    assert counters.messages_polled == 3
    assert counters.events_validated == 3
    assert counters.events_processed == 3
    assert counters.influx_write_success == 3
    assert counters.offset_commit_success == 3
    assert counters.dlq_success == 0

    # Kiểm tra dữ liệu được ghi lên InfluxDB
    assert len(writer.written_records) == 3
    first_record = writer.written_records[0]
    assert first_record["turbine_id"] == "T1"
    assert first_record["wind_speed"] == 9.0
    assert first_record["actual_power"] == 950.0
    assert first_record["predicted_power"] == 900.0


def test_consumer_app_handles_corrupted_message_to_dlq(kafka_settings):
    """Kiểm tra message sai schema được chuyển vào DLQ và commit offset tương ứng."""
    fake_consumer_client = FakeConsumerClient({})
    fake_dlq_client = FakeDLQProducerClient({})

    # Đưa 1 message hỏng vào queue
    fake_consumer_client.queue.append(
        FakeKafkaMessage(
            topic="scada.raw.v1",
            partition=0,
            offset=99,
            value=b"corrupted raw data string",
            key=b"T1",
        )
    )

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer_client,
        dlq_producer_factory=lambda cfg: fake_dlq_client,
    )
    predictor = FakePredictor()
    writer = FakeWriter()

    counters = run_consumer(
        cli_args=["--exit-after-idle-seconds", "0.2", "--quiet"],
        consumer_instance=consumer,
        predictor_instance=predictor,
        writer_instance=writer,
    )

    assert counters.messages_polled == 1
    assert counters.events_validated == 0
    assert counters.dlq_success == 1
    assert counters.offset_commit_success == 1
    assert len(fake_dlq_client.messages) == 1
    assert len(writer.written_records) == 0


def test_consumer_app_duplicate_replay_detection(kafka_settings):
    """Kiểm tra phát hiện message trùng lặp (replay)."""
    fake_consumer_client = FakeConsumerClient({})
    fake_dlq_client = FakeDLQProducerClient({})

    event = ScadaEvent(
        schema_version=1,
        event_id="duplicate-id-1",
        run_id="run-test",
        event_number=1,
        emitted_at_ns=1700000000000000000,
        measurement_timestamp="01 01 2018 00:00",
        turbine_id="T1",
        source_row_index=0,
        scenario="normal",
        injected_anomaly=False,
        wind_speed=8.0,
        theoretical_power=1000.0,
        wind_direction=180.0,
        actual_power=950.0,
    )

    # Đưa 2 message có cùng event_id
    fake_consumer_client.queue.append(
        FakeKafkaMessage(offset=1, value=event.to_json().encode("utf-8"))
    )
    fake_consumer_client.queue.append(
        FakeKafkaMessage(offset=2, value=event.to_json().encode("utf-8"))
    )

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer_client,
        dlq_producer_factory=lambda cfg: fake_dlq_client,
    )
    predictor = FakePredictor()
    writer = FakeWriter()

    counters = run_consumer(
        cli_args=["--max-messages", "2", "--quiet"],
        consumer_instance=consumer,
        predictor_instance=predictor,
        writer_instance=writer,
    )

    assert counters.events_processed == 2
    assert counters.duplicates_or_replays_observed == 1
