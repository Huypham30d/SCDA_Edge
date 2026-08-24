import pytest
from apps.producer_app import run_producer
from config.settings import KafkaSettings
from src.event_schema import ScadaEvent
from src.kafka_producer import ScadaKafkaProducer
from tests.test_kafka_producer import FakeKafkaProducerClient


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
        consumer_poll_timeout_seconds=1.0,
        consumer_max_processing_retries=5,
        consumer_retry_backoff_seconds=1.0,
    )


def test_producer_app_runs_and_emits_scada_events(kafka_settings):
    """Kiểm tra luồng điều phối của Producer App từ Simulator sang ScadaKafkaProducer."""
    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    counters = run_producer(
        cli_args=[
            "--max-events", "10",
            "--scenario", "normal",
            "--events-per-second", "100",
            "--run-id", "test-run-42",
            "--quiet",
        ],
        producer_instance=producer,
    )

    assert counters.events_generated == 10
    assert counters.produce_attempted == 10
    assert counters.kafka_acknowledged == 10
    assert counters.kafka_failed == 0
    assert counters.flush_remaining == 0

    assert len(fake_client.messages) == 10
    first_record = fake_client.messages[0]
    assert first_record["topic"] == "scada.raw.v1"

    # Giải mã event và kiểm tra cấu trúc
    event = ScadaEvent.from_json(first_record["value"])
    assert event.run_id == "test-run-42"
    assert event.event_id == "test-run-42:1"
    assert event.event_number == 1
    assert event.turbine_id.startswith("T")
    assert event.schema_version == 1
    assert event.emitted_at_ns > 0


def test_producer_app_anomaly_injection_scenario(kafka_settings):
    """Kiểm tra kịch bản anomaly_injection trong Producer App."""
    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    counters = run_producer(
        cli_args=[
            "--max-events", "20",
            "--scenario", "anomaly_injection",
            "--events-per-second", "100",
            "--quiet",
        ],
        producer_instance=producer,
    )

    assert counters.events_generated == 20
    assert counters.kafka_acknowledged == 20


def test_producer_app_measurement_timestamp_format(kafka_settings):
    """Xác nhận measurement_timestamp do Producer tạo ra luôn có định dạng '%d %m %Y %H:%M'."""
    from datetime import datetime
    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    run_producer(
        cli_args=[
            "--max-events", "5",
            "--scenario", "normal",
            "--events-per-second", "100",
            "--quiet",
        ],
        producer_instance=producer,
    )

    assert len(fake_client.messages) == 5
    for msg in fake_client.messages:
        event = ScadaEvent.from_json(msg["value"])
        # Kiểm tra không gây ValueError khi parse theo format chuẩn %d %m %Y %H:%M
        parsed_dt = datetime.strptime(event.measurement_timestamp, "%d %m %Y %H:%M")
        assert parsed_dt is not None


def test_producer_event_to_consumer_datetime_parsing_roundtrip(kafka_settings):
    """Xác nhận chuỗi round-trip: Producer serialize event -> Consumer parse datetime -> Feature Engineering."""
    import pandas as pd
    from src.feature_engineering import add_time_features

    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    run_producer(
        cli_args=[
            "--max-events", "3",
            "--scenario", "normal",
            "--events-per-second", "100",
            "--quiet",
        ],
        producer_instance=producer,
    )

    for msg in fake_client.messages:
        event = ScadaEvent.from_json(msg["value"])
        # Consumer parse logic
        dt = pd.to_datetime(event.measurement_timestamp, format="%d %m %Y %H:%M")
        raw_dict = {
            "Date/Time": dt,
            "Wind Speed (m/s)": event.wind_speed,
            "Theoretical_Power_Curve (KWh)": event.theoretical_power,
            "Wind Direction (?)": event.wind_direction,
            "LV ActivePower (kW)": event.actual_power,
        }
        df_single = pd.DataFrame([raw_dict])
        df_features = add_time_features(df_single)
        assert "Day_sin" in df_features.columns
        assert "Day_cos" in df_features.columns
        assert "Month_sin" in df_features.columns
        assert "Month_cos" in df_features.columns

