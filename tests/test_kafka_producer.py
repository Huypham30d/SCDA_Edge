import json
import pytest
from config.settings import KafkaSettings
from src.event_schema import ScadaEvent
from src.kafka_producer import ScadaKafkaProducer


class FakeKafkaProducerClient:
    """Mock của confluent_kafka.Producer cho unit tests."""

    def __init__(self, config: dict):
        self.config = config
        self.messages: list[dict] = []
        self.flush_called = False
        self.fail_next_produce = False
        self.raise_buffer_error_times = 0
        self.poll_called_count = 0

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery=None):
        if self.raise_buffer_error_times > 0:
            self.raise_buffer_error_times -= 1
            raise BufferError("Producer queue full")

        record = {
            "topic": topic,
            "key": key,
            "value": value,
            "on_delivery": on_delivery,
        }
        self.messages.append(record)

        if on_delivery is not None:
            if self.fail_next_produce:
                on_delivery(RuntimeError("Delivery simulated failure"), self)
            else:
                on_delivery(None, self)

    def poll(self, timeout: float = 0):
        self.poll_called_count += 1
        return 0

    def flush(self, timeout: float = 10):
        self.flush_called = True
        return 0


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


@pytest.fixture
def sample_scada_event() -> ScadaEvent:
    return ScadaEvent(
        schema_version=1,
        event_id="run-1:1",
        run_id="run-1",
        event_number=1,
        emitted_at_ns=1700000000000000000,
        measurement_timestamp="01 01 2018 00:00",
        turbine_id="T2",
        source_row_index=0,
        scenario="normal",
        injected_anomaly=False,
        wind_speed=8.5,
        theoretical_power=1200.0,
        wind_direction=180.0,
        actual_power=1150.0,
    )


def test_producer_send_event_success(kafka_settings, sample_scada_event):
    """Kiểm tra producer gửi event thành công, đúng topic, key và value."""
    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    producer.send(sample_scada_event)

    assert len(fake_client.messages) == 1
    sent_msg = fake_client.messages[0]
    assert sent_msg["topic"] == "scada.raw.v1"
    assert sent_msg["key"] == b"T2"

    decoded_event = ScadaEvent.from_json(sent_msg["value"])
    assert decoded_event == sample_scada_event

    assert producer.produce_attempted == 1
    assert producer.produce_acknowledged == 1
    assert producer.produce_failed == 0


def test_producer_delivery_failure_increments_failed_counter(kafka_settings, sample_scada_event):
    """Kiểm tra khi broker trả về lỗi delivery thì counter produce_failed tăng."""
    fake_client = FakeKafkaProducerClient({})
    fake_client.fail_next_produce = True
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    producer.send(sample_scada_event)

    assert producer.produce_attempted == 1
    assert producer.produce_acknowledged == 0
    assert producer.produce_failed == 1


def test_producer_buffer_error_retry(kafka_settings, sample_scada_event):
    """Kiểm tra xử lý BufferError thông qua poll retry trước khi gửi thành công."""
    fake_client = FakeKafkaProducerClient({})
    fake_client.raise_buffer_error_times = 2
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    producer.send(sample_scada_event, max_buffer_retries=5)

    assert len(fake_client.messages) == 1
    assert fake_client.poll_called_count >= 2
    assert producer.produce_acknowledged == 1


def test_producer_buffer_error_exceeds_max_retries(kafka_settings, sample_scada_event):
    """Kiểm tra BufferError ném ra ngoại lệ khi vượt quá số lần retry tối đa."""
    fake_client = FakeKafkaProducerClient({})
    fake_client.raise_buffer_error_times = 10
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    with pytest.raises(BufferError, match="Kafka producer buffer đã đầy"):
        producer.send(sample_scada_event, max_buffer_retries=3)

    assert producer.produce_failed == 1


def test_producer_flush_and_close(kafka_settings):
    """Kiểm tra flush và close gọi hàm flush của client."""
    fake_client = FakeKafkaProducerClient({})
    producer = ScadaKafkaProducer(
        settings=kafka_settings,
        producer_factory=lambda cfg: fake_client,
    )

    rem = producer.flush()
    assert rem == 0
    assert fake_client.flush_called is True

    producer.close()
