import json
import pytest
from config.settings import KafkaSettings
from src.event_schema import ScadaEvent
from src.kafka_consumer import ConsumedMessage, ScadaKafkaConsumer


class FakeKafkaMessage:
    """Mock của confluent_kafka.Message."""

    def __init__(
        self,
        topic: str = "scada.raw.v1",
        partition: int = 0,
        offset: int = 10,
        value: bytes | None = None,
        key: bytes | None = None,
        error=None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self):
        return self._error

    def timestamp(self):
        return (1, 1700000000000)


class FakeConsumerClient:
    """Mock của confluent_kafka.Consumer."""

    def __init__(self, config: dict):
        self.config = config
        self.subscribed_topics: list[str] = []
        self.queue: list[FakeKafkaMessage] = []
        self.committed_messages: list[FakeKafkaMessage] = []
        self.closed = False

    def subscribe(self, topics: list[str]):
        self.subscribed_topics = list(topics)

    def poll(self, timeout: float = 1.0):
        if self.queue:
            return self.queue.pop(0)
        return None

    def commit(self, message=None, offsets=None, asynchronous=False):
        if message is not None:
            self.committed_messages.append(message)

    def close(self):
        self.closed = True


class FakeDLQProducerClient:
    """Mock của DLQ Producer."""

    def __init__(self, config: dict):
        self.config = config
        self.messages: list[dict] = []
        self.fail_dlq = False
        self.flushed = False

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery=None):
        record = {"topic": topic, "key": key, "value": value}
        self.messages.append(record)
        if on_delivery is not None:
            if self.fail_dlq:
                on_delivery(RuntimeError("DLQ delivery error"), self)
            else:
                on_delivery(None, self)

    def flush(self, timeout: float = 5.0):
        self.flushed = True
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
        turbine_id="T3",
        source_row_index=0,
        scenario="normal",
        injected_anomaly=False,
        wind_speed=7.5,
        theoretical_power=1000.0,
        wind_direction=175.0,
        actual_power=980.0,
    )


def test_consumer_poll_valid_event(kafka_settings, sample_scada_event):
    """Kiểm tra đọc và parse ScadaEvent hợp lệ từ Kafka topic."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})

    raw_msg = FakeKafkaMessage(
        topic="scada.raw.v1",
        partition=2,
        offset=105,
        value=sample_scada_event.to_json().encode("utf-8"),
        key=b"T3",
    )
    fake_consumer.queue.append(raw_msg)

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    consumed = consumer.poll(timeout=1.0)
    assert consumed is not None
    assert consumed.topic == "scada.raw.v1"
    assert consumed.partition == 2
    assert consumed.offset == 105
    assert consumed.error is None
    assert consumed.event == sample_scada_event
    assert consumed.is_eof is False


def test_consumer_poll_invalid_schema_returns_error(kafka_settings):
    """Kiểm tra đọc message bị lỗi cú pháp JSON trả về ConsumedMessage kèm error."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})

    broken_msg = FakeKafkaMessage(
        topic="scada.raw.v1",
        partition=1,
        offset=50,
        value=b"not a valid json string {",
        key=b"T1",
    )
    fake_consumer.queue.append(broken_msg)

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    consumed = consumer.poll(timeout=1.0)
    assert consumed is not None
    assert consumed.event is None
    assert consumed.error is not None
    assert consumed.offset == 50


def test_consumer_commit_synchronous(kafka_settings):
    """Kiểm tra commit synchronous gọi client commit chính xác."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})

    raw_msg = FakeKafkaMessage(partition=0, offset=12)
    consumed = ConsumedMessage(
        event=None,
        topic="scada.raw.v1",
        partition=0,
        offset=12,
        kafka_timestamp=None,
        raw_payload="",
        raw_message=raw_msg,
    )

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    consumer.commit(consumed)
    assert len(fake_consumer.committed_messages) == 1
    assert fake_consumer.committed_messages[0] == raw_msg


def test_consumer_send_to_dlq_success(kafka_settings):
    """Kiểm tra gửi message hỏng tới DLQ với đúng định dạng metadata."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    ok = consumer.send_to_dlq(
        raw_payload="corrupted_data_string",
        error_type="EventSchemaError",
        error_message="Invalid schema format",
        original_topic="scada.raw.v1",
        original_partition=2,
        original_offset=42,
    )

    assert ok is True
    assert len(fake_dlq.messages) == 1
    dlq_record = fake_dlq.messages[0]
    assert dlq_record["topic"] == "scada.dlq.v1"
    assert dlq_record["key"] == b"scada.raw.v1:2:42"

    payload_dict = json.loads(dlq_record["value"].decode("utf-8"))
    assert payload_dict["original_topic"] == "scada.raw.v1"
    assert payload_dict["original_partition"] == 2
    assert payload_dict["original_offset"] == 42
    assert payload_dict["error_type"] == "EventSchemaError"
    assert payload_dict["raw_payload"] == "corrupted_data_string"


def test_consumer_send_to_dlq_failure(kafka_settings):
    """Kiểm tra khi DLQ produce bị lỗi thì trả về False."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})
    fake_dlq.fail_dlq = True

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    ok = consumer.send_to_dlq(
        raw_payload="bad",
        error_type="Err",
        error_message="Fail",
        original_topic="scada.raw.v1",
        original_partition=0,
        original_offset=1,
    )

    assert ok is False


def test_consumer_close(kafka_settings):
    """Kiểm tra close consumer và DLQ producer."""
    fake_consumer = FakeConsumerClient({})
    fake_dlq = FakeDLQProducerClient({})

    consumer = ScadaKafkaConsumer(
        settings=kafka_settings,
        consumer_factory=lambda cfg: fake_consumer,
        dlq_producer_factory=lambda cfg: fake_dlq,
    )

    consumer.close()
    assert fake_consumer.closed is True
    assert fake_dlq.flushed is True
