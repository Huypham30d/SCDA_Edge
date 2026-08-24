from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any, Callable
import confluent_kafka
from confluent_kafka import KafkaError, TopicPartition

from config.settings import KafkaSettings
from src.event_schema import EventSchemaError, ScadaEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsumedMessage:
    """Biểu diễn một message đọc từ Kafka cùng metadata và dữ liệu đã parse."""

    event: ScadaEvent | None
    topic: str
    partition: int
    offset: int
    kafka_timestamp: int | None
    raw_payload: str
    error: Exception | None = None
    is_eof: bool = False
    raw_message: Any = None


class ScadaKafkaConsumer:
    """Module quản lý việc đọc message từ Kafka topic, commit thủ công và chuyển DLQ."""

    def __init__(
        self,
        settings: KafkaSettings,
        consumer_factory: Callable[[dict[str, Any]], Any] | None = None,
        dlq_producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        """Khởi tạo ScadaKafkaConsumer.

        Args:
            settings: Cấu hình KafkaSettings.
            consumer_factory: Factory tùy chọn khởi tạo Consumer (phục vụ test).
            dlq_producer_factory: Factory tùy chọn khởi tạo DLQ Producer (phục vụ test).
        """
        self.settings = settings
        self.raw_topic = settings.raw_topic
        self.dlq_topic = settings.dlq_topic

        consumer_config = settings.to_consumer_config()
        if consumer_factory is not None:
            self._consumer = consumer_factory(consumer_config)
        else:
            self._consumer = confluent_kafka.Consumer(consumer_config)

        # Producer riêng cho Dead Letter Queue (DLQ)
        producer_config = settings.to_producer_config()
        if dlq_producer_factory is not None:
            self._dlq_producer = dlq_producer_factory(producer_config)
        else:
            self._dlq_producer = confluent_kafka.Producer(producer_config)

        self._subscribed = False

    def subscribe(self, topics: list[str] | None = None) -> None:
        """Đăng ký lắng nghe các topic trên Kafka."""
        topic_list = topics or [self.raw_topic]
        self._consumer.subscribe(topic_list)
        self._subscribed = True

    def poll(self, timeout: float | None = None) -> ConsumedMessage | None:
        """Đọc một message từ Kafka broker.

        Returns:
            ConsumedMessage nếu nhận được dữ liệu hoặc EOF/Lỗi, None nếu hết timeout.
        """
        if not self._subscribed:
            self.subscribe()

        poll_timeout = (
            timeout if timeout is not None else self.settings.consumer_poll_timeout_seconds
        )
        msg = self._consumer.poll(poll_timeout)

        if msg is None:
            return None

        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        ts_tuple = msg.timestamp()
        kafka_timestamp = ts_tuple[1] if ts_tuple and ts_tuple[0] != confluent_kafka.TIMESTAMP_NOT_AVAILABLE else None

        # Kiểm tra broker error
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return ConsumedMessage(
                    event=None,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    kafka_timestamp=kafka_timestamp,
                    raw_payload="",
                    is_eof=True,
                    raw_message=msg,
                )
            return ConsumedMessage(
                event=None,
                topic=topic,
                partition=partition,
                offset=offset,
                kafka_timestamp=kafka_timestamp,
                raw_payload="",
                error=RuntimeError(f"Kafka error: {err}"),
                raw_message=msg,
            )

        raw_bytes = msg.value() or b""
        try:
            raw_str = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as dec_err:
            raw_str = raw_bytes.decode("utf-8", errors="replace")
            return ConsumedMessage(
                event=None,
                topic=topic,
                partition=partition,
                offset=offset,
                kafka_timestamp=kafka_timestamp,
                raw_payload=raw_str,
                error=EventSchemaError(f"Lỗi giải mã UTF-8 payload: {dec_err}"),
                raw_message=msg,
            )

        # Parse ScadaEvent
        try:
            event = ScadaEvent.from_json(raw_str)
            return ConsumedMessage(
                event=event,
                topic=topic,
                partition=partition,
                offset=offset,
                kafka_timestamp=kafka_timestamp,
                raw_payload=raw_str,
                error=None,
                raw_message=msg,
            )
        except Exception as exc:
            return ConsumedMessage(
                event=None,
                topic=topic,
                partition=partition,
                offset=offset,
                kafka_timestamp=kafka_timestamp,
                raw_payload=raw_str,
                error=exc,
                raw_message=msg,
            )

    def commit(self, message: ConsumedMessage | Any) -> None:
        """Thực hiện commit synchronous offset cho message đã xử lý thành công.

        Args:
            message: ConsumedMessage hoặc raw message từ confluent_kafka.
        """
        raw_msg = message.raw_message if isinstance(message, ConsumedMessage) else message
        if raw_msg is None:
            raise ValueError("Không thể commit message rỗng hoặc không có raw_message.")

        # Commit synchronous
        try:
            self._consumer.commit(message=raw_msg, asynchronous=False)
        except Exception as exc:
            raise RuntimeError(
                f"Lỗi khi commit offset cho message tại partition={getattr(raw_msg, 'partition', lambda: '?')()}, "
                f"offset={getattr(raw_msg, 'offset', lambda: '?')()}: {exc}"
            ) from exc

    def send_to_dlq(
        self,
        raw_payload: str,
        error_type: str,
        error_message: str,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        timeout: float = 5.0,
    ) -> bool:
        """Gửi message hỏng vào Dead Letter Queue (DLQ) và đợi xác nhận.

        Returns:
            True nếu gửi DLQ thành công và đã được broker ACK, False nếu thất bại.
        """
        dlq_record = {
            "original_topic": original_topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
            "error_type": error_type,
            "error_message": error_message,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload": raw_payload,
        }

        dlq_bytes = json.dumps(dlq_record, ensure_ascii=False).encode("utf-8")
        ack_success = False

        def dlq_ack(err: Any, msg: Any) -> None:
            nonlocal ack_success
            if err is None:
                ack_success = True
            else:
                logger.error("Lỗi khi ghi vào DLQ: %s", err)

        try:
            self._dlq_producer.produce(
                topic=self.dlq_topic,
                key=f"{original_topic}:{original_partition}:{original_offset}".encode("utf-8"),
                value=dlq_bytes,
                on_delivery=dlq_ack,
            )
            # Flush để chắc chắn DLQ message đã được ghi nhận trước khi commit raw offset
            self._dlq_producer.flush(timeout)
            return ack_success
        except Exception as exc:
            logger.error("Ngoại lệ khi gửi tới DLQ: %s", exc)
            return False

    def close(self) -> None:
        """Đóng consumer và DLQ producer an toàn."""
        try:
            if hasattr(self, "_consumer") and self._consumer is not None:
                self._consumer.close()
        except Exception as exc:
            logger.warning("Lỗi khi đóng Kafka consumer: %s", exc)

        try:
            if hasattr(self, "_dlq_producer") and self._dlq_producer is not None:
                self._dlq_producer.flush(5.0)
        except Exception as exc:
            logger.warning("Lỗi khi flush DLQ producer: %s", exc)

    def __enter__(self) -> "ScadaKafkaConsumer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
