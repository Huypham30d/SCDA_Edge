import logging
import time
from typing import Any, Callable
import confluent_kafka

from config.settings import KafkaSettings
from src.event_schema import ScadaEvent

logger = logging.getLogger(__name__)


class ScadaKafkaProducer:
    """Module quản lý việc gửi ScadaEvent lên Apache Kafka bằng confluent-kafka."""

    def __init__(
        self,
        settings: KafkaSettings,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        """Khởi tạo ScadaKafkaProducer.

        Args:
            settings: Cấu hình KafkaSettings.
            producer_factory: Factory tùy chọn (dùng cho testing/mocking).
        """
        self.settings = settings
        self.topic = settings.raw_topic

        # Bộ đếm trạng thái gửi message
        self.produce_attempted: int = 0
        self.produce_acknowledged: int = 0
        self.produce_failed: int = 0

        producer_config = settings.to_producer_config()
        if producer_factory is not None:
            self._producer = producer_factory(producer_config)
        else:
            self._producer = confluent_kafka.Producer(producer_config)

    def _delivery_callback(
        self,
        err: Any,
        msg: Any,
        user_callback: Callable[[Any, Any], None] | None = None,
    ) -> None:
        """Callback nội bộ nhận thông báo xác nhận (ACK) từ Kafka broker."""
        if err is not None:
            self.produce_failed += 1
            logger.error(
                "Kafka delivery failed: topic=%s error=%s",
                getattr(msg, "topic", lambda: self.topic)(),
                err,
            )
        else:
            self.produce_acknowledged += 1

        if user_callback is not None:
            try:
                user_callback(err, msg)
            except Exception as cb_exc:
                logger.warning("Lỗi trong user-provided delivery callback: %s", cb_exc)

    def send(
        self,
        event: ScadaEvent,
        on_delivery: Callable[[Any, Any], None] | None = None,
        max_buffer_retries: int = 10,
        buffer_retry_backoff: float = 0.05,
    ) -> None:
        """Gửi ScadaEvent vào Kafka topic với key là turbine_id.

        Xử lý BufferError với cơ chế poll/retry có giới hạn.

        Args:
            event: Instance ScadaEvent hợp lệ.
            on_delivery: Callback tùy chọn khi message được broker xác nhận.
            max_buffer_retries: Số lần thử lại tối đa nếu buffer producer bị đầy.
            buffer_retry_backoff: Thời gian chờ poll giữa các lần thử lại (giây).
        """
        if not isinstance(event, ScadaEvent):
            raise TypeError(f"Dữ liệu gửi phải là ScadaEvent. Nhận được: {type(event).__name__}")

        key_bytes = event.turbine_id.encode("utf-8")
        value_bytes = event.to_json().encode("utf-8")

        def callback(err: Any, msg: Any) -> None:
            self._delivery_callback(err, msg, on_delivery)

        retries = 0
        while True:
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=value_bytes,
                    on_delivery=callback,
                )
                self.produce_attempted += 1
                break
            except BufferError as buf_err:
                retries += 1
                if retries > max_buffer_retries:
                    self.produce_failed += 1
                    raise BufferError(
                        f"Kafka producer buffer đã đầy sau {max_buffer_retries} lần poll/retry "
                        f"cho event_id='{event.event_id}'."
                    ) from buf_err
                # Poll để giải phóng buffer queue
                self._producer.poll(buffer_retry_backoff)

    def poll(self, timeout: float = 0.0) -> int:
        """Kích hoạt xử lý các delivery callbacks đang chờ trong hàng đợi."""
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        """Đợi toàn bộ message trong buffer được gửi và nhận xác nhận từ broker.

        Returns:
            Số lượng message còn sót lại chưa thể flush (0 nếu thành công toàn bộ).
        """
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(
                "Kafka producer flush timeout: còn %d message chưa được gửi.", remaining
            )
        return remaining

    def close(self, timeout: float = 10.0) -> None:
        """Xả buffer và đóng producer an toàn."""
        self.flush(timeout=timeout)

    def __enter__(self) -> "ScadaKafkaProducer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
