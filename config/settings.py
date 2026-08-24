from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

# Xác định thư mục gốc của dự án bằng pathlib.Path
BASE_DIR = Path(__file__).resolve().parent.parent

# Tải cấu hình từ file .env nếu có
load_dotenv(BASE_DIR / ".env")

# Đường dẫn tuyệt đối ổn định tới các tài nguyên
CONFIG_PATH = BASE_DIR / "config.json"
DATA_PATH = BASE_DIR / "test.csv"
MODEL_PATH = BASE_DIR / "xgboost_model.json"
SCALER_PATH = BASE_DIR / "scaler.joblib"
SIMULATOR_CONFIG_PATH = BASE_DIR / "config" / "simulator.json"
KAFKA_CONFIG_PATH = BASE_DIR / "config" / "kafka.json"


@dataclass(frozen=True)
class KafkaSettings:
    """Cấu hình tích hợp Apache Kafka cho producer và consumer."""

    bootstrap_servers: str
    raw_topic: str
    dlq_topic: str
    consumer_group: str
    auto_offset_reset: str
    producer_acks: str
    producer_enable_idempotence: bool
    producer_compression_type: str
    producer_linger_ms: int
    producer_batch_size: int
    producer_delivery_timeout_ms: int
    consumer_enable_auto_commit: bool
    consumer_enable_auto_offset_store: bool
    consumer_poll_timeout_seconds: float
    consumer_max_processing_retries: int
    consumer_retry_backoff_seconds: float

    def to_producer_config(self) -> dict[str, Any]:
        """Tạo cấu hình cho confluent_kafka.Producer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.producer_acks,
            "enable.idempotence": self.producer_enable_idempotence,
            "compression.type": self.producer_compression_type,
            "linger.ms": self.producer_linger_ms,
            "batch.size": self.producer_batch_size,
            "delivery.timeout.ms": self.producer_delivery_timeout_ms,
        }

    def to_consumer_config(self) -> dict[str, Any]:
        """Tạo cấu hình cho confluent_kafka.Consumer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.consumer_enable_auto_commit,
            "enable.auto.offset.store": self.consumer_enable_auto_offset_store,
        }


@dataclass(frozen=True)
class InfluxSettings:
    """Cấu hình kết nối InfluxDB và các tham số vận hành streaming."""

    url: str
    token: str
    org: str
    bucket: str
    turbine_id: str = "T1"
    stream_interval_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "InfluxSettings":
        """Đọc và kiểm tra cấu hình InfluxDB từ biến môi trường."""
        url = os.getenv("INFLUX_URL", "http://localhost:8086").strip()
        token = os.getenv("INFLUX_TOKEN")
        org = os.getenv("INFLUX_ORG", "scada_org").strip()
        bucket = os.getenv("INFLUX_BUCKET", "turbine_metrics").strip()
        turbine_id = os.getenv("TURBINE_ID", "T1").strip()
        interval_str = os.getenv("STREAM_INTERVAL_SECONDS", "1").strip()

        if not token or not token.strip():
            raise ValueError(
                "Biến môi trường 'INFLUX_TOKEN' là bắt buộc nhưng chưa được thiết lập. "
                "Vui lòng cấu hình trong file .env hoặc biến môi trường hệ thống."
            )

        try:
            stream_interval_seconds = float(interval_str)
            if stream_interval_seconds < 0:
                raise ValueError("Thời gian chờ phải lớn hơn hoặc bằng 0.")
        except ValueError as exc:
            raise ValueError(
                f"Giá trị STREAM_INTERVAL_SECONDS không hợp lệ: '{interval_str}'"
            ) from exc

        return cls(
            url=url,
            token=token.strip(),
            org=org,
            bucket=bucket,
            turbine_id=turbine_id,
            stream_interval_seconds=stream_interval_seconds,
        )


@dataclass(frozen=True)
class SimulatorSettings:
    """Cấu hình vận hành SCADA Simulator."""

    enabled: bool
    turbine_count: int
    events_per_second: float
    loop_data: bool
    max_events: int | None
    scenario: str
    random_seed: int
    burst_start_after_seconds: float
    burst_duration_seconds: float
    burst_multiplier: float
    anomaly_probability: float
    actual_power_multiplier: float


def parse_bool_env(value: Any) -> bool:
    """Chuyển đổi chuỗi/giá trị môi trường sang kiểu boolean an toàn."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes", "t", "y"):
            return True
        if normalized in ("false", "0", "no", "f", "n"):
            return False
        raise ValueError(
            f"Giá trị boolean không hợp lệ: '{value}'. "
            "Chỉ chấp nhận các giá trị: true, false, 1, 0, yes, no."
        )
    raise TypeError(f"Kiểu dữ liệu không hợp lệ khi chuyển đổi boolean: {type(value)}")


def load_config(config_path: Path | str = CONFIG_PATH) -> dict:
    """Tải và trả về nội dung cấu hình từ file json."""
    resolved_path = Path(config_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file cấu hình: {resolved_path}")

    with open(resolved_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_kafka_settings(
    config_path: Path | str = KAFKA_CONFIG_PATH,
) -> KafkaSettings:
    """Tải, validate và trả về cấu hình Kafka.

    Cho phép biến môi trường override:
    - KAFKA_CONFIG_PATH
    - KAFKA_BOOTSTRAP_SERVERS
    - KAFKA_RAW_TOPIC
    - KAFKA_DLQ_TOPIC
    - KAFKA_CONSUMER_GROUP
    """
    env_config_path = os.getenv("KAFKA_CONFIG_PATH")
    if config_path == KAFKA_CONFIG_PATH and env_config_path:
        resolved_path = Path(env_config_path)
        if not resolved_path.is_absolute():
            resolved_path = BASE_DIR / resolved_path
    else:
        resolved_path = Path(config_path)

    if not resolved_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file cấu hình Kafka: {resolved_path}")

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except Exception as exc:
        raise ValueError(
            f"Không thể đọc file JSON cấu hình Kafka '{resolved_path}': {exc}"
        ) from exc

    if not isinstance(raw_data, dict):
        raise ValueError("Cấu hình Kafka phải là một JSON Object (dict).")

    # Bootstrap servers
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or raw_data.get("bootstrap_servers", "")
    if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
        raise ValueError("Trường 'bootstrap_servers' không được để trống.")
    bootstrap_servers = bootstrap_servers.strip()

    # Raw topic
    raw_topic = os.getenv("KAFKA_RAW_TOPIC") or raw_data.get("raw_topic", "")
    if not isinstance(raw_topic, str) or not raw_topic.strip():
        raise ValueError("Trường 'raw_topic' không được để trống.")
    raw_topic = raw_topic.strip()

    # DLQ topic
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC") or raw_data.get("dlq_topic", "")
    if not isinstance(dlq_topic, str) or not dlq_topic.strip():
        raise ValueError("Trường 'dlq_topic' không được để trống.")
    dlq_topic = dlq_topic.strip()

    # Consumer group
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP") or raw_data.get("consumer_group", "")
    if not isinstance(consumer_group, str) or not consumer_group.strip():
        raise ValueError("Trường 'consumer_group' không được để trống.")
    consumer_group = consumer_group.strip()

    # Auto offset reset
    auto_offset_reset = raw_data.get("auto_offset_reset", "earliest")
    if auto_offset_reset not in ("earliest", "latest", "error"):
        raise ValueError(f"Trường 'auto_offset_reset' không hợp lệ: '{auto_offset_reset}'.")

    # Producer settings
    prod_cfg = raw_data.get("producer", {})
    if not isinstance(prod_cfg, dict):
        raise ValueError("Trường 'producer' phải là một dictionary.")

    producer_acks = str(prod_cfg.get("acks", "all")).strip()
    if producer_acks not in ("all", "-1", "1", "0"):
        raise ValueError(f"Trường 'producer.acks' không hợp lệ: '{producer_acks}'.")

    producer_enable_idempotence = prod_cfg.get("enable_idempotence", True)
    if not isinstance(producer_enable_idempotence, bool):
        raise ValueError("Trường 'producer.enable_idempotence' phải là boolean.")

    producer_compression_type = str(prod_cfg.get("compression_type", "snappy")).strip()

    producer_linger_ms = prod_cfg.get("linger_ms", 10)
    if type(producer_linger_ms) is not int or producer_linger_ms < 0:
        raise ValueError("Trường 'producer.linger_ms' phải là số nguyên >= 0.")

    producer_batch_size = prod_cfg.get("batch_size", 65536)
    if type(producer_batch_size) is not int or producer_batch_size <= 0:
        raise ValueError("Trường 'producer.batch_size' phải là số nguyên > 0.")

    producer_delivery_timeout_ms = prod_cfg.get("delivery_timeout_ms", 120000)
    if type(producer_delivery_timeout_ms) is not int or producer_delivery_timeout_ms <= 0:
        raise ValueError("Trường 'producer.delivery_timeout_ms' phải là số nguyên > 0.")

    # Consumer settings
    cons_cfg = raw_data.get("consumer", {})
    if not isinstance(cons_cfg, dict):
        raise ValueError("Trường 'consumer' phải là một dictionary.")

    consumer_enable_auto_commit = cons_cfg.get("enable_auto_commit", False)
    if consumer_enable_auto_commit is not False:
        raise ValueError(
            "Cấu hình 'consumer.enable_auto_commit' bắt buộc phải là False để đảm bảo "
            "offset chỉ được commit sau khi xử lý thành công."
        )

    consumer_enable_auto_offset_store = cons_cfg.get("enable_auto_offset_store", False)
    if consumer_enable_auto_offset_store is not False:
        raise ValueError(
            "Cấu hình 'consumer.enable_auto_offset_store' bắt buộc phải là False để "
            "kiểm soát chính xác việc lưu trữ offset."
        )

    consumer_poll_timeout_seconds = cons_cfg.get("poll_timeout_seconds", 1.0)
    if (
        type(consumer_poll_timeout_seconds) not in (int, float)
        or isinstance(consumer_poll_timeout_seconds, bool)
        or consumer_poll_timeout_seconds <= 0
    ):
        raise ValueError("Trường 'consumer.poll_timeout_seconds' phải là số > 0.")
    consumer_poll_timeout_seconds = float(consumer_poll_timeout_seconds)

    consumer_max_processing_retries = cons_cfg.get("max_processing_retries", 5)
    if type(consumer_max_processing_retries) is not int or consumer_max_processing_retries < 0:
        raise ValueError("Trường 'consumer.max_processing_retries' phải là số nguyên >= 0.")

    consumer_retry_backoff_seconds = cons_cfg.get("retry_backoff_seconds", 2.0)
    if (
        type(consumer_retry_backoff_seconds) not in (int, float)
        or isinstance(consumer_retry_backoff_seconds, bool)
        or consumer_retry_backoff_seconds <= 0
    ):
        raise ValueError("Trường 'consumer.retry_backoff_seconds' phải là số > 0.")
    consumer_retry_backoff_seconds = float(consumer_retry_backoff_seconds)

    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        raw_topic=raw_topic,
        dlq_topic=dlq_topic,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
        producer_acks=producer_acks,
        producer_enable_idempotence=producer_enable_idempotence,
        producer_compression_type=producer_compression_type,
        producer_linger_ms=producer_linger_ms,
        producer_batch_size=producer_batch_size,
        producer_delivery_timeout_ms=producer_delivery_timeout_ms,
        consumer_enable_auto_commit=consumer_enable_auto_commit,
        consumer_enable_auto_offset_store=consumer_enable_auto_offset_store,
        consumer_poll_timeout_seconds=consumer_poll_timeout_seconds,
        consumer_max_processing_retries=consumer_max_processing_retries,
        consumer_retry_backoff_seconds=consumer_retry_backoff_seconds,
    )

def load_simulator_settings(
    config_path: Path | str = SIMULATOR_CONFIG_PATH,
) -> SimulatorSettings:
    """Tải, validate và trả về cấu hình SCADA Simulator.

    Cho phép biến môi trường override:
    - SIMULATOR_CONFIG_PATH: Đường dẫn tới file cấu hình simulator.
    - SIMULATOR_ENABLED: Bật (true) hoặc tắt (false) simulator.
    """
    env_config_path = os.getenv("SIMULATOR_CONFIG_PATH")
    if config_path == SIMULATOR_CONFIG_PATH and env_config_path:
        resolved_path = Path(env_config_path)
        if not resolved_path.is_absolute():
            resolved_path = BASE_DIR / resolved_path
    else:
        resolved_path = Path(config_path)

    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file cấu hình simulator: {resolved_path}"
        )

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except Exception as exc:
        raise ValueError(
            f"Không thể đọc file JSON cấu hình simulator '{resolved_path}': {exc}"
        ) from exc

    if not isinstance(raw_data, dict):
        raise ValueError("Cấu hình simulator phải là một JSON Object (dict).")

    # Validate enabled
    enabled_raw = raw_data.get("enabled", True)
    if not isinstance(enabled_raw, bool):
        raise ValueError(
            f"Trường 'enabled' phải là boolean (true/false). Nhận được: {enabled_raw}"
        )
    enabled = enabled_raw

    # .env override cho SIMULATOR_ENABLED
    env_enabled = os.getenv("SIMULATOR_ENABLED")
    if env_enabled is not None and env_enabled.strip():
        enabled = parse_bool_env(env_enabled)

    # Validate turbine_count
    turbine_count = raw_data.get("turbine_count")
    if type(turbine_count) is not int or turbine_count < 1:
        raise ValueError(
            f"Trường 'turbine_count' phải là số nguyên >= 1. Nhận được: {turbine_count}"
        )

    # Validate events_per_second
    events_per_second = raw_data.get("events_per_second")
    if (
        type(events_per_second) not in (int, float)
        or isinstance(events_per_second, bool)
        or events_per_second <= 0
    ):
        raise ValueError(
            f"Trường 'events_per_second' phải là số dương > 0. Nhận được: {events_per_second}"
        )
    events_per_second = float(events_per_second)

    # Validate loop_data
    loop_data = raw_data.get("loop_data", False)
    if not isinstance(loop_data, bool):
        raise ValueError(
            f"Trường 'loop_data' phải là boolean. Nhận được: {loop_data}"
        )

    # Validate max_events
    max_events = raw_data.get("max_events")
    if max_events is not None:
        if type(max_events) is not int or max_events < 1:
            raise ValueError(
                f"Trường 'max_events' phải là null hoặc số nguyên >= 1. Nhận được: {max_events}"
            )

    # Validate scenario
    scenario = raw_data.get("scenario", "normal")
    valid_scenarios = ("normal", "burst", "anomaly_injection")
    if scenario not in valid_scenarios:
        raise ValueError(
            f"Trường 'scenario' không hợp lệ: '{scenario}'. "
            f"Phải là một trong: {valid_scenarios}"
        )

    # Validate random_seed
    random_seed = raw_data.get("random_seed", 42)
    if type(random_seed) is not int or isinstance(random_seed, bool):
        raise ValueError(
            f"Trường 'random_seed' phải là số nguyên. Nhận được: {random_seed}"
        )

    # Validate burst
    burst_cfg = raw_data.get("burst", {})
    if not isinstance(burst_cfg, dict):
        raise ValueError("Trường 'burst' phải là một dictionary chứa cấu hình burst.")

    burst_start = burst_cfg.get("start_after_seconds", 10)
    if (
        type(burst_start) not in (int, float)
        or isinstance(burst_start, bool)
        or burst_start < 0
    ):
        raise ValueError(
            f"Trường 'burst.start_after_seconds' phải là số >= 0. Nhận được: {burst_start}"
        )
    burst_start = float(burst_start)

    burst_duration = burst_cfg.get("duration_seconds", 10)
    if (
        type(burst_duration) not in (int, float)
        or isinstance(burst_duration, bool)
        or burst_duration <= 0
    ):
        raise ValueError(
            f"Trường 'burst.duration_seconds' phải là số > 0. Nhận được: {burst_duration}"
        )
    burst_duration = float(burst_duration)

    burst_multiplier = burst_cfg.get("multiplier", 5)
    if (
        type(burst_multiplier) not in (int, float)
        or isinstance(burst_multiplier, bool)
        or burst_multiplier <= 0
    ):
        raise ValueError(
            f"Trường 'burst.multiplier' phải là số > 0. Nhận được: {burst_multiplier}"
        )
    burst_multiplier = float(burst_multiplier)

    # Validate anomaly_injection
    anomaly_cfg = raw_data.get("anomaly_injection", {})
    if not isinstance(anomaly_cfg, dict):
        raise ValueError(
            "Trường 'anomaly_injection' phải là một dictionary chứa cấu hình chèn bất thường."
        )

    anomaly_prob = anomaly_cfg.get("probability", 0.05)
    if (
        type(anomaly_prob) not in (int, float)
        or isinstance(anomaly_prob, bool)
        or not (0.0 <= float(anomaly_prob) <= 1.0)
    ):
        raise ValueError(
            f"Trường 'anomaly_injection.probability' phải nằm trong khoảng [0.0, 1.0]. Nhận được: {anomaly_prob}"
        )
    anomaly_prob = float(anomaly_prob)

    actual_power_multiplier = anomaly_cfg.get("actual_power_multiplier", 1.8)
    if (
        type(actual_power_multiplier) not in (int, float)
        or isinstance(actual_power_multiplier, bool)
        or actual_power_multiplier <= 0
    ):
        raise ValueError(
            f"Trường 'anomaly_injection.actual_power_multiplier' phải là số > 0. Nhận được: {actual_power_multiplier}"
        )
    actual_power_multiplier = float(actual_power_multiplier)

    return SimulatorSettings(
        enabled=enabled,
        turbine_count=turbine_count,
        events_per_second=events_per_second,
        loop_data=loop_data,
        max_events=max_events,
        scenario=scenario,
        random_seed=random_seed,
        burst_start_after_seconds=burst_start,
        burst_duration_seconds=burst_duration,
        burst_multiplier=burst_multiplier,
        anomaly_probability=anomaly_prob,
        actual_power_multiplier=actual_power_multiplier,
    )
