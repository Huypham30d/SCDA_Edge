import json
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    BASE_DIR,
    CONFIG_PATH,
    DATA_PATH,
    MODEL_PATH,
    SCALER_PATH,
    SimulatorSettings,
    load_config,
)
from src.anomaly_detector import detect_anomaly
from src.data_loader import load_data
from src.feature_engineering import add_time_features
from src.inference import PowerPredictor
from src.scada_simulator import SCADASimulator, SimulationEvent
from experiments.experiment_writer import ExperimentWriter

DEFAULT_EXPERIMENT_CONFIG_PATH = BASE_DIR / "config" / "loss_experiment.json"


@dataclass(frozen=True)
class LossExperimentSettings:
    """Cấu hình tham số cho thí nghiệm Direct Pipeline Loss."""

    experiment_id: str
    turbine_count: int
    source_events_per_second: float
    total_events: int
    queue_capacity: int
    consumer_delay_seconds: float
    continue_on_write_error: bool
    drain_queue_after_source_stops: bool
    scenario: str
    random_seed: int
    result_directory: str


def load_loss_experiment_settings(
    config_path: Path | str = DEFAULT_EXPERIMENT_CONFIG_PATH,
) -> LossExperimentSettings:
    """Tải và validate cấu hình thí nghiệm từ file JSON.

    Args:
        config_path: Đường dẫn tới file JSON cấu hình thí nghiệm.

    Returns:
        LossExperimentSettings đã được xác thực.

    Raises:
        FileNotFoundError: Nếu file cấu hình không tồn tại.
        ValueError: Nếu nội dung JSON hoặc giá trị các trường không hợp lệ.
    """
    resolved_path = Path(config_path)
    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file cấu hình thí nghiệm: {resolved_path}"
        )

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as exc:
        raise ValueError(
            f"Không thể đọc file JSON cấu hình thí nghiệm '{resolved_path}': {exc}"
        ) from exc

    if not isinstance(raw, dict):
        raise ValueError("Cấu hình thí nghiệm phải là một JSON Object (dict).")

    # 1. experiment_id
    experiment_id = raw.get("experiment_id")
    if not isinstance(experiment_id, str) or not experiment_id.strip():
        raise ValueError("Trường 'experiment_id' phải là chuỗi không được để trống.")
    experiment_id = experiment_id.strip()

    # 2. turbine_count
    turbine_count = raw.get("turbine_count")
    if type(turbine_count) is not int or turbine_count < 1:
        raise ValueError(
            f"Trường 'turbine_count' phải là số nguyên >= 1. Nhận được: {turbine_count}"
        )

    # 3. source_events_per_second
    source_rate = raw.get("source_events_per_second")
    if (
        type(source_rate) not in (int, float)
        or isinstance(source_rate, bool)
        or source_rate <= 0
    ):
        raise ValueError(
            f"Trường 'source_events_per_second' phải là số > 0. Nhận được: {source_rate}"
        )
    source_rate = float(source_rate)

    # 4. total_events
    total_events = raw.get("total_events")
    if type(total_events) is not int or total_events < 1:
        raise ValueError(
            f"Trường 'total_events' phải là số nguyên >= 1. Nhận được: {total_events}"
        )

    # 5. queue_capacity
    queue_capacity = raw.get("queue_capacity")
    if type(queue_capacity) is not int or queue_capacity < 1:
        raise ValueError(
            f"Trường 'queue_capacity' phải là số nguyên >= 1. Nhận được: {queue_capacity}"
        )

    # 6. consumer_delay_seconds
    consumer_delay = raw.get("consumer_delay_seconds")
    if (
        type(consumer_delay) not in (int, float)
        or isinstance(consumer_delay, bool)
        or consumer_delay < 0
    ):
        raise ValueError(
            f"Trường 'consumer_delay_seconds' phải là số >= 0. Nhận được: {consumer_delay}"
        )
    consumer_delay = float(consumer_delay)

    # 7. continue_on_write_error
    continue_on_error = raw.get("continue_on_write_error")
    if not isinstance(continue_on_error, bool):
        raise ValueError(
            f"Trường 'continue_on_write_error' phải là boolean. Nhận được: {continue_on_error}"
        )

    # 8. drain_queue_after_source_stops
    drain_queue = raw.get("drain_queue_after_source_stops")
    if not isinstance(drain_queue, bool):
        raise ValueError(
            f"Trường 'drain_queue_after_source_stops' phải là boolean. Nhận được: {drain_queue}"
        )

    # 9. scenario
    scenario = raw.get("scenario", "normal")
    valid_scenarios = ("normal", "burst", "anomaly_injection")
    if scenario not in valid_scenarios:
        raise ValueError(
            f"Trường 'scenario' không hợp lệ: '{scenario}'. Phải là một trong: {valid_scenarios}"
        )

    # 10. random_seed
    random_seed = raw.get("random_seed", 42)
    if type(random_seed) is not int or isinstance(random_seed, bool):
        raise ValueError(
            f"Trường 'random_seed' phải là số nguyên. Nhận được: {random_seed}"
        )

    # 11. result_directory
    result_directory = raw.get("result_directory", "experiment_results")
    if not isinstance(result_directory, str) or not result_directory.strip():
        raise ValueError(
            "Trường 'result_directory' phải là chuỗi không được để trống."
        )
    result_directory = result_directory.strip()

    return LossExperimentSettings(
        experiment_id=experiment_id,
        turbine_count=turbine_count,
        source_events_per_second=source_rate,
        total_events=total_events,
        queue_capacity=queue_capacity,
        consumer_delay_seconds=consumer_delay,
        continue_on_write_error=continue_on_error,
        drain_queue_after_source_stops=drain_queue,
        scenario=scenario,
        random_seed=random_seed,
        result_directory=result_directory,
    )


class ExperimentCounters:
    """Bộ đếm thread-safe theo dõi các trạng thái sự kiện trong thí nghiệm."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.source_generated: int = 0
        self.queue_accepted: int = 0
        self.queue_dropped: int = 0
        self.events_processed: int = 0
        self.influx_write_success: int = 0
        self.influx_write_failed: int = 0

    def inc_source_generated(self) -> None:
        with self._lock:
            self.source_generated += 1

    def inc_queue_accepted(self) -> None:
        with self._lock:
            self.queue_accepted += 1

    def inc_queue_dropped(self) -> None:
        with self._lock:
            self.queue_dropped += 1

    def inc_events_processed(self) -> None:
        with self._lock:
            self.events_processed += 1

    def inc_influx_write_success(self) -> None:
        with self._lock:
            self.influx_write_success += 1

    def inc_influx_write_failed(self) -> None:
        with self._lock:
            self.influx_write_failed += 1

    def get_snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "source_generated": self.source_generated,
                "queue_accepted": self.queue_accepted,
                "queue_dropped": self.queue_dropped,
                "events_processed": self.events_processed,
                "influx_write_success": self.influx_write_success,
                "influx_write_failed": self.influx_write_failed,
            }

    def drop_rate(self) -> float:
        with self._lock:
            if self.source_generated == 0:
                return 0.0
            return (self.queue_dropped / self.source_generated) * 100.0

    def actual_throughput(self, elapsed_seconds: float) -> float:
        with self._lock:
            if elapsed_seconds <= 0:
                return 0.0
            return self.influx_write_success / elapsed_seconds


@dataclass
class QueueItem:
    """Gói dữ liệu event đưa vào bounded queue."""

    event: SimulationEvent
    source_timestamp_ns: int


def producer_worker(
    event_generator: Iterator[SimulationEvent],
    event_queue: queue.Queue,
    counters: ExperimentCounters,
    stop_event: threading.Event,
    producer_done_event: threading.Event,
) -> None:
    """Worker tạo sự kiện từ generator và đẩy vào Bounded Queue không chờ (put_nowait)."""
    try:
        for event in event_generator:
            if stop_event.is_set():
                break

            counters.inc_source_generated()
            timestamp_ns = time.time_ns()
            item = QueueItem(event=event, source_timestamp_ns=timestamp_ns)

            try:
                event_queue.put_nowait(item)
                counters.inc_queue_accepted()
            except queue.Full:
                counters.inc_queue_dropped()
    finally:
        producer_done_event.set()


def consumer_worker(
    experiment_id: str,
    event_queue: queue.Queue,
    predictor: Any,
    writer: Any,
    threshold: float,
    target_column: str,
    consumer_delay_seconds: float,
    continue_on_write_error: bool,
    drain_queue_after_source_stops: bool,
    counters: ExperimentCounters,
    stop_event: threading.Event,
    producer_done_event: threading.Event,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Worker lấy sự kiện từ Queue, thực hiện suy luận, phát hiện bất thường và ghi InfluxDB."""
    while True:
        # Kiểm tra điều kiện dừng
        if stop_event.is_set():
            if not drain_queue_after_source_stops or event_queue.empty():
                break
        elif producer_done_event.is_set():
            if not drain_queue_after_source_stops or event_queue.empty():
                break

        try:
            item: QueueItem = event_queue.get(timeout=0.05)
        except queue.Empty:
            if producer_done_event.is_set() or stop_event.is_set():
                break
            continue

        try:
            event = item.event
            actual_power = float(event.row[target_column])
            wind_speed = float(event.row["Wind Speed (m/s)"])

            # 1. Dự đoán công suất
            pred_power = predictor.predict(event.row)

            # 2. Phát hiện bất thường
            residual, is_anomaly = detect_anomaly(
                actual_power=actual_power,
                predicted_power=pred_power,
                threshold=threshold,
            )

            counters.inc_events_processed()

            # 3. Mô phỏng độ trễ xử lý của consumer nếu cấu hình
            if consumer_delay_seconds > 0:
                sleep_fn(consumer_delay_seconds)

            # 4. Ghi bản ghi vào InfluxDB
            try:
                writer.write_event(
                    experiment_id=experiment_id,
                    turbine_id=event.turbine_id,
                    event_sequence=event.event_number,
                    wind_speed=wind_speed,
                    actual_power=actual_power,
                    predicted_power=pred_power,
                    residual=residual,
                    anomaly_flag=is_anomaly,
                    source_timestamp_ns=item.source_timestamp_ns,
                )
                counters.inc_influx_write_success()
            except Exception as exc:
                counters.inc_influx_write_failed()
                if continue_on_write_error:
                    print(
                        f"⚠️ Lỗi ghi InfluxDB [{event.turbine_id}] (Seq {event.event_number}): {exc}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"❌ Lỗi ghi InfluxDB [{event.turbine_id}] (Seq {event.event_number}): {exc}. "
                        "Dừng consumer do continue_on_write_error=False.",
                        file=sys.stderr,
                    )
                    break
        finally:
            event_queue.task_done()


def print_experiment_summary(
    settings: LossExperimentSettings,
    counters: ExperimentCounters,
    queue_pending: int,
    elapsed_seconds: float,
) -> None:
    """In định dạng bảng tổng kết thí nghiệm Direct Pipeline Loss."""
    drop_rate = counters.drop_rate()
    throughput = counters.actual_throughput(elapsed_seconds)

    print("\n===== DIRECT PIPELINE LOSS EXPERIMENT =====")
    print(f"Experiment ID:      {settings.experiment_id}")
    print(f"Source rate:        {settings.source_events_per_second} events/s")
    print(f"Queue capacity:     {settings.queue_capacity}")
    print(f"Generated:          {counters.source_generated}")
    print(f"Queue accepted:     {counters.queue_accepted}")
    print(f"Queue dropped:      {counters.queue_dropped}")
    print(f"Processed:          {counters.events_processed}")
    print(f"Influx success:     {counters.influx_write_success}")
    print(f"Influx failed:      {counters.influx_write_failed}")
    print(f"Queue pending:      {queue_pending}")
    print(f"Drop rate:          {drop_rate:.2f}%")
    print(f"Actual throughput:  {throughput:.2f} events/s")
    print("===========================================\n")


def save_experiment_results(
    settings: LossExperimentSettings,
    counters: ExperimentCounters,
    queue_pending: int,
    elapsed_seconds: float,
    output_dir: Path | str | None = None,
) -> Path:
    """Lưu kết quả tổng kết thí nghiệm vào file JSON không chứa thông tin nhạy cảm."""
    target_dir = Path(output_dir) if output_dir else BASE_DIR / settings.result_directory
    target_dir.mkdir(parents=True, exist_ok=True)

    result_file = target_dir / f"{settings.experiment_id}.json"

    drop_rate = counters.drop_rate()
    throughput = counters.actual_throughput(elapsed_seconds)

    payload = {
        "experiment_id": settings.experiment_id,
        "source_generated": counters.source_generated,
        "queue_accepted": counters.queue_accepted,
        "queue_dropped": counters.queue_dropped,
        "events_processed": counters.events_processed,
        "influx_write_success": counters.influx_write_success,
        "influx_write_failed": counters.influx_write_failed,
        "queue_pending": queue_pending,
        "drop_rate_percent": round(drop_rate, 2),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "actual_throughput": round(throughput, 2),
    }

    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return result_file


def run_experiment(
    settings: LossExperimentSettings,
    predictor: Any,
    writer: Any,
    df: Any,
    threshold: float,
    target_column: str,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> tuple[ExperimentCounters, dict[str, Any], float]:
    """Thực thi một vòng thí nghiệm với Producer và Consumer trên hai luồng độc lập.

    Args:
        settings: Cấu hình tham số thí nghiệm.
        predictor: Đối tượng dự đoán (PowerPredictor hoặc mock).
        writer: Đối tượng ghi InfluxDB (ExperimentWriter hoặc mock).
        df: DataFrame chuỗi thời gian đã qua feature engineering.
        threshold: Ngưỡng phát hiện bất thường.
        target_column: Tên cột mục tiêu.
        sleep_fn: Hàm sleep (cho phép injection để test).
        monotonic_fn: Hàm lấy thời gian monotonic.

    Returns:
        tuple (counters, result_dict, elapsed_seconds)
    """
    event_queue: queue.Queue[QueueItem] = queue.Queue(maxsize=settings.queue_capacity)
    counters = ExperimentCounters()
    stop_event = threading.Event()
    producer_done_event = threading.Event()

    sim_settings = SimulatorSettings(
        enabled=True,
        turbine_count=settings.turbine_count,
        events_per_second=settings.source_events_per_second,
        loop_data=True,
        max_events=settings.total_events,
        scenario=settings.scenario,
        random_seed=settings.random_seed,
        burst_start_after_seconds=10.0,
        burst_duration_seconds=10.0,
        burst_multiplier=5.0,
        anomaly_probability=0.05,
        actual_power_multiplier=1.8,
    )

    simulator = SCADASimulator(
        dataframe=df,
        settings=sim_settings,
        target_column=target_column,
        sleep_fn=sleep_fn,
        monotonic_fn=monotonic_fn,
    )
    event_generator = simulator.generate()

    start_time = monotonic_fn()

    producer_thread = threading.Thread(
        target=producer_worker,
        kwargs={
            "event_generator": event_generator,
            "event_queue": event_queue,
            "counters": counters,
            "stop_event": stop_event,
            "producer_done_event": producer_done_event,
        },
        name="LossExperiment-Producer",
    )

    consumer_thread = threading.Thread(
        target=consumer_worker,
        kwargs={
            "experiment_id": settings.experiment_id,
            "event_queue": event_queue,
            "predictor": predictor,
            "writer": writer,
            "threshold": threshold,
            "target_column": target_column,
            "consumer_delay_seconds": settings.consumer_delay_seconds,
            "continue_on_write_error": settings.continue_on_write_error,
            "drain_queue_after_source_stops": settings.drain_queue_after_source_stops,
            "counters": counters,
            "stop_event": stop_event,
            "producer_done_event": producer_done_event,
            "sleep_fn": sleep_fn,
        },
        name="LossExperiment-Consumer",
    )

    producer_thread.start()
    consumer_thread.start()

    try:
        producer_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        print("\n🛑 Nhận tín hiệu ngắt (Ctrl+C). Đang dừng thí nghiệm an toàn...")
        stop_event.set()
        producer_thread.join(timeout=2.0)
        consumer_thread.join(timeout=5.0)

    elapsed_seconds = monotonic_fn() - start_time
    queue_pending = event_queue.qsize()

    print_experiment_summary(
        settings=settings,
        counters=counters,
        queue_pending=queue_pending,
        elapsed_seconds=elapsed_seconds,
    )

    saved_path = save_experiment_results(
        settings=settings,
        counters=counters,
        queue_pending=queue_pending,
        elapsed_seconds=elapsed_seconds,
    )
    print(f"📁 Kết quả đã được lưu tại: {saved_path}")

    summary_dict = {
        "experiment_id": settings.experiment_id,
        "source_generated": counters.source_generated,
        "queue_accepted": counters.queue_accepted,
        "queue_dropped": counters.queue_dropped,
        "events_processed": counters.events_processed,
        "influx_write_success": counters.influx_write_success,
        "influx_write_failed": counters.influx_write_failed,
        "queue_pending": queue_pending,
        "drop_rate_percent": round(counters.drop_rate(), 2),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "actual_throughput": round(counters.actual_throughput(elapsed_seconds), 2),
    }

    return counters, summary_dict, elapsed_seconds


def main() -> None:
    """Entrypoint chạy thí nghiệm Direct Pipeline Loss từ file cấu hình."""
    config_file = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else DEFAULT_EXPERIMENT_CONFIG_PATH
    )

    print(f"Loading experiment settings from: {config_file}")
    settings = load_loss_experiment_settings(config_file)

    app_config = load_config(CONFIG_PATH)
    threshold = float(app_config["anomaly_threshold"])
    features = list(app_config["features"])
    target = str(app_config["target"])

    print("Loading SCADA dataset and preparing features...")
    df_raw = load_data(DATA_PATH, features=features, target=target)
    df = add_time_features(df_raw)

    print("Initializing PowerPredictor (loaded once)...")
    predictor = PowerPredictor(
        model_path=MODEL_PATH,
        scaler_path=SCALER_PATH,
        features=features,
    )

    print("Initializing ExperimentWriter...")
    writer = ExperimentWriter.from_env()

    print(f"\n🚀 Bắt đầu thí nghiệm: '{settings.experiment_id}'")
    print(f"   Turbines: {settings.turbine_count} | Source rate: {settings.source_events_per_second} events/s")
    print(f"   Total events: {settings.total_events} | Queue capacity: {settings.queue_capacity}")
    print(f"   Consumer delay: {settings.consumer_delay_seconds}s | Drain: {settings.drain_queue_after_source_stops}")

    try:
        with writer:
            run_experiment(
                settings=settings,
                predictor=predictor,
                writer=writer,
                df=df,
                threshold=threshold,
                target_column=target,
            )
    except Exception as exc:
        print(f"\n❌ Thí nghiệm thất bại: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
