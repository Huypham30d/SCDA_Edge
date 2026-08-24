from dataclasses import asdict, dataclass
import json
import math
from typing import Any


class EventSchemaError(ValueError):
    """Ngoại lệ khi event không tuân thủ schema hoặc chứa dữ liệu không hợp lệ."""
    pass


@dataclass(frozen=True)
class ScadaEvent:
    """Biểu diễn dữ liệu SCADA thô từ một tuabin phát điện tại một thời điểm đo."""

    schema_version: int
    event_id: str
    run_id: str
    event_number: int
    emitted_at_ns: int
    measurement_timestamp: str
    turbine_id: str
    source_row_index: int
    scenario: str
    injected_anomaly: bool
    wind_speed: float
    theoretical_power: float
    wind_direction: float
    actual_power: float

    def __post_init__(self) -> None:
        """Tự động kiểm tra tính hợp lệ của dữ liệu khi khởi tạo object."""
        self.validate()

    def validate(self) -> None:
        """Kiểm tra toàn diện tính hợp lệ của các trường trong event."""
        # 1. Schema version
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise EventSchemaError(
                f"schema_version không được hỗ trợ: {self.schema_version}. Chỉ hỗ trợ phiên bản 1."
            )

        # 2. String fields
        for field_name in ("event_id", "run_id", "measurement_timestamp", "turbine_id", "scenario"):
            val = getattr(self, field_name)
            if not isinstance(val, str) or not val.strip():
                raise EventSchemaError(
                    f"Trường '{field_name}' phải là chuỗi ký tự không rỗng. Nhận được: {repr(val)}"
                )

        # 3. Integer fields
        if type(self.event_number) is not int or self.event_number < 1:
            raise EventSchemaError(
                f"Trường 'event_number' phải là số nguyên dương >= 1. Nhận được: {self.event_number}"
            )

        if type(self.source_row_index) is not int or self.source_row_index < 0:
            raise EventSchemaError(
                f"Trường 'source_row_index' phải là số nguyên >= 0. Nhận được: {self.source_row_index}"
            )

        if type(self.emitted_at_ns) is not int or self.emitted_at_ns <= 0:
            raise EventSchemaError(
                f"Trường 'emitted_at_ns' phải là số nguyên dương tính bằng nanosecond. Nhận được: {self.emitted_at_ns}"
            )

        # 4. Boolean field
        if not isinstance(self.injected_anomaly, bool):
            raise EventSchemaError(
                f"Trường 'injected_anomaly' phải là boolean. Nhận được: {repr(self.injected_anomaly)}"
            )

        # 5. Float numerical fields (không cho phép NaN hoặc Infinity)
        float_fields = (
            ("wind_speed", self.wind_speed),
            ("theoretical_power", self.theoretical_power),
            ("wind_direction", self.wind_direction),
            ("actual_power", self.actual_power),
        )
        for name, val in float_fields:
            if type(val) not in (int, float) or isinstance(val, bool):
                raise EventSchemaError(
                    f"Trường '{name}' phải là số thực (float/int). Nhận được: {repr(val)}"
                )
            float_val = float(val)
            if math.isnan(float_val) or math.isinf(float_val):
                raise EventSchemaError(
                    f"Trường '{name}' không được là NaN hoặc Infinity. Nhận được: {float_val}"
                )

    def to_dict(self) -> dict[str, Any]:
        """Chuyển đổi event sang dictionary Python chuẩn."""
        return asdict(self)

    def to_json(self) -> str:
        """Tuần tự hóa event sang chuỗi JSON định dạng UTF-8."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScadaEvent":
        """Khởi tạo và xác thực ScadaEvent từ dictionary."""
        if not isinstance(data, dict):
            raise EventSchemaError(
                f"Dữ liệu đầu vào phải là dict, nhận được: {type(data).__name__}"
            )

        # Kiểm tra không chứa các time-features phái sinh trong raw event
        forbidden_features = {"Day_sin", "Day_cos", "Month_sin", "Month_cos"}
        found_forbidden = forbidden_features.intersection(data.keys())
        if found_forbidden:
            raise EventSchemaError(
                f"Raw ScadaEvent không được chứa các đặc trưng phái sinh: {found_forbidden}"
            )

        required_fields = {
            "schema_version",
            "event_id",
            "run_id",
            "event_number",
            "emitted_at_ns",
            "measurement_timestamp",
            "turbine_id",
            "source_row_index",
            "scenario",
            "injected_anomaly",
            "wind_speed",
            "theoretical_power",
            "wind_direction",
            "actual_power",
        }

        missing = required_fields - set(data.keys())
        if missing:
            raise EventSchemaError(f"Dữ liệu event thiếu các trường bắt buộc: {missing}")

        try:
            return cls(
                schema_version=int(data["schema_version"]),
                event_id=str(data["event_id"]),
                run_id=str(data["run_id"]),
                event_number=int(data["event_number"]),
                emitted_at_ns=int(data["emitted_at_ns"]),
                measurement_timestamp=str(data["measurement_timestamp"]),
                turbine_id=str(data["turbine_id"]),
                source_row_index=int(data["source_row_index"]),
                scenario=str(data["scenario"]),
                injected_anomaly=bool(data["injected_anomaly"]),
                wind_speed=float(data["wind_speed"]),
                theoretical_power=float(data["theoretical_power"]),
                wind_direction=float(data["wind_direction"]),
                actual_power=float(data["actual_power"]),
            )
        except (ValueError, TypeError) as exc:
            raise EventSchemaError(f"Lỗi chuyển đổi kiểu dữ liệu trường trong event: {exc}") from exc

    @classmethod
    def from_json(cls, json_str: str | bytes) -> "ScadaEvent":
        """Khởi tạo ScadaEvent từ chuỗi hoặc bytes JSON."""
        try:
            if isinstance(json_str, bytes):
                json_str = json_str.decode("utf-8")
            data = json.loads(json_str)
        except Exception as exc:
            raise EventSchemaError(f"Không thể giải mã JSON UTF-8: {exc}") from exc

        return cls.from_dict(data)
