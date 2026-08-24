import json
import math
import pytest
from src.event_schema import EventSchemaError, ScadaEvent


@pytest.fixture
def valid_event_kwargs() -> dict:
    return {
        "schema_version": 1,
        "event_id": "run-test123:1",
        "run_id": "run-test123",
        "event_number": 1,
        "emitted_at_ns": 1700000000000000000,
        "measurement_timestamp": "01 01 2018 00:00",
        "turbine_id": "T1",
        "source_row_index": 0,
        "scenario": "normal",
        "injected_anomaly": False,
        "wind_speed": 5.31,
        "theoretical_power": 416.32,
        "wind_direction": 259.99,
        "actual_power": 380.04,
    }


def test_scada_event_creation_and_validation(valid_event_kwargs):
    """Xác nhận ScadaEvent khởi tạo thành công và chứa đầy đủ dữ liệu."""
    event = ScadaEvent(**valid_event_kwargs)
    assert event.schema_version == 1
    assert event.turbine_id == "T1"
    assert event.event_id == "run-test123:1"
    assert event.actual_power == 380.04
    assert event.injected_anomaly is False


def test_scada_event_json_and_dict_roundtrip(valid_event_kwargs):
    """Xác nhận tuần tự hóa sang dict/json và giải mã trở lại hoàn toàn chính xác."""
    event = ScadaEvent(**valid_event_kwargs)
    d = event.to_dict()
    assert isinstance(d, dict)
    assert d["turbine_id"] == "T1"

    json_str = event.to_json()
    assert isinstance(json_str, str)

    event_from_dict = ScadaEvent.from_dict(d)
    assert event_from_dict == event

    event_from_json = ScadaEvent.from_json(json_str)
    assert event_from_json == event

    # Test from bytes
    event_from_bytes = ScadaEvent.from_json(json_str.encode("utf-8"))
    assert event_from_bytes == event


def test_scada_event_rejects_unsupported_schema_version(valid_event_kwargs):
    """Từ chối schema_version khác 1."""
    data = dict(valid_event_kwargs, schema_version=2)
    with pytest.raises(EventSchemaError, match="schema_version không được hỗ trợ"):
        ScadaEvent(**data)


def test_scada_event_rejects_empty_strings(valid_event_kwargs):
    """Từ chối các trường string bị rỗng."""
    for field in ("event_id", "run_id", "measurement_timestamp", "turbine_id", "scenario"):
        data = dict(valid_event_kwargs)
        data[field] = "   "
        with pytest.raises(EventSchemaError, match="chuỗi ký tự không rỗng"):
            ScadaEvent(**data)


def test_scada_event_rejects_invalid_numbers(valid_event_kwargs):
    """Từ chối event_number < 1 hoặc source_row_index < 0."""
    data_inv_ev = dict(valid_event_kwargs, event_number=0)
    with pytest.raises(EventSchemaError, match="event_number"):
        ScadaEvent(**data_inv_ev)

    data_inv_row = dict(valid_event_kwargs, source_row_index=-1)
    with pytest.raises(EventSchemaError, match="source_row_index"):
        ScadaEvent(**data_inv_row)

    data_inv_ns = dict(valid_event_kwargs, emitted_at_ns=0)
    with pytest.raises(EventSchemaError, match="emitted_at_ns"):
        ScadaEvent(**data_inv_ns)


def test_scada_event_rejects_nan_and_infinity(valid_event_kwargs):
    """Từ chối giá trị NaN hoặc Infinity trong các trường số thực."""
    for float_field in ("wind_speed", "theoretical_power", "wind_direction", "actual_power"):
        # Test NaN
        data_nan = dict(valid_event_kwargs)
        data_nan[float_field] = float("nan")
        with pytest.raises(EventSchemaError, match="không được là NaN hoặc Infinity"):
            ScadaEvent(**data_nan)

        # Test Infinity
        data_inf = dict(valid_event_kwargs)
        data_inf[float_field] = float("inf")
        with pytest.raises(EventSchemaError, match="không được là NaN hoặc Infinity"):
            ScadaEvent(**data_inf)


def test_scada_event_rejects_derived_time_features(valid_event_kwargs):
    """Từ chối raw event nếu chứa các đặc trưng phái sinh như Day_sin, Day_cos."""
    raw_dict = dict(valid_event_kwargs)
    raw_dict["Day_sin"] = 0.5
    with pytest.raises(EventSchemaError, match="không được chứa các đặc trưng phái sinh"):
        ScadaEvent.from_dict(raw_dict)


def test_scada_event_missing_required_fields():
    """Từ chối from_dict nếu thiếu trường bắt buộc."""
    incomplete_dict = {
        "schema_version": 1,
        "event_id": "1",
        "turbine_id": "T1",
    }
    with pytest.raises(EventSchemaError, match="thiếu các trường bắt buộc"):
        ScadaEvent.from_dict(incomplete_dict)


def test_scada_event_invalid_json():
    """Từ chối chuỗi JSON sai cú pháp."""
    with pytest.raises(EventSchemaError, match="Không thể giải mã JSON"):
        ScadaEvent.from_json("invalid json string {{{")
