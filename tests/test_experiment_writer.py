from unittest.mock import MagicMock, patch
import pytest
from influxdb_client import Point
from experiments.experiment_writer import ExperimentWriter


def test_experiment_writer_init_validation() -> None:
    """Kiểm tra khởi tạo ExperimentWriter yêu cầu bucket và token hợp lệ."""
    with pytest.raises(ValueError, match="Tên bucket thí nghiệm không được để trống"):
        ExperimentWriter(
            url="http://localhost:8086",
            token="token123",
            org="scada_org",
            bucket="",
        )

    with pytest.raises(ValueError, match="Token kết nối InfluxDB không được để trống"):
        ExperimentWriter(
            url="http://localhost:8086",
            token="  ",
            org="scada_org",
            bucket="scada_experiment",
        )


def test_experiment_writer_from_env_missing_bucket(monkeypatch) -> None:
    """Kiểm tra from_env() báo lỗi khi thiếu EXPERIMENT_INFLUX_BUCKET."""
    monkeypatch.delenv("EXPERIMENT_INFLUX_BUCKET", raising=False)
    monkeypatch.setenv("INFLUX_TOKEN", "valid_token")

    with pytest.raises(
        ValueError, match="Biến môi trường 'EXPERIMENT_INFLUX_BUCKET' là bắt buộc"
    ):
        ExperimentWriter.from_env()


def test_experiment_writer_from_env_missing_token(monkeypatch) -> None:
    """Kiểm tra from_env() báo lỗi khi thiếu INFLUX_TOKEN."""
    monkeypatch.setenv("EXPERIMENT_INFLUX_BUCKET", "scada_experiment")
    monkeypatch.delenv("INFLUX_TOKEN", raising=False)

    with pytest.raises(
        ValueError, match="Biến môi trường 'INFLUX_TOKEN' là bắt buộc"
    ):
        ExperimentWriter.from_env()


def test_experiment_writer_from_env_success(monkeypatch) -> None:
    """Kiểm tra from_env() khởi tạo thành công khi đủ biến môi trường."""
    monkeypatch.setenv("EXPERIMENT_INFLUX_BUCKET", "scada_experiment")
    monkeypatch.setenv("INFLUX_TOKEN", "my_secret_token")
    monkeypatch.setenv("INFLUX_URL", "http://influx.local:8086")
    monkeypatch.setenv("INFLUX_ORG", "test_org")

    with patch("experiments.experiment_writer.InfluxDBClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        writer = ExperimentWriter.from_env()
        assert writer.bucket == "scada_experiment"
        assert writer.org == "test_org"
        assert writer.url == "http://influx.local:8086"
        assert writer.token == "my_secret_token"
        mock_client_cls.assert_called_once_with(
            url="http://influx.local:8086",
            token="my_secret_token",
            org="test_org",
        )


def test_experiment_writer_write_event_schema() -> None:
    """Kiểm tra Point được tạo đúng measurement, tag, field và kiểu dữ liệu."""
    with patch("experiments.experiment_writer.InfluxDBClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api
        mock_client_cls.return_value = mock_client

        writer = ExperimentWriter(
            url="http://localhost:8086",
            token="test_token",
            org="scada_org",
            bucket="scada_experiment",
        )

        writer.write_event(
            experiment_id="exp_test_01",
            turbine_id="T2",
            event_sequence=42,
            wind_speed=12.5,
            actual_power=1500.0,
            predicted_power=1480.0,
            residual=20.0,
            anomaly_flag=0,
            source_timestamp_ns=1700000000000000000,
        )

        mock_write_api.write.assert_called_once()
        call_kwargs = mock_write_api.write.call_args.kwargs
        assert call_kwargs["bucket"] == "scada_experiment"
        assert call_kwargs["org"] == "scada_org"

        point: Point = call_kwargs["record"]
        assert point._name == "direct_pipeline_experiment"
        assert point._tags["experiment_id"] == "exp_test_01"
        assert point._tags["turbine_id"] == "T2"

        # event_sequence phải là field, không phải tag
        assert "event_sequence" not in point._tags
        assert point._fields["event_sequence"] == 42
        assert isinstance(point._fields["event_sequence"], int)

        assert point._fields["wind_speed"] == 12.5
        assert isinstance(point._fields["wind_speed"], float)
        assert point._fields["actual_power"] == 1500.0
        assert isinstance(point._fields["actual_power"], float)
        assert point._fields["predicted_power"] == 1480.0
        assert isinstance(point._fields["predicted_power"], float)
        assert point._fields["residual"] == 20.0
        assert isinstance(point._fields["residual"], float)
        assert point._fields["anomaly_flag"] == 0
        assert isinstance(point._fields["anomaly_flag"], int)
        assert point._fields["source_timestamp_ns"] == 1700000000000000000
        assert isinstance(point._fields["source_timestamp_ns"], int)
        assert point._time == 1700000000000000000


def test_experiment_writer_write_error_propagated() -> None:
    """Kiểm tra lỗi write API được ném ra ngoài dưới dạng RuntimeError."""
    with patch("experiments.experiment_writer.InfluxDBClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_write_api.write.side_effect = ConnectionError("InfluxDB connection refused")
        mock_client.write_api.return_value = mock_write_api
        mock_client_cls.return_value = mock_client

        writer = ExperimentWriter(
            url="http://localhost:8086",
            token="test_token",
            org="scada_org",
            bucket="scada_experiment",
        )

        with pytest.raises(RuntimeError, match="Lỗi khi ghi dữ liệu experiment"):
            writer.write_event(
                experiment_id="exp_fail",
                turbine_id="T1",
                event_sequence=1,
                wind_speed=10.0,
                actual_power=100.0,
                predicted_power=100.0,
                residual=0.0,
                anomaly_flag=0,
            )


def test_experiment_writer_close_and_context_manager() -> None:
    """Kiểm tra close() và context manager đóng client InfluxDB."""
    with patch("experiments.experiment_writer.InfluxDBClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        writer = ExperimentWriter(
            url="http://localhost:8086",
            token="test_token",
            org="scada_org",
            bucket="scada_experiment",
        )

        writer.close()
        mock_client.close.assert_called_once()

        mock_client.reset_mock()
        with writer:
            pass
        mock_client.close.assert_called_once()
