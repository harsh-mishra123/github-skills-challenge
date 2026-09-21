import json
import runpy
from pathlib import Path

from src.aiops_pipeline import load_data, run_pipeline
from src.anomaly_detector import AnomalyDetector
from src.event_producer import EventProducer
from src.event_topic import EventTopic

SRC_PIPELINE = Path(__file__).resolve().parents[1] / "src" / "aiops_pipeline.py"


def make_record(**overrides):
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 100,
        "cpu_percent": 40,
        "memory_percent": 40,
        "log_level": "INFO",
        "message": "ok",
    }
    record.update(overrides)
    return record


def write_data(path, records):
    path.write_text(json.dumps(records), encoding="utf-8")


# ---- AnomalyDetector ----

def test_high_response_time_reason():
    event = AnomalyDetector().detect(make_record(response_time_ms=900))
    assert event["reasons"] == ["High response time"]


def test_high_cpu_reason():
    event = AnomalyDetector().detect(make_record(cpu_percent=95))
    assert event["reasons"] == ["High CPU utilization"]


def test_high_memory_reason():
    event = AnomalyDetector().detect(make_record(memory_percent=95))
    assert event["reasons"] == ["High memory utilization"]


def test_warning_log_level_is_flagged():
    event = AnomalyDetector().detect(make_record(log_level="WARNING"))
    assert event is not None
    assert event["type"] == "ANOMALY"


def test_multiple_reasons_are_collected():
    event = AnomalyDetector().detect(
        make_record(response_time_ms=900, cpu_percent=95, memory_percent=95)
    )
    assert len(event["reasons"]) == 3


def test_custom_thresholds_are_used():
    detector = AnomalyDetector(response_time_threshold=50)
    assert detector.detect(make_record(response_time_ms=100)) is not None


# ---- EventProducer / EventTopic ----

def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_topic_clear_removes_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})
    topic.clear()

    assert topic.get_messages() == []


# ---- Pipeline ----

def test_load_data_reads_json(tmp_path):
    data_file = tmp_path / "data.json"
    write_data(data_file, [make_record()])

    assert load_data(str(data_file)) == [make_record()]


def test_run_pipeline_counts_records_and_anomalies(tmp_path):
    data_file = tmp_path / "data.json"
    write_data(data_file, [make_record(), make_record(cpu_percent=99)])

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["anomalies_detected"][0]["service"] == "payment-service"


def test_pipeline_main_prints_summary(tmp_path, monkeypatch, capsys):
    (tmp_path / "data").mkdir()
    write_data(tmp_path / "data" / "service_data.json", [make_record(cpu_percent=99)])
    monkeypatch.chdir(tmp_path)

    runpy.run_path(str(SRC_PIPELINE), run_name="__main__")

    output = capsys.readouterr().out
    assert "AIOps Pipeline Result" in output
    assert "Records processed: 1" in output
    assert "Anomalies detected: 1" in output
