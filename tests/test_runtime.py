import pytest

from src import runtime


def test_lock_has_exact_versions_and_recorded_hash():
    record = runtime.runtime_record()
    assert len(record["pins"]) >= 20
    assert record["pins"]["scipy"] == "1.17.0"
    assert len(record["lock_sha256"]) == 64


def test_reference_guard_rejects_dependency_or_interpreter_drift(monkeypatch):
    record = runtime.runtime_record()
    record.update(python="3.12.14", system="Linux", machine="x86_64", installed=dict(record["pins"]))
    monkeypatch.setattr(runtime, "runtime_record", lambda: record)
    assert runtime.require_reference_runtime() == record
    record["installed"]["numpy"] = "different"
    with pytest.raises(ValueError, match="locked"):
        runtime.require_reference_runtime()
    record["installed"] = dict(record["pins"])
    record["python"] = "3.12.13"
    with pytest.raises(ValueError):
        runtime.require_reference_runtime()
