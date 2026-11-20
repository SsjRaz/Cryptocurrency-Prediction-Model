"""Reference runtime lock verification; wheels/interpreter authenticity is external."""
import hashlib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import platform
import re
import sys

LOCK = Path(__file__).resolve().parents[1] / "requirements-final.lock"


def runtime_record():
    text = LOCK.read_text()
    pins = dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^\s\\]+)", text, flags=re.MULTILINE))
    actual = {}
    for name in pins:
        try:
            actual[name] = version(name)
        except PackageNotFoundError:
            actual[name] = None
    return {"lock_sha256": hashlib.sha256(text.encode()).hexdigest(), "pins": pins, "installed": actual,
            "python": platform.python_version(), "system": platform.system(), "machine": platform.machine()}


def require_reference_runtime():
    record = runtime_record()
    if record["python"] != "3.12.14" or record["system"] != "Linux" or record["machine"] != "x86_64" or record["pins"] != record["installed"]:
        raise ValueError("Final runner requires locked CPython 3.12.14 Linux x86_64 runtime; install requirements-final.lock")
    return record
