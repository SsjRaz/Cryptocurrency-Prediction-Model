import json

import pandas as pd
import pytest

from src.snapshot import acquire, development_only, load_snapshot, save_snapshot
from src.features import labeled_daily, prepare_daily


START = "2024-01-01T00:00:00Z"
END = "2024-01-04T00:00:00Z"
AS_OF = "2024-01-05T00:00:00Z"


def bars():
    return [[int(t.timestamp() * 1000), 10, 12, 9, 11, 5]
            for t in pd.date_range(START, periods=3, freq="D")]


class Provider:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []

    def fetch_ohlcv(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        return next(self.pages)


def test_overlap_and_unordered_pages():
    a, b, c = bars()
    provider = Provider([[b, a], [c, b]])
    frame, quality = acquire(provider, start=START, end=END, as_of=AS_OF)
    assert len(frame) == 3
    assert quality == {"pages": 2, "identical_overlaps": 1}
    assert provider.calls[1][1]["since"] == c[0]


@pytest.mark.parametrize("case", ["empty", "stall", "gap", "conflict", "invalid", "misaligned"])
def test_acquisition_failures(case):
    a, b, c = bars()
    pages = {"empty": [[a], []], "stall": [[a], [a]], "gap": [[a, c]],
             "conflict": [[a, b], [b[:-1] + [6], c]],
             "invalid": [[a, b, c[:-1] + [-1]]],
             "misaligned": [[a, [b[0] + 1] + b[1:], c]]}[case]
    with pytest.raises(ValueError):
        acquire(Provider(pages), start=START, end=END, as_of=AS_OF)


@pytest.mark.parametrize("as_of", ["2024-01-04T00:00:00Z", "2024-01-05"])
def test_availability_and_timezone(as_of):
    provider = Provider([])
    with pytest.raises(ValueError):
        acquire(provider, start=START, end=END, as_of=as_of)
    assert not provider.calls


def test_snapshot_roundtrip_tampering_and_overwrite(tmp_path):
    frame, quality = acquire(Provider([bars()]), start=START, end=END, as_of=AS_OF)
    path = tmp_path / "snapshot"
    manifest = save_snapshot(path, frame, start=START, end=END, as_of=AS_OF, acquisition=quality)
    loaded, actual = load_snapshot(path)
    pd.testing.assert_frame_equal(frame, loaded)
    assert manifest == actual
    with pytest.raises(FileExistsError):
        save_snapshot(path, frame, start=START, end=END, as_of=AS_OF)
    with (path / "candles.csv").open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(ValueError, match="checksum"):
        load_snapshot(path)


def test_manifest_identity_and_coverage(tmp_path):
    frame, _ = acquire(Provider([bars()]), start=START, end=END, as_of=AS_OF)
    path = tmp_path / "snapshot"
    manifest = save_snapshot(path, frame, start=START, end=END, as_of=AS_OF)
    manifest["rows"] = 4
    (path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="coverage"):
        load_snapshot(path)


def test_holdout_is_removed_before_labeling():
    n = 200
    times = pd.date_range(START, periods=n, freq="D")
    frame = pd.DataFrame({"timestamp": times, "open": 10., "high": 12., "low": 9., "close": 11., "volume": 5.})
    boundary = times[150]
    dev = development_only(frame, holdout_start=boundary, as_of=times[-1])
    changed = frame.copy()
    changed.loc[150:, ["open", "high", "low", "close"]] *= 100
    other = development_only(changed, holdout_start=boundary, as_of=times[-1])
    pd.testing.assert_frame_equal(labeled_daily(dev), labeled_daily(other))
    assert labeled_daily(dev).timestamp.max() == times[148]
    assert development_only(frame, holdout_start=boundary, as_of=times[100]).timestamp.max() == times[98]


def test_nanosecond_misalignment():
    frame, _ = acquire(Provider([bars()]), start=START, end=END, as_of=AS_OF)
    frame.loc[0, "timestamp"] += pd.Timedelta(nanoseconds=1)
    with pytest.raises(ValueError, match="midnight|00:00"):
        prepare_daily(frame)


def test_naive_candle_timestamp_rejected():
    frame, _ = acquire(Provider([bars()]), start=START, end=END, as_of=AS_OF)
    frame["timestamp"] = frame.timestamp.dt.tz_localize(None)
    with pytest.raises(ValueError, match="timezone"):
        prepare_daily(frame)


def test_cli_requires_holdout_and_excludes_it(tmp_path, capsys):
    from src.main import main
    n = 180
    import numpy as np
    prices = 100 * np.exp(np.cumsum(0.01 * np.sin(np.arange(n))))
    frame = pd.DataFrame({"timestamp": pd.date_range(START, periods=n, freq="D"),
                          "open": prices, "high": prices * 1.01, "low": prices * .99,
                          "close": prices, "volume": 10.})
    path = tmp_path / "input.csv"
    frame.to_csv(path, index=False)
    with pytest.raises(SystemExit):
        main(["--csv", str(path)])
    result = main(["--csv", str(path), "--holdout-start", frame.timestamp.iloc[160].isoformat(),
                   "--as-of", frame.timestamp.iloc[-1].isoformat(), "--mode", "backtest", "--splits", "3"])
    assert result["backtest"]["rows_labeled"] == 145
    assert result["backtest"]["folds"][-1]["test_end_utc"] == frame.timestamp.iloc[158].isoformat()
