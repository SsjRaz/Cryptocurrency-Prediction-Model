# Frozen final runner: implementation, not authorization to unblind

This milestone used generated synthetic prices only. No actual 2025+ candles,
fits, predictions or scores were acquired or inspected. The development ZIP
transfer was not retried. The committed preholdout.json dates, target,
comparators, calibration, costs and success criterion are unchanged. Its state
still says no evaluation is authorized. Human authorization and independent
review remain prerequisites for any real final invocation; a CLI acknowledgement
is a guard against mistakes, not proof of permission.

## Three functions, two sealed stages

`freeze_model` fits the final model/calibrator before any final input. Prediction
then seals feature-only probabilities; the outcome-scoring stage consumes the
sealed artifacts without refitting. No pickle or executable model serialization
is used: coefficients, scaler mean/scale, calibrator mapping, prior, timing bounds,
context and identities are JSON.

The fixed freeze time is one nanosecond before 2025-01-01 UTC, satisfying the
existing strictly-before rule. Daily features are built only from trailing bars.
Final training labels must mature strictly before freeze and both classes must
exist. Scaler and logistic fit use exactly that matured prefix; prior is its
label mean. The calibrator is trained on distinct raw OOF predictions generated
by the existing expanding development schedule, with labels strictly matured
before freeze. A fitted two-class calibrator is mandatory: no post-holdout
calibration, calibration fallback or within-holdout parameter update is allowed.
Insufficient history rejects the final primary evaluation, rather than changing
its population after seeing outcomes. Model settings remain the existing ones.

The development frame must contain continuous pre-holdout bars ending at the
2024-12-31 candle open and no 2025 row. That last candle is **not** available at
freeze, and is excluded from fitting/history through the availability/maturity
filters. Fourteen trailing pre-boundary candles are stored separately as feature
context. Some become available after freeze but before the first prediction;
context is not an exception to the fit filter. Synthetic poisoning of the latest
unavailable context proves it cannot affect fitted means/scales/coefficients,
calibration or prior. It can legitimately affect later features once observed.

`predict_features` accepts only the exact planned feature window, no label
construction. It applies frozen arrays to trailing features. Outcome columns
such as target_up are ignored; changing future valid OHLCV cannot change earlier
probabilities. It rejects empty/partial/gapped/overlapping inputs. Metadata checks
prove the declared fit bounds remain constant and strictly pre-holdout, with
proper entry/exit/maturity timing. Input/report hashes independently retained
by the caller bind artifacts; hashes are content identities, not authenticity.
A maliciously forged bundle plus newly accepted hash is outside this guarantee.

`score_once` requires expected canonical hashes of freeze and predictions plus
expected outcome data hash, explicit unblind acknowledgement and a persistent
state directory. The CLI additionally requires the raw outcome file SHA256
retained before metrics. It verifies outcome window, input hashes and exact
feature overlap, independently reconstructs the sealed feature-only predictions,
and never fits a model. Only then does it reserve an exclusive local attempt
marker, construct labels, score all five methods on identical rows, and calculate
the committed paired block uncertainty/success criterion. Fit bounds, receipt,
counts and domains are retained in the report.

The attempt marker is created before label construction/metrics, not before all
file reads/validation. Known existing attempts are rejected by the CLI before
opening outcome files; atomic exclusive creation prevents two callers from
both scoring in the same state location. Concurrent callers could both read
before one wins reservation. Failures after reservation leave the attempt marked;
there is no automatic reset/retry. The requested report includes raw-file
metadata before it is written and fsynced; a separate completion record is
created only afterward and hashes those exact bytes. A crash can leave a report
without completion. Preserve both artifacts and inspect the state rather than
retrying automatically.

This is **not** a global one-unblind mechanism: removing records, choosing a
new state directory, changing accepted model hashes or modifying code can bypass
it. Access control, protected storage and an experiment register remain human/
infrastructure responsibilities. Output files also use exclusive creation.

## Exact final support and separate data interval

With the unchanged 2025 calendar window, aligned target and 60-second lag:

- Feature opens: **2025-01-01 through 2025-12-27 inclusive**, 361 forecasts.
- Decisions: January 2 through December 28 at 00:01 UTC.
- Entry opens: January 3 through December 29; exit opens: January 4 through
  December 30. Last label matures December 31 at 00:01, before the fixed end.
- Full scored outcomes input: **[2025-01-01T00:00:00Z,
  2025-12-31T00:00:00Z)**, exactly 364 daily candles, last open December 30.
- Feature-only file is its first 361 candles, end-exclusive December 28.
- Pre-boundary warm-up is already in the frozen development context:
  December 18–31, 2024. Do not fetch it again as overlapping final rows.

No 2026 candle or extra terminal padding is necessary. The final four feature
opens of the calendar year are omitted by the existing full-exit-candle maturity
rule, not selected from outcomes. Extra rows, even a seemingly harmless December
31 candle, are rejected to keep the domain exact. All 361 scored observations
have calibrated/raw/prior/neutral/persistence predictions; primary prior support
is exactly 361 too. Final reporting also applies the unchanged three cost scenarios and threshold
to the sealed probabilities using the tested long/cash accounting engine. All
strategies, passive and cash share January 3 through December 30 inclusive (362
open marks), terminal liquidation and the existing 1000 quote-unit normalization.
These are explicitly descriptive final cost diagnostics; they do not affect the
statistical success criterion. They do not claim economic returns or exact-
midnight fills.
No such data interval was fetched in this milestone.

## Runtime lock and verification actually performed

Reference: **CPython 3.12.14, Linux x86_64**. `.python-version` selects the patch
version. `requirements-final.in` retains the observed numeric model versions;
`requirements-final.lock` pins the transitive graph with accepted package hashes.
No model versions were optimized from development scores. Runtime guard checks
the patch/platform and every installed locked package; the receipt fingerprints
these and relevant source bytes. Other environments cannot use the final runner
without an explicit reviewed new runtime/receipt. Broad baseline CI still tests
3.11/3.12; final-pipeline tests skip outside the reference lock and a dedicated
3.12.14 hash-locked CI job runs the full suite.

This Cloud task generated the lock using pip-tools 7.7.0/pip 26.3, downloaded only
binary wheels with hash verification, installed them in a **new isolated venv**
using --no-index and --require-hashes, and passed pip check plus the full synthetic
suite. `runtime/reference-wheelhouse.json` records exact wheel names, sizes and
SHA256s; wheel binaries are not committed. Reproduce in the reference interpreter:

```sh
python -m venv /tmp/btc-final-env
/tmp/btc-final-env/bin/python -m pip download --only-binary=:all: --require-hashes \
  -r requirements-final.lock --dest /tmp/btc-final-wheels
/tmp/btc-final-env/bin/python -m pip install --no-index \
  --find-links /tmp/btc-final-wheels --only-binary=:all: --require-hashes \
  -r requirements-final.lock
/tmp/btc-final-env/bin/python -m pip check
/tmp/btc-final-env/bin/python -m pytest -q
```

Network is needed for the download unless that verified wheelhouse is already
available. A future blocked/unavailable index or missing compatible wheels must
be reported, never bypassed with unlocked packages/source builds. Hash-lock
installation and offline installation **were verified here**; offline acquisition,
other architectures/interpreters, all optional dependency extras, OS/libc/kernel,
CPU numerics/BLAS determinism and the interpreter binary/container image digest
were **not** fully locked/verified. Bootstrap pip and lock-generation tools are
not runtime research dependencies and are documented by version rather than
included in the final hash lock. No bit-identical cross-machine claim is made.

Earlier receipts remain as history and are invalidated by this pre-unblinding
source/runtime amendment. A new final-runner receipt is retained in
protocols/final-runner-receipt.json, generated in the clean locked environment.
Dates/criteria have not been amended. Different CPython builds can intentionally
fail the receipt even with the same patch version because sys.version is retained.

## Commands after separate authorization and audit

Use the reference interpreter for every command. Keep independently reviewed
hashes in the experiment register, not recomputed automatically from an arbitrary
replacement input. The canonical JSON hash printed by commands is **not** the
raw file byte hash; the outcome flag intentionally takes raw CSV file SHA256.

1. Verify the development snapshot and current receipt; invoke final-freeze on
   development only with receipt and independently retained protocol hash.
2. Archive the resulting canonical freeze hash; separately receive only the
   exact permitted final interval and record its CSV hash before any metrics.
3. Produce the exact feature-only prefix; final-predict receives the freeze,
   feature CSV and expected freeze hash and seals predictions without outcomes.
4. Retain the canonical prediction hash. After explicit one-evaluation approval,
   final-score receives freeze, predictions, full exact outcomes, their expected
   hashes, a single preserved state directory and --acknowledge-unblind.
5. Preserve outputs/markers even for failure or underperformance. Report all
   fixed sensitivities, support and limitations; never retune from final scores.

Use `python -m src.research final-freeze --help`, final-predict --help and
final-score --help for precise arguments. Full CLI integration tests execute
these only on generated synthetic fixtures, including repeated-attempt rejection.
This implementation does not itself authorize a real final run.

## Independent audit corrections: authenticated buffers and durable reports

Unblinding remains **blocked pending independent re-review**, including any
additional issues still under audit. The previous implementation had two
verified check/use gaps: final-score and load_snapshot hashed a CSV read and
then reopened the pathname for pandas. Both now parse exactly the authenticated
byte buffer with io.BytesIO, never a second pathname read. Thus changing even
the last three outcome-only rows after hashing cannot change the scored prices.
The canonical frame identity used downstream is derived only from those verified
bytes, not a fresh read. Associated source/runtime/protocol receipts are checked;
no real data was routed or recovered to exercise these changes.

Final-freeze now requires independently retained
`--expected-development-csv-sha256`, `--development-pin` and
`--expected-development-pin-sha256` in addition to the receipt/protocol hash.
The pin uses canonical JSON hashing. Production mode binds its hash to the
receipt's approved reference at `protocols/approved-development.json`: schema 1,
Coinbase BTC/USD 1d, [2020-01-01,2025-01-01), exactly 1827 continuous UTC midnight
candles, 60-second assumed lag, historical_publication_verified false, CSV SHA256
77563ae0f45a06ae3f8b63d925543978a8c05e98c9ce4156d6656f2ac3064d8e. The loader checks
all supplied reference fields (including additional archived fields if pinned),
exact CSV column schema, cadence/coverage, source identity, availability and
valid timezone-aware acquisition/as-of metadata. A new self-consistent local
manifest cannot replace the independent CSV/reference pins.

Reusable synthetic inputs require **explicit** --synthetic-development-pin and
their own independent reference/CSV hashes. That mode is recorded in the frozen
bundle; it is not production data approval. The lower-level DataFrame freeze API
also requires an explicit expected frame hash. Computing replacement pins from
an arbitrary new file defeats independent review; hashes are not access control.

Completion semantics have also changed: attempt file and state directory are
flushed/fsynced before metrics. The scorer itself writes the exact requested
final report exclusively, including raw outcome-file identity, flushes/fsyncs
report and parent directory, and only then writes/fsyncs the completion record.
The completion record binds both exact report-file bytes and canonical report
content. The CLI does not rewrite or add metadata after completion. Write/fsync
failure leaves an attempt with **no completion**; a partial or durable report
may exist. Preserve it and state, obtain independent recovery review, and verify
hashes rather than recomputing/rescoring automatically. Report and completion
are not a cross-filesystem atomic transaction; a crash after durable report but
before completion is still possible. The local-marker bypass limitations remain.

Mutable-path, tail-outcome mutation, unapproved-history, manifest-field and
report write/fsync regressions use synthetic data only. The exact maturity
cutoff, 361-feature/364-price support, dates and statistical criteria are unchanged.
