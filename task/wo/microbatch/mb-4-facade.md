# MB-4 — the facade: builders, StreamingQuery, the SES-DECL flips                       grade: B   engine band: Muse at max   release: 1.7

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why and what is out of scope

D-8's door: `readStream.format("iceberg")`, `writeStream`, `trigger`,
`foreachBatch`, `StreamingQuery`; the SES-DECL rows flip from refusal to
answer and the three IPI-47 cells (`R-STREAM-READ`, `R-STREAM-READ-SKIP`,
`W-STREAM-WRITE-FILESRC`) get measured. Surface first against a stub
binding, wire-up last. Out of scope, named: the driver (MB-3), multi-source
(MB-5), continuous mode, Kafka.

## 1. Rulings already made

- D-8, the sketch's error-class table (§4), the Q1/Q2/Q3/Q9 answers, the
  MB-0 cells behind them, MB-3 merged for wire-up (surface needs only the
  sketch).

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `python/repark/src/repark/spark/streaming/__init__.py` | new: the builders and `StreamingQuery` re-exports | default 1000 (`scripts/check_lib_py.py`) |
| `python/repark/src/repark/spark/streaming/readers.py` | new: `readStream` / `writeStream` builders, `trigger`, `foreachBatch` | default 1000 |
| `python/repark/src/repark/spark/streaming/query.py` | new: `StreamingQuery` (`start`, `awaitTermination`, `stop`, `lastProgress`) | default 1000 |
| `python/repark/src/repark/spark/streaming/map.md` | new: one row per file | lockstep |
| `crates/repark-python/src/streaming.rs` | new: the binding module | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-python/src/lib.rs` | edited: one `mod` line (185 lines, headroom) | default 1000 |
| `python/repark/src/repark/spark/session/session_surface.py` | edited: `readStream` / `streams` flip from refusal (635 lines, headroom) | default 1000 |
| `docs/spark-sql-iceberg-parity.md` | edited: the SES-DECL-readStream / SES-DECL-streams rows flip | prose |

`python/repark/src/repark/spark/session/session_core.py` (exception
baseline 2277, exact) is NOT edited.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the builder method signatures, the `StreamingQuery`
state machine (Q7), the `foreachBatch` callable signature (Q9), the exact
error class per refusal (Q1…Q3, O-3, O-5, O-6), and the stub-binding seam
for the surface-first half.

## 4. Steps

Step 1. Surface against a stub binding: builders, `StreamingQuery`, error
classes, facade docstrings mirroring PySpark's. Step 2. The IPI-47 cells
measured against the stub. Step 3. Wire-up to MB-3. Step 4. The SES-DECL
rows flip; the registry records the carve-out from IPI-47 closed on the
three cells. Step 5. Commit (surface and wire-up may be two commits).

## 5. Gates and their expected output

- The MB-0 cells replayed through the facade: equal to the oracle.
- The three IPI-47 cells EQUAL.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- `uvx typos@1.47.2 python/repark/src/repark/spark/streaming` — exit 0.
- One scoped Opus verifier pass on wire-up only.

## 6. Halt rules

1. The sketch's error-class table or Q9 answer is missing.
2. Wire-up opens before MB-3 merges.
3. A facade behaviour needs Python row compute (the Rust rule): halt, do
  not build it in Python.
4. Any neighbour cell (the session-surface pins) changes its answer beyond
  the two SES-DECL flips.

## 7. Hand-back

`{"status":"DONE|HALT","ipi47":"3 EQUAL|…","verifier":"pass|fail|n/a-surface-only","questions":[]}`
plus the commit sha(s).

## Carried from MB-3

The open S3 findings of the MB-3 re-verify (`/tmp/oc-worker/direct/wo/microbatch/mb3/reverify3/verdict.json`,
orchestrator scratch, not in the repo). Titles only, verbatim. Each is a `findings` entry with `sev` S3; the verdict gives these entries no closed status.

- No pin runs a real race through the fence at the driver level: the three pins that need the fence each inject one commit at one fixed load
- A restart after the fence's RecoveryRequired ending is not pinned and I did not run it
