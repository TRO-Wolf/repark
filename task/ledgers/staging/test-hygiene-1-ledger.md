# Charter ledger — TEST-HYGIENE-1 · four test-hygiene fixes, test code only

**Date:** 2026-09-18 · **Branch:** `test/test-hygiene-1` · **Base:** `889bac015b5212dbe71a43fe0120ca4c4382fc8e` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `TZ-9` (new OPEN row, item 3).

**Retires:** the orchestrator's departure edit moves this ledger to `../completed/`; this unit
leaves `STATUS.md`, CI workflows and product code untouched by instruction.

**Scope:** test code only — one Rust test helper, three Python test files plus
`conftest.py`, four `map.md` files, one registry row, this ledger. No product
code, no dependency, no workflow change. No code comment added anywhere
(`comment-ban hits=0` post-commit); Python docstrings only where the presence
gate needs them.

## Measurements (decide-then-build evidence)

**M-1 — the Spark v3 fixture cannot relocate without editing Avro bytes.**
`metadata/v8.metadata.json` carries 15 `/tmp/repark-v3-1-spark-mor` hits
(`location` + per-snapshot `manifest-list`). The deflate blocks inside Avro carry
more: every `manifest_path` in `snap-*.avro` is absolute
(`/tmp/repark-v3-1-spark-mor/metadata/<name>-m0.avro`) and every data-file path in
`*-m0.avro` is absolute (`/tmp/repark-v3-1-spark-mor/data/…parquet`,
`…-deletes.puffin`). Read with `/tmp/avro_paths.py` (scratch, not committed).
A copy under another root cannot read without rewriting Avro bytes, which the
brief forbids. Decision: keep the path, exclude cross-process.

**M-2 — the v3_dv fixture is the same shape.** `metadata/v1.metadata.json`
`location` is `/tmp/repark-torture-v3dv/ns/v3dv` (1 hit); `snap-*.avro`
`manifest_path` entries and `*-m0.avro` data paths are absolute under
`/tmp/repark-torture-v3dv` inside deflate blocks. Decision: keep the path,
exclude cross-process with `fcntl.flock`.

**M-3 — RePark answers the UTC date under a non-UTC session zone.**
Probe on the release module (`.venv`, 2026-09-18):

```text
Pacific/Kiritimati got: [datetime.date(2026, 9, 18)] session-zone date: 2026-09-19 utc date: 2026-09-18 match-session: False match-utc: True
Etc/GMT+12 got: [datetime.date(2026, 9, 18)] session-zone date: 2026-09-18 utc date: 2026-09-18 match-session: True match-utc: True
UTC got: [datetime.date(2026, 9, 18)] session-zone date: 2026-09-18 utc date: 2026-09-18 match-session: True match-utc: True
```

`spark.conf.get("spark.sql.session.timeZone")` echoes each zone, so the pin can
read and set the zone through the session conf. The Kiritimati leg disagrees
with the session-zone date: product defect, not a test bug — filed as registry
row `TZ-9` (OPEN), pinned strict-xfail, no product code touched. Spark's
session-zone answer was measured by the orchestrator (run 22b, 2026-09-18 13:03 UTC,
PySpark 4.1.2, one local JVM): `SELECT current_date()` answered `2026-09-19` under
`Pacific/Kiritimati` and `2026-09-18` under `UTC` and `Etc/GMT+12`, each equal to the
zone's calendar date at that instant.

## PROPOSITION LEDGER — TEST-HYGIENE-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `call_register.rs` excludes cross-process while keeping the baked-in path: an exclusive `std::fs::File::lock` on a lock file beside the path, held for the fixture lifetime, wipe-and-copy under the lock. | The diff; `cargo test -p repark-spark --lib call_register` green twice concurrently (same target dir serializes the build, binaries run concurrently). | **PROVEN** | Round 1 used an atomic `create_dir` lock directory; the orchestrator replaced it (run 22b) with `SparkV3FileLock`: `std::fs::File::lock` (stable since Rust 1.89, pure `std`, no new dependency) on `/tmp/repark-v3-1-spark-mor.lock`, which the kernel releases when the process exits, so a killed test run cannot leave a stale lock that stalls every later run. The in-process `Mutex` stays the fast path; the fixture holds both. Re-proven after the change: Single: 12 passed in 0.19 s. Concurrent double: `a_exit=0 b_exit=0`, both `12 passed; 0 failed`. pins: test-hygiene-1/C-001 |
| C-002 | `test_torture_v3_dv.py` holds an exclusive cross-process lock while the canonical copy is materialized AND read. | The diff; file alone green; two copies concurrently green. | **PROVEN** | Module-scoped `v3dv_table` wraps materialize + register + yield in `_hold_canonical_table_lock` (`fcntl.LOCK_EX` on `/tmp/repark-torture-v3dv-module.lock`, beside the canonical root and distinct from `_support`'s `v3dv.lock` dir so the inner `_DirLock` never deadlocks). Alone: `7 passed, 1 skipped in 0.61 s`. Concurrent double (detached, backgrounding blocked in this shell): `a_exit=0 b_exit=0`, both `7 passed, 1 skipped`. pins: test-hygiene-1/C-002 |
| C-003 | The `current_date` pin compares against the session-zone date; the deterministic Kiritimati / `Etc/GMT+12` pin is strict-xfail under `TZ-9` with a dated OPEN registry row. | The diff; the file green with 2 xfails; the new pin fails with `--runxfail` (bite); `TZ-9` in the registry. | **PROVEN** | `test_q14_current_date_bare_and_paren` reads the zone from `spark.conf` (`ZoneInfo`) with a before/after midnight guard. New `test_q14_current_date_answers_the_session_zone_date` sets each zone via `conf.set`, asserts the round-trip, guards midnight. File: `112 passed, 2 xfailed in 0.45 s`. `--runxfail`: `2 failed` (both ANSI legs, Kiritimati assert) — the pin bites. Registry `### TZ-9` added before `### FN-1` in `docs/spark-sql-iceberg-parity.md` (repark / Spark / Pin / Rationale, OPEN 2026-09-18). pins: test-hygiene-1/C-003 |
| C-004 | The 500-table RSS leg carries a registered `perf` marker; nothing selects it out in CI or the default config. | The diff; collected by default; deselected by `-m "not perf"`; file green; both CI pytest lines unchanged. | **PROVEN** | `@pytest.mark.perf` on `test_peak_rss_over_five_hundred_tables_stays_within_the_default_cache_budget`; `pytest_configure` in `python/repark/tests/conftest.py` registers `perf`. `--collect-only | grep five_hundred`: 1 line collected. Same with `-m "not perf"`: 0 lines, `35/36 tests collected (1 deselected)`. `wheels.yml:64` (`pytest python/repark/tests -q -n 4`) and `parity-live.yml:62` untouched — verified by `git diff --stat` showing no workflow file. File: `34 passed, 2 skipped in 72.73 s`. pins: test-hygiene-1/C-004 |
| C-005 | `task/ledgers/staging/map.md` carries each ledger entry once: the two extra `array-null-1` blocks and the short `fnp-11b` charter line are gone; no other exactly-duplicated block exists. | The diff; a block-identity scan over the file reports no duplicate. | **PROVEN** | Removed the 2nd and 3rd `array-null-1` blocks (kept the first; all three byte-identical) and the one-line `fnp-11b` charter entry (kept the full `FNP-11B step 1` block). Scan: 107 entry blocks, zero with count > 1. pins: test-hygiene-1/C-005 |

## Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m ruff check` on the 4 touched Python files | `All checks passed!` (after one `open()` → `Path.open()` fix, PTH123) |
| `.venv/bin/python -m ruff format --check` on the same files | `4 files already formatted` |
| `cargo test -p repark-spark --lib call_register` | `12 passed; 0 failed` (0.19 s) |
| same, two processes concurrently | `a_exit=0 b_exit=0`, `12 passed` each |
| `pytest python/repark-parity/tests/torture/test_torture_v3_dv.py -q -p no:cacheprovider` | `7 passed, 1 skipped` (0.61 s) |
| same, two copies concurrently | `a_exit=0 b_exit=0`, `7 passed, 1 skipped` each |
| `pytest python/repark/tests/test_spark_sql_grammar_1.py -q -p no:cacheprovider` | `112 passed, 2 xfailed` (0.45 s) |
| new pin with `--runxfail` | `2 failed` (bite proof) |
| `pytest python/repark/tests/test_perf_ice_catalog_io_1.py -q -p no:cacheprovider` | `34 passed, 2 skipped` (72.73 s) |
| `--collect-only` / `-m "not perf"` on the perf file | collected (1 line) / `35/36 collected (1 deselected)` |
| `cargo clippy -p repark-spark --tests -- -D warnings` | reds identically on main (3670 `disallowed_methods` hits, the class `make rust-clippy` allows for tests per `clippy.toml`); the new code adds no other lint (one `duration_suboptimal_units` fixed to `from_mins(2)`) |
| `cargo fmt -p repark-spark -- --check` | clean |
| `check_docstring_presence.py` on the 4 touched Python files | `295 files clean`, exit 0 |
| `comment_ban.py /tmp/lb-build origin/main` | `hits=0` (post-commit) |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: test-hygiene-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the tree and a run — both fixed-path fixtures run twice concurrently green (C-001, C-002), the session-zone pin green with its strict-xfail twin biting under --runxfail (C-003), the perf leg collected by default and deselected by -m "not perf" (C-004), the staging map scanned for duplicate blocks (C-005).
      artifacts: [crates/repark-spark/src/tests/call_register.rs, python/repark-parity/tests/torture/test_torture_v3_dv.py, python/repark/tests/test_spark_sql_grammar_1.py, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries — a killed holder (the kernel file lock releases on exit; the round-1 lock directory did not), a midnight crossing (before/after guard), two zones always on different calendar dates.
      artifacts: [crates/repark-spark/src/tests/call_register.rs, python/repark/tests/test_spark_sql_grammar_1.py]
    - id: AT-3
      status: N/A
      justification: Test code only; no product path changes.
    - id: AT-4
      status: ATTACKED
      evidence: Cross-process exclusion is the unit's subject — std::fs::File::lock and fcntl.flock, each held for the fixture's lifetime.
      artifacts: [crates/repark-spark/src/tests/call_register.rs, python/repark-parity/tests/torture/test_torture_v3_dv.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret or injection surface.
    - id: AT-6
      status: ATTACKED
      evidence: CI keeps the perf leg — no -m filter in wheels.yml or parity-live.yml; only the lane gate deselects it.
      artifacts: [python/repark/tests/conftest.py]
    - id: AT-7
      status: N/A
      justification: No performance path changes; the 500-table leg is only marked.
    - id: AT-8
      status: N/A
      justification: No dependency, build or CI configuration change.
    - id: AT-9
      status: N/A
      justification: No log format or error text change.
    - id: AT-10
      status: ATTACKED
      evidence: TZ-9 states RePark's measured UTC-date answer and Spark 4.1.2's measured session-zone answer (orchestrator probe, 2026-09-18 13:03 UTC).
      artifacts: [docs/spark-sql-iceberg-parity.md]
```


Every clause above is PROVEN with a quoted command and its output; no clause is
OPEN. Touched files per clause: C-001 `crates/repark-spark/src/tests/call_register.rs`
+ `crates/repark-spark/src/tests/map.md`; C-002
`python/repark-parity/tests/torture/test_torture_v3_dv.py` +
`python/repark-parity/tests/torture/map.md`; C-003
`python/repark/tests/test_spark_sql_grammar_1.py` + `python/repark/tests/map.md`
+ `docs/spark-sql-iceberg-parity.md` (TZ-9); C-004
`python/repark/tests/test_perf_ice_catalog_io_1.py` +
`python/repark/tests/conftest.py` + `python/repark/tests/map.md`; C-005
`task/ledgers/staging/map.md` (dedupe) plus this ledger's own entry there.
`Cargo.toml` / `Cargo.lock` / `.github/` / product code: untouched
(`git diff --stat` shows none). Whole-workspace `cargo test`, `make verify`,
facade suite and parity harness not run per the brief's machine rule; CI runs
the rest.

**Outcome:** all five items landed test-only, gates green within the brief's
command list. Hand-back: `handback.json` at the clone root (git-excluded).
