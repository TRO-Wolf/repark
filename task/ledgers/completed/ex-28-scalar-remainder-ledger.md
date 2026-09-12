# Unit ledger — EX-28 · v1.1 example backfill, the `F.*` scalar remainder (34 names)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-28 merges, or when the owner closes the
slate row.

**Unit:** EX-28 · **Date:** 2026-09-06 · **Model:** grok-4.6 · **Branch:** `docs/ex-28-scalar-remainder` · **Base:** `57f21b9b` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-28 lane brief (34 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_functions_b.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 34 scalar `F.*` backlog names of the lane brief, re-derived at the base
`57f21b9b` (all 34 present on `docs/examples/backlog.txt`; C-001 records the exact list). The
oracle is live PySpark 4.1.2 (ANSI on, UTC session zone): every asserted value in every example
was measured there first, then matched on repark. Seven names are covered by extending three
existing family scripts. The other 27 diverge or refuse and stay on the backlog with their
existing EX-FN / BL-17 / FNP-15 / FNP-16 rows, plus two new §7 rows (EX-FN-20
`try_to_timestamp`; EX-FN-21 the `unix_timestamp` format arm on a covered name), pinned by two
tests in `test_examples_functions_b.py`. No new example file ships: every family that is all
refusals already has a stay row, so no honest example exists for those families. No product
file is touched.

**Roster (34):** `F.base64`, `F.decode`, `F.encode`, `F.expr`, `F.format_number`, `F.from_csv`,
`F.hash`, `F.input_file_block_length`, `F.input_file_block_start`, `F.input_file_name`,
`F.java_method`, `F.kurtosis`, `F.make_timestamp`, `F.mode`, `F.monotonically_increasing_id`,
`F.months_between`, `F.raise_error`, `F.reflect`, `F.replace`, `F.schema_of_csv`, `F.sentences`,
`F.skewness`, `F.spark_partition_id`, `F.split`, `F.to_csv`, `F.to_unix_timestamp`,
`F.try_reflect`, `F.try_to_time`, `F.try_to_timestamp`, `F.uniform`, `F.unix_timestamp`,
`F.user`, `F.validate_utf8`, `F.version`.

**Grouping (3 extended files; no new script):**

| File | New `COVERS` (roster names) | Why these together |
|---|---|---|
| `functions/utf8.py` | `F.validate_utf8` | The loud sibling of `try_validate_utf8` on the same binary path. |
| `functions/dates_more.py` | `F.unix_timestamp`, `F.to_unix_timestamp`, `F.try_to_time` | Epoch seconds on the default pattern, plus Spark's TIME-type refusal. |
| `functions/session_misc.py` | `F.user`, `F.version`, `F.uniform` | Session identity (shape-checked) and seeded uniform draws measured on Spark. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 34-name roster above is exactly the lane brief's list, every name re-derived from `docs/examples/backlog.txt` at the base `57f21b9b`; 7 names are covered by the three example units in the grouping table and 27 stay on the backlog with a stay row each (existing EX-FN / BL-17 / FNP-15 / FNP-16, plus EX-FN-20 for `F.try_to_timestamp`); no new example file ships because every all-refusal family already has a stay row. | The backlog grep at dispatch (all 34 present), the shipped examples, and the oracle table (34 rows, one per roster name). | **PROVEN** |
| C-002 | `functions/utf8.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured `F.validate_utf8` answers on valid bytes (`abc` / `Café` / empty / NULL) and the `INVALID_UTF8_STRING` raise on a lone 0xFF; every `COVERS` name is used in the body. | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle row for the name. | **PROVEN** |
| C-003 | `functions/dates_more.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured epoch seconds `1718452800` / `0` / NULL through `F.unix_timestamp` and `F.to_unix_timestamp` (string and timestamp columns), shape-checks zero-argument `unix_timestamp()` as a current-epoch int stable across rows, and asserts `F.try_to_time` raises `UNSUPPORTED_TIME_TYPE` as Spark does; the format argument is disclosed as EX-FN-21, not asserted; every `COVERS` name is used in the body. | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its three names. | **PROVEN** |
| C-004 | `functions/session_misc.py` runs green under `python <path>` with no network and no JVM, shape-checks `F.user` as a non-empty string agreeing with `F.current_user` and `F.version` as a stable non-empty string, and asserts the Spark-measured seeded `F.uniform` draws (ints `[6, 7, 8, 7, 5, 5, 8, 8]`, floats the eight-value list) plus unseeded ints in `[5, 9)`; every `COVERS` name is used in the body. | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its three names. | **PROVEN** |
| C-005 | The 7 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 7, 136 → 129, with no other `scripts/` change; the backlog delta is exactly the covered set; the gate's static half and its `--require-execute` leg both exit 0 (782 covered; 129 backlog; 207 examples on this tree). | The gate's own counts line at the base `57f21b9b` (775/136/207, the unit's before-line) and on the shipped tree (782/129/207), plus the red-first provocation below. | **PROVEN** |
| C-006 | Every stayed name carries a stay row naming its measured cell (existing EX-FN-3..18, BL-17, FNP-15, FNP-16, plus EX-FN-20 for `try_to_timestamp`); the `unix_timestamp` format arm is EX-FN-21 (BACKLOG ARM on a covered name); both new rows carry a pin in `python/repark/tests/test_examples_functions_b.py` (2 tests, all green). | The two registry rows, the pin file's green run, and the oracle table's stayed column. | **PROVEN** |

`LOGIC_SCORE` = **6/6 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — the backlog ratchet (on this tree):** the seven new `COVERS` names stripped
from `utf8.py` / `dates_more.py` / `session_misc.py` (temporary, restored from a gitignored
backup before the next run), while `docs/examples/backlog.txt` kept the 7 roster rows deleted
and `BACKLOG_BASELINE` stood at 129 (`136 − 7`, as if the whole covered set were taught): the
gate exits **1** with exactly **7 findings**, every one `public name F.<name> has no example
COVERS row…` naming exactly `to_unix_timestamp`, `try_to_time`, `uniform`, `unix_timestamp`,
`user`, `validate_utf8`, `version`, and no other finding. Restoring the `COVERS` rows returns
the static gate to **0**.
`pins: ex-28-scalar-remainder/C-001, C-005`

**Provocation 2 — the execute-leg control (on this tree):** one provocation injected into
`functions/dates_more.py` (temporary, never committed, restored from backup before the next
run): the asserted noon epoch `1718452800` overwritten with `1718452801`. Full gate
`.venv/bin/python scripts/check_example_coverage.py --require-execute`: exit **1** with
exactly one execute finding,
`example …/docs/examples/functions/dates_more.py exited 1: … F.unix_timestamp from_str [1718452800, 0, None] != [1718452801, 0, None]`
— the control names the script and both values. Restoring returns the full gate to **0**.
`pins: ex-28-scalar-remainder/C-003, C-005`

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-06)

The measurement instrument is two shared probe scripts (`scratch/ex28/spark_probe.py`,
`scratch/ex28/spark_probe2.py`, gitignored) run once per engine with identical cells. Spark
halves below are verbatim cell output. Each shipped example was also run standalone from the
lane root before any commit (exit 0, all three). The lane venv resolves `repark` to the lane
with a release native (`repark._native.__debug_assertions__` False). One JVM at a time beside
the FNP-9 critic lane's JVM; every oracle JVM this unit started was stopped by its script
(`spark.stop()`). Seeded `F.uniform` matched Spark's XORShift draws bit-identically on a
single partition (Spark seeds each partition with `seed + partitionIndex`, so `range(8)` under
`local[2]` answers a different stream; the example runs `local[1]`), so those values are
asserted, not only shaped.

| Name | Spark cell | repark cell | Verdict | File / row |
|---|---|---|---|---|
| `F.base64` | padded (`U3Bhcms=`, `QQ==`) | unpadded (`U3Bhcms`, `QQ`) | stayed | BL-17 |
| `F.decode` | UTF-8 round trip `AB` | refuses: no charset codec | stayed | EX-FN-3 |
| `F.encode` | utf-8 `4142`; utf-16 `feff00410042` | refuses: no charset codec | stayed | EX-FN-3 |
| `F.expr` | `a + 1` binds to `[2, 3, None]` | column refs raise; literals agree | stayed | EX-FN-4 |
| `F.format_number` | `12,332.12`, `0.50`, `-9,876.54` | refuses R-FN-BATCH3 | stayed | EX-FN-5 |
| `F.from_csv` | `(1,hello)`, `(2,None)`, NULL | refuses E1 | stayed | EX-FN-6 |
| `F.hash` | `-559580957`, `1765031574`, `42` | refuses R-FN-BATCH1 | stayed | EX-FN-7 |
| `F.input_file_block_length` | `16` on a 16-byte two-row CSV | FNP-15 unreachable | stayed | FNP-15 |
| `F.input_file_block_start` | `0` | FNP-15 unreachable | stayed | FNP-15 |
| `F.input_file_name` | `file:///tmp/…/letters.csv` | refuses R-FN-BATCH4 | stayed | EX-FN-13 |
| `F.java_method` | `java.lang.Math.abs(-5)` answers `"5"` | FNP-15 unreachable | stayed | FNP-15 |
| `F.kurtosis` | `-1.1517159763313605` over `[1,2,2,3,4,5]` | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.make_timestamp` | `2024-01-15T10:30:05` | refuses R-FN-BATCH3 | stayed | EX-FN-10 |
| `F.mode` | `2` over `[1,2,2,3,4,5]` | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.monotonically_increasing_id` | `0..4` single partition | refuses R-FN-BATCH4 | stayed | EX-FN-12 |
| `F.months_between` | `3.0`, `-8.0`, NULL | refuses R-FN-BATCH1 | stayed | EX-FN-11 |
| `F.raise_error` | `[USER_RAISED_EXCEPTION] boom` P0001 | refuses E1 at build | stayed | EX-FN-14 |
| `F.reflect` | same `"5"` as `java_method` | FNP-15 unreachable | stayed | FNP-15 |
| `F.replace` | lit search; `$1` literal `$1$1$1` | plain-str search; `$1` backslash | stayed | EX-FN-15 |
| `F.schema_of_csv` | `STRUCT<_c0: INT, _c1: STRING>` | refuses E1 | stayed | EX-FN-16 |
| `F.sentences` | `[["Hello","world"],["How","are","you"]]` | refuses R-FN-BATCH2 | stayed | EX-FN-17 |
| `F.skewness` | `0.3053162697580512` over `[1,2,2,3,4,5]` | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.spark_partition_id` | `0` | refuses R-FN-BATCH4 | stayed | EX-FN-12 |
| `F.split` | `[a,b,c]`, `[a,'',c]`, `['']`, NULL | refuses R-FN-BATCH1 | stayed | EX-FN-18 |
| `F.to_csv` | `"1,hello"`, `"2,"` | FNP-16 deferred-by-cost | stayed | FNP-16 |
| `F.to_unix_timestamp` | `1718452800` on `"2024-06-15 12:00:00"` | identical | covered | `functions/dates_more.py` |
| `F.try_reflect` | `"5"` on `Math.abs(-5)` | FNP-15 unreachable | stayed | FNP-15 |
| `F.try_to_time` | `[UNSUPPORTED_TIME_TYPE] … SQLSTATE: 0A000` | identical raise | covered | `functions/dates_more.py` |
| `F.try_to_timestamp` | `2024-06-15T12:00:00`, NULL, NULL | refuses R-FN-BATCH3 | stayed | EX-FN-20 |
| `F.uniform` | seeded ints `[6,7,8,7,5,5,8,8]`; floats the eight-value list; unseeded ints in `[5,9)` | identical | covered | `functions/session_misc.py` |
| `F.unix_timestamp` | `1718452800` / `0` / NULL; format arm `1718409600` / `86400` | no-format identical; format refuses | covered + BACKLOG ARM | `functions/dates_more.py` + EX-FN-21 |
| `F.user` | non-empty string (login-dependent) | `repark`, agrees with `current_user` | covered (shape-check) | `functions/session_misc.py` |
| `F.validate_utf8` | binary `abc` / empty / NULL; 0xFF raises `INVALID_UTF8_STRING` | identical values and error class | covered | `functions/utf8.py` |
| `F.version` | `4.1.2 f0bb2e6a…` non-empty string | `repark-1.0.1`, stable across rows | covered (shape-check) | `functions/session_misc.py` |

## Gates (2026-09-06, on this tree)

| Command | Exit |
|---|---|
| `make check-example-coverage` | **0** (`782 covered; 129 backlog; 2 exceptions; 207 examples`; execute skipped without the venv native) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`782 covered; 129 backlog; 2 exceptions; 207 examples`; every assert executed) |
| `make check-python-conventions` | **0** |
| `make py-lint` | **0** |
| `make py-format-check` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q` | **0** (79 passed, including 2 new EX-FN pins) |
| three extended `docs/examples/functions/{utf8,dates_more,session_misc}.py` standalone | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `uvx typos@1.47.2 .` | **0** |

Counts line (static half, on this tree; the base `57f21b9b` is `775 covered; 136 backlog; 207 examples`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 782 covered; 129 backlog; 2 exceptions; 207 examples`

Before this unit: `775 covered; 136 backlog; 207 examples` (`BACKLOG_BASELINE` 136). On this unit's
tree: `782 covered; 129 backlog; 207 examples` (`BACKLOG_BASELINE` 136 → 129) — exactly the 7
roster names, +7 / −7 / +0 files.

## Cost

The Grok (grok-4.6) leg started 2026-09-06: read the contract, the EX-27/EX-25 ledgers, the
coverage gate, and the `F.*` remainder; measured live Spark 4.1.2 cells; extended three
example files, two §7 rows, two pins, the backlog ratchet, the maps, and this ledger.

## Disk

Pickup: `df -h` 838 GB free of 1.8 TB. The oracle probe lives under the gitignored
`scratch/ex28/` (removable at close). Native module rebuilt once by `uv sync --locked --extra
record` (`repark._native.__debug_assertions__` is False). Ivy cache copied to `.ivy2` in-lane
(untracked).

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-28 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-28-scalar-remainder
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 34 roster names are in the oracle table; seven are covered by three extended example files and 27 stay with a named stay row.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/functions/utf8.py, docs/examples/functions/dates_more.py, docs/examples/functions/session_misc.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first provocation 1 stripped the seven new COVERS names with the backlog rows deleted and the baseline at 129; the gate exited 1 with exactly 7 findings and the backlog is an exact baseline 129.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every F.* COVERS name on the functions door alias; the examples call each new name through F.<name>, so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/functions/utf8.py, docs/examples/functions/dates_more.py, docs/examples/functions/session_misc.py]
    - id: AT-4
      status: N/A
      justification: The gate and the examples are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the three local examples; they drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; provocation 1 ran the AST-only half with exactly 7 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the extended examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citations for C-001..C-006 live in scripts/map.md and python/repark/tests/test_examples_functions_b.py, and this ledger cites its clauses in the red-first and oracle sections. Two §7 rows EX-FN-20..21 are pinned by two tests.
      artifacts: [scripts/map.md, python/repark/tests/test_examples_functions_b.py, python/repark/tests/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_functions_b.py](../../../python/repark/tests/test_examples_functions_b.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-FN-20..21
- Siblings: [ex-27-ml-ledger.md](ex-27-ml-ledger.md), [ex-25-functions-a-ledger.md](ex-25-functions-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-28-scalar-remainder
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-28-scalar-remainder
  artifacts_verified:
    ledger: PASS (C-001..C-006 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (no in-lane critic findings this round)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, F.* scalar remainder — 7 covered, 27 stayed, two §7 rows
  verdict: PENDING
  rejection_route: N/A
```
