# Unit ledger — RANGE-TVF-ID-2 · `range(...)` argument edge cases answer Spark 4.1.2

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `fix/range-id-2` · **Base:** `origin/main` (`6a140eb3`) ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `RANGE-TVF-ID-2` **FIXED 2026-09-19**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** `crates/repark-core/src/range_table.rs` (merged in #708,
RANGE-TVF-ID-1) is the SQL door's `range(...)` table function. A post-merge
logic critic filed four findings (L-001 NULL bounds answer empty, L-002
overflow bounds wrap forever, L-003 narrow widths refuse, L-004 the 4th
argument is unchecked), and the orchestrator MEASURED every one on Spark 4.1.2
and on RePark at main; the claims hold. The fixture for this unit is the
orchestrator truth file with 19 cells (NULL bounds, narrow widths, decimal and
float bounds, folded expressions, both overflow bounds, the near-max count,
and every `numPartitions` shape).

**Not in this unit:** `generate_series` semantics, the native door's lack of
`Y`/`S`/`D` suffix-literal spellings (dialect-level, pinned as facade-only),
`STATUS.md`, `Cargo.toml`, `Cargo.lock`, tier-2 live runs.

## Error-class mapping (brief ruling, stated here)

RePark carries Spark's refusal class the way it already does for the same
Spark class elsewhere — the class is RePark's, the Spark error token rides in
the message:

- Spark `AnalysisException` (`UNEXPECTED_INPUT_TYPE`: NULL bounds, NULL
  `numPartitions`) → RePark `AnalysisException` via a DataFusion plan error
  (precedent: FN-APPROXPCT-ACC-TYPE-1, whose SQL-door pins assert the class
  plus the token).
- Spark `NumberFormatException` (`CAST_INVALID_INPUT`: malformed strings) →
  RePark base `PySparkException` via a DataFusion execution error carrying
  Spark's `CAST_INVALID_INPUT` sentence (precedent: the `repark-functions`
  malformed casts, whose pins assert `PySparkException` plus the token).
- Spark `IllegalArgumentException` (`Positive number of partitions required`)
  → RePark `IllegalArgumentException` via the `IllegalArgumentMarker` path
  (precedent: ICE-RDF-OPTIONS-1), message verbatim.

Bounds and partitions coerce through one shared converter because Spark routes
both through the same `castAndEval` path (the `parts_string` cell proves
strings are attempted for the 4th argument too).

## Decisions on unmeasured inputs

- A negative partition count on an EMPTY range answers empty: the brief pins
  only `0` on empty, and Spark's empty-range short-circuit sits below the
  partition-count throw, so only the type validation runs when the count is 0.
- Out-of-range numerics (overflowing `UInt64`/float/decimal bounds, a 4th
  argument past `i32`) refuse loud as planning errors; silent saturation or
  wrapping would repeat L-002.
- `Y`/`S`/`D` suffix literals do not parse on the native ANSI door
  (`ParseException` at the literal, dialect-level); those three cells pin the
  facade door, and both doors pin the same widths via `CAST` (plus the Rust
  `CAST(... AS SMALLINT/TINYINT/DOUBLE)` pins).
- Unaliased `count(*)` renders `count(*)` where Spark renders `count(1)` —
  the systemic SQL-door display form, identical on `VALUES`, so the count pin
  asserts rows and the `int64` type with the name pinned explicitly, as ID-1
  did for `sum(range().id)`.

## PROPOSITION LEDGER — RANGE-TVF-ID-2 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | NULL start, end, step, or `numPartitions` refuses with `AnalysisException` carrying `UNEXPECTED_INPUT_TYPE` on both SQL doors. | `test_sql_range_id2_refusal_matches_mapped_class_and_token` over `range_null`, `range_null_end`, `range_cast_null_bigint`, `parts_null`; §1 verbatim commands. | **PROVEN** | Red on main (silent empty table); green after the fix on the rebuilt release native (§4). |
| C-002 | Every width Spark accepts answers Spark's rows, and malformed strings refuse with RePark's `CAST_INVALID_INPUT` surfacing. | Answer pins over `range_cast_int`, `range_tinyint`, `range_smallint`, `range_decimal`, `range_double`, `range_expr`, `range_neg` plus the `string_bogus` refusal; the `3.5D` cell verifies truncation toward zero; §1 verbatim commands. | **PROVEN** | Red on main (`must be an INTEGER` refusals, token-less string message); green after the fix (§4). Suffix literals run facade-only (native door has no such spelling). |
| C-003 | Overflow bounds emit exactly Spark's single row under a bounded read, and the near-max `count(*)` answers 7 with the declared display name. | Overflow pins under `LIMIT 8` over `range_overflow_up`, `range_overflow_down`; `test_sql_range_id2_near_max_count_answers_rows_with_door_display_name`; §1 verbatim commands. | **PROVEN** | Red on main (wrapped rows); green after the fix (§4). The provider precomputes the `i128` element count, so collection is finite by construction. |
| C-004 | `numPartitions` validates: non-positive counts on a non-empty range refuse with `IllegalArgumentException`, an empty range with 0 partitions answers empty, malformed strings and NULLs refuse per the mapping. | Refusal pins over `parts_zero`, `parts_negative`, `parts_string`, `parts_null` plus the `parts_zero_empty` answer pin; §1 verbatim commands. | **PROVEN** | Red on main (every shape answered rows); green after the fix (§4). |
| C-005 | A dated FIXED registry row names the repark before/after, the Spark oracle, the pins, and the rationale. | Registry row `RANGE-TVF-ID-2` in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Row `RANGE-TVF-ID-2` sits beside `RANGE-TVF-ID-1`: repark before/after, Spark recorded oracle, pins, rationale, dated 2026-09-19; the ID-1 row gains the pointer line. |
| C-006 | Maps, the recorder, and the mechanical gates are lockstep with the change. | Fixture map, tests map, core src map, range_table map, staging map rows; `check_ledger_grammar.py`, `check_docs_links.py`, `sync_map_md.py --check` clean (§4). | **PROVEN** | All four map rows plus the ledger row landed in the unit's commits; the three gates report clean (§4). |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Measured on the release native in `.venv` (verbatim)

### 1a. The pins are red on main for the named reason (2026-09-19)

```
.venv/bin/python -m pytest python/repark/tests/test_range_tvf_id_2.py -q -p no:cacheprovider -n 4
30 failed, 8 passed in 1.62s
```

The 8 passes are the already-equal cells (`range_expr`, `range_neg`,
`parts_zero_empty`, the near-max count rows). Every other cell fails for its
table reason: NULL cells answer an empty table instead of refusing, width
cells refuse `must be an INTEGER or NULL, got Int8/Int16/Int32/Decimal128/
Float64`, `string_bogus` refuses without the `CAST_INVALID_INPUT` token,
`parts_zero`/`parts_negative`/`parts_string`/`parts_null` answer rows instead
of refusing, the overflow cells answer wrapped rows under the bound, and the
count cell renders `count(*)` where Spark renders `count(1)`.

### 1b. Green after the fix (2026-09-19)

```
.venv/bin/python -m pytest python/repark/tests/test_range_tvf_id_2.py python/repark/tests/test_range_tvf_id_1.py -q -p no:cacheprovider -n 4
64 passed in 1.26s
```

```
cargo test -p repark-core range_table
32 passed, 0 failed
```

## 2. Fix design (Rust-first)

`SparkRangeFunc` coerces first, then streams. `spark_long_value` converts one
folded literal to `i64` for bounds (`BIGINT`) and to `i32` for `numPartitions`
(`INT`): NULL of any width refuses with `UNEXPECTED_INPUT_TYPE`; narrow and
unsigned ints widen; decimal scales divide (negative scales multiply checked)
and floats truncate toward zero with finite-and-in-range guards; strings parse
as `i64` with Spark's `CAST_INVALID_INPUT` sentence on malformed input;
anything else refuses loud. Step zero keeps its ID-1 refusal. A non-empty
range with a non-positive partition count refuses with Spark's
`Positive number of partitions required` sentence through the
`IllegalArgumentMarker` path.

Row generation is RePark's own `RangeTable` provider with one
`RangePartition` stream over `StreamingTableExec`: the element count is
Spark's `ceil((end - start) / step)` in `i128` arithmetic clamped at 0, each
emitted value is `i64`-checked against that count so generation can never
wrap, batches follow the session batch size, the scan projection and limit
thread through, and the step-direction sort ordering stays. `DataFusion`'s
`GenerateSeriesTable` no longer serves `range(...)`; `generate_series` is
untouched.

## 3. Targeted checks

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_range_tvf_id_2.py python/repark/tests/test_range_tvf_id_1.py -q -p no:cacheprovider -n 4` | 0 — 64 passed |
| `cargo test -p repark-core range_table` | 0 — 32 passed |
| `cargo fmt --all --check` | 0 — clean |
| `cargo clippy -p repark-core --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 — clean (brief step-5 flags) |
| `cargo clippy --locked -p repark-core --lib -- -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable` | 0 — clean (repo `rust-panic-ban` flags, crate scope) |
| `.venv/bin/python -m ruff check` + `ruff format --check` on the new and folded Python files | 0 — clean |
| `check_ledger_grammar.py`, `check_docs_links.py`, `sync_map_md.py --check` | 0 — clean |

Per the brief's machine rule, `make verify`, `make preflight`, `make py-test*`, a
whole-workspace `cargo test`, the whole facade suite, and the parity harness were
deliberately not run. The new pins carry no live leg (like ID-1), so no
`REPARK_PARITY_LIVE` leg runs.

## 4. Gate log

Step-1: release native rebuilt from unmodified main (`maturin develop --release`).

Step-2 commit `d84c0627`: ID-2 fixture verbatim, fixture map provenance plus
SHA-256, recorder fold (`_SQL_CELLS_2`, both fixtures under `--check`), red
pins (`30 failed, 8 passed`), tests map row.

Step-3 commit `d98b806c`: the Rust fix with branch pins (32 passed), the two
core maps, and the pin adjustment scoping `Y`/`S`/`D` suffix literals to the
facade door (the native door has no such spelling); pins green with ID-1
(64 passed).

Step-4 commit: registry row `RANGE-TVF-ID-2` (**FIXED 2026-09-19**) plus the
ID-1 pointer line, this ledger (C-005 and C-006 PROVEN, verdict 6/6), the
staging map row.

## Verification critic (Grok 4.6, 2026-09-19, orchestrator-run)

Verdict **PASS** on head `6945f572`. Mutation first: reverting each of the four fixes (NULL arm,
the i128-counted `RangeTable`, the width arms, the numPartitions check) turns the
`range_table` Rust tests red, four of four. One P3, recorded here as residue:

- **V-001 (P3).** `RangeTable` precomputes the Spark element count but exposes no statistics,
  so `count(*)` over a very large `range(n)` streams every element (linear) where Spark reads
  a row count. It is not a wrong answer, and `LIMIT` stays bounded (`StreamingTableExec
  fetch=3`). Follow-up: report exact `num_rows` from an execution plan that carries
  statistics, or short-circuit an empty projection.

## 5. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: range-tvf-id-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every recorded cell is measured against the release native on
        the SQL doors it can spell (§1a red, §1b green); each answer pin asserts
        the measured schema string, non-nullability, and rows, and the overflow
        pins read through a LIMIT bound so a regression cannot hang CI.
      artifacts: [python/repark/tests/test_range_tvf_id_2.py]
    - id: AT-2
      status: ATTACKED
      evidence: The Spark cells are the verbatim orchestrator recording (Spark
        4.1.2, local[2], UTC), committed under range_tvf_id_1/ with SHA-256;
        the folded recorder cell table matches the fixture SQL cell for cell
        and re-derives both fixtures with --check drift detection.
      artifacts: [python/repark/tests/_record_range_tvf_id_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is pinned by RePark's mapped class plus Spark's
        token on both doors (NULL bounds and NULL partitions, malformed bound
        and partition strings, non-positive partitions); coercion widths,
        truncation, both overflow bounds, near-both-end counts, limit, and
        projection are pinned at the Rust level. No silent path remains.
      artifacts: [python/repark/tests/test_range_tvf_id_2.py, crates/repark-core/src/range_table/tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The new provider holds no mutable state across queries (pure
        argument coercion plus a precomputed count; the stream cursor is
        per-execution); registration still runs once inside the sync build().
      artifacts: [crates/repark-core/src/range_table.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Memory sessions under temp dirs only; no network, no
        credentials, no AWS; the Spark oracle arrived as a recorded file, not a
        live JVM run, and the pins carry no live leg.
      artifacts: [task/ledgers/staging/range-tvf-id-2-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (§1, §3 verbatim
        blocks), not prose: the row lists, the AnalysisException /
        PySparkException / IllegalArgumentException classes, the Spark tokens,
        the declared count(*) display name.
      artifacts: [python/repark/tests/test_range_tvf_id_2.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: git status shows only the fixture, the recorder fold, the pins,
        the provider plus tests, the four maps, the registry row, and this
        ledger; no Cargo.toml, lockfile, workflow, or pin change.
      artifacts: [task/ledgers/staging/range-tvf-id-2-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The fix lives in its registered home (registry row
        RANGE-TVF-ID-2); the fixture map, the tests map, the staging map, the
        core src map, and the range_table map carry the entries in the same
        commits.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: N/A
      justification: Single-round fix unit; ID-1 stays green beside it (64
        passed covers both files), so no prior round regresses.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
six clauses PROVEN, pins green, comment-ban hits=0.
