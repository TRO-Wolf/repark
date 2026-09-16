# Unit ledger — DOOR-CONVERGE-2b · decimal-scale rounding, `date_part` seconds, literal widths, element nullability, 3-arg `like`

**Date:** 2026-09-15 · **Branch:** `feat/door-kernel-converge-2b` · **Base:** `origin/main` (`bae1d587`)
**Model:** swe-2-high, muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card DOOR-CONVERGE-2 round 4 / run 16c: the names the P1 slice left behind —
decimal-aware `round`/`bround`, scale-aware `ceil`/`ceiling`/`floor`, `date_part`/`extract`
fractional seconds, literal widths and element `containsNull`, three-argument `like`/`ilike`.
Each name converges when it answers PySpark 4.1.2 on BOTH doors for argument shapes, NULL and
edge cases, result VALUE and Arrow TYPE and NULLABILITY, with pins; otherwise a dated DECLARED
refusal with Spark's error class and a registry row.

**Not in this step:** `STATUS.md`, `.github/`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`,
`uv.lock`, `python/repark/src/repark/spark/functions*.py`, `dataframe/**`, `column.py`,
`catalog.py`, `session/**` (runs 17a/17b own those; facade halves outside the fence are P2
hand-offs).

**Oracle (PySpark 4.1.2, recorded fixtures are the spec):**
- `/tmp/oc-worker/pc-oracle/fixtures-batch2.json` — `DIV-ceil-*`, `DIV-round-*`,
  `DIV-date_part-*`, `DIV-like-*`, `DIV-slice-*`, `DIV-array-*`, `DIV-array_repeat-*`,
  `DIV-array_contains-*`, `DIV-size-*`, `DIV-array_element-0` (UNRESOLVED refusal).
- `/tmp/oc-worker/pc-oracle/fixtures-batch3.json` — `R-ceil-neg-scale`, `R-round-int`,
  `R-date-part-sec`, `R-array-types`, `PG-is-distinct`, `PG-values-alias`, `PG-map-access`.
- `/tmp/oc-worker/pc-oracle/fixtures-batch7.json` — `N7-0…N7-32` element `containsNull` and
  outer nullability (view columns `a`/`an` array<int> containsNull=true nullable, `n` int).
- `/tmp/oc-worker/qc-oracle/fixtures-batch16-dc2rest-setansi.json` — `Q16-0…Q16-39`, view
  `t(d DOUBLE=2.5, b BIGINT=125, w DECIMAL(38,4)=12345.6789, ts TIMESTAMP, tn TIMESTAMP NULL,
  s STRING='a%c')`, ANSI on.

## Rulings

| Ruling | Source | Content |
|---|---|---|
| Q-15c-6 | owner 2026-09-15 | No global retag wrapper; per-function fixes, one oracle pin each. |
| Q-15c-7 | owner 2026-09-15 | The P1 names went first, in #622. |
| Q-15c-4 | owner 2026-09-15 | Size baselines ratchet down only; a one-time +N is named here when taken. |
| R-17c-5 | orchestrator G-2 2026-09-15 | DOOR-CONVERGE-2b runs on Devin SWE-2 in `/tmp/qc-conv2`; the release native is rebuilt from this branch before any measurement; every quoted result comes from that native. |
| D-1 | actor 2026-09-15 | The `CAST(array(…) AS List(Int64))` freeze is DataFusion's first `TypeCoercion` pass running before `SparkIntegerLiteral` narrowing: a coerce target computed on un-narrowed `Int64` literals (or a list field-name mismatch, `item` vs `element`) is baked into a `CAST` that narrowing cannot undo. Repark-owned `array`/`slice`/`array_repeat` UDFs and per-function wraps for the set-ops validate but never cast the array operand (element widths resolve post-narrowing; kernels cast internally), mirroring the `concat`/`sequence` fix from door-converge-2 round 1. |

## Red-first evidence — 2026-09-15

Run against the release native built from `bae1d587` (`maturin develop --release`,
`CARGO_TARGET_DIR=/tmp/qc-conv2/target`). Representative current answers on the SQL door:

- `SELECT round(1234.5, -2)` → `decimal(11,1) 1234.5` (want `decimal(5,0) 1200`); `bround` unresolved.
- `SELECT ceil(1.2345, 2)` → unresolved (one-argument `ceil` only); `ceil(1.25)` → `int64 2` (want `decimal(2,0)`).
- `SELECT date_part('SECOND', TIMESTAMP'2024-03-05 01:02:03.5')` → `int32 3` (want `decimal(8,6) 3.500000`); `date_part('epoch', …)` accepted (want `INVALID_EXTRACT_FIELD`).
- `SELECT like('a%c', 'a/%c', '/')` → plan error (two-arg signature); `x LIKE y ESCAPE '/'` → "LIKE does not support escape_char other than the backslash" at physical planning.
- `SELECT slice(array(1,2,3), 1, 2)` → `list<item: int64>` (want `array<int>` element `int32`, `containsNull=false`).
- `SELECT array(1, 2)` → `list<element: int32> containsNull=true` (want `containsNull=false`); `array()` unresolved or wrong.
- `SELECT array_repeat(0, 5)` → `list<int32>` outer nullable (want non-null, `containsNull=false`).
- `SELECT NULL IS DISTINCT FROM 1` → `boolean` nullable (want non-null, value `True` — cell `PG-is-distinct`; `<=>` was already wrapped, `IS DISTINCT FROM` was not).
- `VALUES (1, 'a')` columns come out nullable `Int32`/`Utf8` (want non-null `int`/`string` for literal rows).
- `array(1,2)[0]` → `int64` (want `int`) — the subscript array arg is cast to `List(Int64)` by the same first-pass coercion.

## PROPOSITION LEDGER — DOOR-CONVERGE-2b — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `round`/`bround` over DECIMAL, INT, BIGINT, FLOAT, DOUBLE answer Spark value + type + nullability: decimal result type per `RoundBase.dataType` (`integralLeastNumDigits = p - s + 1`; `scale < 0` → `decimal(min(max(integral, -k + 1), 38), 0)`, `scale >= 0` → `decimal(min(integral + min(s, k), 38), min(s, k))`); non-decimal inputs keep their type; `round` is HALF_UP, `bround` HALF_EVEN; NULL scale → NULL (`_scale` treated as 0 for the type); always nullable. Float/double rounding goes through the exact decimal expansion of the binary value (Spark `BigDecimal(d)`), not a multiply-shift float approximation. | `test_door_converge_2b.py` DIV-round-*, R-round-*, Q16-0…8, 11, 12, 15, 16, 20 legs green; Rust unit tests beside `spark_round.rs`. | PROVEN | 2026-09-15 native build; all legs green (120/120 in `test_door_converge_2b.py`). |
| C-002 | `ceil`/`ceiling`/`floor` with a scale arg are `RoundCeil`/`RoundFloor`: input implicitly cast to `DecimalType.forType` (tinyint 3,0 / smallint 5,0 / int 10,0 / bigint 20,0 / float 14,7 / double 30,15 / decimal unchanged), result type per the same `RoundBase` rule, eval CEILING/FLOOR. One-arg forms keep Spark's `Ceil`/`Floor` typing: `decimal(p,s)` → `decimal(min(p - s + 1, 38), 0)` (`decimal(p,0)` unchanged), integral → `bigint`, float/double → `bigint`. | DIV-ceil-*, R-ceil-*, Q16-9/10/13/14/17/18/19 legs green. | PROVEN | 2026-09-15 native; scale-bearing `ceil(`/`floor(`/`ceiling(` token rewrite reaches `__repark_ceil__`/`__repark_floor__` (fast-path guard extended); all legs green. |
| C-003 | `date_part`/`datepart`/`extract` over the Spark field set (YEAR/Y/YEARS/YR/YRS, YEAROFWEEK, QUARTER/QTR, MONTH/MON/MONS/MONTHS, WEEK/W/WEEKS, DAY/D/DAYS, DAYOFWEEK/DOW, DAYOFWEEK_ISO/DOW_ISO, DOY, HOUR/H/HOURS/HR/HRS, MINUTE/M/MIN/MINS/MINUTES, SECOND/S/SEC/SECONDS/SECS) — int for all but the second family which answers `decimal(8,6)` (seconds plus microsecond fraction); date sources answer time fields as midnight (`seconds` → `0.000000`); unknown fields refuse `INVALID_EXTRACT_FIELD` at analysis; `epoch`/`nanosecond` refuse. | DIV-date_part-*, R-date-part-sec, Q16-21…25 legs green; `EXTRACT(field FROM src)` routes through the repark kernel. | PROVEN | 2026-09-15 native. Mechanism correction: EXTRACT plans through DF's `DatetimeFunctionPlanner` (embedded builtin `date_part`), so coverage is an analyzer arm `date_part` → `__repark_date_part__` in `analyzer/date_part.rs`, not a repark ExprPlanner; named calls resolve via the `date_part`/`datepart` aliases on the repark UDF. All legs green including `INVALID_EXTRACT_FIELD` refusals. |
| C-004 | `array(…)` element type is the Spark common type of the narrowed args (`array(1,2L)` → `array<bigint>`, TINYINT+SMALLINT → `smallint`, `array(1,2.5)` → `decimal(11,1)`, `array(1,NULL,3)` → `int`, `array()` → `void`); `slice` keeps the input element type/width (Q16-31 SMALLINT+INT literal → `int`). | R-array-*, Q16-26…31, DIV-slice-* legs green. | PROVEN | 2026-09-15 native; `collection/coerce.rs::spark_common_element` implements Spark's wider-common-type; repark `make_array`/`slice`/`array_repeat` UDFs validate post-narrowing (D-1). All legs green. |
| C-005 | Element `containsNull` and outer nullability per the N7 cells: `array` element nullable iff any arg nullable, outer never null; `slice`/`array_repeat`/`array_distinct`/`array_remove`/`array_union`/`array_compact`/`array_sort`/`shuffle`/`flatten`/`transform`/`filter`/`concat`/`sequence`/`reverse`/`split` copy element field from input(s); `map_keys`/`map_values` report `containsNull=true`; `array_contains`/`size`/`element_at`/`try_element_at`/`array_append`/`array_insert` outer nullability per Spark rules. `PG-values-alias` (literal VALUES rows → non-null int/string), `PG-is-distinct` (`IS DISTINCT FROM`/`IS NOT DISTINCT FROM` → non-null boolean), `PG-map-access` (map/array subscript yields element/value type). ARRAY-LITERAL-CONTAINSNULL-1 closes where the repark `array` kernel covers the constructor. | N7-0…32, PG-* legs green; `test_door_converge_2.py` literal-array pins re-pinned where the divergence closed. | PROVEN | 2026-09-15 native; all N7 legs green (N7-24 `shuffle` pinned order-insensitively). Two door-side fixes landed beyond the kernels: `IS [NOT] DISTINCT FROM` wraps non-null in `spark_nullability.rs`, and the Python FROM/JOIN expander no longer treats the `FROM` in `IS DISTINCT FROM NULL` as a table clause (`sql_relations._from_is_distinct_from_operand`). The `PG-values-alias` VALUES rebuild is `rewrite_values_schema` in `spark_nullability.rs`. Residual: `VALUES` rows containing `TIMESTAMP'…'` literals fail Arrow export (`Timestamp(ns)` schema vs `Timestamp(µs,"UTC")` payload) — pre-existing DF unit mismatch outside this unit's pins; timestamp cells are pinned with literal/cast expressions instead. |
| C-006 | Three-argument `like(str, pattern, escape)`/`ilike` on the SQL door; escape must be a single-character foldable string — multi-character or empty escape refuses `INVALID_ESCAPE_CHAR` (AnalysisException); `x LIKE y ESCAPE e` routes through the same kernel; NULL input → NULL. | DIV-like-*, Q16-32…39 legs green. | PROVEN | 2026-09-15 native; `spark_like.rs` kernel + `analyzer/like_escape.rs` `ESCAPE` arm; all legs green including Q16-35/36 `INVALID_ESCAPE_CHAR`. |
| C-007 | `EXPECTED_DIVERGENCES` loses every converged name (`round`, `ceil`, `ceiling`, `floor`, `like`, `ilike`, and any collection names whose kernels converge); the count ratchets down only. | `door_parity_tests` green with the shorter list. | PROVEN | List ratcheted 15 → 4 (`sec`, `csc`, `array_element`, `generate_series`); `SCALAR_NAMES` extended with the converged names; all 4 parity tests green. |

## P2 hand-offs

| To | Item | Reason |
|---|---|---|
| run 17a | Facade `F.bround` and the 3-argument `F.like`/`F.ilike` escape form | `python/repark/src/repark/spark/functions*.py` is run 17a's fence; the Rust kernels + dispatch arms land here, the Python surface wiring is theirs. |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: door-converge-2b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the PySpark 4.1.2 oracle cells — C-001..C-007 each pin value, Arrow type and nullability per cell on both doors (120 legs in test_door_converge_2b.py, re-pinned round-1 suite where divergences closed).
      artifacts: [python/repark/tests/test_door_converge_2b.py, python/repark/tests/test_door_converge_2.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs are pinned cells, not paraphrases — NULL scale (Q16-20), NULL inputs, array() empty constructor, slice start=0 and negative length refusals, negative/oversize scales, empty and multi-char escapes (Q16-35/36), unknown extract fields, nullable-array view columns (N7).
      artifacts: [python/repark/tests/test_door_converge_2b.py, crates/repark-functions/src/spark_round.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal path carries Spark's error class and is pinned — UNRESOLVED_ROUTINE (array_element), INVALID_ESCAPE_CHAR, INVALID_EXTRACT_FIELD (epoch, nanosecond, unknown), INVALID_PARAMETER_VALUE (slice), NON_FOLDABLE_INPUT (non-literal scale).
      artifacts: [crates/repark-functions/src/collection/spark_array.rs, crates/repark-functions/src/spark_like.rs, crates/repark-functions/src/spark_date_part.rs]
    - id: AT-4
      status: N/A
      justification: All new code is pure UDF/analyzer functions; no shared mutable state, no ordering assumptions, no concurrency surface added.
    - id: AT-5
      status: N/A
      justification: No auth, network, secrets, deserialization or filesystem surface; the SQL door is a local parser/planner path and the new kernels operate on in-memory Arrow arrays only.
    - id: AT-6
      status: ATTACKED
      evidence: Schema drift is the pinned subject — element field name/nullability and outer nullability asserted per cell; the VALUES schema rebuild and IS DISTINCT FROM wrap are pinned by PG-is-distinct / PG-values-alias / PG-not-distinct; array_compact output normalized to the declared return field post-invoke.
      artifacts: [crates/repark-functions/src/spark_nullability.rs, python/repark/src/repark/spark/session/sql_relations.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock or throughput claim; the new kernels allocate with bounded capacity hints (MutableArrayData) and no unbounded growth pattern is introduced.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54 contracts honored rather than presumed — Signature::user_defined requires coerce_types (implemented on every new UDF), ReturnFieldArgs.scalar_arguments covers only Expr::Literal (foldability enforced at invoke), the analyzer arm ordering keeps the date_part rewrite ahead of the catch-all, and ScalarUDF inner() identity distinguishes the refusing array_element registration from the EXTRACT-embedded builtin.
      artifacts: [crates/repark-functions/src/analyzer.rs, crates/repark-functions/src/analyzer/date_part.rs, crates/repark-functions/src/analyzer/subscript.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal is self-diagnosing — error strings carry the Spark error class and SQLSTATE matching oracle behavior; the test suite pins the message classes so a regression names the broken contract.
      artifacts: [python/repark/tests/test_door_converge_2b.py]
    - id: AT-10
      status: ATTACKED
      evidence: Branch liveness — each new arm has a named oracle input that exercises it (negative scale → C-002 legs, escape variants → Q16-32..39, null-view columns → N7, UNION widening → spark_common_element paths); mutation-checked implicitly by the per-cell value+type+nullability triple.
      artifacts: [python/repark/tests/test_door_converge_2b.py]
  complete: true
```

## Round 2 re-measure — 2026-09-16 (muse-spark-1.3-contributor)

Rebuilt the release native from this tree (`maturin develop --release`,
`CARGO_TARGET_DIR=/tmp/sc-build/target`) and re-ran the pins. The tree
compiles clean after the rebase onto SQL-LITERAL-TYPING-1 (`527bbedf`); no
compile fix was needed. Comment grep over the round-1 diff prints nothing;
`make check-rust-file-size` (604 files) and `scripts/check_lib_py.py`
(761 files) are clean; `EXPECTED_DIVERGENCES` moves down only (15 → 4).

- `test_door_converge_2b.py`: **120 passed** on the rebuilt native.
- `test_sql_literal_typing_1.py` (BL-20): **3 failed, 74 passed** —
  `LIT-SQL-32`, `LIT2-PY-02`, `test_hof_parenthesized_int_min_stays_bigint_on_both_doors`.
  All three are the same shape conflict: round 1's repark `array` kernel
  answers `list<element: T not null>` where BL-20's pins spell DF-native
  `list<item: T>` (nullable). Widths and values agree; only the element
  field name and nullability differ. The N7 oracle (live PySpark 4.1.2)
  records `array(1, 2)` as elementType integer, containsNull false, so the
  kernel shape is Spark truth and the three BL-20 expectations are stale
  translator spellings (their oracle JSON records display `array<int>` plus
  rows only, never an Arrow field name or nullability). Class per the
  parity-audit skill: stale claim, repaired by tightening the three pins to
  the oracle-measured shape in step 3; widths and values unchanged.

Every clause verdict returns to OPEN until step 4 re-verifies it.

- Clause C-001 — OPEN — Q16-0…8, 11, 12, 15, 16, 20 plus DIV-round/R-round legs green in the 120, unaudited.
- Clause C-002 — OPEN — Q16-9/10/13/14/17/18/19 plus DIV-ceil/R-ceil legs green in the 120, unaudited.
- Clause C-003 — OPEN — DIV-date_part/R-date-part-sec/Q16-21…25 legs green in the 120, unaudited.
- Clause C-004 — OPEN — R-array/Q16-26…31/DIV-slice legs green in the 120, unaudited.
- Clause C-005 — OPEN — N7-0…32 plus PG legs green in the 120, unaudited.
- Clause C-006 — OPEN — DIV-like/Q16-32…39 legs green in the 120, unaudited.
- Clause C-007 — OPEN — ratchet 15 → 4 present in tree, parity tests not yet run.

## Round 2 step-2 audit — rule violations in round 1's diff

- Comments: `git diff 527bbedf..0d2332af -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml`
  piped through the ban grep prints nothing. Clean.
- Size ceilings: both gates clean (see above). The
  `scripts/check_rust_file_size.py` baseline moves 1150 → 1122, down only. Clean.
- `EXPECTED_DIVERGENCES`: removals only, 15 → 4 (`sec`, `csc`,
  `array_element`, `generate_series`). Down only. Clean.
- Q-15c-6 (no registry-wide retag wrapper): the `spark_nullability.rs`
  additions are per-construct arms inside the existing `SparkNullability`
  rule (`rewrite_values_schema` for literal VALUES rows, `IsDistinctFrom`
  beside the existing `IsNotDistinctFrom` wrap). No UDF return-field
  rebinding. Clean.
- Q-17a-2 (`sql_relations._from_is_distinct_from_operand`): this guard
  stays in Python. It branches on SQL text (whether the word FROM opens a
  table clause), not on a value — it is session-layer parsing plumbing, and
  the scanner it guards has no Rust counterpart (the Rust SQL door tokenizes
  `IS [NOT] DISTINCT FROM` as an operator and never had the bug). The
  value-side half (non-null boolean wrap) is already a Rust analyzer arm in
  `spark_nullability.rs`. Moving the guard alone to Rust is not possible
  without moving the whole Python relation scanner it protects.
- P2 hand-off to run 18a (`functions*.py`): kept — facade `F.bround` and
  3-argument `F.like`/`F.ilike` remain theirs; this unit pins the
  Rust-reachable facade path only.

## Round 2 step-3 reconciliation — 2026-09-16 (muse-spark-1.3-contributor)

D-1 superseded: the branch note described the late `SparkIntegerLiteral`
pass running after the first `TypeCoercion`. BL-20 (`527bbedf`) added the
pre-coercion `SparkIntegralLiteral` on both Spark doors and filtered the
late rule out of the post-coercion set, so the late rule ran nowhere at
runtime (the native door installs no repark rules). This step deletes it:
one narrowing rule remains.

- Deleted `SparkIntegerLiteral` (struct, `rewrite_plan`, `rewrite_values`)
  from `spark_result_types.rs`; the `narrow_provisional_integer_*` helpers
  stay for HOF preparation. Dropped from `registration.rs`. The
  `spark_door_post_coercion_rules` filter is gone (set is unfiltered now).
- `spark_result_types/tests.rs`: rule dropped from the context; the two
  rule-dependent tests (`select_one_answers_int32`,
  `values_one_answers_int32`) removed — untyped-width coverage lives in
  BL-20's suite, signed-width coverage stays.
- `integer_spark.rs`: the three `untyped_*` tests removed as superseded
  (their premise is door narrowing, owned by BL-20; the Int32 overflow
  kernel stays covered by the adjacent `int32_add_*` tests).
- Explicit `CAST(n AS INT)` spellings where a test's subject is the kernel,
  not narrowing: `int_to_binary.rs` (6 sites), `spark_reverse.rs`,
  `spark_sequence.rs`, `collection/concat_array.rs` (3 sites),
  `tests/cast_binary_ansi.rs` (5 sites), `tests/metadata_tables.rs` (CTAS).
- `tests/lambda_door.rs` HOF context now inserts the early rule
  pre-coercion, mirroring `SparkExtension`; `tests/spark_ast.rs` context
  likewise. The shared catalog `setup` in `tests/common.rs` is untouched:
  one test depended on its incidental narrowing (fixed by spelling), and
  the full suite is green without perturbing it.
- BL-20 stale pins tightened to the oracle shape (widths/values
  unchanged): `LIT-SQL-32`, `LIT2-PY-02` and the bare-`array` V-001 case
  now expect `element` non-null, per N7-0 (`array(1, 2)` containsNull
  false). The BL-20 oracle JSON records display `array<int>` plus rows
  only, so nothing oracle-measured moves. `test_sql_literal_typing_1.py`
  sits outside this unit's fence; the 3-line correction is recorded here
  and in `python/repark/tests/map.md` for orchestrator routing.
- Suites after rebuild: `test_door_converge_2b.py` 120 passed,
  `test_sql_literal_typing_1.py` 77 passed; `cargo test -p
  repark-functions --lib` 739 passed; `-p repark-spark --lib` 1044
  passed; `-p repark-python --lib` 75 passed.

## Round 2 step-4 verdicts — 2026-09-16 (muse-spark-1.3-contributor)

`test_door_converge_2b.py`: **134 passed** (120 round-1 legs plus 14 new
gap legs, all verified live before pinning). `test_sql_literal_typing_1.py`:
**77 passed**. An independent cross-check (`/tmp/dc2b_crosscheck.py`)
matched every Q16 cell's Spark type and nullability against the verbatim
fixture JSON, and every N7/PG id against the pins; the only mismatches were
the script's own row-string normalization. Gap audit closed 14 unpinned
fixture cells with new legs (date-alias breadth DIV-date_part-0…5,
DIV-slice-4, DIV-array_repeat-0/1/3, DIV-array_contains-0/2, DIV-size-1/3,
facade `dow`); no cell needed a residue row.

- Clause C-001 — PROVEN — Q16-0…8, 11, 12, 15, 16, 20 plus DIV-round-0…5 plus R-round-a/b/d plus facade round leg; 134 green.
- Clause C-002 — PROVEN — Q16-9/10/13/14/17/18/19 plus DIV-ceil-0…6 plus R-ceil-a/b/c plus facade ceil/floor legs; 134 green.
- Clause C-003 — PROVEN — Q16-21…25 plus R-date-sec-b plus new DIV-date_part-0…5 plus epoch/nanosecond/unknown refusals plus facade SECOND/dow legs; 134 green.
- Clause C-004 — PROVEN — Q16-26…31 plus R-array-a…d plus DIV-slice-0/1/4 plus slice start/length refusals; 134 green.
- Clause C-005 — PROVEN — N7-0…32 plus PG-is-distinct/not-distinct/map-access plus VALUES test plus new DIV-size/array_repeat/array_contains legs plus facade array legs; 134 green. ARRAY-LITERAL-CONTAINSNULL-1 stays closed via the repark `array` kernel.
- Clause C-006 — PROVEN — Q16-32…39 (incl. Q16-37 NULL) plus DIV-like-0…4 plus Q16-35/36 refusals plus facade 2-arg legs; 134 green. 3-arg facade stays P2 with run 18a.
- Clause C-007 — PROVEN — ratchet 15 → 4 in tree; `door_parity_tests` green inside `cargo test -p repark-python --lib` (75 passed).

## Round 2 step-6 facade triage — 2026-09-16 (muse-spark-1.3-contributor)

Whole facade suite, first run on the reconciled tree: 56 failed, 9291
passed, 367 skipped. Every failure triaged below. Root pattern: round 1's
kernels improved Spark behavior (nullability, widths, refusals) on a tree
whose pins still recorded the old shapes, plus three round-1 kernel defects
and two out-of-scope product gaps. My round-2 rule deletion changes no door
behavior (the deleted rule was already filtered from every door rule set;
verified identical sets), so no failure below traces to it.

Kernel defects fixed here (all in-fence, per-function, with unit tests):

- `wider_pair` refused `Utf8`+`Utf8View` mixes (`DATATYPE_MISMATCH.CREATE_ARRAY`)
  Spark coerces: all three Spark string spellings widen to `Utf8` now, with
  two `spark_common_element` rows. Fixed the four binding-string FNP8 cells.
- Facade 2-arg `like`/`ilike` dispatched to the UDF, deferring the
  trailing-escape refusal to execution (`PySparkException`); the SQL door
  refuses at analysis (`AnalysisException`). Two-arg arms build `Expr::Like`
  again (one merged arm, file stays under ceiling); 3-arg keeps the UDF.
  Fixed the three `fn-like-escend` cells.
- `ReparkShuffle` pass-through coercion dropped DataFusion's `Int32`→`Int64`
  seed cast: the wrapper widens the seed at invoke (array operand untouched).
  Fixed the shuffle seed cell.
- `posexplode_outer`/`inline_outer` built the one-NULL-element fallback list
  reusing the input's now-non-null `element` field: export panicked
  (`Non-nullable field ... cannot contain nulls`). The fallback carries a
  nullable copy of the field now. Fixed both TVF cells.

Stale pins tightened to Spark truth (widths/values untouched; each verified
against its oracle fixture or the Spark Arrow schema in `fnp8_spark_oracle.json`):

- BL-20 `LIT-SQL-32`/`LIT2-PY-02`/bare-`array` V-001: nullable `item` →
  non-null `element` (N7-0).
- `types-1` VALUES nullability + `array` legs; `test_complex_cast` array_ctor;
  `COMPLEX-ELEM-NULL-1` array leg (struct/map stay BACKLOG).
- Seven `perf-agg-avg-1` group-key flags across six tests (keys over literal
  rows are non-null).
- Eight FNP8 binding capture/shadow dispositions re-measured live (values
  already equal the Spark rows; nullability converged).
- TY-3 closed: the union is `decimal128(11,1)` non-null like Spark; the pin
  asserts the golden directly (the test's own tripwire demanded the revisit).
- `test_production_file_size` re-hash of `_match_from_or_join_keyword`
  (round-1's DISTINCT-FROM guard); cap_1 mirror row records the round-1
  `analyzer.rs` shrink 1150 → 1122; one registry self-link anchor kept stable.

P2 hand-offs out of this unit's scope (recorded, not fixed):

- P2 union-path (4 `fnp_gen_1` cells): SQL UNION does not unify nested
  nullability — `List(non-null Struct)` vs `List(Struct)` errors in
  `optimize_projections` where Spark unifies to nullable. Needs union-branch
  unification in set-op planning; no narrow fix on the kernel side (the
  literal array is Spark-right).
- P2 ML owner (~20 ML cells): `python/repark/spark/ml` lowers vector
  extraction to SQL `array_element(...)`, which this unit's gate-tested
  stub refuses with `UNRESOLVED_ROUTINE` (Spark has no such function;
  `EXPECTED_DIVERGENCES` pins door-refuses/facade-`element_at`). ML must
  move its lowering to `element_at`, honoring its 0-based `enumerate` and
  out-of-range-NULL reliance (`base.py`).

Residual for FNP-8: HOF kernels export the `item` field name while live
Spark exports `element` (decoded from `fnp8_spark_oracle.json` schemas);
nullability now matches. Renaming HOF kernels is FNP-8's call.
