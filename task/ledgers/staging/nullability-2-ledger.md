# Unit ledger — NULLABILITY-2 · the analyzer's remaining nullability and cast residues

**Retires:** this ledger moves to `../completed/` when the orchestrator merges this lane.

**Unit:** NULLABILITY-2 · **Date:** 2026-09-05 · **Model:** muse-spark-1.3 ·
**Branch:** `fix/nullability-2` · **Base:** `main` `bc7c76cc`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`CAST-NULL-1`, `CAST-BOOL-DEC-1`, `DEC-9` (remainder), `G6-4`, `G12-1`, `G12-2`,
`CUTOVER-NULLDEPTH-1`, `READ-TSNTZ-DTYPE-1`.
**Continues:** [cutover-schema-1-ledger.md](cutover-schema-1-ledger.md) (rules R-1..R-6,
blast-radius method, live-cell rules).

**Rubric:** STANDARD. `risk_tier: elevated` — analyzer-wide rule changes.

**Writable paths:** `crates/repark-functions/src/` (analyzer rules),
`crates/repark-core/src/spark_nullable.rs` (reader relax),
`crates/repark-python/src/` (cast serving if needed),
`python/repark/src/repark/spark/` (facade dtype mapping),
`python/repark/tests/test_nullability_2.py` (+ flips in `test_cutover_schema_1.py`,
`test_cast_failure_parity.py`, `test_three_valued_logic_parity.py`),
`docs/spark-sql-iceberg-parity.md`, lockstep `map.md` files, this ledger.
Closed: `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, AWS, dependencies.

## 1. Scope, as checkable propositions

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Cast nullability is Spark's on the measured matrix (string→numeric/date/timestamp nullable; date→timestamp non-null; timestamp→date non-null; timestamp→int nullable; numeric narrowing nullable; numeric→string, boolean→numeric non-null; identical both ANSI modes), Spark door only — the native-door fence pin stays green. | `test_nullability_2.py` cast pins + live legs | OPEN |
| C-002 | Overflow-capable binary arithmetic (`INT * INT`, `BIGINT + BIGINT`, decimal arithmetic that can overflow) is nullable exactly where Spark marks it, both ANSI modes. | `test_nullability_2.py` arithmetic pins + live legs | OPEN |
| C-003 | `CAST(bool AS DECIMAL(p,s))` serves on both doors (true → 1, false → 0 at target scale; `DECIMAL(1,0)` and `DECIMAL(2,2)` edges measured), Spark-equal value/type/nullability. | `test_nullability_2.py` bool-dec pins + live legs | OPEN |
| C-004 | `<=>` (SQL) and `eqNullSafe` (DataFrame) produce non-null boolean; plan schema pin + written parquet/Iceberg schema reflect it. | `test_nullability_2.py` nullsafe pins + live legs | OPEN |
| C-005 | Reader relax covers every nesting level (iterative walk, no depth bound inside Arrow's supported range); pins at depth 40 and 200; deep schemas complete without stack overflow. | `test_nullability_2.py` depth pins + Rust pins + live legs | OPEN |
| C-006 | Facade `dtypes`/`schema`/`printSchema` report `timestamp_ntz` for tz-naive Arrow timestamps on read.parquet/csv/json and `createDataFrame`; Spark-equal `printSchema` text. | `test_nullability_2.py` dtype pins + live legs | OPEN |
| C-007 | Registry rows flipped or narrowed with date + unit id; ledger + maps lockstep. | flipped rows + maps | OPEN |
| C-008 | Red-first battery red on base, green after; per-rule mutations red the named subsets; gates green. | mutation table §6 + gate table §7 | OPEN |

`LOGIC_SCORE` = **0/8 `PROVEN** (pickup).

## 2. Oracle table

| Engine | Pin |
|---|---|
| live PySpark 4.1.2 + Iceberg 1.11.0 | banner at measurement time; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC` |
| repark 1.0.1 (`bc7c76cc` + this unit) | memory catalog `ice`, facade session |

## 3. The measured rules

Oracle: live PySpark 4.1.2, tz UTC, `local[2]` (banner `version 4.1.2, tz UTC`),
2026-09-05. Verbatim output in `/tmp/nullab2_spark.out` (round 1, 40 cells),
`/tmp/nullab2_spark2.out` (round 2, 25 cells), `/tmp/nullab2_spark3.out`
(round 3, 14 cells), `/tmp/nullab2_spark4.out` (round 4, arith half) + the
tsntz spellings; repark halves in `/tmp/nullab2_repark*.out`. Every cast and
arithmetic cell measured under BOTH ANSI modes on both engines.

| # | Rule | Oracle evidence |
|---|---|---|
| R-1 | A cast from a non-null child is nullable iff the conversion can fail: string→{integral, float, boolean, date, timestamp, decimal} nullable; float→integral nullable; timestamp→{int8, int16, int32} nullable; decimal→{integral, float, string} nullable (DataFusion agrees, no rule needed); date→timestamp NON-null; integral↔integral (narrow or wide), integral→{float, string, bool}, bool→{integral, string}, date→string, timestamp→{date, long, double, float, string}, int/long→timestamp non-null. The nullability flag is IDENTICAL under ANSI on/off on every cell. | round 1–3 matrix; the brief's "narrowing nullable" hypothesis is false for integral narrowing (`bigint_to_int` non-null) |
| R-2 | Decimal `+`/`-`/`*` over non-null operands is nullable iff ANSI is OFF (Spark `CheckOverflow`, null-on-overflow); ANSI ON propagates operand nullability. Integral arithmetic is non-null over non-null operands in both modes (ANSI-off wraps, ANSI-on raises — never NULL). Decimal div/rem nullable in both modes (both engines agree). Float arithmetic propagates (both agree). | `dec_add/dec_sub/dec_mul/dec_col_*` flip T→F across ANSI off→on; `int_add/mul`, `bigint_add`, `colarith` non-null both modes; `dec38_add` non-null ANSI-on |
| R-3 | `CAST(bool AS DECIMAL(p,s))`: true→1, false→0 at target scale; nullable iff the target holds no integer digit (`(10,2)`/false→`0.00` non-null; `(1,0)`→`1` non-null; `(2,2)` nullable — ANSI-on raises `NUMERIC_VALUE_OUT_OF_RANGE`, ANSI-off NULL). | `bool_to_dec*` cells both modes; logical schema nullable=True for `(2,2)` even ANSI-on |
| R-4 | `<=>` and `eqNullSafe` are non-null boolean on every input shape (nulls, values, mixed). | `nse_sql_*` + `eqnullsafe_df` all `bool` non-null both modes |
| R-5 | Spark relaxes every nesting level; repark's relax must not bind below Arrow's own ceiling. Transport ceilings (not relax): parquet read fails past flatbuffers footer depth 64 (`arrow-ipc` default `max_footer_fb_depth`; repark reads 40-deep, refuses 80+ with `DepthLimitReached`); pyarrow/Spark-`toArrow` IPC caps near 124; JSON reads cap at serde's recursion limit. So the 200 pin is structural (Rust) + an honest refusal pin at the facade. | depth probe `/tmp/rdeep*.parquet`; `arrow-ipc-57.3.1/src/reader.rs` `with_max_footer_fb_depth` doc |
| R-6 | Tz-naive TIMESTAMP reads `timestamp_ntz` via `dtypes`, `schema.simpleString()` (`struct<id:string,ingestion_timestamp:timestamp_ntz>`), and `printSchema` (`timestamp_ntz (nullable = true)`); Arrow says `timestamp[us]`. | tsntz probe both engines |

Out-of-scope observations (measured, not in the eight rows; reported, not fixed):
O-1 `CAST(DATE AS INT/BIGINT)` ANSI-off: Spark serves NULL, repark refuses.
O-2 failing casts ANSI-off (`'abc'→int`, overflowing `ts→short/byte`): Spark NULLs,
repark errors — cast-failure VALUE leniency, a separate feature from the flag.
O-3 `arrow_type_key` deliberately collapses top-level smallint/tinyint→`int` and
float→`double` in the logical schema (comment at `dataframe.rs:163`); Arrow agrees.
O-4 `9*9`-style literal arithmetic widens to int64 on repark (int32 on Spark);
nullability agrees. O-5 overflowing `CAST(int AS DECIMAL(2,2))` errors on repark
under ANSI-off where Spark NULLs (pre-existing, same class as O-2).

## 4. Baseline (pre-fix, `bc7c76cc`)

TBD — facade + parity suites before the first fix commit.

## 5. Blast-radius classification

TBD — every red is class (a) Spark-answer flip with citation or class (b) regression
to fix, per the CUTOVER-SCHEMA-1 method.

## 6. Mutation table

TBD — red-first battery + one knob per rule (cast, bool-dec, nullsafe, relax, dtype).

## 7. Gates

| Gate | Exit |
|---|---|
| `make ci` | TBD |
| `make verify` | TBD |
| `make check-python-conventions` | TBD |
| `make rust-panic-ban` | TBD |
| facade `python/repark/tests -q` | TBD |
| parity `python/repark-parity/tests -q` | TBD |
| live `REPARK_PARITY_LIVE=1 … test_parity_live.py test_nullability_2.py test_cutover_schema_1.py test_sql_harden_cutover.py -q` | TBD |
| `make py-test-dbt` | TBD |
| `make check-map-sync` | TBD |
| `make check-ledger-grammar` | TBD |
| `make check-ledgers` | TBD |
| `make check-docs-compaction` | TBD |
| `ledger_lifecycle.py check --base origin/main` | TBD |
| `typos .` | TBD |

## 8. Delivery template

TBD at departure.

## 9. Coverage attestation

TBD at departure (Critic's artifact).
