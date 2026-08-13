# Unit ledger — Z-3 / DEC U1+U2: un-coerce `avg(DECIMAL)` + literal-default deferral

**Unit:** Z-3 · campaign DEC-5 (U1) + DEC-1 (U2) · **Date:** 2026-08-13 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-z3` · **Branch:** `grok/z3-dec-u1u2` ·
**Base (FROZEN):** `9b2dce3c73af402e8705923135d7de014da5501f`
(`fix(g5b-r): window RANGE residuals — R3 no longer wraps (#72)`)

**Charter:** `BRIEF-z3-dec-u1u2.md` + `DEC-DESIGN.md` §4 U1/U2 + §1.5 +
BRIEF-y9 2026-08-13 addendum + conductor-5 A1/A2/A3. **SEPMO:** acc + C4
(`claims_critic=true`).

This ledger is the unit record. It does **not** edit
`docs/spark-sql-iceberg-parity.md` — §6 is paste-true for Z-5.

---

## 0. §0 blast + overwrite hunt (before code)

**Second `avg` overwrite?** Grep of `crates/repark-functions/src/analyzer.rs` for
`avg` = **zero hits**. The facade overwrite is `SparkAvgWithRetract` in
`crates/repark-functions/src/aggregate.rs`, registered from
`lib.rs::register_all` after `datafusion-spark`'s `SparkAvg`. A1 is confirmed:
`analyzer.rs` stays closed.

**Q2 retract-vs-refuse (U1):** DataFusion 54.1 `DecimalAvgAccumulator` already
implements `retract_batch` and evaluate via `DecimalAverager` (~85 + ~70 lines).
That is a **small copy**. Decision: **U1 = un-coerce decimal + copy decimal
retract**. Float retract stays. `SparkAvgWithRetract` is **not** deleted (that
would re-break X2 float sliding avg). Sliding decimal does **not** stay on the
float path.

**U2 cascade (Spark-door `parse_float_as_decimal=true`):** this changes the type
of every bare float literal through the Spark door. Sweep of the facade corpus
+ the G-7b Rust pins (files **outside** A1's writable set that would go red):

| File | Row / pin | Today | After U2 | Writable? |
|---|---|---|---|---|
| `test_decimal128_parity.py` | `literal_1_23_…` / `0_1_…` / `123_456_…` | float64 vs Spark decimal | intended flip to equality | **yes** (A1) |
| `test_decimal128_parity.py` | `overflow_max_decimal38_plus_one_…` | float-path residue i128 | memo §2.5: exact then wrap `10^38` | yes (corpus) |
| `crates/repark-spark/src/tests/decimal.rs` | `pin_literal_1_23_infers_float64` | f64 bits of 1.23 | decimal128(3,2) | **no** |
| `crates/repark-spark/src/tests/decimal.rs` | `pin_overflow_max_decimal38_plus_one_wrong_value_i128` | residue i128 | wrap `10^38` (if the 38-nines token re-parses) | **no** |
| `test_union_distinct.py` | `test_union_inline_decimal_literal_diverges_from_spark` (TY-3) | float64 nullable vs Spark `(11,1)` non-null | memo: `(21,1)` nullable — still not Spark; pin must move | **no** |
| `test_columns.py` | `test_division_is_float` (`SELECT 7.0 / 2.0`) | float 3.5 | decimal `/` → `Decimal('3.5')` vs `3.5` | **no** |
| `test_columns.py` | `test_cast_accepts_long_and_bigint` (`SELECT 2.9`) | cast result int64 | likely still green | n/a |
| `test_fn_batch4.py` | `test_stats_aggregates` (`VALUES (1.0)…`) | float compares / `collect_list == [1.0,2.0,3.0]` | Decimal vs float | **no** |
| `test_session.py` | `test_to_numpy_numeric_matrix` (`VALUES (1.5, 2.5)`) | `dtype == float64` | decimal matrix dtype | **no** |
| `test_sql_passthrough_parity.py` | `1.0/0.0`, `5.0 % 0.0` | float NULL types | decimal `/0` type | **no** |
| `test_cast_failure_parity.py` | `VALUES (123.45)` CAST overflow | both raise | still raise | likely green |
| `test_sliding_avg_parity.py` | `1.0` mixed with `CAST(NULL AS DOUBLE)` | float64 sliding | union with DOUBLE likely stays float | maybe green |
| `test_window_parity.py` | `avg(v)` over int `v` | float64 | unaffected (int avg) | green |
| `test_float_agg_parity.py` | `CAST(… AS DOUBLE)` fixture | float64 | CAST wins | green |
| `test_group_agg.py` | `avg` of int column | float64 | unaffected | green |
| `_live_parity.py` | existing SCENARIOS | — | **CLOSED** (Z-2 A6) | no |
| ANSI-door session builder | G11 | default false | **must stay** | closed |

**U2 decision:** cascade is **too wide for one reviewable PR** under A1's closed
writable set (Rust literal/overflow pins + TY-3 + several type-pin tests sit
outside it). **U2 is a named morning deferral** — never silent, never stacked.
`extension.rs::configure` is **untouched**. Q16 DEVELOPMENT.md note is A3
never-touch; morning follow-through.

**TY-3:** Q4 lean A — stays DECLARED, residual U3. Dated here: 2026-08-13,
U2 deferred, so the dated revisit is itself deferred with U2.

---

## 1. Decisions

**D-Z3-1 — U1 = decimal retract, not refuse-loud.** Q2 lean A is cheap: DF
54.1 already has `DecimalAvgAccumulator::retract_batch` + `DecimalAverager`.
Copied into `aggregate.rs` (Decimal32/64/128/256). Signature is DF `Avg`'s
(`Decimal` exact + Integer/Float → Float64), **not** `SparkAvg`'s
`Numeric → Float64`.

**D-Z3-2 — Do not delete `SparkAvgWithRetract`.** That re-breaks X2 float
sliding avg. Float retract stays.

**D-Z3-3 — U2 deferred.** See §0 blast table. Not a silent scope shrink.

**D-Z3-4 — Corpus flip, don't rename.**
`avg_money_stays_decimal_in_spark_double_in_repark` keeps its name so registry
citations still resolve; `repark=None` (equality). Spark half was already
`decimal128(14,6)` nullable `1.650000`.

**D-Z3-5 — Rust pins live in `aggregate.rs`.** Charter originally said
"analyzer layer"; A1 corrected the overwrite home. G-7b
`pin_avg_money_stays_decimal128_14_6_i128` already holds the Rust Spark door
(that door does **not** call `register_all`) and is not edited.

**D-Z3-6 — ANSI door / DEVELOPMENT.md / registry / `_live_parity.py` /
`analyzer.rs` / `Cargo.lock` / `uv.lock` untouched.**

**D-Z3-7 — Empty-group `default_value` follows the return type.** SparkAvg
always returned `Float64(None)`; that would type-error an empty decimal avg.

---

## 2. Pins

### Rust (`crates/repark-functions/src/aggregate.rs`)

| Test | Claim | Mutation-red |
|---|---|---|
| `group_avg_decimal128_stays_decimal_14_6_i128` | `avg(DECIMAL(10,2))` of 1.10, 2.20 → `(14,6)` nullable i128=`1_650_000` through `register_all` | restore Numeric→Float64 coerce |
| `sliding_avg_decimal128_retracts` | sliding `avg(DECIMAL)` plans + returns `(14,6)` values 1/2/4 | drop decimal `retract_batch` / leave decimal on float path |
| `decimal_retract_batch_returns_to_empty` | retract of the contributing batch → count 0, evaluate NULL | delete `retract_batch` |
| `empty_group_decimal_avg_is_null_at_14_6` | empty SELECT of `avg(DECIMAL(10,2))` → `(14,6)` NULL | restore `default_value` → always Float64(None) |
| `integer_avg_still_returns_float64` | `avg` of ints stays Float64 3.0 | accidental Decimal signature-only |
| existing `sliding_avg_over_rows_succeeds_after_shim` | X2 float sliding still works | delete float retract |

### Facade corpus

| Node | Kind after U1 |
|---|---|
| `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[avg_money_stays_decimal_in_spark_double_in_repark]` | **equality** (was disclosure) |

Budget after flip: G2 24 rows, **13** equalities, **11** disclosures (min 8 / max 20 still hold).

---

## 3. Files

| Path | Role |
|---|---|
| `crates/repark-functions/src/aggregate.rs` | U1 engine + Rust pins |
| `crates/repark-functions/src/lib.rs` | `register_all` aggregate-block comment only |
| `python/repark/tests/test_decimal128_parity.py` | avg row flip to equality |
| `python/repark-parity/bench/tpch/sf1_status_ledger.json` | forced Q1 OK→WRONG-RESULT (DuckDB vs Spark avg) |
| `python/repark-parity/bench/tpch/map.md` | lockstep |
| `crates/repark-functions/map.md` | lockstep |
| `crates/repark-functions/src/map.md` | lockstep |
| `python/repark/tests/map.md` | lockstep (13/11 counts) |
| `task/map.md` | both-add this ledger |
| `task/z3-dec-u1u2-ledger.md` | this file |

**Not edited (A1 / A3 / U2 deferral):** `extension.rs`, `analyzer.rs`,
`DEVELOPMENT.md`, `decimal.rs`, `docs/spark-sql-iceberg-parity.md`,
`_live_parity.py`, ANSI session builder, `Cargo.lock`, `uv.lock`.

---

## 4. Deviations

- **U2 not landed.** FLAG: A1 writable set cannot absorb the blast in §0.
  Named morning follow-through: Spark-door default + flip every row in the
  blast table (including G-7b literal/overflow pins and TY-3 dated revisit)
  + Q16 DEVELOPMENT.md note (A3 expired whitelist).
- **Row name not renamed** on the avg flip. FLAG: registry citations use the
  old node id; renaming would ghost them. Note in the row records the flip.
- **Decimal32/64/256 copied with Decimal128.** FLAG: `TypeSignatureClass::Decimal`
  is all widths; implementing only 128 would plan then `not_impl` those
  widths (today they coerced to float and "worked"). Same DF copy for all four.
- **TPC-H Q1 SF1 ledger OK → WRONG-RESULT.** FLAG: A1 did not list
  `sf1_status_ledger.json`. Forced preflight red: `avg(l_discount)` is now
  Spark `(19,6)` `0.050081` vs DuckDB float `0.050081339…` (rel ~6.8e-6 >
  1e-6). Updating the scoreboard pin is the same class as flipping the
  decimal corpus (honest DuckDB-divergence, Spark-correct). Not a silent
  absorb.

---

## 5. Gate evidence

### 5.1 JVM lock + record driver (2026-08-13)

Waited for `/tmp/grok-z2-probe-released` (`2026-08-13T08:03:26-04:00`). No
`/tmp/grok-jvm-record.lock`. `pgrep` showed only this shell (no local
`pyspark`/`SparkSubmit` driver; standing HiveThrift ignored).

Acquired (`set -o noclobber`):
```
MARKER=z3-dec
PID=3894802
ISO=2026-08-13T08:19:00-04:00
```
`trap` RELEASE-ON-EXIT. No stale-rm.

Record command:
```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  VIRTUAL_ENV=/tmp/grok-z3/.venv uv run --no-project \
  python python/repark/tests/_record_decimal128_goldens.py
```
PySpark **4.1.2**. Transcript (PASS lines):
```
[G2] avg_money_stays_decimal_in_spark_double_in_repark PASS
… (all other G2/G13/CTAS spark halves PASS)
record mode: 33 spark halves re-derived, 0 mismatch(es)
RECORD_EC:0
LOCK_RELEASED own-marker z3-dec pid=3894802
```
Lock file gone after release. `uv sync --extra record` uninstalled the
maturin wheel (logged); rematurin before subsequent facade runs.

### 5.2 Targeted tests

```
cargo test -p repark-functions --lib aggregate --offline
# 10 passed (incl. 4 new U1 pins)

PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=/tmp/grok-z3/.venv \
  uv run --no-project python -m pytest python/repark/tests/test_decimal128_parity.py -q
# 35 passed

PYTHONPATH=… pytest test_sliding_avg_parity.py test_group_agg.py \
  test_float_agg_parity.py test_select_global_agg.py -q
# 90 passed, 1 skipped
```

### 5.3 `make verify`

`make verify` (warm re-run after a first-pass timeout mid `rust-test`):
**VERIFY_EC=0**. `ci` (fmt, clippy, panic-ban, crate-DAG, lib.rs, rust
file-size, lib.py, manifest, parity-live dual-wire, cargo check, ruff,
uv lock, taplo, typos) green on the first pass; `cargo test --locked
--workspace` **RUST_TEST_EC=0**.

### 5.4 `make preflight`

First facade pass red: `test_tpch_sf001_matches_sf1_ledger[1]` —
`repark=0.050081` vs DuckDB `0.05008133906964238` (Q1 `avg(l_discount)`).
Flipped `sf1_status_ledger.json` Q1 to WRONG-RESULT (forced fallout).

Warm re-run: **PREFLIGHT_EC=0**. Facade: `2901 passed, 71 skipped` in 100.60s.
audit (cargo-audit/deny + pip-audit) green. workflows-parse 11 + zizmor
"No findings to report."

---

## 6. Registry rows — READY TO PASTE, **not** landed

Z-5 owns the registry file. Paste-true after this PR merges and the pins
resolve on `main`.

### DEC-5 / registry DEC-4 (`avg`) → FIXED

- **repark** — facade `avg(DECIMAL(p,s))` returns Spark's
  `DECIMAL(min(38,p+4), min(38,s+4))` (group and sliding). The overwrite is
  `SparkAvgWithRetract` in `crates/repark-functions/src/aggregate.rs` (not
  `analyzer.rs`). Float sliding avg is unchanged (X2).
- **Apache Spark** — `Average.scala`: `DECIMAL(p,s) → bounded(p+4, s+4)`.
  Corpus `(10,2) → (14,6)` nullable `1.650000`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[avg_money_stays_decimal_in_spark_double_in_repark]`
  (equality); Rust
  `crates/repark-functions/src/aggregate.rs::group_avg_decimal128_stays_decimal_14_6_i128`
  + `sliding_avg_decimal128_retracts`. G-7b
  `pin_avg_money_stays_decimal128_14_6_i128` remains the Rust-door cell.
- **Rationale** — campaign DEC-5 / Z-3 U1. Facade cell closed. Entry-point
  split in G-7b † is discharged on the facade.

### DEC-1 (literal inference) → **still BACKLOG** (U2 deferred)

- **repark** — unchanged this PR: bare `1.23` is `float64`.
- **Apache Spark** — `DECIMAL(3,2)` non-null `1.23`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[literal_1_23_infers_decimal_in_spark_double_in_repark]`
  (still disclosure) + G-7b `pin_literal_1_23_infers_float64`.
- **Rationale** — Z-3 named morning deferral: U2's `parse_float_as_decimal=true`
  Spark-door default reds files outside A1. Do **not** mark DEC-1 FIXED.

### TY-3 — still DECLARED, residual U3

Dated 2026-08-13: U2 did not land, so the registry-mandated dated revisit of
TY-3 rides with the U2 morning unit. Residual after U2 would still be
`(21,1)` nullable vs Spark `(11,1)` non-null (campaign DEC-8 / U3).
