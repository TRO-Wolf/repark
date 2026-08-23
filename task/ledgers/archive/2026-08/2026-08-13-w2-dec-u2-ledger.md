# Unit ledger — W-2 / DEC U2: Spark-door `parse_float_as_decimal=true`

**Unit:** W-2 · campaign DEC-1 (U2) · **Date:** 2026-08-13 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-w2` · **Branch:** `grok/w2-dec-u2` ·
**Base (FROZEN):** `c7e6589088111ded62848751a30a45adfea0973a`
(`fix(tz4): LTZ instant producers emit µs+UTC; TIMESTAMP → timestamptz (#79)`)

**Charter:** `BRIEF-w2-dec-u2.md` + `DEC-DESIGN.md` §4 U2 + BRIEF-y9 addendum
(Q3=A, Q4=A) + conductor-6 A3/A8 + Z-3 U2 blast table. **SEPMO:** acc + C4
(`claims_critic=true`). Floor S1.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` — W-5 owns
the registry. DEC-1 FIXED text is paste-true in §6.

---

## 0. Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Spark door default is `datafusion.sql_parser.parse_float_as_decimal=true` in `SparkExtension::configure`. | PROVEN — `apply_spark_float_as_decimal` + hook call. Pin `configure_defaults_parse_float_as_decimal`. |
| C-002 | ANSI door is unchanged (G11). | PROVEN — no `repark-sql` / core session-default edit. `cargo test -p repark-sql --lib` 223 passed. |
| C-003 | Corpus 3 literal rows flip to equality (`repark=None`). | PROVEN — `literal_1_23` / `0_1` / `123_456`. G2 16 eq / 8 disc. |
| C-004 | Overflow 38-nines pin is wrap-not-residue (`10^38` at (38,0)), not a DEC-6/7 semantics fix. | PROVEN — Rust i128 `10^38`; Python `_dec_raw_i128` (PyArrow ctor rejects 39 digits at p=38). Spark still raises. |
| C-005 | `pin_literal_1_23` is decimal128(3,2) i128=123. | PROVEN — renamed `pin_literal_1_23_infers_decimal128_3_2_i128`. |
| C-006 | TY-3 dated-decision: still DECLARED, residual U3 (Q4=A). | PROVEN — repark `decimal128(21,1)` nullable vs Spark `(11,1)` non-null. |
| C-007 | Named blast list flipped with re-recorded evidence. | PROVEN — §2. DataFusion `SELECT 1.0` is DECIMAL(2,1) (DF `parse_decimals_5`). |
| C-008 | Sweep wider than the list. | PROVEN — §2 wider table. Full facade 2923 passed / 71 skipped. |
| C-009 | DEVELOPMENT.md gains exactly the A8 sentence after the JVM paragraph. | PROVEN — one added sentence, nothing else in that file. |
| C-010 | TPC-H DuckDB-diff re-run; ledger honest. | PROVEN — SF1 21 OK + Q1 WRONG-RESULT (same as Z-3). No new query moved. |
| C-011 | Decimal32/64/256 accumulator arms have revert-red pins (Z-3 S3). | PROVEN — `group_avg_decimal32/64/256_*`. |
| C-012 | Live-tier files untouched. If U2 reds live tier → narrower cut or defer. | PROVEN — `_live_parity.py` / `test_parity_live.py` not in diff. `make parity-live`: 3016 passed, 3 skipped, **1 failed** = expected Apache collation smoke (`test_udf_with_collated_string_types`). No U2-forced live red. |
| C-013 | No registry / lockfile / `.github` / ANSI knob / DecimalPrecision edits. | PROVEN — diff names. |

---

## 1. Decisions

**D-W2-1 — Unconditional Spark-door default.** `configure` always sets the
flag (Spark-door invariant). Opt-out is the ANSI door, not a builder key
after the hook. Shared helper `apply_spark_float_as_decimal` is also used by
Spark-door `setup()` fixtures so G-7b pins see production wiring.

**D-W2-2 — TY-3 = Q4 A.** After U2, `VALUES (1) ∪ VALUES (2.5)` is
`decimal128(21,1)` nullable (Int64→DECIMAL(20,0) ∪ DECIMAL(2,1)). Spark is
`(11,1)` non-null. Still DECLARED; residual is U3 / DEC-8.

**D-W2-3 — Overflow reclass is IN W-2 (A3).** Not a DEC-6 raise. The 38-nines
token now parses as exact DECIMAL; `+ 1` wraps to `10^38` at declared (38,0).

**D-W2-4 — `1.0` keeps scale 1.** DataFusion `parse_decimal("1.0")` is
`Decimal128(Some(10),2,1)` — Spark's DECIMAL(2,1). No trailing-zero strip.

**D-W2-5 — Column `/` of two decimals stays Float64.** SQL `SELECT 7.0 / 2.0`
is decimal128(7,5). DataFrame `F.col("a") / F.col("b")` is still double 3.5.
Both pinned (`test_division_is_float` + `test_sql_float_literal_division_is_decimal`).

**D-W2-6 — Fixtures that *mean* IEEE float CAST to DOUBLE.** NaN mixes,
signed zero, sliding-avg Float64 shim, ML labels. Not a silent absorb —
the Spark door now matches Spark SQL inference.

**D-W2-7 — CountVectorizer TF type is a named residual.** ML SQL emits
`THEN 1.0 ELSE 0.0` (not in tonight's writable set). After U2 those vectors
are `list<decimal128(12,1)>`. Test compares via `float(sum(vec))`. Product
fix (`CAST(1.0 AS DOUBLE)` / integer `1`) is a later unit. Registry is W-5.

**D-W2-8 — Registry paste-true only.** DEC-1 → FIXED text in §6. W-5 lands it.

---

## 2. Blast verification (on this base) + flips

Z-3 table verified on `c7e6589`, then flipped:

| File | Pin | Before | After U2 |
|---|---|---|---|
| `test_decimal128_parity.py` | 3 literals | float64 vs Spark decimal | **equality** |
| `test_decimal128_parity.py` | overflow 38-nines | residue i128 | wrap `10^38` at (38,0) |
| `crates/repark-spark/src/tests/decimal.rs` | `pin_literal_1_23_*` | f64 bits | decimal128(3,2) i128=123 |
| same | overflow i128 | residue | wrap `10^38` |
| `test_union_distinct.py` TY-3 | float64 nullable | `decimal128(21,1)` nullable; Spark `(11,1)` non-null; still DECLARED |
| `test_columns.py` | `7.0` SQL `/` | float 3.5 | SQL: decimal128(7,5) `3.50000`; Column `/`: still float64 3.5 |
| `test_fn_batch4.py` | stats | float compares | med/list DECIMAL(2,1); stddev still float |
| `test_session.py` | `to_numpy` | float64 | object of Decimal |
| `test_sql_passthrough_parity.py` | `1.0/0.0` | float NULL | decimal128(7,5) NULL; `5.0%0.0` is (1,1) NULL |

Wider sweep (floor, not ceiling):

| File | Change |
|---|---|
| `test_sliding_avg_parity.py` | CAST `1.0`/`3.0`/`6.0` AS DOUBLE (keep Float64 retract claim) |
| `test_group_agg.py` | signed-zero via `createDataFrame` (SQL `-0.0` is DECIMAL 0) |
| `test_ml_feature_oracle.py` | NaN-mix CAST; CountVectorizer `float(sum)` |
| `test_ml_estimators_oracle.py` | intercept-only labels CAST AS DOUBLE |
| `test_pivot.py` | NaN-key UNION CAST to DOUBLE |
| `test_dataframe_actions.py` | NaN/None collect CAST to DOUBLE |

Full facade after flips: **2923 passed, 71 skipped, 0 failed**.
`repark-spark --lib`: 423 passed. Decimal32/64/256 pins: 4 passed (with 128).

---

## 3. Files

| Path | Role |
|---|---|
| `crates/repark-spark/src/extension.rs` | default + helper |
| `crates/repark-spark/src/extension/tests.rs` | option + collect pins |
| `crates/repark-spark/src/tests/common.rs` | setup uses helper |
| `crates/repark-spark/src/tests/decimal.rs` | literal + overflow pins |
| `crates/repark-functions/src/aggregate.rs` | Decimal32/64/256 pins |
| `python/repark/tests/test_decimal128_parity.py` | 3 eq + wrap overflow |
| named blast + wider test files | §2 |
| `DEVELOPMENT.md` | A8 one sentence |
| `python/repark-parity/bench/tpch/sf1_status_ledger.json` | Q1 note (status unchanged) |
| maps + this ledger | lockstep |

**Not edited:** `_live_parity.py`, `test_parity_live.py`, registry,
`Cargo.lock`, `uv.lock`, ANSI door, `column.py`, `.github/`.

---

## 4. Deviations / residuals

- **TY-3 still DECLARED** (Q4=A). Residual U3.
- **CountVectorizer / ML SQL `1.0` → decimal vectors.** Named residual; ML
  transformers are outside tonight's writable set.
- **DEC-6/7 semantics** stay campaign-body (A3).
- **Live-tier §0** waits on `/tmp/grok-w1-first-released`.

---

## 5. Gate evidence

### 5.1 JVM lock

Waited for `/tmp/grok-w1-first-released`
(`conductor-stale-rm w1-blast … 13:25:18 … FIFO open`). No lock present
then. Acquired (`set -o noclobber`):

```
MARKER=w2-dec
PID=2598238
ISO=2026-08-13T13:37:30-04:00
lane=w2-dec-u2
```

`trap` RELEASE-ON-EXIT. First acquire in a short shell released on exit
(trap fired). Second acquire held for record + `make parity-live`. After
that command the lock file was still present with
`restored-by=w1-after-accidental-overwrite` (W-1 restored our marker after
an overwrite). Explicit own-marker `rm` after JVM work done (pid 2598238
dead). No stale-rm of anyone else's marker. No local `pyspark`/`SparkSubmit`
(HiveThrift ignored).

Record:

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  VIRTUAL_ENV=/tmp/grok-w2/.venv uv run --no-project \
  python python/repark/tests/_record_decimal128_goldens.py
```

PySpark **4.1.2**. `record mode: 33 spark halves re-derived, 0 mismatch(es)`.
`RECORD_EC:0`. Three literal rows PASS. Overflow RAISE:ArithmeticException PASS.

### 5.2 Targeted (pre-lock)

```
cargo test -p repark-spark --lib configure_defaults_parse_float_as_decimal  # ok
cargo test -p repark-spark --lib configure_makes_bare_1_23                  # ok
cargo test -p repark-spark --lib pin_literal_1_23                           # ok
cargo test -p repark-spark --lib pin_overflow_max_decimal                   # ok
cargo test -p repark-functions --lib group_avg_decimal                      # 4 ok
cargo test -p repark-spark --lib                                            # 423 ok
cargo test -p repark-sql --lib                                              # 223 ok
pytest named blast + wider first sweep                                     # 372 ok
pytest python/repark/tests                                                 # 2923 passed, 71 skipped
TPC-H SF1 --repeats 1                                                      # 21 OK, Q1 WRONG-RESULT
```

### 5.3 Gates (cd-fused, real exit codes)

| Gate | EC | Notes |
|---|---|---|
| `make verify` | **0** | After bindings pin flip. rust-file-size 195 clean. |
| `make preflight` | **0** | verify + facade + audit + workflows. Facade 2923 passed / 71 skipped (earlier isolated run). cargo-deny / pip-audit / zizmor clean. |
| `make parity-live` | **1** | Expected base red only: `test_compat_smoke_suite_in_subprocess` → Apache `test_udf_with_collated_string_types`. 3016 passed, 3 skipped, 1 failed. No U2 live-tier edit. |

TPC-H SF1 DuckDB-diff (`--repeats 1`): 21 OK + Q1 WRONG-RESULT. No new query moved.

---

## 6. Registry rows — READY TO PASTE, **not** landed (W-5)

### DEC-1 (literal inference) → FIXED

- **repark** — Spark door default
  `datafusion.sql_parser.parse_float_as_decimal=true` via
  `SparkExtension::configure` (`apply_spark_float_as_decimal`). Bare `1.23` /
  `0.1` / `123.456` are `decimal128(3,2)` / `(1,1)` / `(6,3)`. `1.0` is
  `decimal128(2,1)` (trailing zero kept). ANSI door unchanged.
- **Apache Spark** — `DECIMAL(len, scale)` from the text; `1.0` is `(2,1)`
  non-null. *(oracle: recorded; Y-9 probe.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[literal_1_23_infers_decimal_in_spark_double_in_repark]`
  (equality; name kept) + Rust
  `pin_literal_1_23_infers_decimal128_3_2_i128` +
  `configure_makes_bare_1_23_decimal128_3_2`.
- **Rationale** — campaign DEC-1 / W-2 U2. Facade + Rust Spark-door cells closed.

### TY-3 — still DECLARED, residual U3

Dated 2026-08-13: U2 landed. Observed repark
`decimal128(21,1)` nullable `Decimal('1.0')`/`Decimal('2.5')` vs Spark
`decimal128(11,1)` non-null. Residual is campaign DEC-8 / U3
(integer-literal min-precision + VALUES nullability).

### DEC-6 overflow — still BACKLOG (wrap-not-residue)

U2 removed the float-residue photograph. Remaining defect: `max DECIMAL(38,0)+1`
stores `10^38` at declared `(38,0)` (no raise). ANSI raise is U5.

---

## 7. ACC + C4

Sequential hats (no spawn; independence weaker than separate agents).

**Context break executed; attacking artifacts, not memory.**

### Critic-1 (coverage / logic)

ATTACKED. Each C-00n clause has a pin (hook option, collect 1.23, three corpus
equalities, overflow wrap, TY-3 dated, DEVELOPMENT.md one-line, TPC-H re-run,
Decimal32/64/256). Fresh public-entry execution (novel vs committed tests):
`SELECT 10.00` → `decimal128(4,2)` non-null `Decimal('10.00')`; `SELECT 9.99` →
`(3,2)`; `SELECT 0.01` → `(2,2)`. Trailing zeros kept.

Findings:
- **F-W2-1 S3** — CountVectorizer SQL `1.0`/`0.0` now emits
  `list<decimal128(12,1)>`. Test-only `float(sum)`. Product CAST-to-DOUBLE is
  outside writable set. `ACCEPTED_FLAGGED`.
- **F-W2-2 S3** — `make parity-live` EC=1 is the Y-7 collation smoke, not U2.
  `ACCEPTED_FLAGGED` (base-state).

Null reports: spec, interface, maintainability, data integrity at S1.

**Critic-1 CLEAN** at floor S1.

### Critic-2 (safety)

ATTACKED. No secrets, no `unsafe`, no prod unwrap/expect. Overflow wrap is
declared leftover (DEC-6), not silent. Lock released. No AWS.

**Critic-2 CLEAN.**

### Critic-4 (claims)

ATTACKED. DEVELOPMENT.md diff is exactly one added sentence. Closed files
absent from `git diff --name-only`. Hygiene greps count 0. DEC-1 not landed
in the registry file. TY-3 still DECLARED. G2 16/8 after three equality flips.
Live-tier files byte-untouched. Identity `%ae` checked at commit.

**Critic-4 CLEAN.**

**Convergence:** `ACC-CONVERGED` (C1+C2+C4; verify 0, preflight 0, parity-live
expected collation-only red).
