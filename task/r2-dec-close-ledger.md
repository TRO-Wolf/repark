# Unit ledger — R-2 / DEC close: U4b `/` + DEC-8 + DEC-6 + TY-3

**Unit:** R-2 · campaign DEC close · **Date:** 2026-08-14 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-r2` · **Branch:** `grok/r2-dec-close` ·
**Base (FROZEN):** `fddf1bc4840ade68274ca5c55993dda0fb182a61`
(`feat(ansi): Spark-door spark.sql.ansi.enabled default TRUE (U5) (#94)`)

**Charter:** `BRIEF-r2-dec-close.md` + `DEC-DESIGN.md` §4/§466 + y9 + S-1 §0.3
DEC-6 + V-2 U4b declaration + conductor-9 A1–A11. **SEPMO:** HIGH — octo + C4.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` or `STATUS.md`.
§6 is paste-true for the deferred registry writer.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | U4b is a division UDF (CAST-after wrongs the value). | PROVEN — `decimal_spark.rs` `__repark_spark_decimal_div__`. |
| C-002 | A5: one NEW slot before SparkExprSemantics; landed four keep order. | PROVEN — `SparkDecimalPrecision → SparkDecimalRewrite → SparkExprSemantics → cardinality → instant_ts`. |
| C-003 | A6: `/0` `% 0` notabool stay as behavior; UDF owns `/` knob. | PROVEN — div UDF raise/NULL; `%` untouched; notabool untouched. |
| C-004 | DEC-8 hook is `ExprPlanner::plan_binary_op` (Multiply). | PROVEN — §0.2; Spark default-true → compute-with-clamp `(38,6)`. |
| C-005 | DEC-6 hook is a new repark-functions UDF reading `ansi.rs`. | PROVEN — checked `+`/`−` when Spark==Arrow `(38,·)`. |
| C-006 | TY-3 is not a `decimal_precision` `fromLiteral` arm. | PROVEN — §0.4; still DECLARED. |
| C-007 | `analyzer.rs` / `extension.rs` / `ansi.rs` / `_live_parity.py` untouched. | PROVEN — diff names. |
| C-008 | A7 `test_columns.py` grant is that row only. | PROVEN — `7.0/2.0` → `(8,6)`. |
| C-009 | TPC-H ledger flipped only with re-run evidence. | PROVEN — untouched; §0.5. |
| C-010 | `make verify` + `make preflight` exit 0. | PROVEN — verify 0; facade 3053 passed / 71 skipped; audit + workflows-lint 0. |

---

## 0. Blast + seams + land-or-defer

### 0.1 U4b — LAND

Spark `/`: `s = max(6, s1+p2+1)`, `p = p1-s1+s2+s`, then `adjustPrecisionScale`.
Arrow: `s = s1+4`. Photographed `(10,2)/(10,2)` is Spark `(23,13)`
`0.2697368421053` vs Arrow `(16,6)` `0.269736`. CAST-after would green the
type and wrong the value.

**Taken hook:** new analyzer rule `SparkDecimalRewrite` in
`crates/repark-functions/src/decimal_spark.rs`, inserted **after**
`SparkDecimalPrecision` and **before** `SparkExprSemantics` (A5). It rewrites
clean `decimal / decimal` (post-`TypeCoercion`) onto
`__repark_spark_decimal_div__`. The UDF owns `/0` (A6): raise
`[DIVIDE_BY_ZERO]` when `spark_ansi_enabled_from_options` is true; NULL when
false. Integer `/` still hits SparkExprSemantics (float64 + the same knob).
`%` resultDecimalType CLOSED.

### 0.2 DEC-8 — LAND (compute-with-clamp)

V-2 photographed refuse at `Projection::try_new` → `BinaryExpr::get_type` →
Arrow `s1+s2>38` **before any AnalyzerRule**. The exact hook is
`ExprPlanner::plan_binary_op` (`datafusion-sql` `build_logical_expr` tries
planners, then constructs `BinaryExpr`).

Spark 4.1.2 default `allowPrecisionLoss=true` **plans** `(38,20)*(38,20)` as
`(38,6)` and succeeds. `allowPrecisionLoss=false` + ANSI raises at
`Decimal(38,38)` — not the corpus face. **Decision: compute-with-clamp**, not
refuse-with-Spark-class.

**Install:** `register_spark_decimal_planner` from `register_all` (SparkExtension
already calls it; `extension.rs` CLOSED) and from Spark-door `setup*` fixtures.

**Named residual:** DataFrame `col * col` of `(38,20)` columns does not go
through the SQL `ExprPlanner` and still refuses. Corpus is SQL.

### 0.3 DEC-6 — LAND (narrow +/− wrap)

S-1 §0.3: wrap lives in Arrow `decimal_op`; `(38,0)+(38,0)` has the same Spark
and Arrow type so U4a CAST-after does not fire. A8: new repark-functions home
reading the landed knob = LAND.

**Taken hook:** the same `SparkDecimalRewrite` replaces decimal `+`/`−` when
Spark `(p,s)` equals Arrow `(p,s)` **and** `p == 38` with
`__repark_spark_decimal_add__` / `_sub__`. Kernel uses `i256`, then:
- ANSI ON → `[NUMERIC_VALUE_OUT_OF_RANGE]` / ArithmeticException
- ANSI OFF → NULL

`*` identity `(38,0)*(38,0)` is **not** wrapped (would collide with the
non-null `mul_38_0_identity` pin / DEC-9). Photographed overflow is add.

CAST-after clamp nodes (`(38,18)+(38,18)` → `(38,17)`) are left on the U4a
path so their non-null literal pins stay.

### 0.4 TY-3 — DECLARE (wider seam)

Observed: `union(VALUES (1), VALUES (2.5))` is repark `decimal128(21,1)`
nullable vs Spark `(11,1)` non-null.

Spark UNION uses `forType(INT)=(10,0)`, not `fromLiteral` digits. Applying
digits-of-`1` would yield `(3,1)` — neither today nor Spark.

The honest hook is DataFusion `TypeCoercion` / `coerce_union` (Int64 →
`DECIMAL(20,0)`). Parse-time INT-vs-Int64 is a session-wide bomb (DEC-DESIGN
Q5=B, rejected). A UNION-only rewrite cannot tell `VALUES (1)` from a BIGINT
column without a plan-shape heuristic. **Not a `decimal_precision.rs` arm.**
Still DECLARED, dated 2026-08-14.

### 0.5 TPC-H

Queries are loaded from DuckDB `tpch_queries()` at runtime. Q8 / Q14 / Q17
contain `/` of `SUM(money)`. No SF1 re-run this unit.
`sf1_status_ledger.json` **untouched**.

### 0.6 Per-unit land-or-defer (critic attacks this table)

| Unit | Disposition | Hook | Evidence |
|---|---|---|---|
| **U4b** `/` | **LAND** | `SparkDecimalRewrite` + `__repark_spark_decimal_div__` | 4 G2 `/` rows equality; passthrough `(23,13)`; `7.0/2.0` `(8,6)`; `/0` types Spark `(38,6)` / `(8,6)` |
| **DEC-8** | **LAND** | `SparkDecimalExprPlanner::plan_binary_op` | `mul_38_20_*` equality `(38,6)` i128=`10^6` |
| **DEC-6** | **LAND** | checked `+`/`−` UDF, ANSI knob | overflow ON shared-raise; OFF NULL |
| **TY-3** | **DECLARE** | none tonight | `test_union_distinct.py` dated R-2 revisit |

NONE silently omitted.

### 0.7 Blast list

| Site | Disposition |
|---|---|
| G2 4 `/` rows | flipped to equality |
| G2 clamp / literal / add-mul equalities | untouched |
| G13 overflow ON | shared-raise (name kept) |
| G13 overflow OFF | equality NULL |
| G13 `/0` ON | stay shared-raise |
| G13 `/0` OFF | type leftover closed (Spark types) |
| G13 `mul_38_20_*` | equality (name kept) |
| G13 3 nullability | stay (DEC-9) |
| CTAS div | Spark `(23,13)` write-back; `spark_select` set |
| Rust `decimal.rs` | `/` i128, DEC-8 plans, DEC-6 raise/NULL |
| `test_sql_passthrough_parity.py` | division VALUE/type only; `/0` `% 0` notabool behavior kept |
| `test_columns.py` | A7 row only |
| `test_union_distinct.py` | TY-3 dated DECLARE |
| `analyzer.rs` unit `/` class pins | untouched (rule not installed there) |
| `cross_door.rs` | not owned; ANSI door keeps Arrow `/` (named residual) |
| `_live_parity.py` | CLOSED |
| `sf1_status_ledger.json` | untouched |

---

## 1. Engine change

- New `crates/repark-functions/src/decimal_spark.rs`.
- `lib.rs`: `pub mod decimal_spark`; A5 insert; `register_all` installs the planner.
- `decimal_precision.rs`: `pub(crate)` formula helpers + Spark `/` type; tests flipped.
- Spark-door `setup*` registers the planner (`extension.rs` closed).

`check_lib_rs.py` ceiling 175: `lib.rs` measured 173 — no bump.

---

## 2. Tests

Owned flips listed in §0.7. Shared-raise shape unchanged. G13 `repark_raises`
budget pin dropped (DEC-8 no longer refuses). CTAS "one spark_select is None"
dropped (last disclosure-path CTAS closed).

---

## 3. JVM lock

| Event | Marker | pid | TS | Action |
|---|---|---|---|---|
| acquire | `r2-record` | 3274009 | 2026-08-13T22:03:36-04:00 | noclobber create `/tmp/grok-jvm-record.lock` |
| record | `r2-record` | 3274009 | 2026-08-13 | PySpark 4.1.2; 37 spark halves, 0 mismatches; `RECORD_EC:0` |
| release | `r2-record` | 3274009 | 2026-08-13 | own-marker `rm` after `MARKER=r2-record` + `lane=r2-dec-close` |

No foreign marker rm. No leftover `pyspark`/`SparkSubmit`.

---

## 4. Gates

| Gate | EC | Notes |
|---|---|---|
| `make verify` | **0** | rust-file-size 198 clean; lib.rs 173 < 175 |
| `make py-test-facade` | **0** | 3053 passed, 71 skipped |
| `make audit` | **0** | cargo-audit / cargo-deny / pip-audit |
| `make workflows-lint` | **0** | zizmor + parse |
| `make preflight` surface | **0** | verify + facade + audit + wf lint |

---

## 5. Identity / hygiene

Per-command `user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer: `Authored-By: Grok (grok-4.5) <noreply@x.ai>`.

---

## 6. Paste-true registry handoff (R-7 / next landing increment)

Do **not** land from this PR.

### DEC-2 (division) — READY TO PASTE as FIXED

```
DEC-2 decimal division — FIXED (R-2 / U4b, 2026-08-14)
Spark `resultDecimalType` for `/` (`s=max(6,s1+p2+1)`, `p=p1-s1+s2+s`, then
adjustPrecisionScale) via `__repark_spark_decimal_div__`. CAST-after was
rejected (value is short). A5 slot `SparkDecimalRewrite` before
SparkExprSemantics; the UDF owns `/0` (raise TRUE / NULL false).
Pins: `test_decimal128_parity.py` `[div_same_precision_scale]` +
`[div_repeating_money]` + `[div_integer_scales]` + `[div_exact_half_type_only]`
(equality); Rust `pin_div_same_precision_scale_repark_i128` i128=2697368421053
at (23,13); `test_sql_passthrough_parity.py::test_decimal_division_stays_decimal`;
A7 `test_columns.py::test_sql_float_literal_division_is_decimal` `(8,6)`.
`%` resultDecimalType CLOSED. ANSI door keeps Arrow `/` (not owned).
```

### DEC-8 (plan-refuse) — READY TO PASTE as FIXED

```
DEC-8 (38,20)*(38,20) — FIXED (R-2, 2026-08-14)
`SparkDecimalExprPlanner::plan_binary_op` replaces Arrow-refusing `*` before
`BinaryExpr::get_type`. Spark default-true compute-with-clamp → (38,6)
`1.000000`. Pin: `[mul_38_20_plans_in_spark_refuses_in_repark]` (equality;
name kept) + `pin_mul_38_20_still_refuses_at_plan` (name kept; now plans).
DataFrame `col*col` of (38,20) columns still refuses (no SQL ExprPlanner).
```

### DEC-6 (ANSI overflow) — READY TO PASTE as FIXED (add/sub face)

```
DEC-6 ANSI overflow — FIXED on photographed add (R-2, 2026-08-14)
Checked `+`/`−` UDF when Spark==Arrow `(38,·)`. ANSI ON raises
`NUMERIC_VALUE_OUT_OF_RANGE`; ANSI OFF NULLs. Reads landed `SparkAnsiConfig`.
Pins: `[overflow_max_decimal38_plus_one_raises_in_spark]` shared-raise;
`[overflow_max_decimal38_plus_one_null_when_ansi_false]` both NULL;
`pin_overflow_max_decimal38_plus_one_wrong_value_i128` (name kept; now raise).
`*` of (38,0) not wrapped (DEC-9 / `mul_38_0_identity` non-null). CAST-after
clamp nodes stay on U4a. Mul overflow of max*max is a named residual.
```

### TY-3 — READY TO PASTE as still DECLARED

```
TY-3 UNION integer-literal typing — still DECLARED (R-2, 2026-08-14)
Observed repark decimal128(21,1) nullable vs Spark (11,1) non-null.
U3 fromLiteral is + − * only. UNION uses Spark forType(INT)=(10,0).
Honest hook is TypeCoercion/coerce_union (Int64→DECIMAL(20,0)), not a
decimal_precision arm. Parse-time INT is a session-wide bomb.
`test_union_inline_decimal_literal_diverges_from_spark` dated 2026-08-14.
```

---

## 7. Octo cycle ledger (sequential hats)

**Context break executed per hat; attacking artifacts, not memory.**
`early_stop=true`, floor S1, `claims_critic=true`. Cycle 1 only (CLEAN).

### Cycle 1 Half A

**Critic-1 (Quality).** ATTACKED: no prod unwrap; file 827 < 1500; lib.rs 173 < 175;
A5 insert never shuffle; tests same commit; maps lockstep; `/0` `% 0` notabool green.
Verdict: CLEAN.

**Critic-2 (Safety).** ATTACKED: no secrets/AWS/unsafe/lockfile/`.github`/`[patch]`;
own-marker lock rm only; overflow raise is fail-loud. Verdict: CLEAN.

**Critic-3 (Logic).** ATTACKED: U3 not on `/`; typed INT fence; integer `/` still
float64; DEC-8 only Arrow-refuse `*`; DEC-6 only same-type `(38,·)` `+`/`−`;
UNION not rewritten to `(3,1)`. Verdict: CLEAN.

**Critic-4 (Claims).** ATTACKED: TY-3 still DECLARED; `%` CLOSED; ANSI door `/`
residual; DataFrame `*` residual; registry/STATUS/`_live_parity.py` untouched;
record 0 mismatches. Verdict: CLEAN.

### Cycle 1 Half B

Empty OPEN queue ≥ S1. No fixes.

### Early stop

Required Critics CLEAN, verify 0, preflight 0 → skip cycles 2–4.

**Octo label:** `OCTO-CONVERGED` (Critic-stage only; not ship).
