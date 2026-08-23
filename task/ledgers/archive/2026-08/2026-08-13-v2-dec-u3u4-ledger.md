# Unit ledger — V-2 / DEC U3+U4a: integer-literal min-precision + SparkDecimalPrecision

**Unit:** V-2 · campaign DEC-8 (U3) + DEC-2/3/4 (U4a) · **Date:** 2026-08-13 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-v2` · **Branch:** `grok/v2-dec-u3u4` ·
**Base (FROZEN):** `8d325d4f47f46154bd954dc515d717434517fca5`
(`fix(tz4): localize zoneless LTZ inputs; distinguish TIMESTAMP_NTZ (#85)`)

**Charter:** `BRIEF-v2-dec-u3u4.md` + `DEC-DESIGN.md` §4 (U3, U4; “one seam, not nine”) +
BRIEF-y9 addendum + conductor-7 A1–A8. **SEPMO:** HIGH — octo + C4
(`claims_critic=true`, cycles=4, early_stop=true, floor S1). Sequential hat-switch
(spawn unavailable).

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` — V-5 owns the
registry. DEC-2/3/4/8 row texts are paste-true in §6.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | U3: integer **literal** × decimal is Spark `fromLiteral` `DECIMAL(digits,0)`. | PROVEN — `(12,2)` i128=750; `50 *` is `(13,2)`. |
| C-002 | Typed INT / BIGINT columns are not min-precision. | PROVEN — `(21,2)` / `(31,2)` pins. |
| C-003 | U4a add/sub/mul clamp matches Spark default-true. | PROVEN — three clamp rows equality; i128 pins. |
| C-004 | `/` is declared U4b with why-long-pole evidence. | PROVEN — §0.2; div pins unchanged; `test_columns.py` untouched. |
| C-005 | DEC-8 still refuses; AnalyzerRule cannot see it. | PROVEN — plan-construction pins. |
| C-006 | TY-3 still DECLARED at `(21,1)` nullable. | PROVEN — dated revisit in `test_union_distinct.py`. |
| C-007 | Insertion position is first, with a stated reason. | PROVEN — §0.1; `lib.rs` prepend. |
| C-008 | Registry / `_live_parity.py` / lockfiles / `extension.rs` / `analyzer.rs` untouched. | PROVEN — diff names. |
| C-009 | Record driver 0 mismatches; value AND type AND nullability. | PROVEN — 33 halves, RECORD_EC=0. |
| C-010 | `make verify` + `make preflight` exit 0. | PROVEN — §4.3. |

---

## 0. Blast + insertion position + `/` land-or-declare

### 0.1 Insertion position (A1 — order is semantic)

`analyzer_rules()` previously returned
`SparkExprSemantics` → cardinality → `instant_ts`. This unit **prepends**
`SparkDecimalPrecision` at index 0:

```
SparkDecimalPrecision → SparkExprSemantics → cardinality → instant_ts
```

**Reason (not “after SparkExprSemantics is free”):**

- All custom rules already run *after* DataFusion `TypeCoercion`. U3 therefore
  sees `CAST(<int lit> AS DECIMAL(forType))`, not a bare `Int64`.
- `SparkExprSemantics::rewrite_division` wraps every numeric divisor in
  `nullif(d, 0)`. A later `/` rewrite (U4b) that matches `BinaryExpr(Divide)`
  would miss `a / nullif(b, 0)` if we ran *after* that wrap.
- Putting DecimalPrecision **first** means U4b can rewrite a clean
  `decimal / decimal`; SparkExprSemantics still applies the `/0` guard on the
  result. Tonight U4a does not touch `/`, so the wrap is unchanged.
- Cardinality / `instant_ts` have no decimal-arithmetic interaction.

Rejected alternatives: last (U4b would pattern-match through `nullif`); after
SparkExprSemantics (same landmine, no benefit for `+ − *`).

### 0.2 `/` land-or-declare (A8 — silent omission forbidden)

**DECLARE U4b.** U4a (add/sub/mul clamp) is a complete honest PR.

Why `/` is the long pole (executed, not guessed):

1. **Different formula, not a clamp.** Spark
   `s = max(6, s1+p2+1)`, `p = p1-s1+s2+s` → `(10,2)/(10,2) = (23,13)`.
   Arrow/Postgres `s = s1+4` → `(16,6)`. Photographed:
   `0.2697368421053` vs `0.269736`.
2. **CAST-after is wrong for `/`.** The *value* is short, not only the type.
   `div_exact_half_type_only` agrees on 2.5 and still diverges on `(23,13)` vs
   `(16,6)`. A type-only CAST would green the schema and **wrong the repeating
   money** row.
3. **Needs a UDF or scaled i128 division** at Spark scale (quotients up to 38
   digits). Not a one-file CAST wrap.
4. **Blast:** 4 G2 div rows + `ctas_div_preserves_repark_result_type` + Rust
   `pin_div_same_precision_scale_repark_i128` + `test_columns.py`
   `SELECT 7.0 / 2.0` (`decimal128(7,5)` after U2). `test_columns.py` is
   **not** flipped tonight.
5. **DEC-7 `/0` result type** would move with the formula (Arrow `(38,4)` vs
   Spark’s division type). DEC-7 is conductor-8 / U5.
6. **Cross-door `/` row** is required by the memo for U4b;
   `crates/repark-sql/tests/cross_door.rs` is **not** in this lane’s file
   ownership.
7. **`%` CLOSED** (UNPROBED-THIS-PASS, DEC-DESIGN `:290`).

### 0.3 DEC-8 plan-refuse — AnalyzerRule cannot see it

`(38,20)*(38,20)` fails in `Projection::try_new` → `BinaryExpr::get_type` →
Arrow `decimal_op` (`s1+s2 > 38`) **before any `AnalyzerRule` runs**.
`ctx.sql(...)` never produces a plan for this rule to rewrite. Pin:
`mul_38_20_still_refuses_before_any_analyzer_rule` (functions) +
`pin_mul_38_20_still_refuses_at_plan` (spark door). Closing DEC-8 needs an
`ExprPlanner` (U4b-adjacent), not this rule. **Declared**, not silently
claimed FIXED.

### 0.4 Blast sweep (enumerated)

| Surface | Outcome |
|---|---|
| G2 equality `+ − *` (`add_same_precision_scale`, `sub_same_precision_scale`, `mul_*`, `add_carry_*`, `null_plus_money`, `mul_negative_money`, `mul_38_0_identity`, `sum_two_money`, `cast_decimal_identity`) | **untouched** — unbounded formulas already match; CAST-after not applied |
| G2 clamp `mul_38_10`, `add_38_18`, `add_38_10` | **flipped to equality** (`repark=None`); names kept for the name-gated family pin |
| G2 `int_times_decimal_promotes_wider_in_repark` | **width closed** `(31,2)→(12,2)`; **nullability still diverges** (Spark nullable, repark non-null) — stays DISCLOSURE (DEC-9 / U5) |
| G2 4 division rows | **stay disclosed** (U4b) |
| G2 3 literal rows + avg | **stay equality** (U1/U2) |
| G13 overflow + 2 `/0` | **stay** (DEC-6/7 / U5; overflow class = named morning deferral) |
| G13 `mul_38_20_*` | **stay repark_raises** (0.3) |
| G13 3 nullability rows | **stay** (DEC-9 / U5) |
| CTAS add/mul | **unchanged** equality write-back |
| CTAS div | **unchanged** repark `(16,6)` (U4b) |
| Rust `decimal.rs` | clamp + INT×DECIMAL pins flipped; DEC-8 refuse pin added; div pin stays `(16,6)` |
| TY-3 `test_union_distinct.py` | **still DECLARED** `(21,1)` nullable. U3 `fromLiteral` is `+ − *` only. UNION uses Spark `forType(INT)=(10,0)`, not digits. Applying fromLiteral would yield `(3,1)` — neither today nor Spark. Dated 2026-08-13. |
| `test_columns.py` `7.0/2.0` | **not edited** (`/` did not ship) |
| group-agg / window rows chaining arithmetic | swept: `test_group_agg.py` is integer; `test_window_parity.py` has no `decimal128`. No flips. |
| TPC-H `sf1_status_ledger.json` | **not flipped** (no re-run this unit). Q1 remains the Z-3 avg-type DuckDB mismatch. `1 - discount` **values** are unchanged; type of `1` as `DECIMAL(1,0)` vs `(20,0)` is schema-only and DuckDB-diff is value-tolerant. |
| `_live_parity.py` | **CLOSED**. Forced live reds = named deferrals; none taken (file untouched). |

Overflow class (DEC-6 wrap of `10^38` at declared `(38,0)`) = **named morning deferral**.
DEC-6/7 semantics stay OUT. DEC-9 / ANSI knob / U5 CLOSED (conductor-8).
`aggregate.rs` **not touched** (lowering did not force it).

---

## 1. Decisions

**D-V2-1 — One seam, two labels.** U3 and U4a share `SparkDecimalPrecision` in
`crates/repark-functions/src/decimal_precision.rs`. One PR, both labeled.

**D-V2-2 — U3 is `+ − *` only.** Applying `fromLiteral` to `/` without Spark’s
division formula would invent a third `(p,s)` (e.g. `5 / DECIMAL(10,2)`:
Arrow `(26,4)` → `(7,4)`, Spark `(14,11)`). That is a silent wrong.

**D-V2-3 — Typed INT columns are a fence.** `CAST(5 AS INT) * DECIMAL(10,2)`
stays `(21,2)`. `CAST(5 AS BIGINT) * …` stays `(31,2)`. `VALUES (CAST(5 AS INT))`
column `*` stays `(21,2)`. Mutation-proof pins in the new file + spark door.

**D-V2-4 — CAST-after, not operand-scale.** Operand-scale on add
`(38,18)+(38,18)` produced Spark `(38,17)` on pass 1; pass 2 recomputed Spark
on `(38,17)+(38,17)` → `(38,16)` and dropped another digit (`2e17` → `2e16`).
CAST-after + `transform_down` Stop on an already-correct wrapper is
idempotent. Pin: `add_clamp_value_survives_a_second_analyze`.

**D-V2-5 — DEC-8 declared (ExprPlanner).** See §0.3.

**D-V2-6 — TY-3 still DECLARED.** See §0.4. Residual is set-op `forType(INT)`,
not U3 `fromLiteral`.

**D-V2-7 — INT×DECIMAL stays a disclosure** (nullability-only after U3).
Flipping to equality would lie about DEC-9.

**D-V2-8 — Registry paste-true only.** V-5 owns the file.

---

## 2. Files

| Path | Role |
|---|---|
| `crates/repark-functions/src/decimal_precision.rs` | **NEW** rule + unit pins |
| `crates/repark-functions/src/lib.rs` | `mod` + `analyzer_rules()` prepend |
| `crates/repark-spark/src/tests/decimal.rs` | i128 pin flips + DEC-8 refuse + typed-INT fence |
| `python/repark/tests/test_decimal128_parity.py` | 3 clamp → equality; INT×DECIMAL repark half `(12,2)`; budget pin comment |
| `python/repark/tests/test_union_distinct.py` | TY-3 dated U3 revisit |
| maps (`repark-functions{,/src}`, `repark-spark/src/tests`, `python/repark/tests`, `task/`) | lockstep |
| this ledger | |

**Not edited:** `analyzer.rs` / `SparkExprSemantics` / `extension.rs` /
`timestamp_cast.rs` / `_live_parity.py` / registry / STATUS / DEVELOPMENT.md /
`predicate_dml.rs` / `spark_ast.rs` / `aggregate.rs` / `test_columns.py` /
`sf1_status_ledger.json` / lockfiles / `.github/`.

---

## 3. Deviations / residuals

- **U4b `/`** — declared, §0.2.
- **DEC-8 plan-refuse** — declared, §0.3 (ExprPlanner).
- **TY-3** — still DECLARED `(21,1)` nullable.
- **INT×DECIMAL nullability** — DEC-9 / U5.
- **DEC-6/7** — morning / conductor-8.
- **`%`** — UNPROBED-THIS-PASS.
- **Explicit `CAST(5 AS DECIMAL(20,0)) * decimal`** — indistinguishable from
  TypeCoercion’s implicit CAST; would receive fromLiteral. Named limitation.

---

## 4. Gate evidence

### 4.1 JVM lock

Waited for `/tmp/grok-v1-first-released` (`26` bytes, present 16:36). No lock
when first checked after the sentinel; V-1 SparkSubmit was earlier (EXISTS
record). Acquired:

```
MARKER=v2-record
PID=241826
ISO=2026-08-13T16:56:45-04:00
lane=v2-dec-u3u4
```

First create was an empty file (`set -C` `: >` then noclobber blocked the
write); marker written immediately into the owned empty file. No foreign
marker overwritten. Record then **own-marker `rm`** after `RECORD_EC:0`.
No stale-rm of anyone else’s lock. No local `pyspark`/`SparkSubmit` at
acquire or release (HiveThrift ignored).

Record:

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_decimal128_goldens.py
```

PySpark **4.1.2**. `record mode: 33 spark halves re-derived, 0 mismatch(es)`.
`RECORD_EC:0`. Clamp / INT×DECIMAL / DEC-8 Spark halves unchanged (as designed).

### 4.2 Targeted (pre-lock / post-lock)

```
cargo test -p repark-functions --lib decimal_precision   # 18 ok
cargo test -p repark-spark --lib tests::decimal          # 13 ok
cargo clippy -p repark-functions -- -D warnings          # ok
```

### 4.3 Gates

| Gate | EC | Notes |
|---|---|---|
| `make verify` | **0** | rust-file-size 196 clean; lib.rs at ceiling 166 |
| `make preflight` | **0** | facade `2980 passed, 71 skipped, 37 warnings` in 113.80s; cargo-deny / pip-audit / zizmor clean |

TPC-H SF1 DuckDB-diff: **not re-run**; ledger JSON not flipped.

---

## 5. Octo (sequential hats)

Context-break executed per hat; spawn unavailable. See §7 after Critics.

---

## 6. Registry rows — READY TO PASTE, **not** landed (V-5)

Paste-true texts for DEC-2 / DEC-3 / DEC-4 / DEC-8 only (charter). DEC-4 is
already FIXED (U1 / #76) — restated so V-5 does not regress it. DEC-5 width
is a U3 side-effect; included because the photographed row moved.

### DEC-2 — still BACKLOG (U4b)

- **repark** — `CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2))` yields
  `decimal128(16,6)` nullable `0.269736`; repeating `(10.00)/(3.00)` → `(16,6)`
  `3.333333`; integer-scale `1/3` at `(10,0)` → `(14,4)` `0.3333`; exact half
  `5.00/2.00` keeps value `2.5` at `(16,6)`.
- **Apache Spark** — `(10,2)/(10,2)` → `decimal128(23,13)` nullable
  `0.2697368421053`; repeating stays `(23,13)`; integer-scale `(21,11)`
  `0.33333333333`; exact half `(23,13)` `2.5000000000000`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[div_same_precision_scale]`
  + `[div_repeating_money]` + `[div_integer_scales]` + `[div_exact_half_type_only]`
  + Rust `pin_div_same_precision_scale_repark_i128`.
- **Rationale** — BACKLOG, intent to FIX. V-2 U4a declared `/` as **U4b**:
  CAST-after wrongs the value; needs a UDF / scaled division. `%` UNPROBED.

### DEC-3 — FIXED (V-2 U4a, add/sub/mul clamp)

- **repark** — Spark `adjustPrecisionScale` (`allowPrecisionLoss=true`) via
  `SparkDecimalPrecision` CAST-after: `(38,10)*(38,10)` → `decimal128(38,6)`
  `1.000000`; `(38,18)+(38,18)` → `(38,17)` `2.00000000000000000`;
  `(38,10)+(38,10)` → `(38,9)` `2.000000000`.
- **Apache Spark** — the same three result types and values. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[mul_38_10_clamps_scale_in_spark]`
  (equality; name kept) + `[add_38_18_clamps_scale_in_spark]` +
  `[add_38_10_clamps_scale_in_spark]` + Rust `pin_mul_38_10_clamps_to_38_6_i128`
  + `pin_add_38_18_clamps_to_38_17_i128` + name-gated budget
  `test_decimal128_row_set_covers_gap_budgets` (≥3 `*clamps_scale_in_spark`).
- **Rationale** — campaign DEC-2/3 / V-2 U4a. A fixed defect gets this dated
  note. Registry DEC-8 (plan-refuse) is a **different altitude** and stays
  BACKLOG.

### DEC-4 — already FIXED (do not regress)

- Keep the landed Z-3 / #76 FIXED note. V-2 did not touch `aggregate.rs`.

### DEC-5 — width FIXED; nullability still BACKLOG (DEC-9)

- **repark** — `5 * CAST(1.50 AS DECIMAL(10,2))` yields `decimal128(12,2)`
  **non-null** `7.50` (U3 `fromLiteral`: `5` → `DECIMAL(1,0)`). Typed
  `CAST(5 AS INT) * …` stays `(21,2)`.
- **Apache Spark** — `decimal128(12,2)` **nullable** `7.50`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[int_times_decimal_promotes_wider_in_repark]`
  (still a disclosure — nullability) + Rust `pin_int_times_decimal_is_12_2_i128`
  + `pin_cast_int_times_decimal_stays_21_2_i128`.
- **Rationale** — U3 closed the **width**. Nullability is registry DEC-9 / U5.
  Do not mark the row FIXED until both faces close, or split the row.

### DEC-8 — still BACKLOG (ExprPlanner, not this AnalyzerRule)

- **repark** — still refuses with `AnalysisException` (`Cannot get result type
  for decimal operation … 38,20 * 38,20`) at **plan construction**
  (`BinaryExpr::get_type` / Arrow `s>38`) before any `AnalyzerRule` runs.
- **Apache Spark** — clamps to `decimal128(38,6)` and succeeds. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[mul_38_20_plans_in_spark_refuses_in_repark]`
  + Rust `pin_mul_38_20_still_refuses_at_plan` +
  `repark_functions::decimal_precision::tests::mul_38_20_still_refuses_before_any_analyzer_rule`.
- **Rationale** — BACKLOG, intent to FIX via an **ExprPlanner** (U4b-adjacent).
  V-2 U4a’s analyzer cannot see a node that never plans. Do not fold this into
  DEC-3’s FIXED note.

### TY-3 — still DECLARED (dated U3)

Dated 2026-08-13 (U3): observed repark `decimal128(21,1)` nullable
`Decimal('1.0')`/`Decimal('2.5')` vs Spark `decimal128(11,1)` non-null.
U3 `fromLiteral` does not apply to UNION set-op widening (Spark uses
`forType(INT)=(10,0)`, not digits). Residual is INT-literal-as-INT, not
min-precision arithmetic.

---

## 7. Octo cycle ledger (sequential hats; spawn unavailable)

**Context break executed per hat; attacking artifacts, not memory.**
`early_stop=true`, floor S1, `claims_critic=true`. Cycle 1 only (CLEAN).

### Cycle 1 Half A

**Critic-1 (Quality / crates / tests).** ATTACKED: `thiserror` N/A (DataFusion
`Result`); no prod `unwrap`/`expect`; no `as` truncating casts (`try_from` /
`bounded_*`); file 731 < 1500; `lib.rs` 166 = ceiling; mutation-proof pins
(fromLiteral digits, typed INT fence, clamp i128, DEC-8 refuse, second-analyze
value, `/` not rewritten, int `/` still float64). Coverage skeptic: reverting
CAST-after Stop re-breaks `add_clamp_value_survives_a_second_analyze`.
Verdict: CLEAN.

**Critic-2 (Safety).** ATTACKED: no secrets, no `unsafe`, no AWS, lock
own-marker rm only, overflow wrap is a named leftover (DEC-6). Partial-failure:
a second analyze no longer silently drops another scale digit (the defect this
unit found in operand-scale). Verdict: CLEAN.

**Critic-3 (Logic).** ATTACKED: U3 does not shrink typed INT columns; U3 does
not retarget `/`; UNION not rewritten (would yield `(3,1)`); CAST-after
idempotent under `transform_down` Stop; DEC-8 still refuses at `get_type`;
INT×DECIMAL stays a nullability disclosure. Verdict: CLEAN.

**Critic-4 (Claims).** ATTACKED: §0 has insertion-position reason and `/`
land-or-declare; TY-3 still DECLARED with observed `(21,1)`; DEC-8 not claimed
FIXED; registry file untouched; `_live_parity.py` untouched; `test_columns.py`
untouched; `sf1_status_ledger.json` untouched; `%ae` checked at commit.
Verdict: CLEAN (identity re-checked after commit).

### Cycle 1 Half B

Empty OPEN queue ≥ S1. No fixes.

### Early stop

Required Critics CLEAN, verify 0, preflight 0 → skip cycles 2–4.

**Octo label:** `OCTO-CONVERGED` (Critic-stage only; not ship).
**SEPMO:** `SEPMO-UNIT-READY` after readiness (gates green, ledger PROVEN,
charter_trace complete).
