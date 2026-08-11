# Unit ledger — G-7: decimal128 differential corpus (Python half; G13 folded)

**Unit:** G-7 / N-1 of the overnight conductor slate
([../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md) gaps G2 + G13) ·
**Date:** 2026-08-10 · **Worktree:** `/tmp/grok-n1` · **Branch:** `grok/g7-decimal-corpus`

**This ledger covers the Python half ONLY.** The 8–10 Rust bit-exact `Decimal128` fixture pins and
the 2 cross-door rows are **G-7b**, deferred until after G-4 merges (they collide with the
`repark-spark/src/tests.rs` ban / the ANSI door). That deviation from G2's full pin budget is
recorded here, not silent.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | `python/repark/tests/test_decimal128_parity.py` | 24 G2 + 7 G13 rows + budget pin + 3 CTAS |
| Record driver | `python/repark/tests/_record_decimal128_goldens.py` | re-derives every Spark half from live 4.1.2 |
| Tests map | `python/repark/tests/map.md` | lockstep navigation + debug |
| This ledger | `task/g7-decimal-ledger.md` | linked from `task/map.md` |

### 1.1 Budget (met)

| Bucket | Budget | Landed |
|---|---|---|
| G2 differential rows | 20–26 | **24** (12 equality + 12 disclosure) |
| G13 overflow rows | 6–8 | **7** (all disclosures: raise-class / nullability) |
| CTAS write-back | 3 | **3** (2 equality-path + 1 disclosure-path type preservation) |
| Control equalities (min) | ≥2 (brief); pin ≥8 | **12** |
| Disclosure ceiling (pin) | max 20 | **19** |
| Rust bit-exact pins | 8–10 | **0 — deferred G-7b** |
| Cross-door rows | 2 | **0 — deferred G-7b** |

### 1.2 G2 row inventory

**Equalities (12)** — repark == Spark today on value AND exact `decimal128(p,s)`:

1. `cast_decimal_identity` — `CAST(1.23 AS DECIMAL(10,2))` → `(10,2)`
2. `add_same_precision_scale` — `(10,2)+(10,2)` → `(11,2)` = 5.79
3. `sub_same_precision_scale` — `(10,2)-(10,2)` → `(11,2)` = -3.33
4. `mul_same_precision_scale` — `(10,2)*(10,2)` → `(21,4)` = 5.6088
5. `add_carry_widens_precision` — `(4,2)+(4,2)` → `(5,2)` = 100.00
6. `mul_mixed_scales` — `(5,1)*(5,2)` → `(11,3)` = 4.140
7. `mul_money_by_quantity` — `(10,2)*(10,0)` → `(21,2)` = 59.97
8. `mul_money_by_tax_rate` — `(10,2)*(6,4)` → `(17,6)` = 8.250000
9. `mul_38_0_identity` — `(38,0)*(38,0)` → `(38,0)` = 1
10. `sum_two_money_values` — `sum(DECIMAL(10,2))` → `(20,2)` nullable = 3.30
11. `null_plus_money_propagates` — `NULL + money` → `(11,2)` nullable NULL
12. `mul_negative_money` — signed mul keeps `(21,4)`

**Disclosures (12)** — pin BOTH halves; CONVERGED-flip-don't-delete:

| Name | Class | Spark | repark |
|---|---|---|---|
| `literal_1_23_…` | literal inference | `decimal128(3,2)` | `float64` |
| `literal_0_1_…` | literal inference | `decimal128(1,1)` | `float64` |
| `literal_123_456_…` | literal inference | `decimal128(6,3)` | `float64` |
| `div_same_precision_scale` | division `(p,s)` | `(23,13)` 0.2697368421053 | `(16,6)` 0.269736 |
| `div_repeating_money` | division | `(23,13)` 3.3333333333333 | `(16,6)` 3.333333 |
| `div_integer_scales` | division | `(21,11)` 0.33333333333 | `(14,4)` 0.3333 |
| `div_exact_half_type_only` | division type-only | `(23,13)` 2.5… | `(16,6)` 2.500000 |
| `mul_38_10_clamps_scale_in_spark` | 38-digit clamp | `(38,6)` | `(38,20)` |
| `add_38_18_clamps_scale_in_spark` | 38-digit clamp | `(38,17)` | `(38,18)` |
| `add_38_10_clamps_scale_in_spark` | 38-digit clamp | `(38,9)` | `(38,10)` |
| `avg_money_stays_decimal_…` | avg type | `(14,6)` 1.650000 | `float64` 1.6500000000000001 |
| `int_times_decimal_promotes_…` | int×decimal width | `(12,2)` nullable | `(31,2)` non-null |

### 1.3 G13 row inventory (7)

| Name | Class | Spark | repark |
|---|---|---|---|
| `overflow_max_decimal38_plus_one_…` | ANSI overflow | **raises** `ArithmeticException` | wrong `decimal128(38,0)` value |
| `div_by_zero_decimal38_…` | ANSI /0 | **raises** | NULL at `(38,4)` |
| `div_by_zero_small_decimal_…` | ANSI /0 | **raises** | NULL at `(6,4)` |
| `mul_38_20_plans_in_spark_refuses_…` | high-scale mul | `(38,6)` success | **raises** `AnalysisException` |
| `mul_single_digit_nullability_…` | nullability | `(3,0)` nullable 81 | `(3,0)` non-null 81 |
| `add_single_digit_nullability_…` | nullability | `(2,0)` nullable 18 | `(2,0)` non-null 18 |
| `mul_three_digit_capacity_…` | nullability | `(7,0)` nullable 998001 | `(7,0)` non-null 998001 |

### 1.4 CTAS write-back (Q1 ruling: repark-only)

1. `ctas_add_money_preserves_decimal128` — equality SELECT → Iceberg → read `(11,2)` 5.79
2. `ctas_mul_money_qty_preserves_decimal128` — equality SELECT → `(21,2)` 59.97
3. `ctas_div_preserves_repark_result_type` — disclosure SELECT; repark's `(16,6)` survives write+read
   (Spark not required equal on SELECT half)

### 1.5 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_decimal128_goldens.py
```

Captured: **`record mode: 33 spark halves re-derived, 0 mismatch(es)`** (31 differential + 2 CTAS
SELECT oracles; the third CTAS has no Spark SELECT half by Q1 design).

---

## 2. Decisions

**D-G7-1 — Template is the timezone unit.** Same idiom: rows as data, disclosure runner with
CONVERGED-vs-regression classification, control equalities, budget pin, committed record driver
importing the same `ROWS`.

**D-G7-2 — Raise-class rows extend the row dataclass.** `spark_raises` / `repark_raises` hold the
exception class-name substring; the raising side's table is `None`. Record mode re-checks the
raise; the suite classifies a new shared raise as CONVERGED.

**D-G7-3 — CTAS = repark-only (Q1 locked).** Spark is the SELECT oracle when equality holds; no
Iceberg-on-Spark provisioned.

**D-G7-4 — G-7b deferral (Rust + cross-door).** Explicit, ledgered. Not a silent budget miss.

**D-G7-5 — Registry rows stay in this ledger** (conductor ban on editing
`docs/spark-sql-iceberg-parity.md`). Ready-to-paste text in §6.

**D-G7-6 — No engine production source edits.** Python tests + docs/ledgers only.

---

## 3. Gate evidence

### 3.1 Record mode (under `gate.sh N1`)

```
record mode: 33 spark halves re-derived, 0 mismatch(es)
```

### 3.2 Facade unit suite

```
python/repark/tests/test_decimal128_parity.py — 35 passed in 0.72s
```

(31 differential + 1 budget + 3 CTAS)

### 3.3 `make ci`

```
crate-dag: 20 internal edges clean …
lib-rs: 9 crate roots clean …
lib-py: 54 files clean …
manifest: 12 components … agree …
uvx ruff@0.15.22 check .  → All checks passed!
uvx ruff@0.15.22 format --check . → 243 files already formatted
uv lock --locked → Resolved 29 packages
uvx typos@1.47.2 → (clean)
gate.sh N1 RELEASE ec=0
```

### 3.4 Facade unit (true counts)

```
python/repark/tests/test_decimal128_parity.py — 35 passed in 0.71s
(31 differential + 1 budget + 3 CTAS)
```

---

## 4. Provocations (octo / acceptance)

### 4.1–4.3 Verbatim provocation output (2026-08-10)

```
=== P1: equality golden value corrupted ===
RED as expected: FrameMismatchError: value mismatch at row 0:

=== P2: disclosure halves made identical (well-formedness) ===
RED as expected: AssertionError: div_repeating_money: the row's two recorded halves are
IDENTICAL, so it is not a disclosure at all - either it converged and was half-edited, or
the Spark half was pasted over the repark half. Flip it to an equality row (repark=None)
or re-record it. …

=== P3: disclosure repark pin wrong (regression classification) ===
RED as expected: AssertionError: div_repeating_money: repark moved OFF its pinned
disclosure and does NOT match the recorded Spark golden either - this is a regression,
not a convergence. Re-derive both halves in record mode …

=== P4: budget pin provoked (too few equalities) ===
live equalities=12 (>= 8)
live disclosures=19 (<= 20)
BUDGET RED as expected: at least 8 control equality rows required so the corpus cannot
degenerate to all-disclosures; got 2

=== P5: CONVERGED classification (actual == spark, repark pin stale) ===
RED as expected: AssertionError: synthetic_convergence_probe: repark and Spark have
CONVERGED - repark now produces the RECORDED SPARK output, so this disclosure is stale.
Do not delete the row: flip it to an equality row (repark=None) and record the
convergence. probe

All provocations done.
```

---

## 5. Deviations from brief

1. **G-7b deferred** (Rust 8–10 bit-exact pins + 2 cross-door) — slate's own split; reason: collide
   with G-4 `repark-spark/src/tests.rs` ban and ANSI door. Not silent.
2. **No engine production changes** — corpus-only unit by design.
3. **Registry file not edited** — ready-to-paste rows in §6 only (conductor ban).

---

## 6. Ready-to-paste divergence-registry rows

> Conductor ban: do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit.
> A later registry sweep owns the file. Rows below are in the registry's **paste-true** bullet
> template (`- **repark**` / `- **Apache Spark**` / `- **Pin**` / `- **Rationale**`) with fully
> resolvable `path::test[case]` node ids so a sweep can drop them into §7 BACKLOG without rewrite.

Pin node-id pattern (parametrized corpus):
`python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[<row.name>]`.

Oracle basis for every Spark half below: **recorded** — goldens re-derivable in-repo via
`python/repark/tests/_record_decimal128_goldens.py` against live PySpark 4.1.2 (ANSI on).

### G2-LIT-1 — bare decimal literal `1.23` infers double

- **repark** — `SELECT 1.23 AS v` yields Arrow `float64` non-null with value `1.23`.
- **Apache Spark** — yields `decimal128(3,2)` non-null with `Decimal('1.23')`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[literal_1_23_infers_decimal_in_spark_double_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G2). A money column written from a bare literal is
  the wrong Arrow type; the pin holds both halves so a silent convergence reds with
  CONVERGED / flip-don't-delete.

### G2-LIT-2 — bare decimal literal `0.1` infers double

- **repark** — `SELECT 0.1 AS v` yields Arrow `float64` non-null with value `0.1`.
- **Apache Spark** — yields `decimal128(1,1)` non-null with `Decimal('0.1')`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[literal_0_1_infers_decimal_in_spark_double_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G2). The classic binary-float landmine as a type
  divergence; same flip discipline as G2-LIT-1.

### G2-LIT-3 — bare decimal literal `123.456` infers double

- **repark** — `SELECT 123.456 AS v` yields Arrow `float64` non-null with value `123.456`.
- **Apache Spark** — yields `decimal128(6,3)` non-null with `Decimal('123.456')`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[literal_123_456_infers_decimal_in_spark_double_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G2). Three fractional digits of literal inference;
  same class as G2-LIT-1/2.

### G2-DIV-1 — `DECIMAL / DECIMAL` result precision and scale

- **repark** — `CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2))` yields
  `decimal128(16,6)` nullable with rounded value `0.269736`. The same narrower result type holds
  for the sibling recipes: repeating money division `(10.00)/(3.00)` → `(16,6)` `3.333333`;
  integer-scale division `1/3` at `(10,0)` → `(14,4)` `0.3333`; exact half `5.00/2.00` keeps value
  `2.5` but still lands `(16,6)` instead of Spark's `(23,13)`.
- **Apache Spark** — same `(10,2)/(10,2)` recipe yields `decimal128(23,13)` nullable
  `0.2697368421053`; repeating money stays `(23,13)` `3.3333333333333`; integer-scale lands
  `(21,11)` `0.33333333333`; exact half is `(23,13)` `2.5000000000000`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[div_same_precision_scale]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[div_repeating_money]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[div_integer_scales]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[div_exact_half_type_only]`
- **Rationale** — BACKLOG, intent to FIX (gap G2 — Spark-compatible division precision/scale
  rules). Value AND type diverge on three rows; the fourth is type-only. A unit-price split is
  silently short under repark's six fractional digits.

### G2-CLAMP-1 — 38-digit result-type clamp on mul/add

- **repark** — keeps `s1+s2` / `max(s)` without Spark's p≤38 scale clamp:
  `(38,10)*(38,10)` → `decimal128(38,20)`; `(38,18)+(38,18)` → `decimal128(38,18)`;
  `(38,10)+(38,10)` → `decimal128(38,10)`.
- **Apache Spark** — reduces scale to keep precision ≤38:
  `(38,10)*(38,10)` → `decimal128(38,6)`; `(38,18)+(38,18)` → `decimal128(38,17)`;
  `(38,10)+(38,10)` → `decimal128(38,9)`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[mul_38_10_clamps_scale_in_spark]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[add_38_18_clamps_scale_in_spark]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[add_38_10_clamps_scale_in_spark]`
  (coverage also pinned by `test_decimal128_row_set_covers_gap_budgets`, which requires ≥3 rows
  named `*clamps_scale_in_spark` so a `DECIMAL(38,…)` equality control alone cannot green the pin)
- **Rationale** — BACKLOG, intent to FIX (gap G2 — 38-digit result-type clamp matching Spark). A
  high-scale product is the wrong width under repark.

### G2-AVG-1 — `avg(DECIMAL)` result type

- **repark** — `avg` over `DECIMAL(10,2)` yields Arrow `float64` nullable with binary residue
  `1.6500000000000001`.
- **Apache Spark** — keeps `decimal128(14,6)` nullable with exact `Decimal('1.650000')`.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[avg_money_stays_decimal_in_spark_double_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G2 — keep avg of decimal as decimal). An average
  unit price is not money-safe under repark's float64 promotion.

### G2-PROM-1 — `INT * DECIMAL` result width and nullability

- **repark** — `5 * CAST(1.50 AS DECIMAL(10,2))` yields `decimal128(31,2)` **non-null** with value
  `7.50`.
- **Apache Spark** — yields `decimal128(12,2)` **nullable** with the same value `7.50`.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[int_times_decimal_promotes_wider_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G2). Value agrees; precision width and nullability
  are a schema-level money divergence.

### G13-OVF-1 — max `DECIMAL(38,0) + 1` under ANSI

- **repark** — returns a corrupted `decimal128(38,0)` value (no raise) for
  `CAST(999…9 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))`.
- **Apache Spark** — under ANSI raises `ArithmeticException` /
  `NUMERIC_VALUE_OUT_OF_RANGE`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[overflow_max_decimal38_plus_one_raises_in_spark]`
- **Rationale** — BACKLOG, intent to FIX (gap G13 — ANSI overflow raise or honest NULL under
  non-ANSI). Silently-wrong-result class on the integrity path.

### G13-DIV0-1 — `DECIMAL / 0` under ANSI

- **repark** — returns NULL at a decimal result type: `(38,0)/(38,0)` → NULL at
  `decimal128(38,4)`; small `(2,0)/(2,0)` → NULL at `decimal128(6,4)`.
- **Apache Spark** — under ANSI raises `ArithmeticException` / `DIVIDE_BY_ZERO` for both recipes.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[div_by_zero_decimal38_raises_in_spark_null_in_repark]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[div_by_zero_small_decimal_raises_in_spark_null_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G13). NULL-vs-raise is an integrity divergence for
  any consumer that distinguishes error from missing.

### G13-PLAN-1 — `DECIMAL(38,20) * DECIMAL(38,20)` plan refuse

- **repark** — refuses at plan time with `AnalysisException` (`Cannot get result type for decimal
  operation … 38,20 * 38,20`).
- **Apache Spark** — clamps the product to `decimal128(38,6)` and succeeds. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[mul_38_20_plans_in_spark_refuses_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G13 / folds into G2's 38-digit clamp follow-on). A
  high-scale multiply that Spark accepts is a hard plan failure here.

### G13-NULL-1 — overflow-capable binary-arithmetic nullability

- **repark** — marks small mul/add results **non-null** while values and `(p,s)` agree with Spark:
  `9*9` → `(3,0)` non-null `81`; `9+9` → `(2,0)` non-null `18`; `999*999` → `(7,0)` non-null
  `998001`.
- **Apache Spark** — marks the same results **nullable** (overflow-capable binary arithmetic)
  at the same types and values. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[mul_single_digit_nullability_differs]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[add_single_digit_nullability_differs]`,
  `…::test_decimal128_row_matches_spark_or_still_diverges[mul_three_digit_capacity_nullability_differs]`
- **Rationale** — BACKLOG, intent to FIX (gap G13). Nullability-only pin; a schema-sensitive
  consumer that trusts non-null is wrong under repark's marking.

---

## 7. Octo cycle ledger

| Cycle | Half A OPEN ≥floor | Half B | Verify | Notes |
|---|---|---|---|---|
| 1 | 1 (CTAS `spark_select` unenforced in pytest) | remediated | 35 pass | + CTAS shape budget pins |
| 2 | 0 | empty | 35 pass | early stop |

**Label:** `OCTO-CONVERGED` (early_stop after CLEAN cycle-2). Scratch:
`/tmp/overnight-conductor/n1/octo/`.

---

## 8. Critic-overload summary

| Wave | Role | OPEN ≥floor | Result |
|---|---|---|---|
| 1 | CCC-α | 1 (raise-flag row shape unenforced) | filed W1-Q-001 |
| 2 | Actor-A/B | remediated | well-formedness asserts in budget test |
| 3 | CCC-β | 0 | early stop |
| 4–5 | skipped | — | early_stop=true |

**Label:** `OVERLOAD-CONVERGED`. Scratch: `/tmp/overnight-conductor/n1/overload/`.
**Anti-groupthink:** single-session exclusive hat-switch (degraded independence noted).

---

## 9. G-7b reserved

- 8–10 Rust bit-exact `Decimal128` fixture pins (need a Rust test surface post G-4 split)
- 2 cross-door rows (native DataFrame / ANSI door)

---

## 10. Clause pins (acceptance)

| Clause | Status |
|---|---|
| 20–26 G2 rows value+type on Arrow path | PROVEN (24) |
| 6–8 G13 rows | PROVEN (7) |
| ≥2 control equalities + budget pin | PROVEN (12 eq, max-disc pin) |
| 3 CTAS write-back | PROVEN |
| Committed record driver; N rows, 0 mismatches | PROVEN (33, 0) |
| Disclosure CONVERGED-flip-don't-delete | PROVEN (runner + provocation) |
| map.md lockstep | PROVEN |
| Ledger linked from task/map.md | PROVEN |
| Ready-to-paste registry rows in ledger only | PROVEN (§6) |
| G-7b deferral declared | PROVEN (§5, §9) |
| No banned paths touched | PROVEN |

---

## 11. Fix-round (PR #42 ACCEPT-WITH-NITS) — 2026-08-11

Orchestrator must-fix from the PR comment + MORNING-FIXES addendum. **Second rebase** onto
`origin/main` after #41 merged (`bf2027a`): both-add conflicts in `task/map.md` and
`python/repark/tests/map.md` — kept **all** ledger rows and both corpus map entries; multi-hunk
sweep clean (`grep -rn '^<<<<<<<'`).

| Finding | Action |
|---|---|
| Ledger §6 not paste-true (wrong bullet template; Pin citations not resolvable node ids) | **Rewrote §6** to the registry bullet template (`- **repark**` / `- **Apache Spark**` / `- **Pin**` / `- **Rationale**`) with full `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[<case>]` ids. Registry file **not** edited (orchestrator lands rows). |
| 38-digit-clamp coverage pin satisfied by a non-clamp control row (`mul_38_0_identity` is DECIMAL(38,…) equality) | **Tightened pin** — requires ≥3 G2 rows whose names match `*clamps_scale_in_spark`; deleting the clamp family goes red. |
| Disclosure note missing `f` prefix (~line 365) — failures printed literal `{FIX_G2}` | **Added `f`** so the note formats the real `FIX_G2` text. |
| Authorship polluted (`Grok (grok-4.5) <noreply@x.ai>`) | **Amended** to `TRO-Wolf <64240326+TRO-Wolf@users.noreply.github.com>` + existing `Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer (per-command `-c` only). Re-verified after this rebase. |

### Rebase note (post-#41)

Suite grows by the MERGE differential corpus now on main; re-gate with full
`make py-test-facade` (not unit-only) so a stale `.so` cannot hide a false green.
