# Unit ledger — W-3 / H-2 gap G4: joins differential corpus

**Unit:** H-2 gap **G4** of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Lane:** grok W-3 · **Branch:** `grok/w3-joins-corpus` · **Worktree:** `/tmp/grok-w3`

**This ledger covers the record-side differential only.** Fixing any divergence found is out of
scope (rows document; fixes are future units). The registry file is not edited from this unit
(§6 paste-true handoff only). Window functions are W-4's.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | [`python/repark/tests/test_join_parity.py`](../python/repark/tests/test_join_parity.py) | 26 rows + lifecycle helpers + classifiers + budget pin + CP-1 monkeypatches |
| Record driver | [`python/repark/tests/_record_join_goldens.py`](../python/repark/tests/_record_join_goldens.py) | Re-derive Spark halves (order-insensitive) |
| Tests map | [`python/repark/tests/map.md`](../python/repark/tests/map.md) | lockstep entry |
| This ledger | `task/w3-joins-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 Row inventory (budget 20–28 → **26**)

| # | Name | Kind | Entry | Outcome |
|---|---|---|---|---|
| 1 | `control_inner_equality` | content equality | sql | repark == Spark |
| 2 | `control_left_equality` | content equality | sql | repark == Spark |
| 3 | `null_keys_inner_no_match` | content equality | sql | NULL≠NULL on INNER |
| 4 | `null_keys_left_outer_fate` | content equality | sql | left NULL orphan preserved |
| 5 | `null_keys_right_outer_fate` | content equality | sql | right NULL orphan preserved |
| 6 | `null_keys_full_outer_fate` | content equality | sql | both NULL orphans |
| 7 | `null_safe_equal_matches_nulls` | content equality | sql | `<=>` matches NULLs |
| 8 | `duplicate_keys_inner_2x2_fanout` | content equality | sql | 2×2 = 4 rows |
| 9 | `duplicate_keys_left_with_unmatched` | content equality | sql | fan-out + solo |
| 10 | `cross_join_sql` | content equality | sql | CROSS 2×2 |
| 11 | `left_semi_sql` | content equality | sql | LEFT SEMI |
| 12 | `left_anti_sql` | content equality | sql | LEFT ANTI |
| 13 | `left_semi_null_keys_no_match` | content equality | sql | SEMI + NULL empty |
| 14 | `type_mismatch_int_string_key` | content equality | sql | int = '1' |
| 15 | `type_mismatch_int_decimal_key` | content equality | sql | int = DECIMAL |
| 16 | `type_mismatch_string_decimal_key` | content equality | sql | '1.00' = DECIMAL |
| 17 | `type_mismatch_string_decimal_malformed_raises` | error | sql | both refuse cast |
| 18 | `left_outer_right_cols_nullable` | content equality | sql | right payload nullable |
| 19 | `right_outer_left_cols_nullable` | content equality | sql | left payload nullable |
| 20 | `full_outer_both_sides_nullable` | content equality | sql | both sides nullable |
| 21 | `multi_key_inner_equality` | content equality | sql | composite equi-join |
| 22 | `df_join_inner_on_name` | content equality | df | CP-11 DF inner |
| 23 | `df_join_left_outer_on_name` | content equality | df | CP-11 DF left |
| 24 | `df_join_eq_null_safe` | content equality | df | DF `eqNullSafe` |
| 25 | `df_left_semi_unsupported` | **split** | df | repark refuses; Spark runs |
| 26 | `df_left_anti_unsupported` | **split** | df | repark refuses; Spark runs |

**Counts by kind:** content equality **23** · error **1** · split **2** · disclosures (repark pin) **0**.

### 1.2 Lifecycle helper

`run_join_content` / `run_join_expect_error` live **in the test module** as the recipe SSOT the
record driver imports. SQL door: `session.sql(sql)`. DF door: `createDataFrame` +
`DataFrame.join` (name / `eqNullSafe` / cross). No Iceberg warehouse required — pure SQL + DF
join recipes (joins do not need table-format semantics; contrast N-2 MERGE which needs Iceberg).

### 1.3 Re-derive command

```bash
# Serialize with W-4 / other JVM recorders:
#   flock -x /tmp/grok-jvm-record.lock …
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_join_goldens.py
```

Record basis: `master("local[2]")`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, UI off. Compare is **order-insensitive** (parity comparator).

---

## 2. Decisions, with rationale

**D-W3-1 — Pure SQL / createDataFrame, not Iceberg tables.** Joins do not need table-format
semantics (MERGE did). Inline subqueries + `createDataFrame` keep the record driver JVM-simple
(no Iceberg GAV) and still exercise the facade `sql()` / `df.join` doors the brief names.
"Over Iceberg tables" in the brief is read as the product's primary store, not a corpus
requirement for join *semantics*.

**D-W3-2 — SQL semi/anti are content equalities; DF semi/anti are splits.** repark's SQL door
accepts `LEFT SEMI` / `LEFT ANTI` / `CROSS JOIN` and matches Spark. The DataFrame facade
`how=` surface supports only inner/left/right/full/cross — `leftsemi`/`leftanti` raise
`AnalysisException` ("Unsupported join type"). Do not invent DF support; the split rows pin the
refuse + the recorded Spark success half.

**D-W3-3 — Null-safe equal via SQL `<=>` and DF `eqNullSafe`.** Both doors match NULLs under
null-safe equal; the DF row post-selects aliased keys so Arrow does not carry duplicate column
names.

**D-W3-4 — Order-insensitive compare for fan-out.** m×n rows do not pin order; the parity
comparator sorts by all columns. The record driver uses the same comparator (not raw
`Table.equals`) so re-derivation does not false-red on row order.

**D-W3-5 — Name-gated family coverage pins (CP-2).** `*null_keys_*`, `*duplicate_keys_*`,
`*type_mismatch_*`, `*nullable*` — a control LEFT JOIN equality cannot green the nullability
family.

**D-W3-6 — CP-1 classifier monkeypatches always-on.** Both CONVERGED and regression arms of the
split classifier are proven by monkeypatch tests in the module (not only ledger manual
provocations).

**D-W3-7 — Registry file not edited.** §6 paste-true rows only; orchestrator lands after merge.

---

## 3. Gate evidence

### 3.1 Corpus suite (repark facade, JVM-free)

```
python/repark/tests/test_join_parity.py — 29 passed
(26 differential + 1 budget + 2 CP-1 classifier monkeypatches)
```

### 3.2 Record mode (zulu-17 + PySpark 4.1.2)

```
record mode: 26 rows re-derived, 0 mismatch(es)
[G4] … every content/split PASS; type_mismatch_string_decimal_malformed_raises [error] PASS (CAST_INVALID_INPUT)
```

### 3.3 `make ci` / facade

```
make ci → exit 0
make py-test-facade → 2663 passed, 46 skipped, 37 warnings in 106.51s
```

---

## 4. Provocations

### P1 — golden value perturbed → FrameMismatchError (not silent pass)

Perturb `control_inner_equality.spark` b-value from `'x'` to `'BUG'`; re-run the single case:

```
perturbed
FAILED … FrameMismatchError: value mismatch at row 0:
  actual  : {'k': 1, 'a': 'a', 'b': 'x'}
  expected: {'k': 1, 'a': 'a', 'b': 'BUG'}
1 failed in 0.42s
restored
1 passed in 0.43s
```

### P2 — split CONVERGED arm (monkeypatch, always-on)

`test_split_classifier_converged_arm` greens — monkeypatch returns Spark golden → message
contains `CONVERGED` + `flip it to a content equality` + `Do not delete`.

### P3 — split regression arm (monkeypatch, always-on)

`test_split_classifier_regression_arm` greens — monkeypatch returns wrong table → message
contains `regression` + `Re-derive` + `not a clean convergence`.

### P4 — budget pin (name-gated nullability)

Deleting all `*nullable*` rows from a live view of `ROWS` would red
`need ≥2 *nullable* schema rows` — a LEFT JOIN control alone does not satisfy the pin.

### P5 — oracle re-derivation spot-check (≥3 sampled rows)

Independent re-record of sample rows under a fresh Spark session (same driver, same warehouse-free
basis) matched committed goldens:

```
SPOT control_inner_equality PASS
SPOT null_keys_full_outer_fate PASS
SPOT df_left_semi_unsupported PASS
SPOT type_mismatch_string_decimal_malformed_raises error ok=True
SPOT df_join_eq_null_safe PASS
spot-check done
```

Full record run: 26 rows, 0 mismatches (§3.2).

---

## 5. Deviations from brief

1. **No Iceberg table materialization** — pure SQL + createDataFrame (D-W3-1). Declared, not silent.
2. **No engine production changes** — corpus-only unit by design.
3. **Registry file not edited** — ready-to-paste rows in §6 only.
4. **No value disclosures found** — 23/26 content rows are equalities; only DF semi/anti refuse
   splits + one shared-refuse error. That is honest measurement, not invented divergence.

---

## 6. Ready-to-paste divergence-registry rows

> Conductor ban: do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit.
> A later registry sweep owns the file. Rows below are in the registry's **paste-true** bullet
> template (`- **repark**` / `- **Apache Spark**` / `- **Pin**` / `- **Rationale**`) with fully
> resolvable `path::test[case]` node ids.

### REG-G4-1 — DataFrame `leftsemi` surface gap

- **repark** — `df.join(other, on="k", how="leftsemi")` raises `AnalysisException` with
  `Unsupported join type` (supported: inner/left/right/full/cross only).
- **Apache Spark** — runs LEFT SEMI; returns left rows with a match, right columns dropped
  (`k=1,a=a` for the recorded seed). *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_semi_unsupported]`
- **Rationale** — BACKLOG, intent to FIX (gap G4 — DF door semi join). SQL door `LEFT SEMI` already
  matches Spark (`left_semi_sql` equality); the gap is the DataFrame `how=` surface only.

### REG-G4-2 — DataFrame `leftanti` surface gap

- **repark** — `df.join(other, on="k", how="leftanti")` raises `AnalysisException` with
  `Unsupported join type`.
- **Apache Spark** — runs LEFT ANTI; returns left rows with no match (`k=2,a=b` for the recorded
  seed). *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_anti_unsupported]`
- **Rationale** — BACKLOG, intent to FIX (gap G4 — DF door anti join). SQL door `LEFT ANTI` already
  matches Spark (`left_anti_sql` equality).

### REG-G4-3 — (none further at land)

All other corpus rows are **equalities** (or shared-error-class agreement on malformed
string→decimal cast). No additional value/type/nullability divergences were observed under the
recorded recipes. If a future engine change flips a content row, the disclosure idiom classifies
CONVERGED vs regression.

---

## 7. Octo / claims_critic

| Stage | Label | Detail |
|---|---|---|
| sepmo-octo | **OCTO-CONVERGED** | cycles=3 planned, early_stop after CLEAN cycle 1; claims_critic=true (C1+C2+C3+C4 quad) |
| Cycle 1 finding | C1-Q-001 S2 REMEDIATED | null_keys pin tightened to `startswith("null_keys_")` (CP-2) |
| Stuck ≥floor after 3 | n/a | no HALT |
| Scratch | `/tmp/critic-octo-w3-joins/` | cycle-1-findings.md, cycle-1-fix.md, OCTO-REPORT.md |

---

## 8. Authorship

Commits authored **TRO-Wolf** + `Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer, per-command
`-c` identity only.
