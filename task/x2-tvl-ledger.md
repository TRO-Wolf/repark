# Unit ledger — X-2 / H-2 gap G12: three-valued logic differential corpus

**Unit:** H-2 gap **G12** of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Lane:** overnight conductor X-2 · **Worktree:** `/tmp/grok-x2` · **Branch:**
`grok/x2-g12-tvl-corpus` · **Base freeze:** `origin/main` `9acb566` (A11 — no mid-flight rebase)

**This ledger covers the record-side differential only.** Out of scope per brief: fixing found
divergences; the registry file; engine code; DML-level NOT IN with NULL (G3-E8 / **PR #54 in
flight** at kickoff — cite, do not duplicate). Critic: `octo` cycles=2 early_stop +
`claims_critic=true`.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | [`python/repark/tests/test_three_valued_logic_parity.py`](../python/repark/tests/test_three_valued_logic_parity.py) | 12 rows + lifecycle helpers + budget pin + CP-1 classifiers |
| Record driver | [`python/repark/tests/_record_tvl_goldens.py`](../python/repark/tests/_record_tvl_goldens.py) | re-derive Spark halves; `--emit` paste helpers |
| Cross-door rows (2) | [`crates/repark-sql/tests/cross_door.rs`](../crates/repark-sql/tests/cross_door.rs) | G12 TwoSession 3VL (X-2 sole writer tonight) |
| Maps | `python/repark/tests/map.md`, `crates/repark-sql/tests/map.md`, `task/map.md` | lockstep |
| This ledger | `task/x2-tvl-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 Budget (met)

| Bucket | Budget | Landed |
|---|---|---|
| G12 differential rows | 10–12 | **12** (10 equality + 2 disclosure) |
| Control equalities (min) | ≥6 | **10** |
| Disclosure ceiling | ≤6 | **2** |
| DataFrame-API rows (CP-11) | ≥2 `entry="df"` + name-gated `df_*` | **2** |
| Truth-table floor (name-gated) | ≥6 `and_*`/`or_*`/`not_*` | **6** |
| Cross-door TwoSession rows | 2 pure 3VL | **2** |

### 1.2 Row inventory (12)

**Equalities (10)** — repark == Spark on value AND Arrow type AND nullability:

| # | Name | Entry | Family | Intent |
|---|---|---|---|---|
| 1 | `and_true_null_is_null` | sql | truth_table | TRUE AND NULL → NULL |
| 2 | `and_false_null_is_false` | sql | truth_table | FALSE AND NULL → FALSE |
| 3 | `or_true_null_is_true` | sql | truth_table | TRUE OR NULL → TRUE |
| 4 | `or_false_null_is_null` | sql | truth_table | FALSE OR NULL → NULL |
| 5 | `not_null_is_null` | sql | truth_table | NOT NULL → NULL |
| 6 | `and_null_null_is_null` | sql | truth_table | NULL AND NULL → NULL |
| 7 | `is_null_vs_eq_null` | sql | is_null | IS [NOT] NULL (two-valued) vs = NULL (UNKNOWN) |
| 8 | `case_when_null_predicate` | sql | case_when | CASE WHEN NULL falls through → next WHEN TRUE |
| 9 | `in_list_with_null_select` | sql | in_list | SELECT-level IN (…, NULL): hit / miss / null-lhs |
| 10 | `df_select_and_or_not_nulls` | df | df_api | CP-11 `&` / `|` / `~` over nullable booleans |

**Disclosures (2)** — value agrees; **nullability of null-safe-equal result** diverges
(Spark: non-nullable bool; repark: nullable bool):

| # | Name | Entry | Family | Divergence |
|---|---|---|---|---|
| 11 | `null_eq_vs_null_safe_eq` | sql | null_compare | `nse` nullability (SQL `<=>`) |
| 12 | `df_eq_null_safe_select` | df | df_api | `nse` nullability (`Column.eqNullSafe`) |

### 1.3 Six load-bearing AND/OR/NOT combos (why these, not all 9×2)

The traps that distinguish UNKNOWN from boolean-FALSE:

1. `TRUE AND NULL → NULL` (not FALSE)
2. `FALSE AND NULL → FALSE` (AND short-circuit)
3. `TRUE OR NULL → TRUE` (OR short-circuit)
4. `FALSE OR NULL → NULL` (not TRUE)
5. `NOT NULL → NULL`
6. `NULL AND NULL → NULL` (both unknown)

Boolean-core pairs (`TRUE AND TRUE`, `FALSE OR FALSE`, …) do not distinguish 3VL from 2VL and
are deliberately omitted.

### 1.4 Cross-door rows (2) — pure 3VL; no #54 ROW 9

| Test | SQL (shared, portable) | Golden | Corpus row |
|---|---|---|---|
| `cross_door_tvl_true_and_null_is_null` | `(TRUE AND CAST(NULL AS BOOLEAN))` | Boolean nullable NULL | `and_true_null_is_null` |
| `cross_door_tvl_case_when_null_predicate` | `CASE WHEN NULL THEN 1 WHEN TRUE THEN 2 ELSE 3` | Int32 non-null 2 | `case_when_null_predicate` |

Protocol: two sessions (native `AnsiDialect` vs `SparkDialect`+`SparkExtension`), independent
warehouses, Arrow-path equality of type + nullability + value. **No Spark-only `<=>`** so both
doors share one string. **Do not re-add** #54's `cross_door_g3e8_refusals_render_identically`
(lives on #54's branch only).

### 1.5 DML NOT-IN twin cite (B5)

DML-level `NOT IN` with NULL is the G3-E8 corpus. At kickoff **PR #54 is in flight** (head
`fcf6f55`, not on freeze `9acb566`). This unit does **not** duplicate that family; one
SELECT-level `IN (…, NULL)` row is enough for the SELECT surface.

### 1.6 Re-derive command

```bash
# Serialize with other JVM recorders:
#   flock -x /tmp/grok-jvm-record.lock …
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_tvl_goldens.py
# --emit prints paste-ready _table / _one_row constructors
```

Record basis: `master("local[2]")`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, UI off. Compare uses the parity comparator (order-insensitive).

---

## 2. Decisions, with rationale

**D-X2-1 — Pure SQL / createDataFrame, not Iceberg tables.** 3VL is expression semantics; no
table-format requirement. Same mold as joins/windows pure-SQL corpora.

**D-X2-2 — Two disclosures on null-safe-equal nullability only.** Value of `NULL <=> NULL` /
`eqNullSafe` matches Spark (TRUE); only Arrow nullability of the boolean result column
diverges. Honest disclosure with both halves pinned; flipped by `FIX_G12`.

**D-X2-3 — Name-gated family coverage (CP-2).** Truth-table requires the six named rows;
`is_null` / `case_when` / `in_list` / `null_eq` / `df_*` are name-gated so a control cannot
green a family.

**D-X2-4 — CP-11 DF door by `entry="df"` + `df_*` name prefix.** SQL that mentions AND cannot
satisfy the DF pin.

**D-X2-5 — Cross-door uses portable SQL only.** `<=>` is Spark-only; CASE WHEN + TRUE AND NULL
are shared.

**D-X2-6 — Registry file not edited.** §6 paste-true rows only; orchestrator lands after merge.

**D-X2-7 — Sole writer of `cross_door.rs` tonight (B2).** No #54 ROW 9 re-add (A2).

---

## 3. Gate evidence

### 3.1 Corpus suite (repark facade, JVM-free)

```
python/repark/tests/test_three_valued_logic_parity.py — 15 passed
(12 differential + 1 budget + 2 CP-1 classifier monkeypatches)
```

### 3.2 Record mode (zulu-17 + PySpark 4.1.2)

```
record mode: 12 spark halves re-derived, 0 mismatch(es)
[G12] every row PASS (full recheck after paste)
```

### 3.3 Cross-door Rust

```
cargo test -p repark-sql --test cross_door cross_door_tvl — 2 passed
  cross_door_tvl_true_and_null_is_null
  cross_door_tvl_case_when_null_predicate
```

### 3.4 `make verify` components (JVM-free)

```
make rust-fmt-check          → exit 0
make rust-clippy             → exit 0
make check-crate-dag check-lib-rs check-rust-file-size
  check-lib-py check-manifest check-parity-live-dual-wire → exit 0
make py-lint py-format-check py-lock-check → exit 0
make rust-check              → exit 0
cargo test --workspace --locked → all suites 0 failed (incl. cross_door 12/12)
TVL pytest                   → 15 passed
```

Equivalent to `make verify` (ci + rust-test) for this corpus-only change.

---

## 4. Provocations

### P1 — golden value perturbed → FrameMismatchError (not silent pass)

Perturb `and_false_null_is_false.spark` from `False` to `True`; re-run the single case:

```
FAILED … FrameMismatchError: value mismatch at row 0:
  actual  : {'v': False}
  expected: {'v': True}
1 failed
restored → 15 passed
```

### P2 — disclosure CONVERGED arm (monkeypatch, always-on)

`test_disclosure_classifier_converged_arm` greens — monkeypatch returns Spark golden → message
contains `CONVERGED` + `flip it to an equality` + `Do not delete`.

### P3 — disclosure regression arm (monkeypatch, always-on)

`test_disclosure_classifier_regression_arm` greens — monkeypatch returns a third table → message
contains `regression` + `Re-derive`.

### P4 — oracle re-derivation spot-check (≥3 sampled rows)

Independent re-record under a fresh Spark session matched committed goldens:

```
SPOT and_true_null_is_null PASS
SPOT in_list_with_null_select PASS
SPOT df_select_and_or_not_nulls PASS
SPOT null_eq_vs_null_safe_eq PASS
spot-check done
```

Full record run: 12 rows, 0 mismatches (§3.2).

---

## 5. Deviations from brief

1. **No engine production changes** — corpus-only unit by design.
2. **Registry file not edited** — ready-to-paste rows in §6 only.
3. **Two nullability disclosures found** (not zero) — honest measurement of null-safe-equal
   Arrow nullability; value parity holds. Declared, not silent.
4. **DML NOT-IN not duplicated** — cites **PR #54 in flight** (B5).

---

## 6. Ready-to-paste divergence-registry rows

> Conductor ban: do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit.
> A later registry sweep owns the file. Rows below are in the registry's **paste-true** bullet
> template (`- **repark**` / `- **Apache Spark**` / `- **Pin**` / `- **Rationale**`) with fully
> resolvable `path::test[case]` node ids (verified via `pytest --collect-only`).

### REG-G12-1 — null-safe equal result nullability (SQL `<=>`)

- **repark** — `SELECT (NULL <=> NULL) AS nse` yields Arrow `bool` **nullable** (value TRUE).
- **Apache Spark** — same expression yields Arrow `bool` **non-nullable** (value TRUE).
  *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_three_valued_logic_parity.py::test_tvl_parity_row[null_eq_vs_null_safe_eq]`
- **Rationale** — BACKLOG, intent to FIX or DECLARE (gap G12 — null-safe-equal result
  nullability). VALUE already matches; only schema nullability diverges. Paired DF door pin
  REG-G12-2.

### REG-G12-2 — null-safe equal result nullability (DataFrame `eqNullSafe`)

- **repark** — `Column.eqNullSafe` select yields Arrow `bool` **nullable** (values match Spark).
- **Apache Spark** — same recipe yields Arrow `bool` **non-nullable**. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_three_valued_logic_parity.py::test_tvl_parity_row[df_eq_null_safe_select]`
- **Rationale** — BACKLOG, intent to FIX or DECLARE (gap G12 — DF door twin of REG-G12-1).
  CP-11 entry-point pair so a class claim on null-safe equal nullability covers both doors.

### REG-G12-3 — (none further at land)

The other 10 corpus rows are **equalities** (value AND type AND nullability). If a future
engine change flips a content row, the disclosure idiom classifies CONVERGED vs regression.

---

## 7. Octo / claims_critic

| Stage | Label | Detail |
|---|---|---|
| sepmo-octo | **OCTO-CONVERGED** | cycles=2 planned, early_stop after CLEAN cycle 1; claims_critic=true |
| Cycle 1 | CLEAN (S0/S1 none) | Claims re-checked against tree: 12 ROWS, 2 disclosures, 2 cross-door, PR #54 cite, no #54 ROW 9, map lockstep, budget pins name-gated |
| Cycle 1 S2 notes | REMEDIATED / N/A | Ruff RUF002/003 unicode cleaned; unused `_I64` removed; golden perturbation + classifier arms + spot-check green |
| Corpus taxonomy null-report | CP-1…CP-12 | CP-1 proven (monkeypatch both arms); CP-2 name-gates; CP-3 real entry points; CP-5 paste-true §6; CP-6 f-strings checked; CP-7 spot-check; CP-10 budget pin; CP-11 entry field + `df_*`; others N/A |
| Stuck ≥floor after 2 | n/a | no HALT |
| Scratch | `/tmp/critic-octo-x2-tvl/` | cycle notes below |

### Claims critic (C4) — sampled claims vs tree

| Claim | Verdict |
|---|---|
| "12 differential rows budget 10–12" | PROVEN — `len(ROWS)==12`, pin enforces |
| "six load-bearing truth-table combos" | PROVEN — named required list in budget test |
| "≥2 DF entry points" | PROVEN — `entry=="df"` + `df_*` |
| "2 cross-door 3VL rows" | PROVEN — `cross_door_tvl_*` tests green |
| "PR #54 in flight for DML NOT-IN" | PROVEN — ledger §1.5; no `not_in` rows |
| "value AND type AND nullability" | PROVEN — `assert_frames_equal` on Arrow path |
| "no engine fixes" | PROVEN — diff is tests/maps/ledger only |

---

## 8. Authorship

Commits authored **TRO-Wolf** + `Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer, per-command
`-c` identity only. Two-pass hygiene before push.
