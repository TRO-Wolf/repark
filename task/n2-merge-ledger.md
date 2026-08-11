# Unit ledger — N-2 / H-2 gap G3: MERGE INTO differential corpus (record-side)

**Unit:** H-2 gap **G3** of the V2 Engine Hardening campaign
([../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md) "G3") ·
**Date:** 2026-08-10 · **Lane:** overnight conductor N-2 · **Branch:** `grok/n2-merge-differential`

**This ledger covers the record-side differential only.** The brief's G3 full budget also names
4 Rust pins and 2 live-tier scenarios — both are **explicitly deferred** (G-4 file ban on
`crates/repark-spark/src/tests.rs`; `_live_parity.py` + `.github/` banned by the conductor). The
deviation is recorded here, not silent.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | [`python/repark/tests/test_merge_differential_parity.py`](../python/repark/tests/test_merge_differential_parity.py) | 10 rows + lifecycle helper + assertions |
| Record driver | [`python/repark/tests/_record_merge_differential_goldens.py`](../python/repark/tests/_record_merge_differential_goldens.py) | Spark+Iceberg provision; re-derive goldens |
| Tests map | [`python/repark/tests/map.md`](../python/repark/tests/map.md) | lockstep entry |
| This ledger | `task/n2-merge-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 Row inventory (budget 8-10 → **10**)

| # | Name | Kind | Outcome |
|---|---|---|---|
| 1 | `basic_upsert_update_and_insert` | content equality | repark == Spark |
| 2 | `duplicate_source_keys_with_matched_raises` | error | both: `MERGE_CARDINALITY_VIOLATION` |
| 3 | `duplicate_source_keys_insert_only_commits_both` | content equality | both insert both rows |
| 4 | `matched_and_arm_order_update_then_delete` | content equality | first-match-wins |
| 5 | `matched_and_threshold_update_or_delete` | content equality | threshold arms |
| 6 | `null_merge_keys_do_not_match` | content equality | NULL=NULL → no match |
| 7 | `insert_only_ignores_matched_source_rows` | content equality | matched source ignored |
| 8 | `delete_matched_removes_target_row` | content equality | DELETE arm |
| 9 | `conditional_matched_update_by_target_predicate` | content equality | `AND target.score > 40` |
| 10 | `not_matched_by_source_repark_refuses` | **split disclosure** | repark refuses; Spark deletes unmatched target |

### 1.2 Lifecycle helper

`create → seed → MERGE → read back` (and the error-path twin) live **in the test module** as the
recipe SSOT the record driver imports — **not** in `_live_parity.py` (banned). Every path drops
the per-row target table and source view in `finally`, so a failed MERGE leaves no stray catalog
tables (pinned by `test_lifecycle_cleanup_after_failed_merge` + the record driver's cleanup probe).

### 1.3 Iceberg GAV pin (Q2 ruling — exact Spark-minor match)

```
org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
```

- **Exact** Spark 4.1 minor match under zulu-17 + PySpark 4.1.2 (no oracle-environment caveat).
- Fetched at **record time only** via `spark.jars.packages` (never committed as a binary).
- CI stays JVM-free — the record driver is not collected by pytest and is not on any CI path.

**Re-derive command:**

```bash
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_merge_differential_goldens.py
```

Record basis: `master("local[2]")`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, UI off, IcebergSparkSessionExtensions, Hadoop catalog
`local` over a temp warehouse.

---

## 2. Decisions, with rationale

**D-N2-1 — Explicit Iceberg DDL + INSERT seed, not CTAS.** CTAS on Spark inferred `id` as
`int32` from uncasted literals; repark CTAS inferred non-null from non-null seeds. Explicit
`CREATE TABLE (id BIGINT, name STRING) USING iceberg` + casted seed queries align both engines
on `int64` + nullable=True, so content equalities are honest full-schema matches (name, type,
nullability) rather than value-only.

**D-N2-2 — repark always sets COW `TBLPROPERTIES`; Spark does not need them.** repark's merge
mode is pinned to copy-on-write for determinism (same as `test_merge_into.py`). Spark Iceberg
1.11 runs MERGE without those props; forcing them is optional (`spark_needs_cow_props=False`).

**D-N2-3 — Error rows pin the shared token, not full messages.** Both engines raise
`MERGE_CARDINALITY_VIOLATION` with different wrappers (Spark: `SparkRuntimeException` + SQLSTATE
23K01; repark: `PySparkException` / datafusion Execution error). The honest class compare is the
shared token.

**D-N2-4 — `WHEN NOT MATCHED BY SOURCE` is a split disclosure, not invented support.** repark
refuses with `NotImplemented` (existing `test_merge_into.py` pin). Spark+Iceberg runs it. The
row pins repark's refuse needle and the recorded Spark success table; do not invent support.

**D-N2-5 — Deferred Rust pins + live-tier scenarios are named, not silent.** G-4 bans
`crates/repark-spark/src/tests.rs`; conductor bans `_live_parity.py` and `.github/`. Follow-up
after G-4 merges owns the 4 Rust pins and the 2 live scenarios (and any workflow changes the
live tier needs for Iceberg).

**D-N2-6 — Lifecycle helper beside the record driver (in the test module).** One helper, one
recipe SSOT; the record driver imports it. Putting it in `_live_parity.py` is banned for this
unit; a live-tier abstraction is a daytime follow-up.

---

## 3. Deviations from the full G3 budget

| G3 asks | This unit | Disposition |
|---|---|---|
| 8-10 differential rows | **10** | in budget |
| 4 Rust pins (dup-key detection, arm ordering) | **0** | deferred — G-4 file ban |
| 2 live-tier scenarios | **0** | deferred — `_live_parity` + workflow banned |
| Record driver + goldens | **yes** | delivered |
| Lifecycle helper (record-side) | **yes** | delivered |

---

## 4. Gate evidence

### 4.1 Corpus suite (repark facade, JVM-free)

```
python/repark/tests/test_merge_differential_parity.py — 13 passed
```

### 4.2 Record mode (zulu-17 + Iceberg GAV)

```
record mode: 10 rows re-derived, 0 mismatch(es)
[G3] lifecycle cleanup after failed MERGE PASS (tables=[])
Iceberg GAV = org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
```

### 4.3 `make ci` / facade

```
make ci → exit 0 (via /tmp/overnight-conductor/gate.sh N2)
make py-test-facade → 2578 passed, 46 skipped, 37 warnings in 104.86s
```

---

## 5. Provocations

### P1 — golden perturbed → correct classification

Perturb `basic_upsert_update_and_insert.spark` name for id=2 from `'bee'` to `'BUG'`; re-run:

```
FAILED … FrameMismatchError: value mismatch at row 1:
  actual  : {'id': 2, 'name': 'bee'}
  expected: {'id': 2, 'name': 'BUG'}
```

Restore golden → `1 passed`. Classification correct (value mismatch, not silent pass).

### P2 — lifecycle cleanup after failed MERGE

`test_lifecycle_cleanup_after_failed_merge` green (always-on). Record driver:

```
[G3] lifecycle cleanup after failed MERGE PASS (tables=[])
```

---

## 6. Ready-to-paste divergence-registry rows

> H-1d owns `docs/spark-sql-iceberg-parity.md`; this unit does **not** edit that file
> (conductor ban). Paste candidates for the registry owner:

### REG-G3-1 — `WHEN NOT MATCHED BY SOURCE` surface gap

- **Class:** DML / MERGE surface
- **repark:** refuses with `NotImplemented` / `WHEN NOT MATCHED BY SOURCE is not supported yet`
- **Apache Spark + Iceberg:** runs; matched UPDATE + unmatched-target DELETE
- **Pin:** `test_merge_differential_parity.py::…not_matched_by_source_repark_refuses` (split)
  + `test_merge_into.py::test_merge_into_not_matched_by_source_engine_rejects`
- **Oracle:** recorded live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`
- **Fix owner:** engine MERGE surface (not this unit)

### REG-G3-2 — (none further at land)

All other corpus rows are **equalities** (or shared-error-class agreements). No additional
value divergences were observed under the recorded recipes. If a future engine change flips a
content row, the disclosure idiom in the test module classifies CONVERGED vs regression.

---

## 7. Octo / overload

| Stage | Label | Detail |
|---|---|---|
| sepmo-octo | **OCTO-CONVERGED** | cycles=4, early_stop=true, CLEAN cycle 1 ≥ S1 |
| critic-overload | **OVERLOAD-CONVERGED** | early_stop after CLEAN CCC-α |
| Scratch | `/tmp/overnight-conductor/n2/octo/`, `/tmp/overnight-conductor/n2/overload/` | cycle + wave artifacts |

S2 accepted-flagged: reverse arm-order row dropped to stay in 8-10 budget (threshold multi-predicate remains).

---

## 8. Fix pass / provocation transcripts

### P1 — golden perturbation (verbatim)

```
perturbed
F
E   repark_parity.compare.FrameMismatchError: value mismatch at row 1:
      actual  : {'id': 2, 'name': 'bee'}
      expected: {'id': 2, 'name': 'BUG'}
FAILED …
restored
.
1 passed in 0.60s
```

### P2 — lifecycle cleanup

Covered by always-on `test_lifecycle_cleanup_after_failed_merge` (suite green) and record-mode
cleanup probe (`tables=[]`).

---

## 9. Fix-round (PR #41 ACCEPT-WITH-NITS) — 2026-08-11

Orchestrator must-fix + A1/A2 from MORNING-FIXES addendum. Rebase onto `origin/main` first
(task/map.md both-add — kept all rows).

| Finding | Action |
|---|---|
| Module docstring named unpinned class (non-last unconditional `WHEN MATCHED`) | **Deleted the docstring claim** (row stayed dropped for budget per §7; no corpus growth) |
| Split-path convergence classifier dead code (NMBS success → bare message) | **A1 path: made reachable** — split arm drives `run_merge_lifecycle`; on commit classifies CONVERGED (matches Spark golden → flip to content equality) vs regression/partial. No `crates/**`, no corpus growth |
| gitignore/scrub `spark-warehouse/` + conductor scratch | **A2 promoted** — `.gitignore` adds `/spark-warehouse/`, `/metastore_db/`, `/derby.log`, `/scratch/`; local residue scrubbed |

### N-2b follow-ups (ledger-noted only this round)

| NIT | Note for N-2b |
|---|---|
| Tautological GAV pin | Derive expected Spark-minor from the pinned pyspark version rather than restating the constant |
| Dead `spark_needs_cow_props` | Field defaults False and no row sets True; remove knob or wire a row that needs it |
| Re-derive recipe wording | Quote the full parity-live sync line in the module docstring / ledger re-derive block |

### Authorship

Original tip carried `Grok (grok-4.5) <noreply@x.ai>`; fix-round amends to
`TRO-Wolf <64240326+TRO-Wolf@users.noreply.github.com>` + existing
`Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer (per-command `-c` only).
