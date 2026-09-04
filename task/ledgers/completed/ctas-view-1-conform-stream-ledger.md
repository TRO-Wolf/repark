# Unit ledger — CTAS-VIEW-1 · unpartitioned CTAS stream conforms view-typed batches

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Unit:** CTAS-VIEW-1 · **Date:** 2026-09-03 · **Model:** grok-4.6 ·
**Branch:** `fix/ctas-view-1-conform-stream` · **Base:** `47f1a1d`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) `CTAS-VIEW-1`.

**Rubric:** STANDARD. `risk_tier: standard`.

**Writable paths:** `crates/repark-iceberg/src/write/`, `crates/repark-spark/src/tests/`,
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, `docs/guide/iceberg-guide.md`,
lockstep `map.md` files, this ledger. Closed: `STATUS.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, AWS.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Red-first: a Rust pin builds a Utf8View+BinaryView batch (one NULL) and runs unpartitioned CTAS; a facade pin writes parquet, `read.parquet`, `createOrReplaceTempView`, unpartitioned CTAS into the memory catalog, reads back equal. Both red today with the Utf8View schema error. | Named tests; red-first error table. | **PROVEN** |
| C-002 | `write_data_files_from_stream_with_concurrency` maps `conform_batch` (via `conform_batch_retaining_unmapped_columns`) per item before the fan-out, using `write_default_column_names`. Already-conformed batches are unchanged. Every other caller of that writer is audited. | Diff at the writer; caller table. | **PROVEN** |
| C-003 | Both C-001 pins green; service-managed view-typed sibling; partitioned CTAS from the same view still works; mutation (remove the conform map) reds both C-001 pins (`N` red of `M`); `make verify`; Spark-measured CTAS type pins (V3-COV-8 `long, required`) unchanged. | Green tests; mutation table; V3-COV-8 control. | **PROVEN** |
| C-004 | Registry row `CTAS-VIEW-1` §7 FIXED 2026-09-03 with the repro table (history: shipped in 1.0.0, fixed for 1.0.1); iceberg-guide one line; this ledger + `staging/map.md`; `map.md` lockstep; `STATUS.md` untouched. | Doc paths. | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ctas-view-1-conform-stream
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Utf8View+BinaryView unpartitioned CTAS pins on Spark door and facade parquet-view path.
      artifacts: [crates/repark-spark/src/tests/ctas_view.rs, python/repark/tests/test_ctas_view_typed.py]
    - id: AT-2
      status: ATTACKED
      evidence: Partitioned control and service-managed sibling; MERGE extras retained so lineage columns are not dropped.
      artifacts: [crates/repark-spark/src/tests/ctas_view.rs, crates/repark-spark/src/tests/service_managed_ctas.rs, crates/repark-iceberg/src/write/conform.rs]
    - id: AT-3
      status: ATTACKED
      evidence: V3-COV-8 stays BACKLOG; conforming does not change CTAS type derivation.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: N/A
      justification: No new concurrency; existing stream fan-out is unchanged.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no .github, no Cargo pin, no secrets.
      artifacts: [crates/repark-iceberg/src/write/merge/mod.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Public writer signature unchanged; conform is internal to the stream map.
      artifacts: [crates/repark-iceberg/src/write/merge/mod.rs]
    - id: AT-7
      status: ATTACKED
      evidence: V3-COV-8 pin path untouched.
      artifacts: [python/repark/tests/test_v3_statement_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: Pins are always-run; no live Spark required.
      artifacts: [crates/repark-spark/src/tests/ctas_view.rs, python/repark/tests/test_ctas_view_typed.py]
    - id: AT-9
      status: ATTACKED
      evidence: Mutation table, one knob (remove the conform map).
      artifacts: [crates/repark-iceberg/src/write/merge/mod.rs]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS.md untouched; maps lockstep; registry CTAS-VIEW-1 FIXED.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-spark/src/tests/map.md]
  complete: true
```

## 2. Red-first (C-001)

| Pin | Red error (pre-fix / mutation) |
|---|---|
| `unpartitioned_ctas_from_view_typed_batches_round_trips` | `External(Unexpected => Arrow Schema Error … expected Utf8 but found Utf8View at column index 0)` |
| `test_unpartitioned_ctas_from_parquet_temp_view_round_trips` | same Utf8View mismatch on the parquet-read door |

## 3. Callers of `write_data_files_from_stream_with_concurrency` (C-002)

| Caller | Path | Effect of this conform |
|---|---|---|
| Spark CTAS unpartitioned | `crates/repark-spark/src/ctas.rs` `write_ctas_stream` | THE FIX — Utf8View/BinaryView batches now cast |
| ANSI CTAS unpartitioned | `crates/repark-sql/src/create_table.rs` `write_stream` | same conforming, no local edit |
| Public unpartitioned append | `append.rs` → `write_data_files_with_concurrency` | already conformed; second pass is identity |
| Overwrite unpartitioned stage | `overwrite.rs` `write_overwrite_staged_files_from_stream` | already positional-mapped; types-already-match skips `try_new` so CAST-NULL empty overwrite keeps source nullability |
| MERGE insert/rewrite stream | `merge/mod.rs` `write_new_data_files_from_stream` | already cast; lineage extras (`_row_id`, `_last_updated_sequence_number`) retained so v3 MERGE is not extra-column refused |
| Batch `write_data_files` | `write_data_files_with_concurrency` | identity on already-matching types |

## 4. Mutation table (C-003)

| Mutation | N red / M |
|---|---|
| Remove the conform map in `write_data_files_from_stream_with_concurrency` | 1 red / 1 unpartitioned C-001 rust pin (`expected Utf8 but found Utf8View`); partitioned control 0 red / 1 |

## 5. Docs (C-004)

| File | Change |
|---|---|
| `docs/spark-sql-iceberg-parity.md` | `CTAS-VIEW-1` FIXED 2026-09-03, repro table, 1.0.0 → 1.0.1 |
| `docs/guide/iceberg-guide.md` | one line on unpartitioned parquet-view CTAS |
| lockstep `map.md` | every touched directory |
| `STATUS.md` | untouched |

## 8. Gates

| Gate | Exit |
|---|---|
| `make develop` | 0 |
| `make verify` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -x --deselect python/repark/tests/test_pyspark_compat_smoke.py` | 0 (4430 passed, 161 skipped; 1 live-Spark CrossValidator node deselected for host SemLock PermissionError) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `uv run --no-sync ruff check python` | 0 |
| `uv run --no-sync ruff format --check python` | 0 |

## 9. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: ctas-view-1-conform-stream
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: STATUS.md untouched; CTAS-VIEW-1 FIXED in the registry
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: ctas-view-1-conform-stream
  flags: []
  count: 0
```
