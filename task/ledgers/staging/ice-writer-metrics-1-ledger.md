# Unit ledger — ICE-WRITER-METRICS-1 · RePark writers honour `write.metadata.metrics.*`

**Date:** 2026-09-20 · **Branch:** `fix/ice-writer-metrics-1` · **Base:** `1ab21e19`
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Fork #313 (F-METRICS-CONFIG-1) put Java's metrics machinery on the pin
(`iceberg::spec::MetricsConfig::for_table` / `for_position_delete_table`, and
`ParquetWriterBuilder::with_metrics_config`). RePark never calls any of it: every
`ParquetWriterBuilder` in `crates/repark-iceberg/src` is built without
`with_metrics_config`, so a table with `write.metadata.metrics.default=none` still gets
full bounds in its manifests — column bounds can hold sensitive values, and Spark's own
writes on the same table would not. Second seam: `writer_props.rs` rebuilds the
position-delete properties by hand and drops the delete-type key/value metadata the
fork's `position_delete_writer_properties_for` writes.

**Oracle.** `/tmp/oc-worker/pd-oracle/metrics_location_truth.json`, key `metrics`, recorded
by `record_metrics_location.py` (Iceberg 1.11.0 on Spark 4.1.2). Twelve property sets over
`(id BIGINT, s STRING, d DOUBLE, st STRUCT<a: STRING, b: INT>, xs ARRAY<INT>)`: `default`,
`none`, `counts`, `truncate4`, `full`, `col_none`, `col_nested`, `max_inferred_2`,
`max_inferred_2_default_set`, `sorted_none`, `sorted_counts`, `bad_mode`. Each cell holds the
`files`-table maps (`column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts`,
`lower_bounds`, `upper_bounds`, hex bytes) for one INSERT of two rows. The `locations` cells
in the same file belong to IPI-10 and are out of scope. Live re-derivation is tier-2
(merged code only); this unit pins the recorded cells.

**Field ids (read off the cells).** `id=1`, `s=2`, `d=3`, `st=4`, `xs=5`, `st.a=6`,
`st.b=7`, `xs.element=8`. Counts and bounds attach to leaves `1,2,3,6,7`; sizes add the
list element `8`.

## Decisions

- **RePark's gzip-level refusal stays.** The fork helper parses `gzip` with a level;
  RePark refuses that combination loud (ICE-WRITE-OPTIONS-1). `position_delete_writer_properties_for`
  validates through RePark's `parse_compression` first, then builds through the fork helper.
- **Unset-zstd default moves 1 → 3 on delete files.** RePark's `parse_compression` defaults
  to `ZstdLevel::default()` (level 1); the fork helper defaults to level 3. Both are valid
  zstd a reader cannot distinguish; the C-004 pin reads key/value metadata and bounds, not
  the codec level, so no pin observes it.
- **Column sizes are not compared.** They ride Parquet encoding choices, not the metrics
  config; C-003 pins counts, null counts, NaN counts, bounds presence/absence and truncation
  width.

## PROPOSITION LEDGER — ICE-WRITER-METRICS-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every RePark Parquet data-file writer applies `MetricsConfig::for_table` of the table it writes: the INSERT path (`write_options.rs`), the fan-out append path (`append_fanout_serial.rs`) and the CoW/MERGE rewrite path (`merge/mod.rs`, `merge/row_lineage.rs`) each have a pin. | `merge/tests/writer_metrics.rs` + `writer_metrics_truth.json` fixture. | **PROVEN** | `insert_path_applies_table_metrics_config` (write_options stage), `fanout_append_path_applies_table_metrics_config` (partitioned fanout, parallel workers), `merge_rewrite_path_applies_table_metrics_config` (merge/mod builder), `merge_lineage_path_applies_table_metrics_config` (row_lineage builder): each stages under `none` and asserts empty counts and bounds. Red pre-fix, green after. |
| C-002 | The position-delete writer applies `MetricsConfig::for_position_delete_table`. | `merge/tests/writer_metrics.rs`. | **PROVEN** | `position_delete_config_carries_table_default_and_row_overlays`: on a `none`-default table with `column.s=full`, the table-resolved config reports default `None`, `file_path`/`pos` `Full`, `row.s` `Full`, `row.id` `None`. The writer calls it at `position_delete.rs`. No file-level observable distinguishes the two constructors on the two-column delete schema (both force `Full`), so the pin is at the resolution seam by design. |
| C-003 | Each of the twelve recorded metrics cells: after a RePark INSERT the manifest's per-field metrics equal Spark's recorded cell; an unmatchable cell is a dated declared divergence with a registry row and a strict xfail, never silent. | `merge/tests/writer_metrics.rs` replays all twelve cells. | **PROVEN** | `metrics_cell_*_matches_spark` × 12 through `append` + manifest read: value/null/nan counts and lower/upper bounds equal the recorded cell on 12/12 (column sizes excluded by decision). Ten cells red pre-fix; `default` and `bad_mode` green throughout as controls. Zero divergences, so no registry rows and no xfails. |
| C-004 | RePark position-delete files carry the delete-type key/value metadata and untruncated `file_path` bounds, proven by reading back a written delete file's Parquet key/value metadata and its column bounds. | `merge/tests/writer_metrics.rs`. | **PROVEN** | `position_delete_files_carry_delete_type_and_full_path_bounds`: the footer carries `delete-type=position` and the manifest holds equal full-path lower/upper bounds for `file_path`. The KV assertion reds pre-fix; the bounds held already (the old call forced `Full` too). |
| C-005 | Mutation proof: removing `with_metrics_config` reds the `none` and `counts` pins; restoring RePark's hand-built delete properties reds the C-004 pin. | Ledger red-run record, reverted. | **PROVEN** | See Mutations: both mutations red the named pins and only those seams, then reverted to 18/18 green. |

## Red runs

- 2026-09-20, pre-fix tree: `cargo test -p repark-iceberg --lib writer_metrics` →
  3 passed, 15 failed. Failing for the named reason in every case: the writer ignores
  `write.metadata.metrics.*`, so `none` still records `{1,2,3,6,7}` counts and bounds,
  `truncate4` keeps the untruncated 16-char string bound instead of the 4-char truncation, and the delete
  file footer carries no `delete-type` marker. Green pre-fix (controls): `default`,
  `bad_mode` (both equal the fallback the builder already uses) and the
  `for_position_delete_table` resolution pin (it calls the fork API directly).

## Mutations

- Mutation A (2026-09-20): the four data-file `with_metrics_config` calls removed
  (`write_options.rs`, `append_fanout_serial.rs`, `merge/mod.rs`, `merge/row_lineage.rs`),
  delete writer untouched. `cargo test -p repark-iceberg --lib writer_metrics` →
  4 passed, 14 failed, with `metrics_cell_none_matches_spark` and
  `metrics_cell_counts_matches_spark` both red. Reverted (byte-restore from scratch copies).
- Mutation B (2026-09-20): `position_delete_writer_properties_for` swapped back to the
  hand-built reconstruction (compression only, no fork helper). Result: 17 passed,
  1 failed — exactly `position_delete_files_carry_delete_type_and_full_path_bounds`
  (the footer marker assertion). Reverted; 18/18 green again.
- Full package after revert: `cargo test -p repark-iceberg` → 599 passed, 0 failed.

## Notes

- No Python file is touched by this unit (behaviour reachable from Rust; pins are Rust).
  The native module was not rebuilt: no Python test consumes it here, and the facade
  suite is out of lane per the brief.
- `merge/mod.rs` and its size baseline. The actor's wiring grew the file by one line (the
  `with_metrics_config` chain link, 1761 → 1762 against its exact baseline) and it HALTED rather
  than raise the ceiling, which is the right call. **Ruling Q-25a-2 (orchestrator, 2026-09-19):
  no ceiling moves up.** The whole builder construction moved instead into
  `writer_props::name_matched_parquet_builder`, beside the other writer-property helpers, so the
  MERGE executor now asks for a configured builder rather than assembling one. `merge/mod.rs` is
  1,756 lines — five BELOW its old baseline — and the exception row is ratcheted down to 1756.

### Verification critic residues (2026-09-20, both P3, both answered here)

- **V-001 — `column_sizes`.** The twelve cells are equal on every metric the clause asserts
  (value counts, null counts, NaN counts, lower and upper bounds). `column_sizes` differs from
  Spark on every cell that records it, and the clause does not assert it: the number is the
  compressed byte size a writer happened to produce, so parquet-rs and parquet-mr disagree on it
  by construction, exactly as the RPD target-size residue does. It is recorded here rather than
  pinned, and it is not a metrics-config question.
- **V-002 — the position-delete config was not mutation-bound.** Swapping
  `MetricsConfig::for_position_delete_table` for the fixed `for_position_delete` left both delete
  pins green, so the clause rested on reading the code. Measured instead, and now pinned
  (`a_metrics_none_table_keeps_the_delete_files_path_bounds`): on a table with
  `write.metadata.metrics.default=none`, the DATA file carries no bounds at all while the
  position-delete file keeps its exact, untruncated `file_path` lower and upper bounds. That is
  the behaviour that matters — a v2 parquet delete carries no `referenced_data_file`, so the
  reader routes on those bounds, and a metrics config that stripped them would silently stop
  applying deletes. The pin fails if the delete writer ever takes the table's data-file config.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-writer-metrics-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its pin: C-001 four path pins, C-002 the
        resolution pin, C-003 the twelve recorded cells, C-004 the footer read-back,
        C-005 both mutations with revert-to-green.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/writer_metrics.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The recorded matrix is the boundary set: malformed mode (bad_mode falls
        back), truncate widths 4/16/full, per-column and nested overrides, the
        max-inferred threshold with and without an explicit default, and sorted-column
        promotion under none and counts. Rows carry no NULLs, as the oracle records.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/writer_metrics_truth.json]
    - id: AT-3
      status: N/A
      justification: No new failure mode: invalid modes fall back inside the fork, an
        invalid max-inferred value propagates as a writer-build error per the fork
        contract, and the pre-existing codec/level refusal pins still pass in the full
        599-test run.
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; every builder is call-local. The
        parallel fanout path (K=4 workers) is pinned green beside the serial paths.
    - id: AT-5
      status: ATTACKED
      evidence: The unit's motive is sensitive values in bounds: the none and counts
        cells prove suppression, and the col_none/col_nested cells prove per-column
        suppression including a nested field.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/writer_metrics.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Delete-file Java interop read back from disk: the footer key/value
        marker and the exact file_path bounds the fork routes on; manifest metrics
        equal Spark's on 12/12 cells so Spark reads what RePark writes.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/writer_metrics.rs]
    - id: AT-7
      status: N/A
      justification: No performance surface: two-row pins, one file each, no growth or
        hot loop introduced.
    - id: AT-8
      status: ATTACKED
      evidence: Fork contracts used at their pinned signatures (for_table,
        for_position_delete_table, position_delete_writer_properties_for); the two
        places RePark deliberately differs from the fork helper (gzip-with-level
        refusal, unset-zstd level 1 vs 3) are recorded in Decisions. No dependency,
        feature, or ceiling change.
      artifacts: [crates/repark-iceberg/src/write/writer_props.rs]
    - id: AT-9
      status: N/A
      justification: No new alarmable path: writer-build errors propagate through the
        existing DataFusion error channel; invalid modes warn through the fork's
        existing tracing.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first 3/18 pre-fix, 18/18 after; mutation A reds none+counts,
        mutation B reds exactly the C-004 pin; full package 599 green. Every added
        branch (four with_metrics_config links, the fork-helper switch, the
        metrics_config_for helper) changes output on a named pin.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/writer_metrics.rs]
  complete: true
```
