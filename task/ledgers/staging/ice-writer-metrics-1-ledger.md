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
  keeps RePark's `parse_compression` validation, then builds through the fork helper.
  (Recorded step 3.)
- **Column sizes are not compared.** They ride Parquet encoding choices, not the metrics
  config; C-003 pins counts, null counts, NaN counts, bounds presence/absence and truncation
  width.

## PROPOSITION LEDGER — ICE-WRITER-METRICS-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every RePark Parquet data-file writer applies `MetricsConfig::for_table` of the table it writes: the INSERT path (`write_options.rs`), the fan-out append path (`append_fanout_serial.rs`) and the CoW/MERGE rewrite path (`merge/mod.rs`, `merge/row_lineage.rs`) each have a pin. | `merge/tests/writer_metrics.rs` + `writer_metrics_truth.json` fixture. | OPEN | Ledger skeleton (step 1). |
| C-002 | The position-delete writer applies `MetricsConfig::for_position_delete_table`. | `merge/tests/writer_metrics.rs`. | OPEN | Ledger skeleton (step 1). |
| C-003 | Each of the twelve recorded metrics cells: after a RePark INSERT the manifest's per-field metrics equal Spark's recorded cell; an unmatchable cell is a dated declared divergence with a registry row and a strict xfail, never silent. | `merge/tests/writer_metrics.rs` replays all twelve cells. | OPEN | Ledger skeleton (step 1). |
| C-004 | RePark position-delete files carry the delete-type key/value metadata and untruncated `file_path` bounds, proven by reading back a written delete file's Parquet key/value metadata and its column bounds. | `merge/tests/writer_metrics.rs`. | OPEN | Ledger skeleton (step 1). |
| C-005 | Mutation proof: removing `with_metrics_config` reds the `none` and `counts` pins; restoring RePark's hand-built delete properties reds the C-004 pin. | Ledger red-run record, reverted. | OPEN | Ledger skeleton (step 1). |

## Red runs

(step 2 records the red-first run here)

## Mutations

(step 5 records both C-005 mutations here)
