# Charter ledger — ICE-ORC-AVRO-1 · ORC and Avro Iceberg data files (IPI-41 RePark half)

**Date:** 2026-09-22 · **Branch:** `fix/ice-orc-avro-1` · **Base:** `origin/main`
`a6d4c0db` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Plan packet `ipi-41-orc-avro.md` §1 names 13 inventory cells Spark
answers while RePark refuses them under `ICE-WRITE-OPTIONS-ORC-AVRO` / `IO-ORC-1`.
The fork half landed the `AnyFileWriter` seam (fork#344) and the RP-47 carrying bump
re-pinned the `iceberg*` family `311b9fa4` → `604edca0`. This ledger proves the
RePark half: the `write-format` / `delete-format` options and the
`write.format.default` / `write.delete.format.default` properties resolve to a real
format, and every owned builder site writes it.

**Not in this unit:** the plain-file ORC writer (`DataFrameWriter.orc(path)` stays
`NOT_IMPLEMENTED` under `IO-ORC-1`, T-8); fork ORC nested-type reads (typed refusal
pinned at C-019, fork follow-up); `STATUS.md`; `claims.txt`; any pin value.

## Rulings recorded at open

- R-ORC-1 (orchestrator, with evidence, 2026-09-22): C-005/C-006 are SERVED by this
  change through the `write-format` option door, not a fork nested-read limit.
  Evidence: WO2b `out.jsonl` shows them failing pre-WO3a with the exact refusal text
  WO3a removed (`write-format "orc"/"avro" has no RePark Iceberg writer`), and the
  S6 tests drive `.option("write-format", …).saveAsTable`. The old "fork-limit
  residue, never fix" line for C-005/C-006 is retired with the registry row.
- R-ORC-2 (packet T-8): `IO-ORC-1` keeps its plain-file writer refusal;
  `test_io_orc_1.py` and `facade_orc_oracle.json` are untouched. Only the Iceberg
  rows retire here.
- R-ORC-3 (packet D-5): precedence is per-write option > table property > parquet
  for data, and v3 delete side is PUFFIN before any setting (M-3). Avro keeps empty
  column metrics (M-6); adding metrics there is a regression, not an improvement.

## Design reasoning

The option was parsed and then dropped: `StatementWriteOptions.write_format` was set
and never read, and `WriterStagingOverrides` had no format field. WO1 added the two
staging fields plus `resolve_data_format` / `resolve_delete_format` in the new
`write/data_format.rs`; WO2a/WO2b routed the five owned builder sites (unpartitioned
staging, partitioned fanout, partitioned lineage, unpartitioned MERGE, position
deletes) and deleted the four real format gates; WO3a opened `validate_write_format`
to orc/avro and inverted the three refusal pins. Several deleted "gates" were
filename readers, not gates: they already fed the table format to
`DefaultFileNameGenerator` while the builder stayed Parquet, which would have
written Parquet bytes under an `.orc` name. The version gates (`format_version < V2`
refusing merge-on-read) are untouched; they guard versions, not formats.

## PROPOSITION LEDGER — ICE-ORC-AVRO-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `D-CREATE-FMT-ORC`: table with `write.format.default='orc'`, INSERT reads back `[[1,a],[2,b]]` with one ORC file. | `test_ice_orc_avro_1.py::test_create_format_orc_insert_reads_back`. | PROVEN | Rows plus `file_format = [["ORC", 1]]`; S6 22P both legs (WO3a). |
| C-002 | `D-CREATE-FMT-AVRO`: same with `'avro'`, one AVRO file. | `test_ice_orc_avro_1.py::test_create_format_avro_insert_reads_back`. | PROVEN | Rows plus `file_format = [["AVRO", 1]]`; S6 22P both legs (WO3a). |
| C-003 | `D-X-SET-FORMAT-ORC-THEN-INSERT`: ALTER to orc, next INSERT lands ORC beside the old PARQUET file. | `test_ice_orc_avro_1.py::test_set_format_then_insert_writes_new_format`. | PROVEN | 2 append snapshots, second data file ORC, first PARQUET; S6 22P (WO3a). |
| C-004 | `TP-FORMAT-ORC`: property table `t.files` reads `[[0, "ORC", 2, 3]]`. | `test_ice_orc_avro_1.py::test_tp_format_orc_files_row`. | PROVEN | Exact `t.files` row; S6 22P both legs (WO3a). |
| C-005 | `W-DF-OPT-WRITE-FORMAT-ORC`: `.option("write-format","orc").saveAsTable` lands ORC bytes, not parquet. | `test_ice_orc_avro_1.py::test_write_format_option_orc`. | PROVEN | 5 rows, 2 snapshots, `["ORC", 2]` in `.files`; R-ORC-1 rules this the option door (WO3a). |
| C-006 | `W-DF-OPT-WRITE-FORMAT-AVRO`: same with `avro`. | `test_ice_orc_avro_1.py::test_write_format_option_avro`. | PROVEN | 5 rows, 2 snapshots, `["AVRO", 2]` in `.files`; R-ORC-1 (WO3a). |
| C-007 | `W-FMT-ORC-DELETE`: copy-on-write DELETE on an ORC table rewrites ORC, never Parquet. | `test_ice_orc_avro_1.py::test_cow_delete_on_orc_rewrites_as_orc`. | PROVEN | Rows after DELETE, every live `content = 0` file ORC; S6 22P (WO3a). |
| C-008 | `W-FMT-ORC-UPDATE`: copy-on-write UPDATE on an ORC table rewrites ORC. | `test_ice_orc_avro_1.py::test_cow_update_on_orc_rewrites_as_orc`. | PROVEN | Rows after UPDATE, every live data file ORC; S6 22P (WO3a). |
| C-009 | `W-FMT-ORC-MERGE`: copy-on-write MERGE on an ORC table rewrites ORC. | `test_ice_orc_avro_1.py::test_cow_merge_on_orc_rewrites_as_orc`. | PROVEN | Rows after MERGE, every live data file ORC; S6 22P (WO3a). |
| C-010 | `W-FMT-AVRO-DELETE`: copy-on-write DELETE on an AVRO table rewrites AVRO. | `test_ice_orc_avro_1.py::test_cow_delete_on_avro_rewrites_as_avro`. | PROVEN | Rows after DELETE, every live data file AVRO; S6 22P (WO3a). |
| C-011 | `W-FMT-AVRO-UPDATE`: copy-on-write UPDATE on an AVRO table rewrites AVRO. | `test_ice_orc_avro_1.py::test_cow_update_on_avro_rewrites_as_avro`. | PROVEN | Rows after UPDATE, every live data file AVRO; S6 22P (WO3a). |
| C-012 | `W-FMT-AVRO-MERGE`: copy-on-write MERGE on an AVRO table rewrites AVRO. | `test_ice_orc_avro_1.py::test_cow_merge_on_avro_rewrites_as_avro`. | PROVEN | Rows after MERGE, every live data file AVRO; S6 22P (WO3a). |
| C-013 | M-1: v2 merge-on-read DELETE on an ORC table writes an ORC position-delete file. | `test_ice_orc_avro_1.py::test_mor_delete_on_orc_writes_orc_delete_file`. | PROVEN | `.files` reads `[[0, ORC, 3], [1, ORC, 1]]`; S6 22P (WO3a). |
| C-014 | M-2: `write.delete.format.default=parquet` overrides data ORC on the delete side. | `test_ice_orc_avro_1.py::test_delete_format_default_overrides`. | PROVEN | `.files` reads `[[0, ORC], [1, PARQUET]]`; S6 22P (WO3a). |
| C-015 | M-3: v3 merge-on-read DELETE writes a PUFFIN side for parquet, orc and avro data. | `test_ice_orc_avro_1.py::test_v3_delete_side_is_puffin_for_every_data_format`. | PROVEN | `[[0, <FMT>, 3], [1, PUFFIN, 1]]` for all three formats; S6 22P (WO3a). |
| C-016 | `W-READ-FOREIGN-ORC`: rows read back from an ORC table. | `test_ice_orc_avro_1.py::test_read_foreign_orc_table`. | PROVEN | `[[1,a,x],[2,b,y],[3,c,x]]`; closed for free with the write (WO3a). |
| C-017 | M-5: ORC files carry full column metrics, `nan_value_count` only on float/double. | `test_ice_orc_avro_1.py::test_orc_metrics_match_spark`. | PROVEN | `readable_metrics` per column match the recorded Spark values; S6 22P (WO3a). |
| C-018 | M-6: Avro files carry no column metrics. | `test_ice_orc_avro_1.py::test_avro_metrics_are_empty`. | PROVEN | Every `readable_metrics` field NULL; S6 22P (WO3a). |
| C-019 | ORC primitives round-trip; nested columns pin the fork reader's typed refusal. | `test_ice_orc_avro_1.py::test_all_types_round_trip_orc`. | PROVEN | 10 primitives read back exact; ARRAY/MAP/STRUCT refuse with `ORC data-file read of nested type for field '<name>'`; fork read-nested follow-up inverts those arms. |
| C-020 | M-7: all 13 probe types round-trip through Avro. | `test_ice_orc_avro_1.py::test_all_types_round_trip_avro`. | PROVEN | Full wide row reads back exact; S6 22P (WO3a). |
| C-021 | Unknown `write-format` refuses with Java's `Invalid file format: <name>` shape. | `test_ice_orc_avro_1.py::test_unknown_write_format_refuses`. | PROVEN | `write-format = csv` refuses, snapshot count stays 1; S6 22P (WO3a). |
| C-022 | Compaction keeps the table format on an ORC table. | `test_ice_orc_avro_1.py::test_compaction_keeps_table_format`. | PROVEN | `rewrite_data_files` leaves ORC files; S6 22P (WO3a). |

## 1. Red-first record

WO1 committed the S6 battery red-first: 11 red by design (the owned-door refusals)
and 11 green via the fork read path, per the packet §6 shape. WO2a/WO2b routed the
builder sites; WO3a inverted the three refusal pins (Rust `expect_err("orc")` unit
test plus `FORMAT-02/03` → `test_write_format_orc_writes` /
`test_write_format_avro_writes` with exact-suffix plus `file_format` asserts per
T-4). Final battery: `test_ice_orc_avro_1.py` 22P offline and 22P live
(`REPARK_PARITY_LIVE=1`, no live-conditional code); `test_ice_write_options_1.py`
68P/1S offline and 69P live; `test_io_orc_1.py` 50P untouched (T-8).

## 2. Gates at assembly

Rebase onto `a6d4c0db`: 17/17 subjects identical in order, merge-base == origin,
branch fork pin still `604edca0`, `comment_ban.py` hits=0. Registry retires
`ICE-WRITE-OPTIONS-ORC-AVRO` to SERVED 2026-09-22; `IO-ORC-1` keeps its plain-file
writer refusal with one round-trip sentence. Lint battery at the final head:
`make rust-clippy` 0, `make rust-panic-ban` 0, `scripts/check_rust_file_size.py`
green, comment-ban hits=0.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-orc-avro-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the packet cells (D-*, TP-*, W-*, M-*) and the R-ORC-1/R-ORC-2/R-ORC-3 rulings; the C-005/C-006 fork-limit line retired with evidence, not consensus.
      artifacts: [task/ledgers/staging/ice-orc-avro-1-ledger.md, python/repark/tests/test_ice_orc_avro_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: All 13 inventory cells plus the M-1/M-2/M-3 delete-format cells, the M-5/M-6 metrics cells, the M-7 wide-type cells, the D-5.1 refusal cell and the compaction cell, each asserting rows and file_format or files rows, not rows alone.
      artifacts: [python/repark/tests/test_ice_orc_avro_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The workspace clippy gate (-D warnings, disallowed-methods) is green at the assembly head; no unwrap/expect added by the 17-commit stack.
      artifacts: [crates/repark-iceberg/src/write/data_format.rs, crates/repark-spark/src/write_options.rs]
    - id: AT-4
      status: N/A
      justification: Format resolution is a pure function of the per-write option, the table properties and the format version; no shared mutable state crosses the write doors.
    - id: AT-5
      status: N/A
      justification: Local catalog writes only. No credential, no network, no secret-bearing option is logged or forwarded by the format routing.
    - id: AT-6
      status: ATTACKED
      evidence: Red-first throughout: WO1 11 red by design, WO3a step-1 red proof (the orc Rust pin failed after the arm opened, before the invert), and the T-4 file_format asserts that would pass on silent parquet without them.
      artifacts: [python/repark/tests/test_ice_orc_avro_1.py, python/repark/tests/test_ice_write_options_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: WO3a receipts on the release module: S6 22P offline and 22P live, write_options 68P/1S offline and 69P live, Rust write_options 21P, clippy/panic-ban/size/comment-ban green, facade T-8 50P untouched; assembly re-runs the lint battery at the final head.
      artifacts: [python/repark/tests/test_ice_orc_avro_1.py, python/repark/tests/test_ice_write_options_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The packet's Spark facts (M-1..M-8 from Spark 4.1.2 + Iceberg 1.11.0, HadoopCatalog) were replayed, not assumed: delete-format precedence, v3 PUFFIN, ORC metrics shape, Avro empty metrics and the exact-suffix rule each carry a pin.
      artifacts: [python/repark/tests/test_ice_orc_avro_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry ICE-WRITE-OPTIONS-ORC-AVRO rewritten to SERVED with the new pin names and the C-005/C-006 ruling; IO-ORC-1 keeps its plain-file writer refusal; every error shape pins Spark's class and message.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Behaviour-move guards run: the filename-reader sites (fanout, lineage) route their builders, not just their names, so no Parquet bytes land under an .orc name; the version gates stay to guard v1 merge-on-read.
      artifacts: [crates/repark-iceberg/src/write/append_fanout_serial.rs, crates/repark-iceberg/src/write/merge/row_lineage.rs, crates/repark-iceberg/src/write/predicate_dml.rs]
  complete: true
```

VERDICT: 22 clauses, 22 PROVEN, 0 OPEN, 0 REJECTED.
