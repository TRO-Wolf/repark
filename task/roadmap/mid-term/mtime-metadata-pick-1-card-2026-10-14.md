# Card MTIME-METADATA-PICK-1: remaining metadata-file picks by modification time share the fixed flake shape

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Retires:** when every listed use below is converted to the version-number pick and the table reads converted on all rows.

## Why

Commit `f8f1d252` fixed one test that picked the latest Iceberg metadata file by
modification time (`st_mtime`) instead of by version number: `_assert_meta` in
`python/repark/tests/resolution/test_casesens_1.py` now sorts by the `_metadata_version` helper,
which parses the integer version from the file name (the digits after the `v` prefix,
before the first `.` or `-`). Before the fix the case passed 5 of 10 runs alone;
after, 30 of 30. Two metadata files written within one timestamp tick order
arbitrarily under `st_mtime`, so every remaining mtime pick is the same flake shape.

## Known facts

- The fixed shape is a test-side pick of the newest `*.metadata.json` by clock
  instead of by the version number the file name already carries. The product never
  reads metadata this way; only the tests do.
- The inventory below was read by `grep -n "st_mtime" python/repark/tests/*.py` on
  this tree. Each row says whether the use picks a metadata file (the flake shape,
  yes) or does something else (no, with what it does instead).
- Unmeasured: which of the yes rows have actually flaked, and how often. The card
  converts on shape, not on observed failures.

## Inventory

| Use | Picks a metadata file |
|---|---|
| `python/repark/tests/iceberg/_record_branch_ops_1.py:562` reads the newest file for the `next-row-id` recording step | yes |
| `python/repark/tests/timestamps/_record_ntz_1_spark_table.py:116` reads the newest file for the recorded schema cell | yes |
| `python/repark/tests/_sql_harden_cutover_run.py:145` (`_latest_metadata`, keyed by `st_mtime` then name) returns the newest metadata pointer | yes |
| `python/repark/tests/iceberg/test_ice_branch_ops_1.py:258` returns the newest metadata file for the table | yes |
| `python/repark/tests/iceberg/test_ice_branch_ops_1.py:408` (`_newest_metadata_file`) returns the newest metadata file | yes |
| `python/repark/tests/iceberg/test_ice_branch_schema_1.py:240` picks the newest Spark-written metadata file to register | yes |
| `python/repark/tests/iceberg/test_ice_column_reorder_1.py:73` (`_metadata_version_number`, keyed by version then mtime) | no, the version number already decides and mtime is only the tiebreak |
| `python/repark/tests/iceberg/test_ice_column_reorder_1.py:560` takes the max file by mtime alone | yes |
| `python/repark/tests/iceberg/test_ice_procedures_1.py:526` reads the newest file's properties | yes |
| `python/repark/tests/iceberg/test_ice_procs_route_1.py:125` (`table_dir`, max over each directory's newest file mtime) | no, it picks a table directory, not a metadata file |
| `python/repark/tests/iceberg/test_ice_procs_route_1.py:135` (`table_metadata`) parses the newest metadata file | yes |
| `python/repark/tests/iceberg/test_ice_procs_route_1.py:143` (`newest_metadata_name`) names the newest metadata file | yes |
| `python/repark/tests/iceberg/test_ice_replace_columns_1.py:105` parses the newest metadata file | yes |
| `python/repark/tests/writes/test_ice_write_df_2.py:127` (`_metadata`, sorted by mtime then name) parses the newest file | yes |
| `python/repark/tests/timestamps/test_ntz_1.py:175` parses the newest metadata file for the current schema | yes |
| `python/repark/tests/iceberg/test_partname_1.py:52` parses the newest metadata file | yes |
| `python/repark/tests/timestamps/test_u9_types_1.py:92` parses the newest metadata file | yes |
| `python/repark/tests/iceberg/test_v3_statement_coverage.py:163` (`_latest_metadata`, keyed by `st_mtime` then name, feeds `register_table`) returns the newest metadata pointer | yes |

## The ask

- Add one shared helper that picks the newest `*.metadata.json` by the version
  number in the file name, following `f8f1d252`'s `_metadata_version`, and convert
  every yes row to it. The two no rows need no change; their shape is recorded here
  so nobody re-litigates them.
- Keep the per-file size ceilings in mind: a helper that would push a test module
  past its ceiling belongs in a sibling module, with a map row, rather than under a
  raised ceiling.
- Each conversion keeps the test's recorded answers identical; a changed answer is a
  finding, not a conversion.
