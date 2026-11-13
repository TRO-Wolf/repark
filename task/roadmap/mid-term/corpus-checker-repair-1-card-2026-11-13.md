# Card CORPUS-CHECKER-REPAIR-1: the corpus checkers compare every outcome

**Date:** 2026-11-13. **Status:** FIXED by this unit (test code only). Built by `claude-opus-5-5` as the second builder, under these rulings:
- owner, 2026-11-11, "Go with the defaults", Q1 and Q10: "The checker pins RePark's text per divergence";
- owner, 2026-11-03: D4, D7 and D8 are stated label exclusions;
- the orchestrator's ruling of 2026-11-13: the five checksum controls and D5 are declared classes.

**Why:** the Hadoop corpus checker accepted RePark outcomes it never compared. Its `?` arm let any refusal pass on 126 rows, it skipped 492 rows and it never read 1994 rows. The same patterns sat in the staged-publish, nested and dq readers.

## Inventory on main `168dd934`

The repaired readers:
- **`crates/repark-core/src/iceberg_path/corpus_cells_tests.rs`**, over `docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl`, 3904 rows:
  - the `?` arm on `spark_missing_version` (126 hint_zero rows, D5);
  - the `crc_kept` skip (20 rows);
  - the codec-by-name skip (472 rows);
  - only `path_select` was read; `path_insert` (1910) and the catalog heads (42 `catalog_select` and 42 `catalog_show_tables`, half on each door) were not.
- **`staged_publish_cleanup.rs`, in repark-spark and repark-sql:** 9 rows skipped per door, 18 in all (6 `kill_before_publish` and 3 `metadata_rename` each). The refusal branch read neither the class column nor the Spark column.
- **`crates/repark-spark/src/tests/map_key_alter.rs`:** a silent `return` when the nested cells file is absent. **`subscript_dot_access.rs`** had the same return.
- **`python/repark/tests/sql/test_dq_identifiers_config_refuse_1.py`:** a missing re-audit file gave `[]` and then a `pytest.skip`.

Findings, not repaired here; each wants its own card or unit:
- **`crates/repark-spark/tests/struct_compare_seat_family.rs`:**
  - Up to 175 sql `known` cells are not compared: rows cells pass on any error, and 59 error cells are never compared.
  - 6 DML cells pass on any error.
  - 24 gap cells skip the class check.
- **`python/repark/tests/sql/test_struct_compare_seat_family_1.py`:**
  - 24 known-error and 148 known-rows frame cells are not compared (an error passes).
  - 19 of 24 name cells do not compare Spark's name.
- **`test_struct_compare_by_position_1.py`:** the 219 `refused` rows of `struct_compare_kind_cells.jsonl` accept any error that is not a panic.
- **`crates/repark-spark/tests/struct_compare_by_position.rs`:** 56 KNOWN keys, with no class compared and no count pinned. The `op` and `types` columns are never read.
- **`test_ident_dialect_1.py`:**
  - 524 measured rows are compared with the recorded RePark column, not with Spark.
  - 504 rows compare only the error condition.
  - The `class` and `severity` columns are never read.
- **`test_signed_zero_normalize_1.py`:** 8 names-only rows, with no pinned count.
- **`python/repark/tests/parity/test_flat_matrix_cells.py`:**
  - 216 native rows are dropped.
  - The compared count is a floor (`>= 200`), not `== 720`.
- **`python/repark/tests/timestamps/test_write_spine_1_doors.py`:** skips when the replay document is absent. It is absent today, so the 52 NLS-1 moves are not compared.
- **`python/repark/tests/writes/test_empty_dml_shared_guard_1.py`:**
  - Spark's `snapshot_changed` is not compared on 20 cells.
  - 4 cells that Spark refused are asserted as no-ops.
  - The counts check can pass vacuously.
- **`subscript_dot_access.rs`:** a both-refuse arm accepts any refusal text.
- **Replay exclusions with no stated reason:** `nested_assign_oracle.rs` (23 cases) and `replace_where_oracle.rs` (134 cases).

## What each check now compares

**`corpus_cells_tests.rs` (rewritten).** All 3820 path rows are read, with no skip and no default arm.
- `path_select` compares Spark's exact rows (959 rows) or Java's exact missing-version body (348 rows).
- Every other row compares RePark's exact class and text (or rows) against its golden in `docs/corpus/hadoop-discovery-repark-goldens-2026-11-13.jsonl`, with the golden's card. Per-card counts:
  - D4: 472;
  - D5 HADOOP-HINT-ZERO-TEXT-1: 126;
  - D7 ICE-WRITE-DF-SAVE-1: 1910;
  - C-005 checksum control: 5.
- A golden must be used, must not restate Spark's outcome, and must match exactly.
- `path_insert` replays the two facade calls `save_target_names_table` and `plan_table_write`. It asserts the refusal, that the fixture tree is byte-identical, and that the recorded `repark_changed` is empty.

**`crates/repark-spark/tests/hadoop_discovery_catalog_cells.rs` (new).** All 84 catalog rows run on the Spark door and the plain door, against D8 HADOOP-FACADE-FRESH-SESSION-1 goldens, with the warehouse unchanged. A probe on the release wheel gave the same four texts as the Rust replay.

**Staged publish, both crates.** All 39 rows per door are pinned.
- `kill_before_publish`: the statement is parked at the publish entry and dropped there, which stands in for SIGKILL.
- `metadata_rename`: the next version file is written in place.
- The D2 refusal reads the class column and compares the Spark column exactly.
- Success rows compare the Spark column exactly.

**Missing files.** A missing cells or goldens file fails in every repaired reader. The dq count is pinned at 11.

**Negative controls.** A wrong value, a wrong text, a changed class, a missing golden, a wrong card and a state change are each fed through the real comparison, and each is rejected.

## Finding list (rows that differ from Spark on main)

- **No S1 or S2 row.** No panic, and no replayed row changed state.
- **Declared, pinned as goldens:**
  - D4, 472 rows: RePark answers where Spark refuses on the codec. 460 rows give `[3, 4]`. 12 rows (`*/gap/hint_stale_first`) give `[2, 3]`, the version the same walk picks for honest names.
  - D5, 126 rows.
  - D7, 1910 refusals with no state change.
  - D8, 84 refusals.
- **Five checksum controls, an S3 over-refusal kept as a control (ledger C-005):**
  - `sp_plain/as_written/none/hint_past/crc_kept` (version 10)
  - `sp_gzip/as_written/none/hint_past/crc_kept` (10)
  - `sp_mixed/as_written/none/hint_past/crc_kept` (11)
  - `sp_cleanup/as_written/none/hint_stale_first/crc_kept` (1)
  - `sp_cleanup/as_written/none/hint_past/crc_kept` (10)

  On each, Spark reads `[3, 4]` and RePark refuses with `AnalysisException: Metadata file for version N is missing under <root>/metadata`. Spark reads because Hadoop's checksum check fails on the stale `.version-hint.text.crc`, so Spark falls back to the listing. The same fixtures without the sidecar refuse on both sides with the same text.
- **D5 text drift (S4).** RePark's zero-hint text is now `no Iceberg table found at '<root>': expected metadata files under '<location>/metadata' or a '<location>/metadata/version-hint.text'`. It prints a literal `<location>` placeholder. The HADOOP-HINT-ZERO-TEXT-1 card still quotes the old `names 'v0.metadata.json'` text.
- **Catalog door drift since the pilot (S4; the refusal is kept, so still D8):**
  - The facade `SHOW TABLES` text now begins `Error during planning: `.
  - The native `SHOW TABLES IN` is now `UnsupportedOperationException: This feature is not implemented: SHOW TABLES IN not supported`. The pilot recorded `AnalysisException: NamespaceNotFound`.

## Execution tier

Check mode: recorded goldens, no JVM, every PR. The Rust replay takes 29.9 s for the 3820 path rows and 0.6 s for the 84 catalog rows (debug build).

**Retires:** retired by this unit. Each finding above that names a reader is a lead for its own card.

## Verifier observations (#1322, appended 2026-11-13; all S4)

#1322's verifier returned PASS with no finding at S1, S2 or S3, and no golden records a wrong read or write as
declared. It merged as `16974986` on the owner's ruling of 2026-11-13
([rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), items 1 and 6, which confirm the C-005
controls and D5 as declared classes). Its six observations, each S4:

- **O1. The `path_insert` state check is vacuous.** The replay stops at `plan_table_write`, so nothing could write;
  "no state change" rests on the plan refusal. The facade does stop there for a path target, and the verifier's
  real-facade probe saw no change on 10 D7 appends.
- **O2. The native D1 staged rows check presence only.** They assert that a failure was seen, not RePark's failure
  text, so a change of the D1 text would pass. The Spark column is compared exactly.
- **O3. Cards are not checked in code.** A golden's card is a free string; only the count per card is pinned. The
  verifier's data audit found every golden on the right fixture class.
- **O4. D7 covers more than divergences.** The D7 label sits on all 1910 appends, including 126 rows where Spark
  refuses with the same `TABLE_OR_VIEW_NOT_FOUND`. Over-broad, not a mis-declaration. D7 names a registry row
  (ICE-WRITE-DF-SAVE-1), not a card file, and D4 has no card id.
- **O5. The builder's door probe ran on an empty metadata directory.** The verifier's probe on populated fixtures
  closed that gap for the 12 sampled catalog rows.
- **O6. D5 is pinned against an open card.** The D5 golden carries RePark's literal `<location>` placeholder, while
  the HADOOP-HINT-ZERO-TEXT-1 card quoted the old text; that card's quote is updated (2026-11-13).
- **Records note.** The `iceberg_path/map.md` row dropped the historical note "Red on `05d0437d` with the pilot's
  600 recorded mismatches"; the sha survives in the hadoop-discovery-rule-1 ledger.
