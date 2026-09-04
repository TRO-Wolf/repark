# map — repark-spark/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Lib-root unit battery for the Spark SQL door. Production code is not here: only tests, shared
fixtures, and the module manifest.
Test documentation may retain model provenance; code-quality grade tags stay outside code.

## Contents

- `mod.rs` — pure module manifest (`mod common;` + one `mod` per leaf).
- `v3_upgrade_calls.rs` — **V3-10:** the catalog-call budget for `ALTER … SET TBLPROPERTIES`,
  counted through a wrapper registered into BOTH the catalog registry and the DF provider: an
  upgrading ALTER is (2 `load_table`, 0 `list_tables`, 0 `namespace_exists`) — one load for the
  resolve and one inside the fork's commit CAS — where the namespace-re-registering version was
  (3, 1, 2); an ordinary property ALTER is (2, 0, 0) unchanged and a same-version request is
  (1, 0, 0). The same test then reads `_row_id` through the session, which is what makes the
  removal safe rather than merely cheaper.
  pins: v3-10-upgrade-v2-to-v3/C-003, C-006
- `v3_upgrade.rs` — **V3-10:** the in-place v2 → v3 upgrade on the Spark door — opt-in gate and
  its without-opt-in twin, downgrade / `'1'` / `'-1'` / `'0'` / `'4'` / `'x'` / `''` / `'3.0'` /
  `' 3 '` refusals in Spark's own two classes, same-version no-op,
  upgrade beside another key as ONE commit, and the post-upgrade v3 paths (append lineage, COW
  DELETE/UPDATE, MoR MERGE deletion vector, `rewrite_data_files`, `register_table`) at live-Spark
  values, plus a **v1** table upgrading straight to v3 behind the same opt-in and a
  **partitioned** v2 table whose append takes Spark's exact `1→2 2→3 3→4 4→0 5→1` map. That pin
  asserted only the id SETS while `F-v3-10-partition-file-order` was open — the fork's
  `FanoutWriter` drained a `HashMap`, so the map flapped; **RP-8 (2026-09-03)** consumes fork
  F-20 (`#261`), which drains ascending, and the map is Spark's in 12 of 12 runs. The legacy-parquet-position-delete cells moved to
  `v3_legacy_delete.rs` in V3-12; the shared helpers (`seed_mor_four`, `merge_delete_sql`,
  `upgrade`, `lineage`, `refuse`, `walk_puffin`) are `pub(super)` for that sibling.
  The V3-2 control `create_table.rs::or_replace_applies_requested_v3_and_alter_upgrades_with_opt_in`
  is this unit's too: its ALTER arm flipped from refuse to upgrade, so it no longer carries
  v3-2-create-v3-opt-in/C-008 (V3-10 negates that clause) and cites C-005 alone.
  pins: v3-10-upgrade-v2-to-v3/C-001, C-003, C-004, C-005
  pins: rp-8-repin-f21-f22/C-004
- `v3_legacy_delete.rs` — **V3-12:** the Spark-SQL-door cells for a v3 merge-on-read write over an
  upgraded table's legacy parquet position deletes. Seven merge cells (MERGE-DELETE and the append
  after it, UPDATE, subquery DELETE, two legacy deletes on one data file, an untouched sibling
  keeping its own, copy-on-write leaving it alone). Spark merges **every** applicable live position
  delete that names a touched data file and removes **only the file-scoped** ones; it COMMITS the
  covering-two-files shape and leaves that delete live forever.
  **RP-8 (2026-09-03):** at pin `c1d6c9de` the engine does too, so the two loud refusals became
  merge cells at Spark's measured values. `a_plain_where_merge_on_read_delete_over_a_legacy_delete_merges_into_the_dv`
  and a new UPDATE twin pin the A2 outcome for the spellings that plan through the fork's own
  delete exec (`V3-UPGRADE-DV-PLAIN-1` FIXED); `a_partition_scoped_legacy_delete_merges_and_keeps_the_parquet_live`
  pins §12's P2 and P4 — the parquet delete stays LIVE beside one DV of `record_count` 2 per
  touched data file, rows `[(4,'d')]` then `[(9,'z')]` (`V3-UPGRADE-DV-PART-1` FIXED). The
  removal rule is unchanged and still the load-bearing half: removing a delete that covers two
  data files would resurrect the untouched sibling's deleted row. Both cells carry an ANSI and a
  facade twin (row per entry point), and the facade twins gained live Spark comparisons.
  Two branch cells close `V3-DV-BRANCH-1`: a second MoR DELETE on a diverged branch must merge
  the BRANCH's own DV (red before V3-12 — the close read `main`, wrote a fresh DV, and the commit
  door refused with "already carries a live deletion vector"), and a legacy parquet delete that
  exists only on a branch merges there. **RP-8 (2026-09-03):** both stay green at `c1d6c9de`
  with no source change — the scanned `snapshot_id` still reaches the close, and now reaches the
  legacy collect INSIDE it, so the branch's own legacy deletes are the ones merged.
  `branch_delete_files` reads `snapshot_for_ref`, not the current snapshot, so a pin that
  passes by reading `main` is not available to it.
  pins: rp-8-repin-f21-f22/C-005
  `measure_legacy_walk_cost` is the `#[ignore]`d before/after cell for the legacy-delete manifest
  walk: it seeds one delete manifest per commit at `commit.manifest-merge.enabled = false` and
  times one more MoR DELETE that finds ZERO candidates, so it isolates the walk from the read.
  RP-8 re-measured it against the F-21/F-22 fork — 8 manifests 337/346/351 ms before against
  329/332/315 ms after, 48 manifests 1.522/1.489/1.459 s against 1.459/1.479/1.451 s: **no
  measurable change**, because the delete-manifest walk RePark stopped making is paid back by
  F-22's always-on data-manifest walk. It is not a wall-clock CI pin.
  **RP-9 (2026-09-03):** `measure_pure_dv_close_cost` is the `#[ignore]`d pure-DV cell (N data
  manifests, 0 legacy deletes, production `DELETE` so the scan supplies a complete map) at 8 /
  48 / 192. Statement-wall medians in the RP-9 ledger; no wall-clock CI pin. The skip itself is
  the hide-and-succeed pin in `dv_close.rs`. Round 2: after `try_allowed_plain_identity` the
  same cell is the real Spark `DELETE WHERE id = 0` path; close-phase opens are zero (hide
  pin). PERF-SCAN-1 r2 strace of this cell: scan-to-puffin 1 × N data-manifest opens,
  close 0, commit 1 × N (`PERF-DVCLOSE-STMT-1`). The RP-9 3 × N scan-phase claim is not
  reproduced on this path. `PERF-SCAN-3PASS-1` stays BACKLOG.
  pins: v3-12-legacy-delete-merge/C-001, C-002, C-003, C-004, C-005, C-006, C-007
  pins: rp-8-repin-f21-f22/C-002, C-003
  pins: rp-9-repin-f23/C-003, C-005
  **RP-10 (2026-09-04):** the same `#[ignore]`d cell is the before/after wall for F-25.
  pins: rp-10-repin-f25/C-003
  pins: perf-scan-1-plan-once/C-003
- `v3_row_order.rs` — **V3-11 (2026-09-02):** same-commit data-file order. The ten-run pin
  `mor_merge_insert_takes_sparks_row_id_in_ten_consecutive_runs` replays the LIVE-v3 sequence
  ten times and requires Spark's exact `_row_id = 11` each time (it read 10 or 11 at random
  before — 24 red of 30 over three batteries). Two pins carry Spark-parity claims because the
  two rules coincide on their partition sets —
  `mor_merge_across_three_partitions_numbers_files_ascending_by_partition_value` and
  `partitioned_ctas_numbers_files_ascending_by_partition_value`; they are named for the rule
  they prove, not for Spark, because Spark's order is a `HashMap` bucket artefact
  (`V3-FILEORDER-1`). Three are **engine-behaviour** pins with no Spark claim at all:
  `a_null_partition_slot_is_numbered_first_whatever_order_it_arrives_in` (Spark answers
  `0, NULL, 1` on one of its three arrival orders — null and int `0` share a bucket),
  `a_two_field_spec_orders_lexicographically_in_spec_field_order` and
  `transform_partitions_order_by_the_transformed_value_ascending` (`truncate`, `bucket`,
  `days`; only the `bucket` arm matches Spark). Mutations: dropping the sort reddens all six;
  reversing it reddens the three value-order pins; dropping only the `append.rs` call reddens
  the CTAS pin; dropping only the `row_lineage.rs` call reddens the two MERGE pins; ordering
  nulls last reddens the null pin alone; comparing only the first spec field reddens the
  two-field pin alone.
  The byte tripwire `v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched` re-records the
  `crates/repark-sql/src/v3/cow.rs` hash for the two ANSI twins V3-11 adds there, and once more
  in the remediation round that renamed them off `sparks_..._order`; the other three hashes are
  untouched.
  pins: v3-11-row-id-determinism/C-002, C-003, C-006
- `declared_refuse.rs` — **FNP-15/16:** Spark-door parse-altitude refusals for the six
  unreachable names and the sketch family; passthrough attach pin.
  pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `spark_string_literals.rs` — **SQP-1:** the string-literal escape pins (C-001..C-008, C-010,
  C-012): the escape domain, `\'`/unpaired-backslash lexing, adjacency + the DataFusion-native
  `OPTIONS` carve-out, quote-runs-are-not-triple-quotes, raw strings, LIKE/RLIKE/backtick controls,
  exactly-once-on-every-path, the one-caller grep pin, the Generic-dialect honesty pin.
- `cast_binary.rs` — **SQP-1 (C-009):** `CAST … AS BINARY` plans to Arrow `Binary` (B1/B8–B10/B13/
  B15), refuses illegal sources (`DATATYPE_MISMATCH`, B2–B7), keeps `VARBINARY` refusing (B12),
  leaves a `BINARY` DDL column untouched; `TRY_CAST(<int>)` refuses without the ANSI-off suggestion.
- `v3_cow.rs` — v3 UPDATE, sequential COW DELETE, and MERGE matched-update keep `_row_id`
  (V3-7 Spark-equal MERGE lift; RP-6 UPDATE/DELETE). V3-8 replaced the subquery keep-refusal
  with the outside-the-hole control (`UPDATE … NOT IN` refuses without `V3-COW-1` and leaves
  the table unmoved). Sequential COW DELETE keeps the survivor id at next-row-id 6 (single-file
  layout). Branch UPDATE keeps branch lineage and leaves main unmoved. MoR MERGE
  matched-update is Spark-equal (`next-row-id` 4).
  pins: v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002, C-003
- `v3_cow_lift.rs` — RP-6 remaining V3-COW-1 sequences plus V3-7 MERGE shapes: created
  v3 COW/MOR MERGE, matched-DELETE, NOT MATCHED INSERT, NMBS DELETE, mixed MERGE.
  MoR INSERT and NMBS DELETE pinned. Every lifted cell asserts data-file, delete-file,
  and manifest counts (mixed MERGE engine data-file counts are 2 COW / 3 MoR;
  Spark is 1 COW / 2 MoR).
  MoR UPDATE pins `next-row-id` 4 with 2 data files, 1 Puffin DV, 3 manifests at the
  single-file seed. Absolute Spark 4.1.2 + Iceberg 1.11.0 values.
  pins: v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002, C-003
- `v3_subquery_dml.rs` — **V3-8 (2026-09-02):** the V3-COW-1 lift for subquery-`WHERE` COW DML
  on created and adopted v3 — `DELETE … IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and
  `UPDATE … IN`, each pinning rows, `(id,_row_id,seq)`, next-row-id / first-row-id /
  added-rows and the live data-file count at the single-file seed. `F_V3_8_UPDATE_FILES` is
  the named layout artefact: the UPDATE cell writes 2 data files where Spark writes 1.
  Also the correlated-to-target `DELETE` (served, created and adopted), its zero-row
  `s.id = tgt.id + 1` variant (`F-v3-8-empty-delete-snapshot`: the engine commits nothing
  where Spark commits an empty overwrite), and — since **V3-9 (2026-09-02)** — the
  merge-on-read lift control in place of that unit's refusal control: single-property
  `write.delete.mode` / `write.update.mode` MoR tables commit and move lineage.
  pins: v3-8-subquery-where-lineage/C-002; v3-9-mor-predicate-dml-dv/C-003
- `v3_mor_dml.rs` — **V3-9 (2026-09-02):** the `V3-MOR-1` lift. Merge-on-read predicate DML on
  created and adopted v3 — `DELETE` plain / `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and
  `UPDATE` plain / `IN` — each pinning rows, `(id,_row_id,seq)`, next-row-id / first-row-id /
  added-rows, the live data-file count and the single delete entry's format (`Puffin`), content
  (`PositionDeletes`), record count and `referenced_data_file` (file-scoped). Controls:
  `write.delete.granularity = 'partition'` is inert on v3; a v2 MoR table still writes one
  Parquet position-delete file with no `referenced_data_file`; a subquery DELETE matching
  nothing writes no delete file and leaves the seed.
  **RP-9 r2:** `created_v3_mor_plain_where_dml_matches_the_subquery_cell` is the Spark-door
  pin that a three-part `DELETE … WHERE id = 2` now takes the identity path
  (`try_allowed_plain_identity`) rather than the fork delete exec's empty partition map;
  the UPDATE twin stays on the fork.
  pins: v3-9-mor-predicate-dml-dv/C-003, C-004
  pins: rp-9-repin-f23/C-005
- `create_table.rs` — also the V3R-1 type pin: `GEOMETRY` / `GEOGRAPHY` / `VARIANT` refuse at
  CREATE (`V3-GEO-1`); **V3-9:** the `format-version = 3` opt-in refusal names the conf and no
  longer claims merge-on-read is unserved. pins: v3-9-mor-predicate-dml-dv/C-006
- `v3_types.rs` — **V3-6:** C-001 ledger matrix + refuse of `UNKNOWN` / `VARIANT` /
  ADD COLUMN DEFAULT; C-003 opt-in v3 `timestamp_ns` / `timestamptz_ns` CREATE,
  ns Arrow round-trip, v2 refuse (asserts the fork's exact
  "timestamp_ns is not supported until v3" phrase); C-005 `ALTER COLUMN SET DEFAULT`
  refuse.
  pins: v3-6-v3-types/C-001, C-003, C-005
- `v3e4.rs` — **V3E-4:** snapshot refs, `VERSION AS OF` over DVs, expire with
  real work, orphan 24h floor on the partitioned-DV fixture after a RePark
  append. RP-6: live-DV UPDATE commits Spark-equal lineage. V3-7: live-DV MERGE on
  the appended fixture keeps `_row_id`. **V3-9 (2026-09-02):** a MoR subquery `DELETE … IN`
  over the shared-Puffin fixture keeps both siblings' file-scoped DVs live with their
  `referenced_data_file`, record counts 2 and 1 and a real blob offset.
  **RP-7 (2026-09-02):** that cell is re-aimed at Spark's measured layout — two containers, the
  touched blob at offset 4, the sibling `(container, offset, record_count)` tuple unchanged, and
  the six snapshot-summary counts (`removed-delete-files`/`removed-dvs`/`removed-position-deletes`
  1, `added-delete-files`/`added-dvs` 1, `added-position-deletes` 2). Registry `V3-DV-1` is
  **FIXED**. `a_later_single_row_delete_writes_one_blob_not_the_whole_container` holds the byte
  budget at 16 blobs (< 1 KiB), and the `#[ignore]`d
  `measure_later_single_row_delete_bytes` is the measurement that recorded 4,830 → 377 B at 16
  blobs and 19,126 → 377 B at 64 across the repin.
  pins: rp-7-f18-repin/C-003, C-004
- `v3_dml_scan.rs` — **RP-7 (2026-09-02):** the key-bounds residual push on the identity DML
  scan. `subquery_delete_opens_only_the_files_the_key_bounds_admit` seeds eight one-row data
  files, hides the seven whose manifest lower bound cannot hold the source key, and requires the
  subquery DELETE to succeed anyway — a scan that still opened them fails closed on a missing
  Parquet file rather than passing quietly. Mutation (return `None` from
  `identity_scan_residual`) 1 red of 1. The `#[ignore]`d
  `measure_v3_mor_subquery_delete_statement_wall` is the statement-wall measurement behind the
  ledger's §10 table; it is a wall clock and deliberately not asserted.
  **Round 3 (2026-09-02):** `subquery_dml_matrix_matches_spark_with_the_residual_pushed` is the
  twelve-cell owner-resolution matrix (ledger §11) — both alias-shadowing spellings delete every
  row, the eight other executable shapes match Spark's survivors, and the two allow-list refusals
  leave the table at the seed. Mutation (independent per-side classification) 1 red of 1.
  pins: rp-7-f18-repin/C-005
  rustdoc cites C-001..C-016 (`Model: Grok 4.6 xHigh`; rp-3-fork-repin/C-004;
  rp-6-fork-repin/C-002, C-003; v3-7-merge-lineage/C-002; v3-9-mor-predicate-dml-dv/C-003).
- `v3_lineage.rs` — **V3-4:** Spark-door `_row_id` / `_last_updated_sequence_number` on the RP-6 re-recorded the `repark-sql/src/v3/cow.rs` hash once more after the pins citation moved from its module doc to the map.
  V3E-3 fixtures (MOR+DV surviving rows), created v3 derivation, v2/v1 unresolved (`No field
  named _row_id`), `SELECT *, _row_id` expands user columns only, qualified/aliased forms,
  unquoted case-fold, JOIN/CTE/subquery/`VERSION AS OF` refuse `V3-ROWID-2`, V3-COW-1 files
  hash-pinned (V3-9 re-records after the merge-on-read lift touched three of the four files;
  later units re-record only for a change they themselves made); the C-001
  matrix pin finds the ledger anywhere under `task/ledgers/` so
  lifecycle moves keep it green.
  pins: rp-4-fork-repin/C-003
  pins: v3-4-serve-lineage-columns/C-001, C-002, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015, C-016, C-018, C-020
- `v3e3.rs` — **V3E-3:** Spark-written partitioned v3 DV fixture and equality-delete
  + DV fixture (`fixtures/v3-spark-part-dv/`, `fixtures/v3-spark-eq-dv/`); live
  rows, partition prune, `.delete_files` content 1/2, B-MOR-3 zeros, RP-3 cells 3–6
  MOR DELETE on the partitioned DV (pins: rp-3-fork-repin/C-004);
  C-007 measured fork R136 as a parquet-to-DV conversion no-op on DV-only (zeros); B-MOR-3 FIXED 2026-09-03;
  the measurement pin does not write outside the crate
  (pins: rp-3-fork-repin/C-007, C-011)
  pins: b-mor-3-rewrite-position-deletes-v3/C-003 (2026-09-03: this row, not an inner-doc header in `call_register.rs` / `call_v3_dv.rs` / `v3e3.rs`, is where the B-MOR-3 pins are cited)
  (`Model: Grok 4.6 xHigh`; rustdoc cites C-013).
  **V3-5:** `rewrite_data_files` on the partitioned DV fixture drops both
  vectors (delete-ratio admits each one-file group);
  `where => 'part = 0'` drops only that vector and keeps the sibling live.
  pins: v3-5-dv-compaction/C-002, C-003, C-005
- `delete_granularity.rs` — **MW-9:** Spark-door `write.delete.granularity` (explicit
  file/partition, unknown refuse on MERGE and identity UPDATE, fork DELETE/UPDATE
  residual, ALTER-then-MERGE).
- `call_rewrite_dangling.rs` — the CALL's
  `'remove-dangling-deletes' => true` reaches the fork's composed GC and reports a true
  `removed_delete_files_count` on a partitioned v2 fixture (C-006).
  **RDF-1 (2026-09-02):** the file-scoped pair, both on RePark-owned MERGE deletes. A delete
  file naming ONE data file has exact, equal `file_path` bounds, so the rewrite that replaces
  that data file drops it with NO `remove-dangling-deletes` option
  (`removed_delete_files_count = 1`, zero delete files after). Its incidental control: a
  `partition`-granularity delete file naming TWO data files has unequal bounds, is not
  file-scoped, and outlives the rewrite (`removed_delete_files_count = 0`, one delete file
  after) with its shadowed rows still shadowed — F-16 residue 2, unchanged. Registry `RDF-1`.
  pins: rdf-1-position-delete-bounds/C-003
- `call_rewrite_options.rs` — **rewrite_data_files options:** `where => 'part = 0'` (and `IN (0)`)
  keeps the **part=1** pre-image paths byte-identical and rewrites part=0 away; unknown strategy
  and bad where use Spark's text; `sort_order` refuses without compacting; named `BINPACK` still
  compacts v2.
  pins: maint-rewrite-data-files-options/C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `write_to_branch.rs` — RP-5 C-004 family pins: INSERT VALUES/SELECT, UPDATE, DELETE,
  MERGE, INSERT OVERWRITE, TRUNCATE, empty overwrite on a diverged branch; two-part
  `t.branch_b` via session defaults; tag and missing-branch Spark-shaped refuse including
  TRUNCATE; a real three-part table named `branch_<x>` is not a selector.
  pins: rp-5-fork-repin/C-004
- `common.rs` — shared fixtures (`setup`, `rows`, `run`, `register_source`,
  `register_view_typed_source` (CTAS-VIEW-1 Utf8View+BinaryView), `table_rows`, …)
  and the cross-cutting helpers that more than one leaf needs (`time_travel_id_multiset`,
  `execute_without_collecting`, unsafe-cast walk helpers). **V3-2:**
  `setup_allow_create_format_version_3` (`Model: Grok 4.6 xHigh`). **U2:** `setup` /
  `setup_allow_local_fs_ddl` / `setup_strict_catalog` call
  `crate::extension::apply_spark_float_as_decimal` so Spark-door unit fixtures match
  production `configure`. **R-2:** those fixtures plus `setup_with_ansi` also call
  `register_spark_decimal_planner`; shared helpers use `pub(super)` visibility and re-exports.
- `create_table.rs` pins `ts TIMESTAMP` → `Timestamptz`;
  `ctas_of_instant_producers_stores_timestamptz` (SQL `current_timestamp` / `to_timestamp(Z)`
  / identity-partitioned CTAS).
- **Production-aligned leaves:**
  `ctas` (`register_memory_catalog` location-less CTAS lands under the warehouse), `create_table`,
  `namespace_ddl` (`IF NOT EXISTS` create-new / same / conflicting / no-location behavior),
  `catalog_ops`, `describe_show`, `alter`, `dml`
  (DELETE/UPDATE + BUG-001 valve; no production `delete`/`update` module), `insert_overwrite`,
  `partition_overwrite` (DML-B dynamic/static snapshot stamps, empty-static `delete`,
  sibling file-path stability, two-key AND + incomplete-static, string/NULL partitions,
  Hive too-many-columns refuse, empty-dynamic guard;
  pins: dml-b-insert-overwrite/C-001, C-002, C-004, C-005),
  `truncate` (DML-C: wipe summary keys, equal empty-overwrite keys, time travel,
  missing-table / view / `INVALID_PARTITION_OPERATION` / IF EXISTS parse refuse;
  pins: dml-c-truncate/C-001, C-002, C-005, C-006, C-007),
  `merge`, `merge_nmbs` (DML-A NMBS COW+MOR, Arrow types, hunt cells: NULL keys,
  MATCHED-predicate miss, extra file, source-empty UPDATE, NMBS-only dup source;
  pins: dml-a-merge-not-matched-by-source/C-001, C-002, C-003, C-004, C-005, C-006, C-007),
  `call`, and `call_orphan`. `call_remove_orphan_files_refuses_a_location_arg_under_the_fallback_root`
  and `call_orphan_shared_ctas_root_rule` pin the fallback-root safety contract. Maintenance tests
  pin Spark's full schemas, typed count sources, deletion-vector refusal, and file-granularity rules.
  `call_v3` (**V3-0 / RP-4**): v3 rewrite preserves lineage, v2 control, and
  `opt_in_create_produces_v3_and_rewrite_runs` (six-file CALL, lineage equal;
  `V3-LINEAGE-1` FIXED; `Model: Grok 4.6 xHigh`); the battery
  `the_engine_still_cannot_produce_a_v3_table` is V3-6 C-006's identity check —
  byte-untouched through V3-6.
  Pin `call_rewrite_data_files_on_v3_preserves_row_lineage`
  (`pins: rp-3-fork-repin/C-005; rp-4-fork-repin/C-003`).
  RP-6: `rewrite_after_same_arity_spec_evolution_stamps_current_spec`
  (pins: rp-6-fork-repin/C-005).
  `call_v3_dv` (**V3-5 / B-MOR-3 / RP-11**): six-file v3 MOR with live Puffin DVs;
  `rewrite_data_files` drops all six (`removed_delete_files_count = 6`,
  count columns Arrow Int32); `rewrite_position_delete_files` returns zeros
  on DV-only and converts five upgraded parquet deletes to PUFFIN
  (`B-MOR-3` FIXED 2026-09-03); below-floor groups (2-file, mixed, partition-2)
  return Spark's four zeros and leave parquet (`B-MOR-3-FLOOR-1` FIXED 2026-09-04).
  pins: v3-5-dv-compaction/C-001, C-002, C-003, C-004, C-007
  pins: b-mor-3-rewrite-position-deletes-v3/C-002, C-003
  pins: rp-11-repin-f24/C-002
  `call_manifests` (**MW-6**) pins the two non-nullable `int` columns, no-op zero result, current
  spec filter, delete-manifest refusal, and `MANIFEST-3` count divergence.
  `call_register` (**V3-1 / RP-3 C-008**): `CALL system.register_table` arguments, three nullable BIGINT columns,
  adoption/read-back, occupied-ident refusal, Hadoop `vN.metadata.json` write bumps to `v(N+1)`,
  S3 Tables register names R126, and the Spark-written `fixtures/v3-spark-mor/`
  fixture (`B-MOR-3` zeros),
  `fixtures/` (Spark-written on-disk Iceberg tables CI can adopt with no JVM),
  `call_orphan` (**MW-3**): full-directory before/after orphan safety and 24-hour cutoff fixtures,
  `ref_ddl` (**REF:** the write-to-branch/tag refusal names the `iceberg-datafusion`
  commit-target gap at fork pin `33be9a0`, not the superseded pin; pins:
  ref-branch-tag-wap/C-004),
  `refs_and_wap` (**REF:** both `WITH SNAPSHOT RETENTION` halves at the oracle's values and the
  reversed order refusing; the `branch_`/`tag_` READ selectors resolving the ref, joining
  against the live table, refusing loud on a missing ref, and claiming neither a
  metadata-table suffix nor a real table whose own name starts with `branch_`; and WAP
  declared — the three publish procedures and the `spark.wap.*` confs all fail closed and leave
  the branch where it was; and the read-vs-write boundary — a selector in a DML statement's
  source, `USING` operand or predicate subquery reads the ref (four classes plus CTAS, each
  asserting the ref's ids and not `main`'s); a branch write target commits onto the branch
  and a tag write target refuses. The oracle stamp the registry rows §2.2
  `REF-1`/`REF-3`/`REF-4` cite lives in the REF ledger C-001 (2026-09-01, live PySpark
  4.1.2 + Iceberg 1.11.0; retention values are the oracle's own `refs` rows).
  pins: ref-branch-tag-wap/C-001, C-002, C-003, C-005, C-007),
  `time_travel`, `metadata_tables` (**RP-5:** the two pins guard the fork behavior with the engine shim gone, pins: rp-5-fork-repin/C-003; **RP-1:** projection battery iterates
  `MetadataTableType::all_types`; `position_deletes` rewrites then scan-refuses.
  **MW-4b:** Glue-shaped `table_exists` — 4-part
  `.snapshots`/`.files` rewrites to `$` despite hierarchical `DataInvalid`; Unexpected
  and single-level DataInvalid stay fatal), `normalize`, `local_fs_ddl`,
  `router` (multi-statement, F-BR-2 eager DML), `decimal` (G-7b bit-exact
  `Decimal128` i128 pins — literal / division / 38-clamp / avg+promotion / overflow+div-zero /
  nullability; cites Python corpus row names.
  `pin_literal_1_23_infers_decimal128_3_2_i128` and overflow wrap `10^38` at (38,0).
  `pin_int_times_decimal_is_12_2_i128`,
  `pin_cast_int_times_decimal_stays_21_2_i128`, clamp pins `(38,6)` / `(38,17)`,
  `pin_mul_38_20_still_refuses_at_plan` (DEC-8 plans `(38,6)`; name retained);
  fixtures go through `apply_spark_float_as_decimal` via `common::setup` plus the DEC-8
  `ExprPlanner`. `setup` installs ANSI ON; `setup_with_ansi(false)` enables legacy NULL `/0`;
  `pin_div_by_zero_decimal38_raises_under_default_ansi` +
  `pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false` (Spark type `(38,6)`);
  `/` uses i128 at `(23,13)`; DEC-6 raises or returns ANSI-OFF NULL;
  DEC-8 plans), `float_agg` (G7 float
  aggregation determinism — catastrophic-cancellation fixture; `sum`/`avg` `f64::to_bits` at
  `target_partitions` 1/2/8; per-count stability + cross-count spread disclosure),
  `join_null_keys` (R-3 / G8 — Spark-door NULL-key join value pin: INNER / LEFT /
  LEFT SEMI / LEFT ANTI; goldens = G4 live Spark 4.1.2, re-verified under the
  JVM lock; cites
  `spark_door_null_keys_never_match_inner_left_semi_anti`).
- **Sibling test modules:**
  `partitioned_ctas`, `partitioned_merge`, `transform_overwrite` (still nests
  `provider_partition_correctness`), `service_managed_ctas`,
  `ctas_view` (**CTAS-VIEW-1, 2026-09-03:** unpartitioned CTAS from Utf8View+BinaryView
  batches with one NULL, plus the partitioned control from the same view).
  pins: ctas-view-1-conform-stream/C-001, C-003
  `service_managed_ctas` also has `ctas_service_managed_from_view_typed_batches_round_trips`.
  pins: ctas-view-1-conform-stream/C-003
- [call_orphan.rs](call_orphan.rs) — orphan safety, cutoff, and fallback-root refusal pins.
- **Time-travel pins:**
  `time_travel_temp_views_do_not_survive_a_{successful,failed}_statement` and
  `time_travel_statement_pins_never_collide_with_a_reader_options_view` (the fix-pass collision
  pin: a reader-options registration made through `repark_core::read_table_at` must SURVIVE a
  Spark-door `VERSION AS OF` statement, and the door must mint from repark-core's counter — the
  second assertion is the one that reds whatever the numbers happen to be), beside the twin
  `time_travel_version_timestamp_branch_tag_and_errors`, plus their three leaf-private helpers
  (`leftover_time_travel_views`, `setup_time_travel_leak_table`, `temp_view_sequence` — used by
  that leaf only, so they stay out of `common.rs`). They read the default catalog/schema directly
  rather than `information_schema`, which this door's `setup` does not enable.
- [time_travel.rs](time_travel.rs) — statement-owned pinned-view cleanup and collision pins.
- `collation.rs` pins parse-altitude refusals for expression `COLLATE`, `ORDER BY COLLATE` (two names),
  `CREATE TABLE` column `COLLATE`, `CAST AS STRING COLLATE`, SET/RESET of a collation
  `SQLConf` key (helper + `execute` + parenthesized SET), `execute_passthrough` +
  spark_ast source attach (Q-001), a string-literal negative (incl. CAST-in-literal),
  and a default (non-COLLATE) `ORDER BY` untouched pin. Ledger:
  [`../../../../task/y7-collation-refuse-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-y7-collation-refuse-ledger.md).
- `window_temporal_range.rs` pins the Spark door's `RANGE` frames on datetime order keys and the
  paths that must remain unchanged:
  `temporal_range_bare_offset_over_timestamp_key_refuses_like_spark` (Spark's error class),
  `temporal_range_bare_offset_over_date_key_means_days` (one day vs thirty days, so a
  one-month reading cannot pass), `temporal_range_interval_bounds_still_match_spark` (asc /
  desc / ties / HOUR-not-DAY), `temporal_range_null_order_keys_match_spark`, and
  `temporal_range_numeric_order_keys_are_untouched` (scope, incl. the mixed-statement
  fallback). Goldens are the live PySpark 4.1.2 halves recorded in the unit's section-0 recon,
  the same oracle the `temporal_range` family in
  `python/repark/tests/test_window_parity.py` pins — one oracle, two halves. Leaf-private
  fixtures (`register_timestamp_seed`, `register_date_seed`, `seed_micros`,
  `days_from_civil`) stay out of `common.rs`: only this leaf uses them. Ledger:
  [`../../../../task/g5b-temporal-range-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-12-g5b-temporal-range-ledger.md).
- The same leaf also pins:
  `temporal_range_negative_offset_is_spark_empty_frame` (R3 HIGH — TIMESTAMP
  `INTERVAL '-1' DAY` is Spark's empty frame; DATE stays empty and is not refused),
  `temporal_range_day_to_second_literal_matches_spark` (R2 — `'1 12:00:00'` and `'1 0:0:0'`),
  plus the residual cases:
  `temporal_range_unquoted_interval_literal_matches_quoted` and R5 to
  `temporal_range_interval_bound_over_int_key_is_numeric_n` (+ magnitude /
  unit-ignored pin `temporal_range_interval_bound_over_int_key_uses_numeric_magnitude`);
  R4 stays `temporal_range_following_to_following_still_includes_current_row` (120 vs 90);
  adds `temporal_range_timestamp_seed_is_microseconds` (µs type pin after #79) and
  `temporal_range_mixed_datetime_and_numeric_interval_leaves_numeric_loud` (mixed-statement
  R5 stays Arrow-cast).
  It also pins `temporal_range_value_inverted_frames_do_not_wrap`
  (same-kind magnitude invert after sign-normalize: `-2 PRECEDING AND -1 PRECEDING`,
  `-1 PRECEDING AND 0 FOLLOWING`, direct `2 FOLLOWING AND 1 FOLLOWING` — Spark
  `WRONG_COMPARISON`, wrapping `-1` is gone; no `10000 YEAR` pair) and
  `temporal_range_mixed_negative_timestamp_and_numeric_bare_refuses` (Q-003). Ledger:
  [`../../../../task/g5br-range-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md),
  [`../../../../task/z4-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z4-residuals-ledger.md),
  [`../../../../task/w4-z-residuals-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md).
- `merge.rs` pins execute-path strictness: `merge_oracle_style_update_where_refuses` / `_delete_where_` /
  `_insert_where_`), M3 (`merge_source_qualified_set_target_refuses`,
  `merge_nested_field_set_target_refuses`,
  `merge_target_qualified_and_bare_set_targets_execute`), M8
  (`merge_insert_without_column_list_refuses`; `INSERT *` stays on
  `merge_star_forms_upsert`), M10 (`merge_non_last_unconditional_matched_refuses`,
  `merge_non_last_unconditional_not_matched_refuses`; first-match-wins stays on
  `merge_clause_order_first_match_wins` / `merge_matched_and_arm_order_update_then_delete`).
  Lowering twins live in `../merge.rs`; ledger: `task/mg2-lowering-strictness-ledger.md`.
- `merge.rs` also pins:
  `merge_duplicate_source_keys_with_matched_raises`,
  `merge_duplicate_source_keys_insert_only_commits_both`,
  `merge_matched_and_arm_order_update_then_delete`,
  `merge_matched_and_threshold_update_or_delete`, plus the leaf-private `score_table_rows`
  helper for the two score-arm pins.
- `dml.rs` pins the `g3e8_*` subquery-predicate valve: the refuse family for both verbs and
  adjacent negatives that prove the valve did
  not widen (non-subquery DML, `INSERT … SELECT` with a subquery, MERGE over a subquery source,
  `UPDATE … SET col = (SELECT …)` with and without a `WHERE`), the guard-ORDER pin against the
  BUG-001 valve, the **FROM-less** `DELETE <table> WHERE …` family + its negative (the panel's
  live bypass — that spelling fails the router's Databricks parse and reaches the executor
  through the passthrough's own parse), and the CTE-prefixed `WITH … DELETE` refusal.
- `g3e8_delete_in_subquery_deletes_exactly_the_matching_row` pins executable IN-DELETE forms and
  the residual refusal family.
  Ledger: [`../../../../task/z1-g3e8-pr1-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-z1-g3e8-pr1-ledger.md).
- `g3e8_delete_not_in_subquery_*` pins NOT IN, NULL, empty, quoted, and FROM-less forms; the
  residual refusal family remains covered.
  Ledger:
  [`../../../../task/w3-g3e8-pr2-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-w3-g3e8-pr2-ledger.md).
- `g3e8_delete_exists_uncorrelated_and_correlated_execute` pins `[NOT] EXISTS` with and without
  correlation and the residual refusal family.
  Ledger:
  [`../../../../task/v1-g3e8-pr3-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-13-v1-g3e8-pr3-ledger.md).
- `g3e8_delete_correlated_in_deletes_exactly_the_matching_row` and
  `g3e8_update_in_subquery_rewrites_only_the_matching_row` pin correlated IN and identity UPDATE IN.
  Ledger: [`../../../../task/r1-g3e8-pr4-ledger.md`](../../../../task/ledgers/archive/2026-08/2026-08-14-r1-g3e8-pr4-ledger.md).
  `normalize.rs` pins the detector
  (`g3e8_subquery_detector_fires_on_every_spelling_and_no_other`) and the statement-level valve
  pin (`g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target`, the entry point
  `spark_ast::execute_passthrough` calls) — production-module alignment: both live in
  `../normalize.rs`. Leaf-private helpers (`g3e8_setup`, `g3e8_seed`, `assert_g3e8_message`) stay
  in `dml.rs`; only that leaf uses them.
  See `task/g3e8-guard-ledger.md`.

## Mapping rule

1. Production-module alignment by name / primary assertion.
2. Two-module tests follow the primary assertion; genuine cross-cutting residue gets a small
   shared home (`dml`, `common`).
3. Nested mods lift to sibling files of the same name (path-preserving).
4. Helpers move byte-identically; only new lines are `mod` decls and `use` adjustments.

The test modules follow production ownership. Archived ledgers remain available from the pointers
above.

## Pointers

- Up: [../map.md](../map.md)
- Registry pins: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
- Surface matrix (also `#[cfg(test)]`): [../matrix.rs](../matrix.rs)

## Debug

- `spark_string_literals.rs` and `cast_binary.rs` are byte-frozen (sha256) by
  `python/repark-parity/tests/test_pr_245_revalidation_record.py`; any edit, a comment rewrap
  included, reds that record — revert, never re-hash.
| Symptom | First check |
|---|---|
| Test identity doubt | `cargo test -p repark-spark --lib -- --list`; compare the production-aligned leaf names |
| Matrix pin string red / stale | `matrix.rs` strings are `--list` names; update with renames only — never re-point a surface |
| Registry pin path stale | filesystem form `crates/repark-spark/src/tests/<file>.rs::leaf` |
| Missing `TempDir` / Arrow types in a leaf | `use super::common::*;` (shared re-exports live in `common.rs`) |
| `spark_door_null_keys_never_match_inner_left_semi_anti` RED | 3VL on JOIN keys moved, or `LEFT SEMI`/`ANTI` stopped parsing. Goldens are G4 live Spark 4.1.2 — do not absorb a match-on-NULL |
| Cross-leaf helper not found | should be `pub(super)` in `common.rs` (or wrongly left private in one leaf) |
| Nested partition pin fails | `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` — manifest-level `DataFile.partition` oracles |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).

