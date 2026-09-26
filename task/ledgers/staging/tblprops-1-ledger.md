# Unit ledger — WO TBLPROPS-1 · `SHOW TBLPROPERTIES` on an Iceberg table answers Spark's rows

**Date:** 2026-09-26 · **Branch:** `feat/tblprops-1` · **Base:** `82e8e871` (rebased on 2026-09-26)
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Cells `D-SHOW-TBLPROPERTIES` and `D-SHOW-TBLPROPERTIES-KEY` (scoreboard
2026-09-26) fall through to DataFusion and refuse `Error during planning: SHOW
[VARIABLE] is not supported unless information_schema is enabled` on a table: the
statement only answers for a catalog view (`execute_show_tblproperties` returns `None`
when the name is not a view). This unit adds the table arm in Rust — a new
`show_tblproperties.rs` module builds Spark's sorted row list from the loaded table's
metadata — plus the empty temp-view frame, while the view arm and the missing-name
refusal stay byte-identical. The dbt `R-SHOW-TBLPROPERTIES` refusal pin moves to
served in the same commit (`DBT-TBLPROPS-1` FIXED).

**What Spark does (measured 2026-09-26; Spark 4.1.2 + Iceberg 1.11,
`tblprops-spark-2026-09-26.json`, probe `tblprops-probe.py` beside it).** Columns are
always `key string NOT NULL, value string NOT NULL`, rows sorted by key. A fresh
`k=v` table with no snapshot answers `current-snapshot-id=none`,
`format=iceberg/parquet`, `format-version=2`, `k=v`,
`write.parquet.compression-codec=zstd`; after `INSERT` the snapshot cell is the decimal
id (`5698251910872392553` in the probe); after `SET TBLPROPERTIES ('k2'='v2',
'write.format.default'='orc')` the seven rows with `format=iceberg/orc`. A v1 table
reports `format-version=1` once. A partitioned table created with an explicit `zstd`
codec and `comment=hello` answers four rows — `comment` is never a row. Keyed:
`('k')`, `('format-version')` and `('current-snapshot-id')` answer their values; a
missing key answers `Table sc.ns.t does not have property: nope`, case-sensitively
(`('K')` misses). Two-part and one-part names after `USE` answer the same rows. A
temporary view answers the two columns with no rows. A missing table and the wrong-case
`sc.ns.T` refuse `TABLE_OR_VIEW_NOT_FOUND` / `42P01`.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A fresh `k=v` table with no snapshot answers Spark's five rows, `key`/`value` `string NOT NULL`, sorted by key. | Rust + facade pins assert the exact list. | **PROVEN** | `tests/show_tblproperties.rs::fresh_table_answers_spark_rows`, `test_tblprops_1.py::test_fresh_table_answers_spark_rows`; the rewritten `show_tblproperties_routing.rs`, `show_create.rs` and `show_table_extended_near_miss.rs` table arms; dbt `test_show_tblproperties_table_answers_spark_rows`. |
| C-002 | After `INSERT` the `current-snapshot-id` cell is the current snapshot id as decimal text and the other rows do not move. | The Rust pin compares against the loaded table's id; the facade pin checks the cell is a decimal id, not `none`, and that the other rows do not move. | **PROVEN** | `tests/show_tblproperties.rs::insert_moves_current_snapshot_id_to_the_decimal_id`, `test_tblprops_1.py::test_insert_moves_snapshot_cell_to_the_decimal_id`. |
| C-003 | After `SET TBLPROPERTIES ('k2'='v2', 'write.format.default'='orc')` the listing is Spark's seven rows with `format=iceberg/orc`. | Rust + facade pins assert the exact list. | **PROVEN** | `tests/show_tblproperties.rs::set_tblproperties_updates_format_and_adds_rows`, `test_tblprops_1.py::test_set_tblproperties_updates_format_and_adds_rows`. |
| C-004 | A v1 table reports `format-version=1` exactly once. | Rust + facade pins assert the exact four rows. | **PROVEN** | `tests/show_tblproperties.rs::v1_table_reports_format_version_once`, `test_tblprops_1.py::test_v1_table_reports_format_version_once`. |
| C-005 | `comment` and RePark's stored `owner` stamp are never rows, and each reads as missing under a keyed lookup. | Rust + facade pins assert the exact lists and the miss rows; the Rust pin proves `owner` is stored. | **PROVEN** | `tests/show_tblproperties.rs::comment_is_never_a_row` and `::owner_is_stored_but_never_a_row`, `test_tblprops_1.py::test_partitioned_table_hides_comment` and `::test_owner_is_never_a_row`. |
| C-006 | Keyed lookups hit stored and built-in values and miss with Spark's `Table <catalog>.<namespace>.<table> does not have property: <key>` text, case-sensitively. | Rust + facade pins assert every hit and miss `==`. | **PROVEN** | `tests/show_tblproperties.rs::keyed_lookup_hits_builtins_and_misses_loudly`, `test_tblprops_1.py::test_keyed_lookup_hits_value_and_builtins` and `::test_keyed_lookup_misses_loudly_and_case_sensitively`. |
| C-007 | Two-part and one-part names answer the same rows as the three-part name after `USE`. | Rust + facade pins assert all three spellings `==`. | **PROVEN** | `tests/show_tblproperties.rs::two_part_and_one_part_names_answer_after_use`, `test_tblprops_1.py::test_two_part_and_one_part_names_answer_after_use`. |
| C-008 | A temporary view answers the two `string NOT NULL` columns with no rows. | Rust + facade pins assert the empty frame. | **PROVEN** | `tests/show_tblproperties.rs::temp_view_answers_the_empty_frame`, `test_tblprops_1.py::test_temp_view_answers_the_empty_frame`. |
| C-009 | A missing table and the wrong-case `sc.ns.T` keep the `[TABLE_OR_VIEW_NOT_FOUND]` / `42P01` refusal naming the resolved name. | Rust + facade pins assert class, condition, SQLSTATE and name. | **PROVEN** | `tests/show_tblproperties.rs::missing_table_keeps_the_not_found_refusal`, `test_tblprops_1.py::test_missing_table_keeps_the_not_found_refusal`. |
| C-010 | The DataFusion `SHOW` fall-through text no longer appears for `SHOW TBLPROPERTIES` on a three-part name or a name completed by `USE` (table, view, temp view, missing); a bare one-part name with no `USE`, an unknown catalog and a four-part name still reach it (residue R-3, verifier round 1 V-001). | Rust + facade sweeps assert no `information_schema` on the covered arms. | **PROVEN** | `tests/show_tblproperties.rs::fallthrough_text_is_unreachable_on_every_arm`, `test_tblprops_1.py::test_fallthrough_text_is_unreachable`. |
| C-011 | A stored `write.parquet.compression-codec` value wins over the default, and after `UNSET` the `zstd` default reappears. | Rust + facade pins assert the stored value and the post-`UNSET` default; the Rust pin proves the stored key is gone. | **PROVEN** | `tests/show_tblproperties.rs::stored_compression_codec_wins_over_the_default` and `::unset_compression_codec_falls_back_to_the_default`, `test_tblprops_1.py::test_stored_compression_codec_wins_over_the_default` and `::test_unset_compression_codec_falls_back_to_the_default`. |

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | drop the `zstd` default injection in `table_rows` | `unset_compression_codec_falls_back_to_the_default` (Rust) reds on the missing fifth row, restored green. First attempt ran M1 against `fresh_table_answers_spark_rows`, which stayed green: the fork stamps `write.parquet.compression-codec=zstd` into stored properties at CREATE (measured in the metadata JSON), so no fresh-table pin can bite the default branch — the UNSET pin exists for exactly that branch. |
| M2 | stop excluding `owner` from the stored rows | `owner_is_stored_but_never_a_row` (Rust) reds on the extra row, restored green |
| M3 | render the keyed miss with the identifier reversed (`Table <table>.<namespace>.<catalog> …`) | `keyed_lookup_hits_builtins_and_misses_loudly` (Rust) reds on the identifier, restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: tblprops-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every measured Spark row list is pinned == on both doors, including the snapshot-id move, the orc retarget, v1, the partitioned comment exclusion, and the keyed hits and misses.
      artifacts: [crates/repark-spark/src/tests/show_tblproperties.rs, python/repark/tests/test_tblprops_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: All name shapes pinned (three/two/one-part after USE), the temp-view empty frame, the missing and wrong-case refusals, and the fall-through sweep over every arm.
      artifacts: [crates/repark-spark/src/tests/show_tblproperties.rs, python/repark/tests/test_tblprops_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The missing-name refusal keeps its exact class, condition, SQLSTATE and text on both doors; catalog-view rows and the load/table-existence fault routing are unchanged and still pinned.
      artifacts: [crates/repark-spark/src/tests/show_tblproperties.rs, crates/repark-spark/src/tests/show_tblproperties_routing.rs, python/repark/tests/test_tblprops_1.py]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface; SHOW reads one loaded table snapshot and pins run on fresh warehouses in one session.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; SQL text in, sorted rows out.
    - id: AT-6
      status: ATTACKED
      evidence: No stored format changes; the rows derive from the loaded table metadata and the scoreboard replay holds both cells EQUAL against Spark's recorded answers.
      artifacts: [target/probe-tblprops-1/replay-1.json]
    - id: AT-7
      status: N/A
      justification: No performance claim; one metadata read per statement, no scan.
    - id: AT-8
      status: ATTACKED
      evidence: One new leaf module under view_ddl plus a view_ddl/mod.rs decl line; lib.rs and the router are untouched, no new crate edge, no dependency; the shared format-version number helper is reused, not duplicated.
      artifacts: [crates/repark-spark/src/view_ddl/show_tblproperties.rs, crates/repark-spark/src/view_ddl/mod.rs]
    - id: AT-9
      status: N/A
      justification: No failure path in scope beyond C-009; a wrong row list fails its pin with expected versus observed.
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact text or rows; M1, M2 and M3 break the default, the exclusion and the miss text and the named tests red before restore.
      artifacts: [crates/repark-spark/src/view_ddl/show_tblproperties.rs, crates/repark-spark/src/tests/show_tblproperties.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (declared, not fixed here): the missing-name refusal keeps the `Error during planning: ` prefix (the registered residue class) and omits Spark's plan tail per the origin-tail convention. Pinned by C-009/C-010; recorded in `D-SHOW-TBLPROPERTIES`. |
| R-2 | Dated 2026-09-26 (declared, not fixed here): a catalog VIEW keeps its existing reserved-then-stored row order, which differs from Spark's Java-map order (R-PR4-ORDER, owned by `D-VIEW-SHOWPROPS-1`); this unit does not touch the view arm. Pinned by the unchanged `show_tblproperties_routing.rs` view tests. |
| R-3 | Dated 2026-09-26 (verifier round 1 V-001; `execute.rs` `complete_view_name` returns `None` on its `Err`, pre-existing): `SHOW TBLPROPERTIES nope` with no `USE`, `SHOW TBLPROPERTIES nocat.ns.t` and a four-part name (`sc.ns.t.snapshots`, `a.sc.ns.t`) still answer `Error during planning: SHOW [VARIABLE] is not supported unless information_schema is enabled`; Spark answers `[TABLE_OR_VIEW_NOT_FOUND]` for the bare missing name (as for the three-part case) and the other two shapes are unmeasured. Not fixed here (outside the two cells); the smallest fix is to surface the `Err` for a one-part name instead of falling through. |
