# MW-4b — Glue dotted metadata-table rewrite

Post-merge `aws-acceptance` on [#218](https://github.com/TRO-Wolf/repark/pull/218)
failed before compact/expire:

```
SELECT snapshot_id FROM glue_catalog.testing_repark_acceptance.testing_mw4_mor_….snapshots
DataInvalid => Invalid database name: NamespaceIdent(["testing_repark_acceptance", "testing_mw4_mor_…"]),
hierarchical namespaces are not supported
```

The Spark metadata-table rewrite's "real table wins" probe asks `table_exists` on the
full 4-part path. Glue and HMS `validate_namespace` return `ErrorKind::DataInvalid`
when the namespace is not exactly one level. Only `NamespaceNotFound` /
`TableNotFound` were treated as absent, so the `$` rewrite never ran. The memory
analog stayed green because the memory catalog allows hierarchical namespaces.

## Clauses

| ID | Claim | Status | Evidence |
|---|---|---|---|
| C-001 | Glue-shaped `table_exists` DataInvalid on `glue_catalog.ns.tbl.snapshots` rewrites to `tbl$snapshots`. | PROVEN | `glue_shaped_catalog_rewrites_four_part_snapshots_and_files` was red (`External(DataInvalid => Invalid database name: NamespaceIdent(["sales", "mt"]), hierarchical namespaces are not supported)`), then green. |
| C-002 | The same probe rewrites `.files` (MW-4 `position_delete_file_count`). | PROVEN | Same test asserts `mt$files`. |
| C-003 | `Unexpected` on a two-level namespace stays fatal (not a silent rewrite). | PROVEN | `glue_shaped_unexpected_on_hierarchical_namespace_stays_fatal` + `hierarchical_unexpected_stays_fatal`. |
| C-004 | Single-level `DataInvalid` stays fatal. | PROVEN | `glue_shaped_data_invalid_on_single_level_namespace_stays_fatal` + `single_level_data_invalid_stays_fatal`. |
| C-005 | `NamespaceNotFound` / `TableNotFound` still map to absent. | PROVEN | `missing_namespace_and_table_are_absent`. |
| C-006 | Exact Glue message is what the stub emits (`hierarchical namespaces are not supported`). | PROVEN | `glue_hierarchical_namespace_error` copies fork `crates/catalog/glue/src/utils.rs` `validate_namespace`. |
| C-007 | Memory-catalog dotted form still rewrites (LRS). | PROVEN | `metadata_tables_spark_dot_form_and_guards` still green. |
| C-008 | No DROP / IAM / `[patch]` / `.github/` change. | PROVEN | Diff is `repark-spark` probe + maps + this ledger. |
| C-009 | Live Glue proof remains the post-merge `aws-acceptance` dispatch. | PROVEN | Local analog cannot talk to Glue; the skip-gated `test_mor_merge_compact_expire_against_glue` is unchanged. |

## Entry-point matrix

| Door | This change |
|---|---|
| Spark facade / Spark SQL door | The rewrite lives here. Pinned through `prepare_metadata_table_sql`. |
| ANSI `repark.sql()` | N/A — dotted metadata-table form is Spark-only. |
| Native DataFrame | N/A. |

## Out of scope

- S3 Tables MOR compact+expire (still MW-4 out of unit).
- Changing Glue/HMS in the fork to return `NamespaceNotFound` instead of `DataInvalid`.
- MW-5 scorecard.

## Gate

`cargo test -p repark-spark --lib metadata_tables` — 25 passed (the Glue-shaped pin was red, then green).
