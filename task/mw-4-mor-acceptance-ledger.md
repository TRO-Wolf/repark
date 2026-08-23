# MW-4 — MOR leg in the tier-2 AWS acceptance

**Unit:** MW-4 · **Date:** 2026-08-23 · **Base:** `89160b6` (`main`, A13 #217) ·
**Branch:** `feat/mw-4-mor-acceptance`

**Design:** [../docs/design/iceberg-maintenance-wave.md](../docs/design/iceberg-maintenance-wave.md) §4
**Slate:** [../briefs/iceberg-maintenance-wave.md](../briefs/iceberg-maintenance-wave.md)
**Charter:** [mw-0-charter-ledger.md](mw-0-charter-ledger.md) OD-3
**Runbook:** [../docs/tier2-aws.md](../docs/tier2-aws.md) §2

## Path + critic engine

HIGH (live catalog, object-delete on the scratch prefix, Iceberg compact+expire).
SEPMO-octo: `critic_engine=octo`, `cycles=4`, `early_stop=true`, `claims_critic=true`,
`severity_floor=S1`.

Entry-point matrix: Spark facade only (`CALL` + `table.files` / `VERSION AS OF`). Native
`repark.sql()` has no catalog-register surface. ANSI door has no Spark `CALL`.

## Proposition ledger

| ID | Clause | Verdict | Evidence |
|---|---|---|---|
| C-001 | Existing COW publish path is unchanged (LRS). | PROVEN | `ICEBERG_TABLE_PROPERTIES` still copy-on-write; `_bronze_dedup_publish_idempotent` untouched. |
| C-002 | Glue is the OD-3 surface. S3 Tables MOR compact+expire is out of this unit. | PROVEN | OD-3 is `s3:DeleteObject` on `REPARK_ACCEPT_WAREHOUSE` + `testing_repark_acceptance/`. Table-bucket delete is still denied in the runbook. |
| C-003 | Sequence: CTAS MOR → MERGEs that strand position-delete files → identical MERGE → compact + expire → Arrow row equality (value AND type). | PROVEN | `run_mor_merge_compact_expire` + `assert_mor_maintenance_outcome`. |
| C-004 | Expire mutation-proof: CTAS snapshot is unreadable via `VERSION AS OF` after `retain_last=1`. | PROVEN | `assert_mor_maintenance_outcome` fails if the snapshot still resolves. |
| C-005 | Compact mutation-proof: ≥2 position-delete files before `rewrite_position_delete_files`; fewer after. | PROVEN | `MOR_MIN_POSITION_DELETE_FILES`; helper raises if compact is a no-op. |
| C-006 | No DROP TABLE / DROP NAMESPACE / DELETE FROM. Unique `testing_mw4_mor_*` name per live run. Glue tables still accumulate. | PROVEN | `mor_ctas_sql` has no `IF NOT EXISTS`; live test uses `uuid4`; structural guard. |
| C-007 | Structural guard still forbids DROP TABLE / DELETE FROM / DROP NAMESPACE. | PROVEN | `test_the_gated_harness_has_no_drop_or_delete_against_aws`. |
| C-008 | Always-run memory analog uses the same helper as the Glue live test. | PROVEN | `test_mor_merge_compact_expire_on_memory_catalog`. |
| C-009 | COW TBLPROPERTIES builder is not reused for the MOR table. | PROVEN | `test_mor_ctas_sql_is_merge_on_read_not_copy_on_write`. |
| C-010 | Runbook + workflow comments record OD-3 object-delete on the scratch prefix; Glue table-delete stays denied. | PROVEN | `docs/tier2-aws.md` §2; `aws-acceptance.yml` comments. |
| C-011 | `map.md` lockstep + this ledger. | PROVEN | `task/map.md`, `python/repark/tests/map.md`, `docs/map.md`, `.github/workflows/map.md`. |
| C-012 | No AWS credentials in the tree; no `Cargo.toml [patch]`; IAM is owner-side. | PROVEN | diff. |
| C-013 | Local gate is the memory analog + helper pins. Glue live is skip-gated; post-merge `aws-acceptance` dispatch is the live proof. | PROVEN | module `pytestmark`; STATUS. |

## Out of scope

- S3 Tables MOR compact+expire (no table-bucket `DeleteObject` in OD-3).
- `remove_orphan_files` on live catalogs.
- Dropping Glue tables.
- Changing the existing COW silver publish job.
