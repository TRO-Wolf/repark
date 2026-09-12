# Unit ledger — ORPHAN-S3TABLES-1 step 1 · `remove_orphan_files` refuses loud on S3 Tables (owner bug, 2026-09-12)

**Unit:** ORPHAN-S3TABLES-1 step 1 · **Date:** 2026-09-12 · **Branch:** `fix/orphan-s3tables-1` · **Base:** `d85883fd` (AP-1-CLOSE-1 merged)
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Measured on the owner's dev table bucket (2026-09-12): an S3 Tables
table's location is the bare table bucket (`s3://<id>--table-s3`) and the bucket
answers **405 MethodNotAllowed** to `ListObjectsV2` — listing is not a supported
operation on a table bucket, so no listing-based orphan sweep can work there.
Today the CALL fails first on the fork's path parser (fork F-S3ROOT-1, separate)
and, past that, on the 405 as an opaque io error. Amazon S3 Tables removes
unreferenced files itself through the table bucket's maintenance configuration
(`unreferencedFileRemoval` in `PutTableBucketMaintenanceConfiguration`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands.

**Not in this step:** the fork's F-S3ROOT-1 parser fix (D-4 — no engine change
waits on it), any other procedure on S3 Tables, any dependency or workflow file,
`STATUS.md`, `briefs/next-sequence.md`, any JVM, any AWS call.

**Decisions.** D-1 `CALL <cat>.system.remove_orphan_files(…)` on a catalog whose
kind is `s3tables` refuses BEFORE any IO naming the table, the reason (table
buckets do not support listing) and the remedy (the service's own
unreferenced-file removal); `dry_run => true` refuses the same way. D-2
`CALL run_maintenance()` skips the orphan step on S3 Tables and records the row
`skipped` with that reason. D-3 pins need no AWS: the refusal keys on the
catalog kind — `LocationPolicy::ServiceManagedLocation` is the registry marker
`CatalogKind::S3Tables` registers under (session.rs), and the pins that need a
readable table stand a memory catalog up under that marker (the fake catalog
kind the card sanctions). D-4 no engine change; F-S3ROOT-1 lands independently.

## PROPOSITION LEDGER — ORPHAN-S3TABLES-1 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `call_refuses_loud` (D-1): `CALL <cat>.system.remove_orphan_files` on an `s3tables`-kind catalog refuses before any IO with one message naming the table, the reason (table buckets do not support listing — the bucket answers ListObjectsV2 405) and the remedy (`unreferencedFileRemoval` in the table bucket's maintenance configuration). | Pin over a real `s3tables_catalog` (constructs offline, a dummy ARN) registered `ServiceManagedLocation`; armed (`dry_run => false`) call expects the refusal — on the base tree it fails past argument resolution on the catalog's `load_table` (AWS GetTable), so the pin is red first. | **PROVEN** | `call_remove_orphan_files_on_s3_tables_refuses_before_any_io` green; refusal text asserted to name `ns.t`, `table buckets do not support listing`, `405`, `unreferencedFileRemoval`, `maintenance configuration`. Red first — see §Red first. |
| C-002 | `dry_run_same_refusal` (D-1): `dry_run => true` and the defaulted dry run refuse identically — the refusal is a property of the catalog kind, not of the deletion being armed. | Same fixture; both spellings produce the identical refusal text as the armed call. | **PROVEN** | `call_remove_orphan_files_on_s3_tables_dry_run_refuses_the_same_way` green; `dry_run => true`, the omitted default and `dry_run => false` all answer the identical string — the refusal runs before the `dry_run` branch. Red first — see §Red first. |
| C-003 | `run_maintenance_skips_orphan` (D-2): `CALL run_maintenance` on an `s3tables`-kind catalog still plans/applies the other steps and reports the `remove_orphan_files` step `skipped` with the reason in `result` — on the dry-run frame and on apply. | Red first on the dry-run frame: a memory catalog registered under `ServiceManagedLocation` (the fake kind) answers the metadata reads the planner needs; on the base tree the orphan row plans `planned` and applies `ran`. The apply-frame pin runs `dry_run => false` end to end on the same fixture. | **PROVEN** | `run_maintenance_on_s3_tables_marks_the_orphan_step_skipped`, `run_maintenance_apply_on_s3_tables_skips_orphan_and_runs_the_rest` and in-module `a_service_managed_catalog_marks_step_5_skipped` green; dry-run and apply rows carry `skipped` + the reason in `result`, later ordinals unaffected. Red first — see §Red first. |
| C-004 | `other_kinds_unchanged`: every other catalog kind keeps today's behavior — the existing `call_orphan` and `run_maintenance` pins stay green. | `cargo test -p repark-spark call_orphan` and `cargo test -p repark-spark run_maintenance` green with no other pin edited. | **PROVEN** | `cargo test -p repark-spark call_orphan`: 11 passed, 0 failed. `cargo test -p repark-spark run_maintenance`: 31 passed, 0 failed. No pre-existing pin touched. |
| C-005 | `docs_row_and_guide`: `docs/guide/maintenance-policy.md` gains the S3 Tables paragraph (the CALL refuses, the service removes unreferenced files itself via `unreferencedFileRemoval`) and `docs/spark-sql-iceberg-parity.md` gains the registry row — Spark's procedure on S3 Tables fails the same way; the divergence is the loud refusal. | `make check-docs-links` green; the row names the pins. | **PROVEN** | Guide `## S3 Tables` section and registry row `### ORPHAN-S3TABLES-1` added; the row names all five pins. `make check-docs-links` green. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Pins were written and run against the base tree (change not yet applied).

C-001 / C-002 — `cargo test -p repark-spark call_orphan` (base):

```text
test tests::call_orphan::call_remove_orphan_files_on_s3_tables_refuses_before_any_io ... FAILED
test tests::call_orphan::call_remove_orphan_files_on_s3_tables_dry_run_refuses_the_same_way ... FAILED
```

The armed call got past argument resolution and died inside the catalog's `load_table` on
the AWS SDK — the opaque io error the card describes:

```text
External error: Unexpected => Operation failed for hitting aws sdk error: ... CredentialsNotLoaded ...
```

and the dry-run spelling failed differently again (`profile file credentials provider
initialization error already taken`) — proving the refusal did not exist before IO and
that the two spellings diverged.

C-003 — `cargo test -p repark-spark run_maintenance` (base): the dry-run pin found the
orphan row `status = "planned"` instead of `skipped`; the apply pin found it `ran`
instead of `skipped`. (First attempt red for a fixture reason too — the `s3t` DataFusion
provider snapshots the table list at registration, so `register_s3t_kind` must register
the provider after the table exists; fixed, then the two semantic reds above were the
recorded evidence.)

## Gates

- `make develop` — exit 0 (wheel rebuilt into the clone's `.venv`).
- `cargo test -p repark-spark call_orphan` — 11 passed, 0 failed.
- `cargo test -p repark-spark run_maintenance` — 31 passed, 0 failed.
- `.venv/bin/python -m pytest python/repark/tests -q -k "orphan or maintenance"` —
  28 passed, 9 skipped, 6271 deselected.
- parity suite `make py-test` (`python/repark-parity/tests`) — 747 passed, 1 skipped,
  11 xfailed.
- `make check-docs-links` — 786 files, 5054 links, clean.
- `make check-ledger-grammar` — 112 live ledgers clean.
- `make verify` — exit 0 (fmt, clippy, panic-ban, full Rust workspace suite).
- `python3 scripts/check_rust_file_size.py` — 468 files clean.
- Comment fence: the card's literal grep flags only the required `#[test]` /
  `#[tokio::test]` attributes on the new pin functions (the `#` arm targets
  Python/shell/TOML/YAML comments where `# noqa` is the escape); a direct grep for
  added `//` lines and for `#` lines that are not attributes prints nothing — no
  comments were added.

## Gates run

All green at commit time — see §Gates for the recorded counts.

## Coverage attestation

```text
COVERAGE_ATTESTATION:
  pr_unit: orphan-s3tables-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The refusal pins drive a real s3tables_catalog (dummy ARN) under ServiceManagedLocation end to end through the CALL door; the run_maintenance pins drive plan and apply frames on a memory catalog re-registered under the same policy marker, and the in-module pin checks the plan-level skip flag on both kinds.
      artifacts: [crates/repark-spark/src/tests/call_orphan.rs, crates/repark-spark/src/tests/run_maintenance.rs, crates/repark-spark/src/call/run_maintenance.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Both CALL pins assert the exact refusal string on armed and dry-run spellings; the maintenance pins assert status/result cells of the produced frames, not just that calls succeed.
      artifacts: [crates/repark-spark/src/tests/call_orphan.rs, crates/repark-spark/src/tests/run_maintenance.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal message itself is the contract under test — asserted verbatim on every clause; a non-s3tables catalog is unaffected (C-004's unchanged pins green).
      artifacts: [crates/repark-spark/src/call.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; the change adds a pure reason-string builder, a policy match, and an Option field on PlannedStep.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no JVM, no credentials: the s3tables_catalog constructs offline and the refusal fires before load_table; maintenance pins run on the memory catalog. `git status` shows no dependency or workflow diff.
      artifacts: [crates/repark-spark/src/tests/call_orphan.rs, crates/repark-spark/src/tests/run_maintenance.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The 405/no-listing fact is the owner's dated measurement restated verbatim in the refusal text and the docs; no behavioral claim was guessed — the base-tree reds were run, not assumed.
      artifacts: [task/ledgers/staging/orphan-s3tables-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: `git status` shows no Cargo.toml/Cargo.lock/pyproject/uv.lock/.github diff; the staged-code comment fence prints nothing.
      artifacts: [crates/repark-spark/src/call.rs, crates/repark-spark/src/call/run_maintenance.rs, crates/repark-spark/src/call/run_maintenance_apply.rs]
    - id: AT-9
      status: ATTACKED
      evidence: map.md in every touched directory moved in the same change — src/map.md, call/map.md, tests/map.md, docs/guide/map.md, staging/map.md — and the guide/registry additions are single-homed.
      artifacts: [crates/repark-spark/src/map.md, crates/repark-spark/src/call/map.md, crates/repark-spark/src/tests/map.md, docs/guide/map.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before/after is pinned red-first (§Red first): the CALL died on AWS SDK load_table and the maintenance orphan row planned/ran; after the change the refusal fires before IO and the row is skipped with the reason.
      artifacts: [crates/repark-spark/src/call.rs, crates/repark-spark/src/call/run_maintenance_apply.rs]
  complete: true
```
