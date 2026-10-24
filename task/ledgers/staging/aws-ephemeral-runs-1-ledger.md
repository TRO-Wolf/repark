# Unit ledger — AWS-EPHEMERAL-RUNS-1 · every aws-acceptance run creates, cross-reads and removes its own tables

**Date:** 2026-10-24 · **Branch:** `feat/aws-ephemeral-runs-1` · **Base:** `7a789fc` (`main`)
**Model:** claude-sonnet-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [../../roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md](../../roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md)
(the owner's rulings of 2026-10-24). **Census:** [../../wo/aws-ephemeral-runs-1/step0-census.md](../../wo/aws-ephemeral-runs-1/step0-census.md).

**Retires:** this ledger moves to `../completed/` when one dispatched `aws-acceptance` run on `main` proves
C-015 to C-018, or when the owner closes the unit.

**This round builds** the local half only: the run prefix at every table-creating site, the cleanup script, and
the separate Spark REST catalog block. The workflow job split is not built; it waits on census finding F-1 (one
shared run prefix lets a leg's cleanup and cross-read reach other legs' tables) and on the question whether the
CI venv can resolve `boto3` (it is absent from `uv.lock`). Everything a live run alone can prove stays OPEN.

## PROPOSITION LEDGER — AWS-EPHEMERAL-RUNS-1 — 2026-10-24

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `run_table_prefix()` returns `testing_r<run_id>_` from `REPARK_ACCEPT_RUN_ID`, and the constant `ACCEPTANCE_TABLE_PREFIX` stays `testing_`. | `test_acceptance_helpers.py` prefix tests | PROVEN | `pins:` in the test module docstring and the parity `map.md` row. |
| C-002 | An absent or empty run id defaults to the local marker `0`; a non-digit id (letters, spaces, signs, a dot, non-ASCII digits) raises `ValueError` naming the variable; a longer id is never a prefix-collision of a shorter one. | the same module, 9 refusal cells plus the collision cell | PROVEN | The local marker keeps the cleanup validator's `testing_r<digits>_` shape. |
| C-003 | Every table-creating site (acceptance, dbt gold, leg U `testing_cu_*`, leg X `testing_xrel_*`) names its tables through `run_table_prefix()`; no literal `testing_cu_` or `testing_xrel_` and no `{ACCEPTANCE_TABLE_PREFIX}` interpolation remains in a leg. | the source guard in `test_acceptance_helpers.py` over the five leg files | PROVEN | Census table in the work order directory. |
| C-004 | The cleanup script refuses any prefix that is not exactly `testing_r<digits>_` before it builds a client. | `test_aws_acceptance_cleanup.py`, 12 refusal cells | OPEN | |
| C-005 | The bench table `ice_read_perf_bench`, the legacy `testing_*` names and every other non-run name are never selected by the prefix mode, the age mode or the deletion guard. | the same module | OPEN | The bench is excluded twice: by shape and by name. |
| C-006 | Objects are listed and deleted only below `<warehouse>/testing_repark_acceptance/`, strictly (the scratch prefix itself, a sibling directory, another bucket, a `..` path and an empty location all refuse), with a trailing-slash listing prefix. | the same module, 12 location cells and the outside-location no-call test | OPEN | |
| C-007 | Glue deletes go out in batches of 100, object deletes in batches of 1000, an error entry in either response raises, and a Glue cleanup deletes the table entries before the objects. | the same module | OPEN | |
| C-008 | `--older-than-hours N` removes a run prefix only when its newest table is strictly older than N hours, whole prefix at a time; a positive age is required. | the same module | OPEN | |
| C-009 | Cross-read discovery keeps only names that start with the run prefix, non-temporary: another run's tables, a longer run id, unprefixed names and the bench table are not discovered. | `test_cross_read_progress.py::test_discovery_keeps_only_the_run_prefix_and_never_the_bench_table` | PROVEN | |
| C-010 | `s3tables_spark_catalog_config(name, arn)` returns the eight-key REST block (`type=rest`, the regional `/iceberg` URI, the ARN as `warehouse`, SigV4 on, signing name `s3tables`, signing region, `S3FileIO`) with the region parsed from the ARN, no literal region in the function, no `catalog-impl`, and `s3tables_catalog_config` unchanged. | `test_acceptance_helpers.py` REST cells | OPEN | A value that is not a table bucket ARN raises. |
| C-011 | The cleanup script's default is a dry run: no delete call is issued without `--execute`. | `test_aws_acceptance_cleanup.py` dry-run and parser cells | OPEN | |
| C-012 | The workflow runs one credential-free `build` job and parallel `acceptance-dbt`, `integrity-u`, `integrity-x` jobs that each mint their own credential. | `make workflows-lint`, the workflow pin tests | OPEN | Not built. Waits on F-1: with one shared run prefix, a finished leg's cleanup removes the tables other legs still use and a leg's cross-read lists them. Options are in the census. |
| C-013 | Each leg job removes its own tables with `if: always()`, and a `backstop` job removes run prefixes older than 24 h. | the same | OPEN | Not built; same wait. |
| C-014 | The CI venv resolves `boto3` for the cleanup script from the existing lock. | `uv sync --locked --extra record` then `import boto3` | OPEN | `uv.lock` holds no `boto3` or `botocore`; the card halts on this. The script imports it lazily and nothing in a dependency file changed. |
| C-015 | One dispatched `aws-acceptance` run on `main` finishes every leg job green inside its own cap. | the dispatched run | OPEN | Behind the owner's scoped IAM grant and the one-time prune (card, "The owner's actions"). Carries ICE-INTEGRITY-PINS-1 C-068. |
| C-016 | After that run exits, the scratch namespaces hold no table of that run. | the dispatched run, a listing of both namespaces | OPEN | |
| C-017 | Each leg's cross-read of its own tables answers EQUAL or a named Spark error that is not the catalog-init failure, and the S3 Tables cross-read reads through Spark's REST catalog. | the dispatched run, the per-table lines | OPEN | Carries ICE-INTEGRITY-PINS-1 C-069. |
| C-018 | Leg U's wall time returns toward the 31 minutes of run 4 on a small namespace, or the listing cost is ruled out. | the dispatched run | OPEN | Card, "Unit shape". |
