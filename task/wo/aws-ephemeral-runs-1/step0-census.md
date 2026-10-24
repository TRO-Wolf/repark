# AWS-EPHEMERAL-RUNS-1 step 0 census (2026-10-24, Claude Sonnet 5.5, read-only)

Read from the tree at `7a789fc`. Paths are under `python/repark/tests/` unless a column says otherwise.
Result: **no leg reads a table it did not create in the same run.** Five findings below the tables
bear on steps 1 to 3; finding F-1 is a question the card does not decide.

## Table-creating sites, by leg

| Leg | Site (file:line) | Name pattern | Fixed or per-run |
|---|---|---|---|
| acceptance, Glue silver | `parity/test_aws_acceptance.py:185` | `testing_<entity>` | **fixed** (entity from `REPARK_ACCEPT_ENTITY`) |
| acceptance, S3 Tables silver | `parity/test_aws_acceptance.py:226` | `testing_<entity>` | **fixed** |
| acceptance, MW-4 Glue / MW-10 S3 Tables | `parity/test_aws_acceptance.py:255`, `:287` | `testing_mw4_mor_<uuid12>`, `testing_mw10_mor_<uuid12>` | per-run |
| acceptance, v3 Glue / S3 Tables | `parity/test_aws_acceptance.py:326`, `:366` | `testing_v3_dv_<uuid12>` | per-run |
| acceptance, S1-S7 cutover (both catalogs) | `parity/test_aws_acceptance.py:398` (stem), `_sql_harden_cutover_run.py:523` (`make_names`) | `testing_h1<program12><uuid6>` and, for the gold program, `<stem>_survey/_visit/_provider/_appointment/_dates/_fct/_agg` | per-run |
| acceptance, CREATE OR REPLACE twice | `parity/test_aws_acceptance.py:530`, `:555` | `testing_replace2_<uuid12>` | per-run |
| acceptance, U12 S3 path write | `parity/test_aws_acceptance.py:577` | objects under `<warehouse>/testing_repark_acceptance/u12s3path_<uuid12>/`, **no catalog entry** | per-run |
| dbt gold | `python/dbt-repark/tests/test_aws_acceptance_gold.py:78` (stem), `:104-116` (profile, aliases, source names) | `testing_dbt1_<uuid8>_<survey|visit|provider|appointment|dates>` (seeded) and `_gold_fct`, `_gold_agg` (dbt aliases `{stem}_{model}`) | per-run |
| integrity U | `parity/test_aws_acceptance_commit_unknown.py:149` (`table_of`), seeded by `seed` at `:196` with `CREATE OR REPLACE TABLE` | `testing_cu_<door>_<cell>_v<format>` | **fixed** |
| integrity X | `parity/test_aws_acceptance_cross_release.py:113`, `:136` (created by `_acceptance_cross_release_runner.py:104-115`, `CREATE OR REPLACE TABLE`) | `testing_xrel_<kind>_v<format>_<writer>` and `testing_xrel_mixed_v<format>_<writer>` | **fixed** |

The fixed names are the ones the card's ruling 1 calls out (`testing_cu_*`, the silver `testing_<entity>`; the
card also names `testing_appointment`, which is the silver entity of the nightly's `REPARK_ACCEPT_ENTITY`
value, not a literal in the tree). `testing_xrel_*` is a third fixed family the card does not list.

## Table-reading sites, and whether the run created the table

| Reader | Site | Table read | Created in this run |
|---|---|---|---|
| silver `tableExists` / row count / MERGE | `parity/test_aws_acceptance.py:126,135,143,147` | `testing_<entity>` | yes today only on a fresh namespace: the code takes CTAS when absent and MERGE into a **prior run's** table when present (`:139` asserts `>=` for that reason). With a run prefix the table is always absent at the start, so it is created in this run. |
| MW-4/MW-10, v3, replace2 observers | `_acceptance.py` (`run_mor_merge_compact_expire`), `parity/_acceptance_v3.py`, `parity/_acceptance_replace.py` | the table the same call just created | yes |
| v3 S3 Tables refusal check | `parity/test_aws_acceptance.py:375` | `tableExists(table)` on the table name of `:366` | yes (asserts it was **not** left behind) |
| S6 leaked-table probes | `parity/test_aws_acceptance.py:422`, `:433` | `<stem>_<suffix>` in the scratch namespace and in `_NAMESPACE` | yes (present check) / absent check in the other namespace |
| dbt gold | `test_aws_acceptance_gold.py:84-87` (seed via `_seed_gold_sql`), `:127-157` | the `{stem}_...` family | yes. **The card says dbt "reads the silver table acceptance creates"; the code does not: dbt seeds its own five silver-shaped tables (`_sql_harden_cutover_run.py:417`). Nothing couples dbt to the acceptance module.** |
| integrity U | `test_aws_acceptance_commit_unknown.py:200-262` | the `table_of` table | yes (`seed` precedes every read) |
| integrity X | `_acceptance_cross_release_runner.py:154-165` (`read`), `_acceptance_cross_release.py:116-170` | the `spec["table"]` table | yes (`cross_cell`: writer creates then reader reads; `mixed_cell`: created, appended, re-read; the previous-release wheel installs in the same job) |
| cross-read Glue / S3 Tables | `parity/test_aws_acceptance_cross_read.py:48-57` (`_discover_tables`), `:90-114` | **every** `testing_` table in `testing_repark_acceptance` (`SHOW TABLES`, prefix `testing_`) | **no, today: it reads every prior run's tables.** This is the one site the unit changes (step 4). |

## The bench table

`ICE-READ-PERF` creates `testing_repark_acceptance.ice_read_perf_bench` (`aws-acceptance.yml` `BENCH_NAMESPACE`
and `BENCH_TABLE`). The name does not begin with `testing_`, so the discovery prefix `testing_` never listed it
and the IAM resource `table/testing_repark_acceptance/testing_*` does not cover it. Exclusion pattern for the
cleanup script: only names matching `^testing_r<digits>_` are candidates; `ice_read_perf_bench` is also
excluded by that shape alone (there is no by-name check). The bench job is dispatch-only and has no `REPARK_ACCEPT_RUN_ID`.

## Where `testing_` is assumed

- `_acceptance.py:66-69`: `ACCEPTANCE_NAMESPACE = "testing_repark_acceptance"`, `ACCEPTANCE_TABLE_PREFIX = "testing_"`.
- `parity/test_aws_acceptance.py` 185, 226, 255, 287, 326, 366, 398, 530, 555 and
  `python/dbt-repark/tests/test_aws_acceptance_gold.py:78`: build names from `ACCEPTANCE_TABLE_PREFIX`.
- `parity/test_aws_acceptance_commit_unknown.py:149` and `parity/test_aws_acceptance_cross_release.py:113,136`:
  **literal** `testing_cu_` and `testing_xrel_`, not through the constant.
- `parity/test_aws_acceptance_cross_read.py:56,97`: the discovery filter and its failure text.
- Pinned by tests: `parity/test_acceptance_helpers.py:158` and `parity/test_acceptance_v3_helpers.py:62,143`
  (`ACCEPTANCE_TABLE_PREFIX == "testing_"`, and an AST check that `ACCEPTANCE_TABLE_PREFIX` feeds the v3 name);
  `python/repark-parity/tests/test_cross_read_progress.py:49,104,131,216` (executes the discovery AST with the
  constant injected and asserts the text `no testing_ tables`).
- `docs/tier2-aws.md` §2 to §3 and the workflow header comment: "never-teardown", "cannot delete Glue tables".
- Local-only uses of `testing_` (`_testing_create_ref`, `test_acceptance_commit_gate.py` `testing_t`) are unrelated.

## Findings

- **F-1 (a question, not decided here).** All three leg jobs would share one `REPARK_ACCEPT_RUN_ID`
  (`github.run_id`) and so one prefix `testing_r<run_id>_`. Then (a) a leg's `if: always()` cleanup deletes the
  tables other legs are still using, and (b) a leg's cross-read lists the other legs' in-flight tables.
  The cleanup validator accepts only `testing_r<digits>_`, so a per-leg label cannot be a letter. Options, none
  picked: (1) one id per leg job built from the run id plus a leg digit; (2) a shared prefix plus a leg stem
  filter on cross-read and cleanup; (3) cleanup only in a last job. Step 3 waits on the answer.
- **F-2.** `testing_xrel_*` is a fixed-name family absent from the card's list; it takes the run prefix too.
- **F-3.** The U12 S3 path write leaves objects under `<warehouse>/testing_repark_acceptance/u12s3path_<uuid>/`
  with no catalog entry. A table-scoped cleanup does not reach them, and no claim in this unit covers them.
- **F-4.** The previous-release wheel in leg X (`repark==1.5.5`) builds table names from the spec the head
  interpreter passes, so the run prefix reaches both sides with no change to the old wheel.
- **F-5.** The dbt statement of the card is inexact (above); the split into `acceptance-dbt` together with the
  acceptance module is therefore a scheduling choice of the card, not a data dependency.

## Resolutions (orchestrator rulings, 2026-10-24)

- **F-1 resolved.** Two variables: `REPARK_ACCEPT_RUN_ID` (the run id, digits) and `REPARK_ACCEPT_LEG` (`acc` for the
  acceptance and dbt jobs, `u`, `x`). Every table above is named `testing_r<run_id>a<attempt>_<leg>_<name>`; a job cleans and
  cross-reads only its own run-and-leg prefix, so no job can reach another job's tables. The backstop matches
  `testing_r<digits>_` of any leg and groups by the full run-and-leg prefix. Local defaults: run id `0`, leg `acc`.
- **F-3 resolved.** The U12 S3 path write names its objects `u12s3path_r<run_id>a<attempt>_<leg>_<uuid12>`; the leg's cleanup
  deletes the objects under `<warehouse>/testing_repark_acceptance/` whose key starts with that stem, behind the same
  warehouse guard. Legacy objects and legacy tables stay untouched.
- **Q5.** `REPARK_ACCEPT_ENTITY` is `appointment` in the environment, so the card's `testing_appointment` is the
  resolved silver name; it now carries the run-and-leg prefix like every other table.
- **Re-run attempts (fix round 2).** A re-run reuses `github.run_id`, so attempt 2 would meet attempt 1's leftovers under
  one prefix. `github.run_attempt` joins the name through `REPARK_ACCEPT_RUN_ATTEMPT` (digits, local default `0`): tables
  are `testing_r<run_id>a<attempt>_<leg>_…`, the per-job validator is `^testing_r[0-9]+a[0-9]+_(acc|u|x)_$`, and age mode
  groups by run, attempt and leg.
