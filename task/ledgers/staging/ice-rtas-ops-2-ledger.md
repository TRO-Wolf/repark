# Unit ledger — ICE-RTAS-OPS-2 · RePark opt-in for the fork's RTAS replace commit (rating row V2-24)

**Date:** 2026-09-18 · **Branch:** `ice-rtas-ops-2` · **Base:** `433a8352`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Fork PR #290 (F-RTAS-OPS-1) landed and reached main with the RP-23
pin bump (`4151b488`). It added
`iceberg::transaction::StagedTableTransaction::with_replace_write(bool)`: when
set, `commit()` stages `overwrite_files().overwrite_by_row_filter(AlwaysTrue)`
instead of `fast_append()` — operation `overwrite` with files, `delete` with
none. RePark never sets it, so the four Spark cells in
`docs/spark-sql-iceberg-parity.md` §RTAS-OPS-1 stay strict xfails. This unit
takes the RP-23 consumer slot the fork-sync row names ("run 20c
ICE-RTAS-BYNAME-1 (RTAS opt-in)").

**Sources.** `python/repark/tests/ice_rtas_byname_1_spark_oracle.json` section
`rtas_ops` (live PySpark 4.1.2 + Iceberg 1.11.0); registry prose
`docs/spark-sql-iceberg-parity.md` §2.3 RTAS-OPS-1; the 2026-09-17 column-def
measurement (orchestrator, live PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog; column-def
`CREATE OR REPLACE` commits no snapshot on any of the three shapes — transcript excerpt under C-007).

**Ruling Q-21c-1 (orchestrator).** The card's "five xfails" was wrong:
`test_dataframe_writeto_appends_by_name` stays xfailed — its reason is
`BLOCKED-ON-FORK F-DML-FIELD-ID-1`, a different fork ask. Only the four RTAS
pins flip.

**Not in this unit:** the column-def replace path (control only, step 2); any
second commit mechanism for the service-managed path; the live tier; push; PRs;
`gh`; `STATUS.md`.

## PROPOSITION LEDGER — ICE-RTAS-OPS-2 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `execute_ctas` sets `.with_replace_write(true)` on the `CtasMode::Replace` (`begin_replace`) branch exactly when the statement carried `OR REPLACE` (`ctas.or_replace`); plain CTAS keeps `append`. | Diff of `crates/repark-spark/src/ctas.rs` + green plain-CTAS control. | **PROVEN** | `crates/repark-spark/src/ctas.rs:243-247` chains `.with_replace_write(ctas.or_replace)` after `begin_replace`; the Replace arm is reachable only with `or_replace` set, plain CTAS passes `false` (fork default). Green: `test_rtas_replace_records_overwrite` (`["append", "overwrite"]`) and `test_plain_ctas_records_append` (`["append"]`). Gate fallout: the two added lines tipped `execute_ctas` over clippy `too_many_lines`, so the function carries bare `#[allow(clippy::too_many_lines)]` (:144) — same-file precedent (`execute_ctas_service_managed`'s bare `too_many_arguments` allow); the round's no-comment rule puts the reason here, not inline. A `.map`-combinator fold was tried and dropped: rustfmt splits it back over the limit. |
| C-002 | Same opt-in on the `CtasMode::StagedCreate` (`begin_create`) branch exactly when `or_replace` (Spark records `overwrite` for an RTAS that creates the table). | Diff + `test_rtas_new_table_records_overwrite` green. | **PROVEN** | `crates/repark-spark/src/ctas.rs:221-224` chains `.with_replace_write(ctas.or_replace)` after `begin_create`. Green: `test_rtas_new_table_records_overwrite` (`["overwrite"]`), `test_rtas_empty_new_records_delete` (`["delete"]`). |
| C-003 | The column-def replace path (`crates/repark-sql/src/create_table.rs:197`, Spark-door twin in `crates/repark-spark/src/create_table.rs`) is untouched — Spark commits no snapshot there and RePark already commits none. | `git diff` shows no change to either file + control pin green. | **PROVEN** | `git status` shows neither file modified. Green: `test_coldef_replace_commits_no_snapshot` — ops stay `["append"]`, zero rows after the replace. |
| C-004 | `execute_ctas_service_managed` disposition recorded: it commits through create-first + `commit_append`, never reaches the staged type, so no second mechanism was invented; the fact is recorded here as a residue with file and line. | Ledger residue + code read. | **PROVEN** | Residue: `crates/repark-spark/src/ctas.rs:419` `execute_ctas_service_managed` creates first via `catalog.create_table` (:438) and commits via `repark_iceberg::write::commit_append` (:446) — the `StagedTableTransaction` type is unreachable there, so the opt-in cannot apply. Left unchanged; no second mechanism invented. Reachable only for a not-yet-existing table on a service-managed catalog, where `OR REPLACE` create-first has no prior snapshot to replace. |
| C-005 | The four RTAS pins pass with their `xfail(strict, BLOCKED-ON-FORK F-RTAS-OPS-1)` markers removed: `test_rtas_replace_records_overwrite` → `["append", "overwrite"]`, `test_rtas_new_table_records_overwrite` → `["overwrite"]`, `test_rtas_empty_new_records_delete` → `["delete"]`, `test_rtas_empty_twice_records_two_deletes` → `["delete", "delete"]`. | pytest run naming the four tests green. | **PROVEN** | RED on the base tree (markers removed, no Rust change yet), 2026-09-18: `4 failed, 37 deselected` — `test_rtas_replace_records_overwrite`, `test_rtas_new_table_records_overwrite`, `test_rtas_empty_new_records_delete`, `test_rtas_empty_twice_records_two_deletes` (e.g. `assert [] == ['delete', 'delete']` on the empty-twice cell). |
| C-006 | New control: plain (no `OR REPLACE`) CTAS still records `["append"]`. | New test green on base and on the fix. | **PROVEN** | `test_plain_ctas_records_append` green in the 7-selected run. Base-tree equivalence holds by code identity: the plain path passes explicit `false`, which is the fork default the base tree ran unconditionally. |
| C-007 | New control: column-def `CREATE OR REPLACE TABLE … (id BIGINT) USING iceberg` over a seeded table leaves snapshot operations at the pre-existing `["append"]` and reads zero rows (the 2026-09-17 transcript answer, cited by date in the docstring). | New test green. | **PROVEN** | `test_coldef_replace_commits_no_snapshot` green; docstring cites "live Spark 4.1.2 answer measured 2026-09-17". Transcript excerpt (2026-09-17): `coldef_replace_existing_with_rows` → ops `["append"]`, count 0; `coldef_replace_new_table` and `coldef_replace_existing_empty` → ops `[]`. |
| C-008 | The `overwrite` cell asserts the parity-row summary keys: `added-data-files`, `added-records`, `total-records`, `total-data-files` present and no key starting with `deleted-`. | Assertion text + green run. | **PROVEN** | `test_rtas_replace_summary_keys` reads the `summary` map off `sc.ns.rt.snapshots` and asserts the four keys plus zero `deleted-*` keys; green in the 7-selected run. |
| C-009 | Mutation proof: with the `ctas.rs` change reverted and the native rebuilt, the four flipped tests FAIL; the change is then restored. | Failing names + counts pasted below. | **PROVEN** | Post-commit-1 revert check, 2026-09-18 (`032e0b0d` reverted in the working tree, native rebuilt): `4 failed, 40 deselected` — `test_rtas_replace_records_overwrite`, `test_rtas_new_table_records_overwrite`, `test_rtas_empty_new_records_delete`, `test_rtas_empty_twice_records_two_deletes`. Fix restored byte-clean (`git diff` empty vs the commit) and the native is rebuilding. |
| C-010 | `_record_ice_rtas_byname_1_oracle.py:104` reads `REPARK_ORACLE_IVY` and only sets `spark.jars.ivy` when it is present, otherwise the default Ivy cache — the `_record_ice_promote_read_1.py:680` convention `python/repark/tests/map.md` describes. No JVM started. | Diff + import check. | **PROVEN** | Diff replaces the hardcoded `/tmp/ic-build/.ivy2` with the `os.environ.get("REPARK_ORACLE_IVY")` conditional (mirrors `_record_ice_promote_read_1.py:680-682`); `py_compile` clean on both touched test files; no JVM started. |
| C-011 | Registry §2.3 RTAS-OPS-1 moved to FIXED: repark paragraph rewritten to what RePark now does, Spark paragraph and oracle citation kept, four pins named without xfail markers, Rationale states the column-def no-snapshot answer pinned as a control (citing the 2026-09-17 measurement), and the "no RePark-side patch can change the recorded operation" sentence corrected. | Before/after line numbers. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` §2.3 `RTAS-OPS-1`: repark paragraph rewritten to `[append, overwrite]` / `[overwrite]` / `[delete]` (twice `[delete, delete]`) with the `execute_ctas` opt-in on both staged arms; four pins named without markers plus the three controls; Rationale FIXED with the column-def no-snapshot control; the "no RePark-side patch can change the recorded operation" sentence is gone. |
| C-012 | `docs/fork-sync.md` RP-23 row names this unit as the consumer that took the RTAS opt-in. | Diff of the Consumers clause. | **PROVEN** | `docs/fork-sync.md` RP-23 row's Consumers clause now names ICE-RTAS-OPS-2 as the unit that took the RTAS opt-in (diff of the one row; RP-24 row kept on the rebase). |
| C-013 | `map.md` lockstep for every touched directory; `python/repark/tests/map.md` carries a `pins: ice-rtas-ops-2/C-NNN` citation on the test-file entry; `task/ledgers/staging/map.md` lists this ledger. | Diffs + `check-map-sync` green. | **PROVEN** | `crates/repark-spark/src/map.md` `ctas.rs` entry (opt-in + the `too_many_lines` allow), `python/repark/tests/map.md` test-file entry with `pins: ice-rtas-ops-2/C-NNN` and the `_acceptance_replace.py` entry, `task/ledgers/staging/map.md` row; `make check-map-sync` clean (285 maps). |
| C-014 | The `_acceptance_replace.py` twice-leg asserter follows the ordered answer: seed `append`, each replace `overwrite` — on all three legs (memory offline, Glue, S3 Tables), with the `python/repark/tests/map.md` entry trued up in the same commit. | `test_acceptance_replace_offline.py` green; AWS legs unrunnable here (tier-2 never runs unmerged code). | **PROVEN** | Full-suite sweep 2026-09-18 reded `test_create_or_replace_twice_on_memory_catalog`: the measured outcome is now `{seed: append, replace1: overwrite, replace2: overwrite}` — the ordered Spark shape, not a regression. The helper's `all append` text encoded the pre-fork engine. Same-commit map update at `python/repark/tests/map.md` `_acceptance_replace.py` entry. |

VERDICT: 14 clauses, 14 PROVEN, 0 OPEN, 0 REJECTED.

## Open questions

None. No ambiguity surfaced.

## Gates

Run by the orchestrator on the rebased head (main `6617d215`, fork pin RP-24 `8fb44a39`), release native
(`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`), 2026-09-18:

- comment ban (`comment_ban.py` vs `origin/main`): 0 hits.
- `test_ice_rtas_byname_1.py` + `test_acceptance_replace_offline.py`: 43 passed, 1 skipped (live), 1 xfailed
  (`test_dataframe_writeto_appends_by_name`, BLOCKED-ON-FORK F-DML-FIELD-ID-1, Q-21c-1).
- whole facade suite `-n 8`: 9692 passed, 2 failed, 398 skipped, 43 xfailed (47 min under shared-box I/O load). The two
  failures are environmental and pass alone (`2 passed in 15.04s`):
  `test_perf_ice_catalog_io_1.py::test_the_second_statement_on_a_many_manifest_table_is_under_the_target` (wall-clock
  budget) and `test_t2_sort_memory.py::test_reverse_sort_succeeds_when_pool_raised`; neither touches CTAS.
- whole parity suite `-n 8`: 756 passed, 3 skipped, 12 xfailed.
- `make verify`: see the PR body (re-run after this ledger was completed).

Round note: the Muse round ran five hours under box load and looped on `make verify`; the orchestrator stopped it with all
content staged, committed it with both trailers, rebased, and ran the gates above.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-rtas-ops-2
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the tree and a green or red run — the four RTAS pins red on the base tree and red again under the ctas.rs revert (C-005, C-009), the three controls green (C-006..C-008), the column-def path untouched and pinned against the live 2026-09-17 cell (C-003, C-007).
      artifacts: [crates/repark-spark/src/ctas.rs, python/repark/tests/test_ice_rtas_byname_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — RTAS on an existing table, on a missing table, with an empty SELECT, twice empty; plain CTAS control; column-def replace on a table with rows.
      artifacts: [python/repark/tests/test_ice_rtas_byname_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The service-managed create-first arm cannot reach the staged type; recorded as residue C-004, never a silent second mechanism.
      artifacts: [crates/repark-spark/src/ctas.rs]
    - id: AT-4
      status: N/A
      justification: No session, cache or concurrency state changes; the opt-in is one builder flag per statement.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, injection or deserialization surface.
    - id: AT-6
      status: ATTACKED
      evidence: The acceptance replace-twice asserter follows the new ordered answer on all three legs (memory offline twin green; Glue and S3 Tables replaces take the same staged begin_replace arm, seed CTAS stays append).
      artifacts: [python/repark/tests/_acceptance_replace.py, python/repark/tests/test_acceptance_replace_offline.py]
    - id: AT-7
      status: N/A
      justification: No performance-relevant path; one flag on an existing builder.
    - id: AT-8
      status: N/A
      justification: No dependency, build or CI configuration change.
    - id: AT-9
      status: N/A
      justification: No log format or error text change.
    - id: AT-10
      status: ATTACKED
      evidence: Registry RTAS-OPS-1 and the fork-sync RP-23 consumer clause state what the code now does, and the live oracle's four cells plus the orchestrator's column-def cell are the only Spark claims.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/fork-sync.md]
```
