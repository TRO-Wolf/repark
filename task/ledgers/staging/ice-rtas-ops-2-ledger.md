# Unit ledger — ICE-RTAS-OPS-2 · RePark opt-in for the fork's RTAS replace commit (rating row V2-24)

**Date:** 2026-09-18 · **Branch:** `ice-rtas-ops-2` · **Base:** `433a8352`
**Model:** muse-spark-1.3-contributor (round 1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Model:** claude-opus-5 (round 2) — closes Critic-3 logic findings L-01 (native ANSI door) and L-02
(service-managed new-table RTAS), both P2, on the rebased head `888d7143`.

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
| C-004 | (Round-1 disposition, superseded by C-018/C-019 in round 2.) `execute_ctas_service_managed` disposition recorded: it commits through create-first + `commit_append`, never reaches the staged type, so no second mechanism was invented; the fact is recorded here as a residue with file and line. | Ledger residue + code read. | **PROVEN** (round-1 fact; superseded by C-018, C-019) | Residue: `crates/repark-spark/src/ctas.rs:419` `execute_ctas_service_managed` creates first via `catalog.create_table` (:438) and commits via `repark_iceberg::write::commit_append` (:446) — the `StagedTableTransaction` type is unreachable there, so the opt-in cannot apply. Left unchanged; no second mechanism invented. Reachable only for a not-yet-existing table on a service-managed catalog, where `OR REPLACE` create-first has no prior snapshot to replace. |
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
| C-015 | L-01: the native ANSI door's `execute_staged_create` (`crates/repark-sql/src/create_table.rs`) chains `.with_replace_write(replace_write)` on both the `begin_create` and the `begin_replace` arm, with `replace_write = create.or_replace && query.is_some()` computed once in `execute_create_table`; the native RTAS cells answer the fixture `rtas_ops` values. | Diff + native pins green. | **PROVEN** | `create_table.rs:126` computes `replace_write`; `:185` and `:205` chain it on `begin_create` / `begin_replace`. Green (`cargo test -p repark-sql --lib rtas_ops_tests`, 9 passed): `native_ctas_then_rtas_records_append_then_overwrite` (`[append, overwrite]`, 2 rows), `native_rtas_creating_the_table_records_overwrite` (`[overwrite]`), `native_empty_rtas_on_new_table_records_delete` (`[delete]`), `native_empty_rtas_twice_records_two_deletes` (`[delete, delete]`, 0 rows). Expected values are the fixture `ctas_then_rtas` / `rtas_new_table` / `rtas_empty_new` / `rtas_empty_twice` operation lists. |
| C-016 | L-01 controls on the native door: plain CTAS records `[append]`; column-def `CREATE OR REPLACE TABLE t (id BIGINT)` over a seeded table keeps `[append]` and reads zero rows (the 2026-09-17 Spark cell). The column-def control now watches the native file, closing Critic-3 Attack 5 ("coldef control cannot see repark-sql"). | Pins green + red under the column-def mutation (C-017). | **PROVEN** | Green: `native_plain_ctas_records_append`, `native_coldef_replace_commits_no_snapshot`. The column-def form carries no query, so `replace_write` is false on it by construction. |
| C-017 | Mutation proof for the native opt-in: reverting the staged opt-in reds the four RTAS cells; letting the column-def form take the opt-in reds the column-def control. | Transcripts. | **PROVEN** | M1 (`.with_replace_write(replace_write)` → `.with_replace_write(false)` on both staged arms): `5 failed, 4 passed` — the four RTAS cells plus `native_service_managed_empty_rtas_records_delete_then_delete` (its second statement takes the staged replace arm). M3 (`replace_write = create.or_replace`, dropping `&& query.is_some()`): `1 failed, 8 passed` — `native_coldef_replace_commits_no_snapshot`, `left: ["append", "delete"]`, `right: ["append"]`. Both restored from backup; `git diff` shows only the fix. |
| C-018 | L-02 fork-API question: the pinned fork (RP-24 `8fb44a39`) exposes a PUBLIC path that commits `overwrite_by_row_filter(AlwaysTrue)` with added files, empty allowed, on an already-created table — so no fork ask and no `RTAS-OPS-SM-1` gap row. RePark calls it through one helper, `repark_iceberg::write::commit_replace_write`; no fork semantics are patched locally (fork rule 3). | Fork source read + helper diff. | **PROVEN** | `crates/iceberg/src/transaction/mod.rs:85` `pub use overwrite_files::OverwriteFilesAction`, `:255` `pub fn overwrite_files`; `overwrite_files.rs:190` `pub fn overwrite_by_row_filter`, `:196` `pub fn allow_empty_commit`. `overwrite_files_operation.rs:40` keeps the operation's field `pub(crate)`; the public entry is the action builder. `materialize_pending` (`staged_table.rs:305-318`) uses the identical chain. Q-20c-2 made the empty-commit permission an explicit opt-in (action default `false`, other callers keep `PreconditionFailed`); the helper opts in explicitly and only for RTAS, so the default the ruling protects is unchanged — `commit_overwrite_replace_all_to` (INSERT OVERWRITE) does not set it. Helper: `crates/repark-iceberg/src/write/overwrite_commit.rs::commit_replace_write`, stamped with `operation_id_and_summary` and folded through `commit_result` like `commit_append`, so the ICE-COMMIT-UNKNOWN-1 class survives. |
| C-019 | L-02 Spark door: `execute_ctas_service_managed` commits through `commit_replace_write` when `ctas.or_replace` (new-table RTAS → `[overwrite]`, empty → `[delete]`) and keeps `commit_append` for plain CTAS; drop-on-abort and the commit-unknown arm are unchanged. | Diff + pins green + mutation red. | **PROVEN** | `crates/repark-spark/src/ctas.rs:446-450`. Green (`cargo test -p repark-spark --lib service_managed_ctas`, 11 passed): `ctas_service_managed_rtas_creating_the_table_records_overwrite` (`[overwrite]`, 3 rows, one `create_table` call), `ctas_service_managed_empty_rtas_records_delete_then_delete` (`[delete]`, then `[delete, delete]`, 0 rows), control `ctas_service_managed_plain_ctas_records_append`; the pre-existing `ctas_service_managed_empty_select_creates_table_without_snapshot` still pins plain-empty at no snapshot. M4 (`if ctas.or_replace` → `if false`): `2 failed, 9 passed` — the two RTAS pins. Restored from backup. |
| C-020 | L-02 native door: `create_first_service_managed` commits through `commit_replace_write` when `replace_write` (so only for `OR REPLACE … AS SELECT`; the column-def form has no query and commits nothing); plain CTAS keeps `commit_append`. | Diff + pins green + mutation red. | **PROVEN** | `crates/repark-sql/src/create_table.rs` `create_first_service_managed` gains `replace_write: bool`. Green: `native_service_managed_rtas_creating_the_table_records_overwrite` (`[overwrite]`, 2 rows), `native_service_managed_empty_rtas_records_delete_then_delete`, control `native_service_managed_plain_ctas_records_append` (`[append]`; plain-empty → no snapshot). M2 (`if replace_write` → `if false` in the service-managed arm): `2 failed, 7 passed` — the two service-managed RTAS pins. Restored from backup. |
| C-021 | Registry and hand-off: `docs/spark-sql-iceberg-parity.md` RTAS-OPS-1 names both doors, the service-managed disposition (existing table → staged replace; new table → create-first + `commit_replace_write`) and the new pins; the `test_ice_rtas_byname_1.py` module docstring no longer says "RTAS operations as xfails" (Critic-3 HANDOFF-CL). | Diffs. | **PROVEN** | RTAS-OPS-1 repark paragraph rewritten ("on **both SQL doors** (ADR-0002 §3)", the service-managed sentence), Pin paragraph lists the native and Spark service-managed Rust pins, Rationale names round 2. Docstring line 1 now reads "…on the Spark door, plus the RTAS operation pins." No `RTAS-OPS-SM-1` row and no `F-RTAS-OPS-SM-1` ask were filed: C-018 found the public path. |
| C-022 | `map.md` lockstep for round 2: `crates/repark-sql/src/map.md`, `crates/repark-sql/src/create_table/map.md` (new `rtas_ops_tests.rs`), `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md`, `crates/repark-iceberg/src/write/map.md`, `python/repark/tests/map.md`; no code comment added (owner ruling 2026-08-26). | `make check-map-sync` + comment-ban gate. | **PROVEN** | See Gates (round 2). |

VERDICT: 22 clauses, 22 PROVEN, 0 OPEN, 0 REJECTED.

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

Round 2 (claude-opus-5, 2026-09-18) targeted gates on `3c2d0e6c`, release native rebuilt from that code
(`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`):

- comment ban (`comment_ban.py` vs `origin/main`): exit 0; staged-diff comment grep empty before each commit.
- `cargo test -p repark-sql`: 444 passed, 0 failed across 19 test binaries (lib 356).
- `cargo test -p repark-spark`: 1197 passed, 0 failed, 4 ignored across 10 test binaries.
- `cargo clippy --locked -p repark-iceberg -p repark-sql -p repark-spark --all-targets -- -D warnings
  -A clippy::disallowed_methods`: clean. `cargo fmt --all -- --check`: clean. rust-file-size: 623 files clean.
- `test_ice_rtas_byname_1.py` + `test_acceptance_replace_offline.py`: 43 passed, 1 skipped (live), 1 xfailed
  (`test_dataframe_writeto_appends_by_name`, F-DML-FIELD-ID-1).
- `scripts/check_ledger_grammar.py`: 188 live ledgers clean. `make check-map-sync`: 285 maps clean.
- mutations M1–M4 as quoted under C-017, C-019, C-020.
- Not run (orchestrator, box memory): whole facade suite, whole parity suite, `make verify`.

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
      evidence: Round 1 recorded the service-managed create-first arm as residue C-004. Round 2 found the fork's public overwrite-by-filter plus allow_empty_commit path (C-018) and routes new-table RTAS on both doors' create-first arms through commit_replace_write (C-019, C-020); plain CTAS keeps commit_append, pinned and mutation-proven. Dual-door coverage (ADR-0002 §3) — the native ANSI door takes the same opt-in on both staged arms, gated on OR REPLACE with a query so the column-def form never takes it; the native column-def control goes red when it does (C-015..C-017).
      artifacts: [crates/repark-spark/src/ctas.rs, crates/repark-sql/src/create_table.rs, crates/repark-iceberg/src/write/overwrite_commit.rs, crates/repark-sql/src/create_table/rtas_ops_tests.rs]
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
