# Unit ledger — PA-LOCAL-FS-POLICY-1 · the local-file write policy holds below the root, through missing parents, and for every UTF-8 path

**Scope (2026-10-15):** audit findings PA-02, PA-03 and PA-04 of
[the production audit](../../../docs/cutover/production-audit-2026-10-11.md), on both SQL doors
and the public `repark.sql()` entry, under
[the order](../../wo/pa-local-fs-policy-1.md) as amended by the owner at launch.

**Date:** 2026-10-15 · **Branch:** `fix/pa-local-fs-policy-1` · **Base:** `27c56f74` ·
**Model:** Claude (claude-opus-5-5), Opus 5.5 high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**This ledger closes when the unit's pull request merges.**

## PROPOSITION LEDGER — PA-LOCAL-FS-POLICY-1 — 2026-10-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Main writes an `EXPLAIN ANALYZE COPY` outside every warehouse on both doors and on `repark.sql()`, refuses the bare `COPY` on both doors, and lets `repark.sql()` write the bare `COPY`, the wrapped `COPY` and a `CREATE EXTERNAL TABLE` outside. | Step 0 probes, §1. | **PROVEN** | §1.1, §1.2, §1.3. |
| C-002 | Main writes `<warehouse>/link/new1/export` and `<warehouse>/link/m1/m2/m3/export` into the symlink's outside target on both doors. | Step 0 probes, §1. | **PROVEN** | §1.1, §1.2. |
| C-003 | Main panics on a multibyte character across byte 5, 7 or 9 of a location on both doors, and classifies `file://localhost/<outside>` as local and refuses it. | Step 0 probes, §1. | **PROVEN** | §1.1, §1.2. |
| C-004 | `PREPARE … AS COPY …` does not parse on either door, so no prepared statement can carry a `COPY`; `EXECUTE` of it answers "does not exist". | Step 0 probes, §1; the `Prepare` unit pin. | **PROVEN** | §1.1, §1.2; DataFusion keeps the prepared plan behind `pub(crate)` `get_prepared`, so the walk checks a `Prepare` node's input at `PREPARE`. |
| C-005 | Main's ANSI door ignores the explicit opt-in: its reader looks up `repark.sql.allow_local_filesystem_ddl` among `ConfigOptions::entries()`, which reports the real `repark.sql` extension's field unprefixed, so a session built with `repark.sql.allowLocalFilesystemDDL=true` still refuses outside. | Step 1 run on main, §2. | **PROVEN** | §2: the opt-in cell of `pa04_ansi_non_ascii_controls` refuses on main. |
| C-006 | PA-02: the guard walks the plan and checks every `Copy` and `CreateExternalTable` node at any depth before execution, so `EXPLAIN ANALYZE COPY … TO '<outside>'` refuses with the named policy error and writes no file on the Spark door, the ANSI door and `repark.sql()`. | The PA-02 pins on all three entries. | **PROVEN** | §6, §7: green on head on all three entries; the M-PA02 revert reds it. |
| C-007 | PA-02: a `Prepare` node's input is walked, so a prepared write is checked when it is prepared; `EXECUTE` cannot carry a `COPY` (C-004). | The `Prepare`-wrapped unit pin in `local_fs_policy.rs`. | **PROVEN** | §7: `the_walk_checks_a_write_below_every_carrier_but_plain_explain` refuses the `Prepare`-wrapped `COPY`; the M-PA02 revert reds it. |
| C-008 | PA-02: plain `EXPLAIN` of a `COPY` outside still answers and writes nothing on all three entries. | The plain-EXPLAIN pins. | **PROVEN** | §6: the plain-EXPLAIN pins pass on main and on head. |
| C-009 | PA-02: the warehouse control and the explicit opt-in control still write through `EXPLAIN ANALYZE COPY` on both doors and on the native dialect. | The PA-02 control pins. | **PROVEN** | §6: the controls pass on head; the M-C017 revert reds every opt-in control. |
| C-010 | PA-02, owner ruling 2026-10-15: `repark.sql()` gets the guard: `PyReparkSession.native()` installs `LocalFsGuardedDialect`, which plans, applies the guard, applies the shared belt and executes; no public signature changes. | The `repark.sql()` pins and the native-dialect pins. | **PROVEN** | §5, §7: the `repark.sql()` pins pass on head; the M-C010 revert reds them. |
| C-011 | PA-03: containment resolves the nearest existing ancestor with `canonicalize` and joins the missing tail, so `<warehouse>/link/new/export` and `<warehouse>/link/m1/m2/m3/export` with `link -> <outside>` refuse on all three entries, for `COPY` and for `CREATE EXTERNAL TABLE`. | The PA-03 pins and the resolver unit pins. | **PROVEN** | §7: the M-PA03 revert reds the symlink-escape pins of both doors and the resolver unit pins. |
| C-012 | PA-03: an unsafe or indeterminate destination refuses: a `..` in the missing tail, a dangling symlink, an ancestor whose metadata read fails for any reason but not-found, and an empty location. | The PA-03 indeterminate pins. | **PROVEN** | §7: the M-PA03 revert reds the `..`, dangling-symlink and empty-location pins. |
| C-013 | PA-03: a symlink that stays inside the warehouse, missing parents under the warehouse, and the opt-in through an outside symlink still write. | The PA-03 control pins. | **PROVEN** | §6: the controls pass on head. |
| C-014 | PA-04: the prefix strip is checked (`str::get`), so a character across byte 5, 7 or 9 classifies as a local path and the statement refuses with the named policy error, not a panic, on all three entries. | The PA-04 pins and the classification unit pins. | **PROVEN** | §7: the M-PA04 revert panics at byte 5 in the unit pins and reds both doors' byte-boundary pins. |
| C-015 | PA-04: ordinary non-ASCII names and the `file://localhost` forms classify: outside refuses, under the warehouse writes, and the opt-in writes outside. | The PA-04 control pins. | **PROVEN** | §6: the PA-04 controls pass on head. |
| C-016 | One implementation: classification, the checked prefix strip, containment, the plan walk and the conf read live in `repark_core::local_fs_policy` beside `CatalogRegistry`; both doors' private copies are deleted; `catalog_state.rs` uses the same strip; no new crate edge. | `make check-crate-dag`; the door files. | **PROVEN** | §4, §6: both doors re-export the one function; `make check-crate-dag` reports the same 25 internal edges as main. |
| C-017 | The conf read is scoped to the `repark.sql` config extension and accepts its field unprefixed or prefixed, so the ANSI door's explicit opt-in writes (C-005 fixed) and an absent extension stays closed. | The ANSI opt-in control pins; the existing `conf_true_opens_the_gate` and `missing_conf_extension_is_closed`. | **PROVEN** | §7: the M-C017 revert reds the conf-read unit pin, the ANSI opt-in controls and the Spark door's existing opt-in tests. |
| C-018 | One fresh Opus 5.5 high verifier, scoped to the pins, the audit's reproductions on both doors and a mutant per finding, finds no defect. | The closeout verifier (order §5). | **OPEN** | After CI; not this round. |

## 1. Step 0 — main measured on both doors (C-001 … C-004)

Main `27c56f74`, release module built from that head with an empty `git status --short`
(repark 1.5.5). Every probe ran in fresh temporary directories: `<warehouse>` is a registered
memory-catalog warehouse, `<outside>` a sibling directory, `<warehouse>/link -> <outside>`.
A line `files under X` lists every file found below X after the statement; a file listed again
under a later statement was written earlier. Error lines are cut at 160 characters.

### 1.1 The Spark door (`ReparkSession.sql`, default conf)

```text
[PA-02 control bare COPY outside] ERR AnalysisException: Error during planning: COPY TO to local filesystem path `<base>/s/outside/direct` is disabled by default. Set conf …
[PA-02 EXPLAIN ANALYZE COPY outside] OK
    files under outside: ['wrapped/jlc6Pd36JfT97FNX_0.parquet']
[PA-02 plain EXPLAIN COPY outside] OK
    files under outside: ['wrapped/jlc6Pd36JfT97FNX_0.parquet']
[PA-02 PREPARE COPY outside] ERR ParseException: SQL error: ParserError("Expected: end of statement, found: STORED at Line: 1, Column: 87")
    files under outside: ['wrapped/jlc6Pd36JfT97FNX_0.parquet']
[PA-02 EXECUTE p_copy] ERR PySparkException: datafusion engine error: Execution error: Prepared statement 'p_copy' does not exist
    files under outside: ['wrapped/jlc6Pd36JfT97FNX_0.parquet']
[PA-02 warehouse control COPY] OK
    files under outside: ['wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-03 one missing parent via link] OK
    files under outside: ['new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-03 three missing parents via link] OK
    files under outside: ['m1/m2/m3/export/Wa4FVfHQTzUSxUs1_0.parquet', 'new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-04 location 'aaaaé'] ERR PySparkException: repark internal error in PyReparkSession.sql: end byte index 5 is not a char boundary; it is inside 'é' (bytes 4..6 of string) (a Rust panic was caught at the …
    files under outside: ['m1/m2/m3/export/Wa4FVfHQTzUSxUs1_0.parquet', 'new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-04 location 'aaaaaaé'] ERR PySparkException: repark internal error in PyReparkSession.sql: end byte index 7 is not a char boundary; it is inside 'é' (bytes 6..8 of string) (a Rust panic was caught at the …
    files under outside: ['m1/m2/m3/export/Wa4FVfHQTzUSxUs1_0.parquet', 'new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-04 location 'file://aaaaaaaaé'] ERR PySparkException: repark internal error in PyReparkSession.sql: end byte index 9 is not a char boundary; it is inside 'é' (bytes 8..10 of string) (a Rust panic was caught at …
    files under outside: ['m1/m2/m3/export/Wa4FVfHQTzUSxUs1_0.parquet', 'new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
[PA-04 location 'file://localhost<base>/s/outside/lhé'] ERR AnalysisException: Error during planning: COPY TO to local filesystem path `file://localhost<base>/s/outside/lhé` is disabled by default. Set conf …
    files under outside: ['m1/m2/m3/export/Wa4FVfHQTzUSxUs1_0.parquet', 'new1/export/OpLVzumX8YkrncgI_0.parquet', 'wrapped/jlc6Pd36JfT97FNX_0.parquet']
    files under warehouse: ['inside/6OV6r5elCmis9NgP_0.parquet']
```

Every PA-04 panic also printed the panic hook on stderr
(`panicked at crates/repark-spark/src/local_fs_ddl.rs:53:42`). Plain `EXPLAIN` wrote nothing.

### 1.2 The ANSI door (`AnsiDialect` on a `ReparkSession`, default conf)

Through a throwaway `crates/repark-sql/tests` probe on the same head (not committed). The
warehouse listing follows `<warehouse>/link` into `<outside>`, so a file there is the same file
as the one under `<outside>`. A panic was caught around the poll.

```text
[PA-02 control bare COPY outside] ERR: Error during planning: COPY TO to local filesystem path `<base>/outside/direct` is disabled by default. Set conf `repark.sql.allowLocalFilesystemDDL` …
[PA-02 EXPLAIN ANALYZE COPY outside] OK
    files under outside: ["wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-02 plain EXPLAIN COPY outside] OK
    files under outside: ["wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-02 PREPARE COPY outside] ERR: SQL error: ParserError("Expected: end of statement, found: STORED at Line: 1, Column: 78")
    files under outside: ["wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-02 EXECUTE p_copy] ERR: datafusion engine error: Execution error: Prepared statement 'p_copy' does not exist
    files under outside: ["wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-02 warehouse control COPY] OK
    files under outside: ["wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["inside/9iAi61uTFsPR0Q0O_0.parquet", "link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-03 one missing parent via link] OK
    files under outside: ["new1/export/9rdoyt2hW0degfO3_0.parquet", "wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["inside/9iAi61uTFsPR0Q0O_0.parquet", "link/new1/export/9rdoyt2hW0degfO3_0.parquet", "link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-03 three missing parents via link] OK
    files under outside: ["m1/m2/m3/export/mh1ErhuMFuPUTrnX_0.parquet", "new1/export/9rdoyt2hW0degfO3_0.parquet", "wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["inside/9iAi61uTFsPR0Q0O_0.parquet", "link/m1/m2/m3/export/mh1ErhuMFuPUTrnX_0.parquet", "link/new1/export/9rdoyt2hW0degfO3_0.parquet", "link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
[PA-04 location "aaaaé"] PANIC: end byte index 5 is not a char boundary; it is inside 'é' (bytes 4..6 of string)
[PA-04 location "aaaaaaé"] PANIC: end byte index 7 is not a char boundary; it is inside 'é' (bytes 6..8 of string)
[PA-04 location "file://aaaaaaaaé"] PANIC: end byte index 9 is not a char boundary; it is inside 'é' (bytes 8..10 of string)
[PA-04 location "file://localhost<base>/outside/lhé"] ERR: Error during planning: COPY TO to local filesystem path `file://localhost<base>/outside/lhé` is disabled by default. Set conf …
    files under outside: ["m1/m2/m3/export/mh1ErhuMFuPUTrnX_0.parquet", "new1/export/9rdoyt2hW0degfO3_0.parquet", "wrapped/b21zePZBmRVB6ugL_0.parquet"]
    files under warehouse: ["inside/9iAi61uTFsPR0Q0O_0.parquet", "link/m1/m2/m3/export/mh1ErhuMFuPUTrnX_0.parquet", "link/new1/export/9rdoyt2hW0degfO3_0.parquet", "link/wrapped/b21zePZBmRVB6ugL_0.parquet"]
```

### 1.3 The `repark.sql()` trace

`repark.sql(query)` (`python/repark/src/repark/__init__.py`) lazily builds
`_native.PyReparkSession.native()` (`crates/repark-python/src/session.rs`), which builds a
`ReparkSessionBuilder` without `with_sql_dialect`, so `ReparkSession::build` installs the
default `repark_core::DataFusionDialect`. Its `execute` runs `PreExecute::run`: plan, the
shared belt `PreExecute::guard` (the sorted-view create guard and the named-source DDL guard),
execute. No local-filesystem guard runs on this entry, and the native session registers no
`repark.sql` config extension, so no opt-in can be set on it. Measured on the same module:

```text
[native bare COPY outside] OK
    files under outside_native: ['direct/GMnqFA7wTsUb6Bm5_0.parquet']
[native EXPLAIN ANALYZE COPY outside] OK
    files under outside_native: ['direct/GMnqFA7wTsUb6Bm5_0.parquet', 'wrapped/vZJoKXLAdKHaCHLH_0.parquet']
[native CREATE EXTERNAL TABLE outside] OK
    files under outside_native: ['direct/GMnqFA7wTsUb6Bm5_0.parquet', 'wrapped/vZJoKXLAdKHaCHLH_0.parquet']
[native aaaaaaé] OK
[native SHOW warehouse roots probe] OK
```

The `aaaaaaé` statement wrote a relative file into the process working directory.

## 2. Step 1 — the pins, red on main

Run on main `27c56f74` with the pins added (the release module of §1 for Python):

| Suite | On main |
|---|---|
| `python/repark/tests/test_pa_local_fs_policy_1.py` | 7 failed, 8 passed |
| `cargo test -p repark-spark --lib -- tests::pa_local_fs_policy` | 6 failed, 9 passed |
| `cargo test -p repark-sql --test pa_local_fs_policy` | 5 failed, 3 passed |

Red on main: every reproduction of §1 (the wrapped `COPY`, both symlink escapes, the
`CREATE EXTERNAL TABLE` escape, the dangling symlink, the three byte-boundary locations, and
`repark.sql()`). Green on main, kept as controls: plain `EXPLAIN`, every warehouse control,
the Spark door's opt-in controls, the outside non-ASCII and `file://localhost` refusals, and
`pa03_dot_dot_after_a_missing_parent_refuses` on the Spark door (main's lexical fallback
already collapses `<warehouse>/missing/../../escaped` to a path outside). The ANSI door's
`pa04_ansi_non_ascii_controls` is red on main at its opt-in cell (C-005), not at a PA-04
refusal:

```text
called `Result::unwrap()` on an `Err` value: Analysis("Error during planning: COPY TO to local
filesystem path `file://localhost<base>/aaaaaaé` is disabled by default. …")
```

DataFusion 54.1's `extensions_options!` reports entries without the extension prefix, and
`ConfigOptions::entries()` appends them unchanged, so the ANSI reader's
`repark.sql.allow_local_filesystem_ddl` matched only the hand-written stand-in of
`guards/tests.rs`, which reports its key prefixed.

## 3. Step 2 — the shared module, no caller

`crates/repark-core/src/local_fs_policy.rs`, declared `pub mod local_fs_policy;` in the crate
root. The root stays at its 155-line ceiling: the declaration is one line and one blank
separator line between the dialect re-export and the pre-execute block is gone. No door calls
it yet. `cargo test -p repark-core --lib -- local_fs_policy`: 12 passed.

## 4. Step 3 — both doors call the shared policy; their copies go

The Spark door's `local_fs_ddl.rs` and the ANSI door's `guards.rs` each re-export
`repark_core::local_fs_policy::refuse_local_filesystem_plan`; their private classification,
strip, canonicalization and containment copies and the ANSI conf reader are deleted, and
`catalog_state.rs` imports the shared strip. The walk was written with the module in step 2
(it is one function of the policy), so the doors adopting it turn the PA-02 pins green in
this step with the PA-03 and PA-04 pins; step 4 wires `repark.sql()`.

One step 1 pin changed shape: through the Spark door `CREATE EXTERNAL TABLE pa03_ext (a INT)
…` answers `This feature is not implemented: CREATE EXTERNAL TABLE is not supported for
Iceberg column-def CREATE yet` before any guard runs, so
`pa03_create_external_table_through_a_warehouse_symlink_refuses` now plans with DataFusion and
calls the door's guard, as the module's own `refuses_create_external_local_outside_warehouse`
does. It stays red on main (lexical containment admits the path).

| Suite | On this head |
|---|---|
| `cargo test -p repark-sql --test pa_local_fs_policy` | 8 passed |
| `cargo test -p repark-sql --lib -- guards` | 43 passed |
| `cargo test -p repark-spark --lib -- local_fs pa_local_fs_policy` | 29 passed |
| `cargo test -p repark-core --lib -- catalog_state local_fs_policy` | 19 passed |
| `make rust-clippy`, `make rust-panic-ban` | exit 0 |

## 5. Step 4 — `repark.sql()` gets the guard (owner ruling 2026-10-15)

`PyReparkSession::native` (`crates/repark-python/src/session.rs`) builds with
`with_sql_dialect(Arc::new(repark_core::local_fs_policy::LocalFsGuardedDialect))`. The
dialect plans, applies `refuse_local_filesystem_plan`, applies the shared `PreExecute::guard`
and executes: the same order as the ANSI door's delegation. No public signature changes; the
default `DataFusionDialect` that `ReparkSession::builder()` installs is untouched, so a bare
Rust session and the repark-core tests that `COPY` into temporary directories keep stock
behaviour. The native session registers no `repark.sql` config extension, so `repark.sql()`
has no opt-in and refuses closed; the typed writer's `note_local_write_root` grandfathers its
own destination on this entry as on the Spark door.

## 6. Step 5 — gates on head `83147df1` and the records

The release module was built from `83147df1` with an empty `git status --short`.

| Command | Result |
|---|---|
| `cargo test --no-fail-fast -p repark-core -p repark-spark -p repark-sql` (inside the build lock) | 48 test binaries, 4906 passed, 0 failed, 11 ignored |
| `.venv/bin/python -m pytest python/repark/tests/test_pa_local_fs_policy_1.py -q` | 15 passed |
| every `python/repark/tests` file that calls `repark.sql()`, plus `test_sec_boundaries.py` | 1361 passed, 130 skipped, 5 xfailed |
| `python -m pytest python/repark/tests -n 16` (the whole suite) | 17009 passed, 488 skipped, 151 xfailed |
| `make rust-clippy`, `make rust-panic-ban`, `cargo fmt --all -- --check` | exit 0 |
| `make check-rust-file-size check-crate-dag` | clean; 25 internal edges, as on main |
| `make check-docs-links check-docs-compaction check-ledger-grammar spell-check` | exit 0 |
| `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| `comment_ban.py` over `origin/main..HEAD` | 0 hits |

`microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope` failed once in
the first, fail-fast run (`the query ends within the bound: Elapsed(())`) and passed alone
and in the full `--no-fail-fast` run. It is the audit's own PA-15 (the micro-batch completion
fails its test bound), and this unit does not touch micro-batch code.

Records: the three dated amendments under PA-02, PA-03 and PA-04 of the audit; five lines in
the 1.5.6 release-notes draft; the order's engine line and the owner's dated amendment line.

### Dated rows

| Date | Row |
|---|---|
| 2026-10-15 | **The concurrent symlink-change race, out of scope by owner ruling.** In the audit's words: "a pathname-only precheck can still have a check/use race when another actor controls the directory." The policy resolves the destination when the statement is planned and DataFusion opens the file later; an actor who can change a symlink inside a registered warehouse between those two moments can still redirect the write. The owner ruled on 2026-10-15 that this "stays a dated row". Closing it needs an open-relative-to-a-directory-handle write path (for example `openat2` with `RESOLVE_BENEATH`), which DataFusion's object store does not offer. |
| 2026-10-15 | **`repark.sql()` has no opt-in.** The native session registers no `repark.sql` config extension, so the entry refuses closed; enabling one would also let a SQL `SET` on that entry open the gate at runtime, which the Spark door deliberately does not allow. |

## 7. Revert checks (one mutant per finding)

Each mutant re-introduces the defect in `repark_core::local_fs_policy` (or removes the native
wiring), runs the unit, Spark, ANSI and guard suites, and is restored with `git checkout`; the
restored head is the green run of §6.

| Mutant | Re-introduces | Killing tests | Failing assertion (first) |
|---|---|---|---|
| M-PA02 | the walk stops at the root (`TreeNodeRecursion::Jump` after the first node) | 4: `the_walk_checks_a_write_below_every_carrier_but_plain_explain`, `the_native_door_refuses_every_executing_write_outside`, Spark `pa02_explain_analyze_copy_outside_refuses_and_writes_nothing`, ANSI `pa02_ansi_explain_analyze_copy_outside_refuses_and_writes_nothing` | ``called `Result::unwrap_err()` on an `Ok` value: ()`` |
| M-PA03 | main's resolver (the target, else its parent, else lexical) | 11: the four resolver unit pins, the native refusal pin, Spark `pa03_create_external_table_through_a_warehouse_symlink_refuses`, `pa03_dangling_symlink_in_the_warehouse_refuses`, `pa03_one_missing_parent_through_a_warehouse_symlink_refuses`, `pa03_three_missing_parents_through_a_warehouse_symlink_refuse`, ANSI `pa03_ansi_missing_parents_through_a_warehouse_symlink_refuse`, `pa03_ansi_dot_dot_and_dangling_symlink_refuse` | ``assertion `left == right` failed`` (`a_dot_dot_after_the_nearest_existing_ancestor_is_indeterminate`) |
| M-PA04 | main's unchecked byte slice in `strip_ascii_prefix_ci` | 5: both classification unit pins, the native refusal pin, Spark and ANSI `pa04_*_non_ascii_relative_locations_refuse_with_the_policy_error` | `end byte index 5 is not a char boundary; it is inside 'é' (bytes 4..6 of string)` |
| M-C017 | main's ANSI conf read (only the prefixed key matches) | 10: the conf-read unit pin, Spark `allow_conf_true_permits_outside`, `bare_copy_to_applies_without_collect`, `copy_to_applies_exactly_once_across_a_later_collect`, the three Spark opt-in pins, the three ANSI control pins | `assertion failed: allow_local_filesystem_ddl(&unprefixed_ctx(true))` |
| M-C010 | `PyReparkSession.native()` without `with_sql_dialect` (main's `DataFusionDialect`), release module rebuilt | 1 of 15: `test_repark_sql_refuses_executing_writes_outside` (the plain-EXPLAIN control stays green) | `Failed: DID NOT RAISE AnalysisException` |
