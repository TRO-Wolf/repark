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
| C-011 | PA-03: containment resolves the nearest existing ancestor with `canonicalize` and appends the remaining components (amended 2026-10-16, ruling 114 (b)), so `<warehouse>/link/new/export` and `<warehouse>/link/m1/m2/m3/export` with `link -> <outside>` refuse on all three entries, for `COPY` and for `CREATE EXTERNAL TABLE`. | The PA-03 pins and the resolver unit pins. | **PROVEN** | §7, §8, §9: the M-PA03 revert (main's resolver) reds the symlink-escape pins of both doors and the resolver unit pins on every head of this unit. |
| C-012 | PA-03: an unsafe or indeterminate destination refuses: a dangling symlink, an ancestor whose metadata read fails for any reason but not-found (a plain file used as a directory, `ENOTDIR`), an unreadable location, an empty location, and, in the resolver, a `..` left in the components after a missing directory (amended 2026-10-16, ruling 114 (b): no door's written reading keeps a `..`, so a `..` the write removes is judged after it is removed, C-029). | The PA-03 indeterminate pins, the metadata-error pins, `the_written_reading_never_keeps_a_dot_dot`. | **PROVEN** | §8 (V-05), §9: `an_ancestor_whose_metadata_read_fails_other_than_not_found_refuses` and both doors' metadata pins kill M-V2; `a_dot_dot_after_the_nearest_existing_ancestor_is_indeterminate` kills M-N4. |
| C-013 | PA-03: a symlink that stays inside the warehouse, missing parents under the warehouse, and the opt-in through an outside symlink still write. | The PA-03 control pins. | **PROVEN** | §6: the controls pass on head. |
| C-014 | PA-04: the prefix strip is checked (`str::get`), so a character across byte 5, 7 or 9 classifies as a local path and the statement refuses with the named policy error, not a panic, on all three entries. | The PA-04 pins and the classification unit pins. | **PROVEN** | §7: the M-PA04 revert panics at byte 5 in the unit pins and reds both doors' byte-boundary pins. |
| C-015 | PA-04: ordinary non-ASCII names and the `file://localhost` forms classify: outside refuses, under the warehouse writes, and the opt-in writes outside. | The PA-04 control pins. | **PROVEN** | §6: the PA-04 controls pass on head. |
| C-016 | One implementation: classification, the checked prefix strip, containment, the plan walk and the conf read live in `repark_core::local_fs_policy` beside `CatalogRegistry`; both doors' private copies are deleted; `catalog_state.rs` uses the same strip; no new crate edge. | `make check-crate-dag`; the door files. | **PROVEN** | §4, §6: both doors re-export the one function; `make check-crate-dag` reports the same 25 internal edges as main. |
| C-017 | The conf read is scoped to the `repark.sql` config extension and accepts its field unprefixed or prefixed, so the ANSI door's explicit opt-in writes (C-005 fixed) and an absent extension stays closed. | The ANSI opt-in control pins; the existing `conf_true_opens_the_gate` and `missing_conf_extension_is_closed`. | **PROVEN** | §7: the M-C017 revert reds the conf-read unit pin, the ANSI opt-in controls and the Spark door's existing opt-in tests. |
| C-018 | One fresh Opus 5.5 high verifier, scoped to the pins, the audit's reproductions on both doors and a mutant per finding, finds no defect. | The closeout verifier (order §5). | **OPEN** | After CI; not this round. |
| C-019 | V-01 (owner ruling 103): a symlink followed by `..` (`<warehouse>/deep/../../out/x`, bare, `file://` and under `EXPLAIN ANALYZE`), which writes outside on the first head (`506fe9ec`), refuses with the policy error and leaves no file on both doors: the guard reads the written path through `ListingTableUrl::parse`, as DataFusion's `COPY` does. | The `v01_*` pins of both doors, the core and Python twins. | **PROVEN** | §8: red at `506fe9ec`, green on head; M-V01 reds them. |
| C-020 | V-02 (ruling 103): `%2e%2e`, `%2E%2E` and `.%2e` in a `file:`, `file://` or `file://localhost` URL, under `EXPLAIN ANALYZE` and in `CREATE EXTERNAL TABLE`, which write outside on the first head and on main, refuse and leave no file on both doors; a literal `%2e%2e` in a bare path is a directory name and stays contained. | The `v02_*` pins. | **PROVEN** | §8: red at `506fe9ec`, green on head; M-V02 reds them. |
| C-021 | V-03 (ruling 103): `file:<relative>` and `file:./<relative>` with the cwd inside a warehouse are read as `/<relative>`, as DataFusion and `absolute_local_from_file_rest` read them, so they refuse and leave no file on both doors; each pin restores the cwd and runs in its own child process. | The `v03_*` pins. | **PROVEN** | §8: red at `506fe9ec`, green on head; M-V03 reds them. |
| C-022 | V-06 and the condition of ruling 103: a warehouse registered through a symlinked path (the macOS `/tmp` shape) accepts a direct write, a write through an in-warehouse symlink and a write with missing middle directories on both doors; the root and the target are resolved by the same walk. | The symlinked-root pins. | **PROVEN** | §8: M-V7 (the root made absolute, not resolved) reds them. |
| C-023 | V-04 (ruling 104): `repark.sql()` has no real opt-in (`SET repark.sql.allowLocalFilesystemDDL = true` fails, a native session built with the key still refuses, the native session installs no `repark.sql` extension), so its refusal says "repark.sql() does not allow local-file DDL", names no conf and no data-deleting action, and the native dialect reads no conf. | The native-dialect unit pins; `test_repark_sql_*`. | **PROVEN** | §8: the probe of §8.3; M-native-opt-in-read reds the native unit pins. |
| C-024 | The same cause as V-02, found in this round: a location with a leading space is a relative path to DataFusion (`cwd/" <warehouse>/x"`) and was trimmed by the guard; it now refuses on both doors when the cwd is outside every warehouse. | The leading-space pins. | **PROVEN** | §8: red at `506fe9ec` (and on main), green on head; M-space reds them. |
| C-025 | Ruling 103's rule, literally: a `..` that steps back over a symlink under the warehouse refuses even when the write lands inside (`<warehouse>/deep/../dd_in`, `deep -> <warehouse>/a/b`; `<warehouse>/s2/../sb_in`, `s2 -> <warehouse>/sib`), while a `..` over a plain directory below an in-warehouse symlink stays contained. | `a_dot_dot_that_cancels_a_symlink_under_the_warehouse_refuses_even_when_it_lands_inside`. | **REJECTED** (2026-10-16, owner ruling 114 (b)) | §8.4: main writes these cells inside the warehouse on both doors, as it writes `file://<warehouse>/x/%2e%2e/pin2` (PA-03's kept refusal of a `..` after a missing directory, now read in the URL form), so this trips the round's halt ("a write main allows inside a warehouse that the fix refuses"); the behaviour is pinned and the owner's acceptance is the open question. Superseded: ruling 114 (b) freed the cells, C-029. |
| C-026 | RV-01 (ruling 114 (b)): the warehouse root is read as a plain path or `file:` URL with no glob split, so a `[`, `?` or `*` in a registered warehouse path is an ordinary character: a write to the root's parent or a sibling, also under `EXPLAIN ANALYZE`, and a bare location under the root that DataFusion cuts at the glob and writes into the parent, refuse on both doors with no file anywhere under the base; the `file:` spelling writes inside. | The `rv01_*` pins. | **PROVEN** | §9: red at `75d9caec` (and the bare cut writes outside on main); M-N1 (the root read through the location parser again) reds them. |
| C-027 | RV-02 (ruling 114 (b)): a `..` over a symlink above the warehouse is judged where the write lands, so `<base>/lnk/../wh/x` with `lnk -> <base>/o1/o2` writes `<base>/wh/x` on both doors, bare and `file://`, as main does. | The `rv02_*` pins. | **PROVEN** | §9: red at `75d9caec`; M-N2 (the whole-path comparison restored) reds them. |
| C-028 | RV-03 (ruling 114 (b)): U+E000 to U+E002 in a location, literal or percent-encoded, are ordinary characters; such a `file:` URL inside the warehouse writes there on both doors, as main does. | The `rv03_*` pins. | **PROVEN** | §9: red at `75d9caec`. |
| C-029 | Ruling 114 (b): the four cells and the same-family pair write inside at the path the write lands on, on both doors: `<wh>/deep/../dd_in` and `<wh>/deep/../m1/dd_in2` (`deep -> <wh>/a/b`), `<wh>/s2/../sb_in` (`s2 -> <wh>/sib`), `file://<wh>/x/%2e%2e/pin2`, `<wh>/x/../pin_bare` and `file://<wh>/x/../pin_url` (`x` absent); the V-01, V-02 and V-03 pins stay green. | The 114-cell pins of the core and both doors. | **PROVEN** | §9: red at `75d9caec`; M-N2 reds them. |

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

## 8. Fix round (2026-10-15, owner rulings 103 to 105; C-012 amended, C-019 … C-025)

The verifier's FAIL (V-01 to V-06) and the owner's rulings 103, 104 and 105 are quoted in the
order's §1. The round changes only `repark_core::local_fs_policy` (no signature outside it, no
`Cargo.toml`, no crate edge) and adds pins; 105's findings are cards on a separate docs PR.

### 8.1 The reading

`ListingTableUrl::parse` is reachable from repark-core through `datafusion::datasource::listing`
with no new dependency. The written path is `/` joined with the parts of its `prefix()`, which is
the path DataFusion's `LocalFileSystem` (root `file:///`) writes: WHATWG dot-segment removal
(`..`, `.%2e`, `%2e.`, `%2e%2e`, either case), one percent decode, `file:x` and `file:./x` as `/x`,
a bare or leading-space path joined to the cwd and reparsed. The as-typed reading reparses a URL
with every `.` and `%2e`/`%2E` replaced by a private-use stand-in, so the parser keeps the dot
segments, then decodes and restores them; a bare path keeps its `std::path` components. Both are
followed by one walk (`follow`); so is every warehouse root, read the same way.

### 8.2 Red at `506fe9ec`, green on head

The product module of `506fe9ec` alone, with the new door pins, both doors
(`fix1/out/red_506fe9ec.txt`, the first assertion each):

| Pin (Spark · ANSI) | At `506fe9ec` | Head |
|---|---|---|
| `v01_*symlink_then_dot_dot_writes_outside_on_todays_head_and_now_refuses` | `must refuse, but answered: COPY … TO '<wh>/deep/../../out/bare'` | green |
| `v02_*percent_encoded_dot_segments_write_outside_on_todays_head_and_now_refuse` | `must refuse, but answered: COPY … TO 'file://<wh>/%2e%2e/out/p1'` | green |
| `v03_*file_relative_with_the_cwd_in_a_warehouse_writes_outside_on_todays_head` | `must refuse, but answered: COPY … TO 'file:tmp/…/out/fc'` | green |
| `*a_leading_space_is_read_from_the_cwd_and_writes_outside_on_todays_head` | `must refuse, but answered: COPY … TO ' <wh>/trap'` | green |
| `*a_warehouse_registered_through_a_symlink_still_writes_inside_it` | green (control) | green |
| `*an_ancestor_whose_metadata_read_fails_refuses_with_the_policy_error` | green (control) | green |

Each outside pin asserts no file in the sibling `out/` (or the cwd) after the refusal; the cwd
pins run in a child process of the test binary and assert the cwd restored.

The Python pins against the verifier's release module of `506fe9ec`
(`fix1/out/pytest_506fe9ec_pins.log`): `test_v01_*`, the five `test_v02_*` forms and both
`test_v03_*` forms fail with `DID NOT RAISE AnalysisException`; the `repark.sql()` pins fail on
the message (`Expected regex: 'repark.sql\(\) does not allow local-file DDL'`, actual: the
conf-naming text); the symlinked-root control passes. All 25 pass on head.

### 8.3 `repark.sql()` has no opt-in (ruling 104)

`fix1/out/repark_sql_opt_in.txt`, head module: `SET repark.sql.allowLocalFilesystemDDL = true`
and the underscore spelling answer `Could not find config namespace "repark"`;
`PyReparkSession.native(config={…: "true"})` with either spelling still refuses; no Python
source reads the key. So the message names no opt-in, the native dialect reads no conf, and the
1.5.6 note carries the dated loss with REPARK-SQL-LOCAL-DDL-1.

### 8.4 Never worse than main, and the halt cells

The verifier's probe set plus this round's (`fix1/w/probe/`, PHYS bases) on today's main module
and on head, Spark door, `repark.sql()` and the opt-in session (`fix1/out/never_worse_python.txt`),
and the ANSI door through `AnsiDialect` (`fix1/out/probe_ansi_*.txt`). Nothing writes outside on
head with the opt-in off on either door except the Spark door's `CREATE TABLE … USING parquet
LOCATION` and its CTAS twin, which write outside identically on main (ruling 105,
LOCAL-FS-CREATE-TABLE-LOCATION-1). The opt-in opens both doors. Every inside write main allows on
the doors still writes, except these cells, which head refuses:

| Cell (Spark and ANSI) | Main | Head | Rule |
|---|---|---|---|
| `<wh>/deep/../dd_in`, `<wh>/deep/../m1/dd_in2` (`deep -> <wh>/a/b`) | writes `<wh>/dd_in`, `<wh>/m1/dd_in2` | refuses | ruling 103: a `..` over a symlink under the warehouse (C-025) |
| `<wh>/s2/../sb_in` (`s2 -> <wh>/sib`) | writes `<wh>/sb_in` | refuses | the same |
| `file://<wh>/x/%2e%2e/pin2` (`x` absent) | writes `<wh>/pin2` | refuses | PA-03's kept refusal of a `..` after a missing directory, now applied to the URL's `%2e%2e`; the bare spelling already refused at `506fe9ec` |

The adversarial pass of the xhigh continuation: `%252e%252e` (a literal `%2e%2e` directory, inside,
as main), mixed-case `FILE:`/`FiLe:` (main's Spark door writes `FiLe:<wh>/%2E%2e/out/mc2` outside;
head refuses), `//` and a trailing `/.` inside a `file:` path, a symlink in the warehouse root's
own ancestry with a `..` under the root, `file://localhost` with `%2f`, and a relative path
climbing out of a cwd inside the warehouse: nothing escapes on head on either door.

### 8.5 Mutants on the fix-round head

Each mutant edits `local_fs_policy.rs` (or the native wiring), runs the core, Spark, ANSI and
guards suites, and is restored (`fix1/out/mutants*.txt`, `fix1/out/mutants/`).

| Mutant | Killing tests (first) |
|---|---|
| M-PA02-root-only | `the_walk_checks_a_write_below_every_carrier_but_plain_explain`, both doors' `pa02_*explain_analyze_copy_outside_refuses_and_writes_nothing` |
| M-PA03-parent-only (main's resolver) | `the_nearest_existing_ancestor_resolves_through_a_symlink`, `pa03_three_missing_parents_through_a_warehouse_symlink_refuse` |
| M-PA04-unchecked-slice | `the_checked_prefix_strip_refuses_a_split_character` |
| M-V2-metadata-error-continues | `an_ancestor_whose_metadata_read_fails_other_than_not_found_refuses`, both doors' metadata pins |
| M-V7-root-not-resolved | the three symlinked-root pins |
| M-V01-physical-reading-only | both doors' `v01_*`, the core `v01_*` |
| M-V02-scheme-stripped-literally | both doors' `v02_*`, `the_location_is_read_the_way_the_write_reads_it` |
| M-V03-file-relative-from-cwd | both doors' `v03_*`, the core `v03_*` |
| M-space-trimmed | both doors' leading-space pins |
| M-cancelled-link-ignored | `a_dot_dot_that_cancels_a_symlink_under_the_warehouse_refuses_even_when_it_lands_inside` |
| M-native-opt-in-read | `the_native_door_refuses_every_executing_write_outside`, `the_native_door_keeps_explain_and_the_warehouse_control_and_has_no_opt_in` |
| M-SQL-no-guarded-dialect | `test_repark_sql_refuses_executing_writes_outside`, `test_repark_sql_names_no_opt_in_and_no_route_opens_it` (`DID NOT RAISE`; release module rebuilt in a separate clone and venv) |

### 8.6 Gates on head

| Command | Result |
|---|---|
| `cargo test --no-fail-fast -p repark-core -p repark-spark -p repark-sql` (inside the build lock) | 4924 tests passed (4928 counting the four child re-runs of the cwd pins), 1 failed, 11 ignored; the failure is `microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope` (audit PA-15, untouched code), which passes alone three times on head and three times on main's tree |
| `pytest python/repark/tests -n 8` on head | 17019 passed, 488 skipped, 151 xfailed; exit 0; no head-only failure |
| `make rust-clippy`, `make rust-panic-ban`, `cargo fmt --all -- --check` | exit 0 |
| `uvx ruff@0.15.22 check` / `format --check` on the changed Python | exit 0 |
| `comment_ban.py` over `origin/main..HEAD` | 0 hits |

The release module was built from `842e92fa` with an empty `git status --short`
(`fix1/module-build.txt`). The docs gates of the records commit are in the hand-back.
