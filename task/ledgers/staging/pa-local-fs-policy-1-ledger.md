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
| C-004 | `PREPARE … AS COPY …` does not parse on either door, so no prepared statement can carry a `COPY`; `EXECUTE` of it answers "does not exist". | Step 0 probes, §1; the `Prepare` unit pin. | **OPEN** (measured §1; pinned at step 4) | §1.1, §1.2; DataFusion keeps the prepared plan behind `pub(crate)` `get_prepared`, so the walk checks a `Prepare` node's input at `PREPARE`. |
| C-005 | Main's ANSI door ignores the explicit opt-in: its reader looks up `repark.sql.allow_local_filesystem_ddl` among `ConfigOptions::entries()`, which reports the real `repark.sql` extension's field unprefixed, so a session built with `repark.sql.allowLocalFilesystemDDL=true` still refuses outside. | Step 1 run on main, §2. | **PROVEN** | §2: the opt-in cell of `pa04_ansi_non_ascii_controls` refuses on main. |
| C-006 | PA-02: the guard walks the plan and checks every `Copy` and `CreateExternalTable` node at any depth before execution, so `EXPLAIN ANALYZE COPY … TO '<outside>'` refuses with the named policy error and writes no file on the Spark door, the ANSI door and `repark.sql()`. | The PA-02 pins on all three entries. | **OPEN** | Step 4. |
| C-007 | PA-02: a `Prepare` node's input is walked, so a prepared write is checked when it is prepared; `EXECUTE` cannot carry a `COPY` (C-004). | The `Prepare`-wrapped unit pin in `local_fs_policy.rs`. | **OPEN** | Step 4. |
| C-008 | PA-02: plain `EXPLAIN` of a `COPY` outside still answers and writes nothing on all three entries. | The plain-EXPLAIN pins. | **OPEN** | Step 4. |
| C-009 | PA-02: the warehouse control and the explicit opt-in control still write through `EXPLAIN ANALYZE COPY` on both doors and on the native dialect. | The PA-02 control pins. | **OPEN** | Step 4. |
| C-010 | PA-02, owner ruling 2026-10-15: `repark.sql()` gets the guard: `PyReparkSession.native()` installs `LocalFsGuardedDialect`, which plans, applies the guard, applies the shared belt and executes; no public signature changes. | The `repark.sql()` pins and the native-dialect pins. | **OPEN** | Step 3. |
| C-011 | PA-03: containment resolves the nearest existing ancestor with `canonicalize` and joins the missing tail, so `<warehouse>/link/new/export` and `<warehouse>/link/m1/m2/m3/export` with `link -> <outside>` refuse on all three entries, for `COPY` and for `CREATE EXTERNAL TABLE`. | The PA-03 pins and the resolver unit pins. | **OPEN** | Step 3. |
| C-012 | PA-03: an unsafe or indeterminate destination refuses: a `..` in the missing tail, a dangling symlink, an ancestor whose metadata read fails for any reason but not-found, and an empty location. | The PA-03 indeterminate pins. | **OPEN** | Step 3. |
| C-013 | PA-03: a symlink that stays inside the warehouse, missing parents under the warehouse, and the opt-in through an outside symlink still write. | The PA-03 control pins. | **OPEN** | Step 3. |
| C-014 | PA-04: the prefix strip is checked (`str::get`), so a character across byte 5, 7 or 9 classifies as a local path and the statement refuses with the named policy error, not a panic, on all three entries. | The PA-04 pins and the classification unit pins. | **OPEN** | Step 3. |
| C-015 | PA-04: ordinary non-ASCII names and the `file://localhost` forms classify: outside refuses, under the warehouse writes, and the opt-in writes outside. | The PA-04 control pins. | **OPEN** | Step 3. |
| C-016 | One implementation: classification, the checked prefix strip, containment, the plan walk and the conf read live in `repark_core::local_fs_policy` beside `CatalogRegistry`; both doors' private copies are deleted; `catalog_state.rs` uses the same strip; no new crate edge. | `make check-crate-dag`; the door files. | **OPEN** | Steps 2 and 3. |
| C-017 | The conf read is scoped to the `repark.sql` config extension and accepts its field unprefixed or prefixed, so the ANSI door's explicit opt-in writes (C-005 fixed) and an absent extension stays closed. | The ANSI opt-in control pins; the existing `conf_true_opens_the_gate` and `missing_conf_extension_is_closed`. | **OPEN** | Step 3. |
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
