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
| C-001 | Main writes an `EXPLAIN ANALYZE COPY` outside every warehouse on both doors and on `repark.sql()`, refuses the bare `COPY` on both doors, and lets `repark.sql()` write the bare `COPY`, the wrapped `COPY` and a `CREATE EXTERNAL TABLE` outside. | Step 0 probes, §1. | **OPEN** (measured §1; proven when the step 1 pins land) | §1.1, §1.2, §1.3. |
| C-002 | Main writes `<warehouse>/link/new1/export` and `<warehouse>/link/m1/m2/m3/export` into the symlink's outside target on both doors. | Step 0 probes, §1. | **OPEN** (measured §1; proven when the step 1 pins land) | §1.1, §1.2. |
| C-003 | Main panics on a multibyte character across byte 5, 7 or 9 of a location on both doors, and classifies `file://localhost/<outside>` as local and refuses it. | Step 0 probes, §1. | **OPEN** (measured §1; proven when the step 1 pins land) | §1.1, §1.2. |
| C-004 | `PREPARE … AS COPY …` does not parse on either door, so no prepared statement can carry a `COPY`; `EXECUTE` of it answers "does not exist". | Step 0 probes, §1. | **OPEN** (measured §1; proven when the step 1 pins land) | §1.1, §1.2; DataFusion keeps the prepared plan behind `pub(crate)` `get_prepared`, so the walk checks a `Prepare` node's input at `PREPARE`. |

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
