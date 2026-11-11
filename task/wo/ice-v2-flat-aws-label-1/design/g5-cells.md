# G5 design: the live CTAS-failure cells (Glue and S3 Tables)

Clerk design, 2026-11-11, Claude Haiku 5.5. Nothing here is built or run. No AWS call, no build, no repository file changed.

Tags: **READ** (a file and line at `origin/main` f341171a, read-only clone), **MEASURED** (a command or a run), **INFERRED** (a reasoning step; the live cells are what check it).

Sources for the failure points: `4-aud-025-ctas.md` sections 1 to 3 (READ), which measured on RePark 1.5.6 with the hadoop and in-memory catalogs only. Nothing has been measured on Glue or S3 Tables (READ, the Fable brief section 4.2).

## 0. Summary

- Six cells per catalog, twelve in total. Two are controls (C4 plain create, C6 successful replace), one is the replace control (C5), and three are failures (C1 to C3).
- The S3 Tables path and the Glue path are different code paths (READ, section 3). The fix on the staged-create path covers Glue; S3 Tables takes the create-first path, which the same fix does not touch.
- **S3 Tables object leftovers cannot be listed with the current role** (stop S-1). Only the catalog and Spark reads are measured there.
- The baseline is recorded without failing: the module always writes a JSON artifact and asserts only the blocking condition (section 5).

## 1. Source data

One scalar source table per catalog, 100,000 rows, under the run prefix:
- `id BIGINT`, `s STRING`.
- `s = '1'` for every row, except row `id = 1`, which is `'q'` (the first-row failure), and row `id = 99999`, which is `'q'` (the late-row failure).

`CAST(s AS INT)` on `'q'` raises the ANSI cast error at execution. The literal `CAST('q' AS INT)` is folded in planning, which is the audit's form (READ, `4-aud-025-ctas.md` section 1).

## 2. Cells

| Cell | Statement (scalar only, RePark Spark door) | Injected failure | Stage of the failure (by code reading) |
|---|---|---|---|
| C1 | `CREATE TABLE <t> AS SELECT id, CAST('q' AS INT) AS n FROM <src> LIMIT 10` | planning (optimizer folds the literal cast) | after the create on the staged and create-first paths (INFERRED from AUD-025 section 2 step 3) |
| C2 | `CREATE TABLE <t> AS SELECT id, CAST(s AS INT) AS n FROM <src>` | execution, first row (`id = 1`) | after the create, during `write_ctas_query` |
| C3 | `CREATE TABLE <t> AS SELECT id, CAST(s AS INT) AS n FROM <src> WHERE id > 1` | execution, late row (`id = 99999`) | after the create, during `write_ctas_query` |
| C4 | `CREATE TABLE <t> AS SELECT id, CAST('1' AS INT) AS n FROM <src> WHERE id BETWEEN 2 AND 100` | none (control: plain create succeeds) | success |
| C5 | pre-create `<t>` with 3 rows; then `CREATE OR REPLACE TABLE <t> AS SELECT id, CAST(s AS INT) AS n FROM <src>` | execution, first row (control: the live table stays readable) | during the staged replace |
| C6 | pre-create `<t>` with 3 rows; then `CREATE OR REPLACE TABLE <t> AS SELECT id, CAST('1' AS INT) AS n FROM <src> WHERE id BETWEEN 2 AND 6` | none (control: successful replace) | success |

C5 and C6 start from a live table, so the statement takes the replace mode, not the create mode (READ, `ctas.rs:192`: `if existed { CtasMode::Replace }`). The replace path is the same on both catalogs, which section 3.3 sets out.

## 3. Code reading of the two paths

READ, `origin/main` f341171a.

### 3.1 Routing

- `crates/repark-core/src/session.rs:531-538`: the Glue kind registers with the default location policy.
- `crates/repark-core/src/session.rs:540-551`: the S3 Tables kind registers with `LocationPolicy::ServiceManagedLocation`, with the comment "S3 Tables assigns table location at create, so CTAS must route create-first".
- `crates/repark-spark/src/ctas.rs:192-198`: `existed` gives `CtasMode::Replace`; otherwise a `ServiceManagedLocation` catalog gives `CtasMode::ServiceManagedCreate` (line 198); otherwise `CtasMode::StagedCreate` (line 199-200).
- `crates/repark-spark/src/ctas.rs:245`: the service-managed mode calls `execute_ctas_service_managed` (defined at line 623).

### 3.2 Glue (staged create)

- `crates/repark-spark/src/ctas.rs:272`: `begin_staged_create`. AUD-025 section 2 step 2 (READ) says this writes `metadata/00000-<uuid>.metadata.json` (`metadata_location.rs:62-66`) before any data is written.
- The data is written after this (`write_ctas_query`, `ctas.rs:337`, 409-426). Nothing removes the begin file on failure (READ, AUD-025 section 2 step 3). The fix in flight is a catalog-free cleanup of this begin file on this path.
- So the Glue fix **covers** Glue: Glue takes this path (READ, routing 3.1), and the fix is on the path (READ).

### 3.3 S3 Tables (service-managed, create-first)

`execute_ctas_service_managed`, `crates/repark-spark/src/ctas.rs`:
- 635: "Location deliberately not set: the service assigns it."
- 646-649: `catalog.create_table(...)`. From this line the table exists in the catalog.
- 651: comment, "From here the table EXISTS in the catalog: any failure below aborts by dropping it."
- 652-706: `write_result`, an async block. Inside it, 655: `write_ctas_query`, then the commit calls (append, or replace with summary on the options path).
- 707-709: `if let Err(write_err) = write_result`. If `is_commit_state_unknown`, return the original error **without** a drop (708-709).
- 711-724: `catalog.drop_table(&table_ident)`.
  - Ok arm (712-717): the error says the incomplete table was dropped (create-first abort).
  - Err arm (718-723): the error says the abort drop also failed and "an incomplete table may remain".
- 728-729: `reregister` and `read_empty`, on success only.

**What happens to the catalog entry when the query fails:** the create at 646-649 has already made an entry. A query failure at 655 (planning, first row, late row) falls into 707, and the drop at 711 removes the entry. If the drop succeeds, no entry remains (READ, the code path; the drop's success is INFERRED for a healthy catalog).

**When the drop itself fails:** the Err arm at 718-723 returns an error that names the leftover, and the entry stays. Nothing in this path retries the drop or records the leftover elsewhere. What a reader sees in that state is INFERRED: a listed table with the create-time metadata and no data, which Spark would read as an **empty table**. That is the blocking case of the reading rule (section 4).

**Not covered by any guard:** the block at 652-706 runs inline with `.await`. There is no `Drop` guard and no `catch_unwind` on this path (READ: AUD-025 section 2 step 6 says the same for the general path, and this block has none either). A cancelled or panicking statement after line 646 therefore leaves the entry with no drop (INFERRED; no cell can force it without a kill, see section 6).

Mark: the drop-failure reading and the cancellation reading are **READ, not MEASURED**. The live cells cannot force a drop failure without denying the role an action, which is a stop (section 6, S-3).

### 3.4 Which fix covers which catalog

| Catalog | Path | Covered by the staged-create cleanup in flight? | Evidence |
|---|---|---|---|
| Glue | staged create (`ctas.rs:272`) | **yes** | READ: routing 3.1; the fix is on this path |
| S3 Tables | create-first (`ctas.rs:646-711`) | **no** | READ: this path never reaches `begin_staged_create`; it aborts by `drop_table` |
| S3 Tables replace (C5, C6) | staged replace (`ctas.rs:302`, `begin_replace`) | not the create-first path | READ: `ctas.rs:192`; AUD-025 section 2 step 2 says replace writes nothing |

So on S3 Tables the risk is the drop-failure branch (711-724) and the unguarded cancellation, not the begin file. A fix on this path is a separate unit; the design does not propose one.

## 4. Expected outcomes and the reading rule

The rule, exactly as written in the brief:

> A leftover object that no catalog reader can see is a storage leak. Card it; it does not block.
> A catalog entry that reads as an empty table blocks, and the fix is the small create-only cleanup the evaluation describes.

Expected outcome by cell (INFERRED from the reading in section 3 and the hadoop and in-memory measurements in AUD-025 section 1; the cells check each line):

| Cell | Glue expected | S3 Tables expected |
|---|---|---|
| C1 planning failure | no catalog entry (the table is not registered until commit; AUD-025 measured "not found" on hadoop and in-memory). Leftover: `00000` metadata file only. **Card** (storage leak), not block | no entry after the drop. If the failure is before the create, nothing is created at all. No object listing (S-1) |
| C2 first-row failure | no entry; leftover `00000` metadata (AUD-025 hadoop: first-row execution leaves the metadata file). **Card** | no entry after the drop. Not listable (S-1). Blocks only if the drop fails, which the cell cannot force |
| C3 late-row failure | no entry; leftover `00000` metadata, and data files if any were written before the failure (AUD-025: late row leaves `00000`, no data on RePark). **Card** | same as C2 |
| C4 plain create | table lists; Spark reads 99 rows, `id` 2 to 100 (control) | table lists; Spark reads 99 rows (control) |
| C5 failing replace (control) | live table lists and reads **3 rows** (AUD-025 hadoop: "replace: nothing; table intact (3 rows)"). A zero-row or error read here is a block under the same rule (see Q-2) | same expectation; replace path is staged, so nothing is written |
| C6 successful replace (control) | table lists and reads 5 rows | table lists and reads 5 rows |

Blocking condition (the only assertion, section 5): **a catalog lists a table that a failed statement created (C1 to C3), and Spark reads it through the catalog as an empty table (zero rows, no error).** A failed statement that leaves a listed table which Spark cannot read at all is not covered by the brief's rule. See Q-1.

Current expectation on main (INFERRED): the blocking condition is **not** met on either catalog, because the S3 Tables drop succeeds in a healthy account and Glue does not register the entry before commit. The baseline is therefore expected to be green, with a leftover card on Glue (C1 to C3). Only the measurement can confirm it.

## 5. Shape of the baseline run

One module, `python/repark/tests/parity/test_aws_acceptance_ctas_failure.py` (proposed name), with one parametrised test per (catalog, cell). It does two things on each run:

1. **Always writes** one JSON artifact, `g5-cells-<catalog>.json`, with one object per cell: the statement, the injected failure, the stage the error reports, and the three recorded reads (section 6). The workflow already uploads artifacts (READ, the ICE-READ-PERF step, `upload-artifact`), so the same pattern applies.
2. **Asserts only the blocking condition** (section 4). Leftover objects and storage leaks are recorded and never asserted. The control cells assert their expected row counts (C4 99, C5 3, C6 5), because a control that fails is a defect in the test or in the path.

Baseline and after: the job runs on `main` twice. The first run is before CTAS-QUERY-FAILURE-CLEANUP-1 merges, and it records the baseline. The second is after the merge. The two artifacts are compared: the Glue leftover count should drop from one metadata file per failing cell to zero, and the blocking condition must hold in both runs. The job does not fail on the leftover count, so the baseline run does not turn the job red.

## 6. Reads, permissions and naming

### 6.1 The three reads per cell

1. **Catalog lists the table.** Glue: `glue:GetTable` (or `glue:GetTables` for the namespace). S3 Tables: `s3tables:GetTable` or `s3tables:ListTables` with the namespace condition.
2. **Spark reads through the catalog.** `SELECT count(*), min(id), max(id) FROM <t>`, through the same Spark factory as the cross-read module. Record the value, or the error class and message.
3. **Objects left under the table's location.** Glue: `s3:ListBucket` on the table's prefix in the warehouse scratch prefix (in the role, READ: runbook section 2; the cleanup script already lists this prefix). S3 Tables: **not listable with the current role** (stop S-1).

### 6.2 Permissions

Every action is in the runbook's role statements (READ: `docs/tier2-aws.md` section 2). The cells use no action outside the list that the design brief in `g6-design.md` section 6.2 gives. No action is added.

The cells cannot force a drop failure on S3 Tables, because that would need a denied `s3tables:DeleteTable`. A deny is a role change, so it is a stop (S-3), and the drop-failure reading stays READ, not MEASURED.

### 6.3 Naming

- Table names: `testing_r<run_id>a<attempt>_<leg>_g5_<catalog>_<cell>`, the workflow's `RUN_PREFIX` pattern (READ). Lowercase only.
- Source table: `<prefix>g5_src`, one per catalog.
- Per-job cleanup and the 24-hour backstop, as in `g6-design.md` section 6.1 (READ).

## 7. Run order and timing

- Six cells per catalog, plus one 100,000-row source per catalog. The source write is the largest cost. The cells are small, and the expected job time is under 15 minutes per catalog (INFERRED, from the measured live job of 11-10 at 9.6 min for eleven tests). The timeout is 30 minutes.
- Same test-only PR as G6 (the design brief's section 7 order): the G5 module lands with the G6 module, before the first dispatch.
- The job is one of the G6 workflow jobs for the owner to add (S-2). It is dispatch-only.

## 8. Stops and questions

**Stops for the owner (permission or change of scope, not a design choice):**
- **S-1.** Listing objects under an S3 Tables table location needs an action the role does not grant (READ: runbook section 2). The design does not request it. Leftover objects on S3 Tables are recorded as "not listable". Whether a grant is wanted is the owner's call.
- **S-3.** Any `AccessDenied` in a cell is a stop. The design does not widen the role.

**Questions the owner must rule (the brief leaves them open):**
- **Q-1.** A failed statement leaves a catalog entry that Spark cannot read at all (an error, not zero rows). The brief's rule covers only "reads as an empty table". This design proposes that an unreadable listed entry also blocks, and asks the owner to confirm.
- **Q-2.** C5: a failed replace that leaves the live table unreadable or empty. The design treats it as blocking, because it is data loss on a live table. The rule as written covers catalog entries that a failed statement created. The owner should confirm.
- **Q-3.** The cancellation reading (section 3.3): a cancelled CTAS on S3 Tables leaves the entry. It is not measured, and a kill-based cell would need the fault harness that the audit calls untracked (READ, `4-aud-025-ctas.md`, section 5 context). The design does not add a kill cell; the owner decides whether to card it.

## 9. Claims I could not check

1. Every Glue and S3 Tables outcome in section 4 is INFERRED. The only measured failure points come from RePark 1.5.6 with the hadoop and in-memory catalogs (AUD-025).
2. Whether the Glue catalog registers the table before commit. INFERRED from the staged path and the hadoop and in-memory measurements; C1 to C3 check it.
3. Whether a failing planning statement on S3 Tables fails before or after the create (`ctas.rs:655` versus the SELECT analysis at about `ctas.rs:204-208`). Both leave no entry, so the expected outcome does not change, but the stage in the artifact is INFERRED.
4. The S3 Tables drop-failure and cancellation readings (READ, not MEASURED).
5. Whether the first-row failure leaves data files on Glue. AUD-025 measured hadoop only, where first-row execution left the metadata file and no data.
6. The 15-minute estimate for the G5 job (INFERRED).
