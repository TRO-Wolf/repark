# Unit ledger — WRITE-BODY-CARRIERS-1 · step 0 measures Spark and main

**Scope (2026-10-14):** measurement only, on base `018d0efdbe8bc25ccaeebf39950704f3ef8ada52`.
Spark 4.1.2 with Iceberg 1.11.0 is the oracle for the CTAS shape; main's release
module (repark 1.5.5, built from `293896ce`) is measured on the same statements
plus the wrapper verbs. No product file changes.

**Date:** 2026-10-14 · **Branch:** `fix/write-body-carriers-1` · **Base:** `018d0efd` ·
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** card
[write-body-carriers-1-card-2026-10-10.md](../../roadmap/mid-term/write-body-carriers-1-card-2026-10-10.md),
owner ruling 2026-10-10.
**This ledger closes when the unit's pull request merges.** Step 0 does not merge that change.

## PROPOSITION LEDGER — WRITE-BODY-CARRIERS-1 — 2026-10-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark 4.1.2 refuses all 20 CTAS cells at parse with PARSE_SYNTAX_ERROR and SQLSTATE 42601, and no table reads back different. | The Spark fixture and its recorder. | **PROVEN** | §1; [spark-cells.json](../../wo/write-body-carriers-1/evidence/spark-cells.json): 20 refused, 0 moved. |
| C-002 | Main's 40 CTAS cells: 24 commit the inner write then fail, 10 refuse already-exists, 6 refuse the MERGE body; format versions 2 and 3 answer identically. | The main fixture and its recorder. | **PROVEN** | §2; [main-cells.json](../../wo/write-body-carriers-1/evidence/main-cells.json): 24 commit-before-fail, 16 fail-before-write, 0 answered, `z` never moves. |
| C-003 | Main's 14 wrapper cells: EXPLAIN ANALYZE runs the four non-MERGE writes and refuses MERGE; PREPARE plus EXECUTE run both writes. | The main fixture and its recorder. | **PROVEN** | §3; 8 answered with the write committed, 2 MERGE refusals with nothing written, 4 prepare pairs answered with the write committed. |
| C-004 | The fix refuses the CTAS shape before any file is written, with Spark's measured class and text. | The build battery (§6). | **PROVEN** | Q1 taken with the orchestrator's lean (parse wins over already-exists); owner ruled (a) 2026-10-14. |
| C-005 | The ANSI door takes or declines the same refusal. | The ANSI pins (§6). | **PROVEN** | Q2 taken with the orchestrator's lean (door declined, already refuses pre-commit); owner ruled (a) 2026-10-14. |

## 1. Spark's answer (C-001)

Recorded 2026-10-14 on live Spark 4.1.2
(`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog) by
[the Spark recorder](../../../python/repark/tests/_record_write_body_carriers_1_spark.py):
2 creators × 5 bodies × 2 `z` states. Every cell refuses identically:
`ParseException`, error class and condition `PARSE_SYNTAX_ERROR`, SQLSTATE
42601, message parameters `{'error': '<KEYWORD>', 'hint': ''}`, first line

```text
[PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601 (line 1, pos 46)
```

with the body's keyword (`INSERT`, `UPDATE`, `DELETE`, `MERGE`; the overwrite
body fails at its leading `INSERT`) and the position of the keyword. The five
`CREATE TABLE <existing> AS <write>` cells also refuse at parse: the parse
error wins over the already-exists analysis. All 20 `t` tables keep 1 snapshot
and 2 rows; all 10 existing `z` tables keep 1 snapshot and 1 row; all 10 new
`z` tables stay absent. The view is
[spark-cells.md](../../wo/write-body-carriers-1/evidence/spark-cells.md).

## 2. Main's CTAS cells (C-002)

Recorded 2026-10-14 on the main release module (repark 1.5.5, memory catalog) by
[the main recorder](../../../python/repark/tests/_record_write_body_carriers_1_main.py):
the 20 Spark cells on format versions 2 and 3. Zero cells answer. Three
failure shapes:

- 24 cells commit the inner write, then fail: the INSERT and INSERT OVERWRITE
  bodies (12 cells) fail with `PySparkException`, no condition or SQLSTATE,
  `DataInvalid => UInt64 is not supported. Use Int64 for values ≤
  9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range.`; the
  UPDATE and DELETE bodies (12 cells) fail with `PySparkException`, no
  condition or SQLSTATE, `datafusion engine error: Arrow error: Invalid
  argument error: must either specify a row count or at least one column`. In
  each, `t` gains exactly one snapshot and the body's rows (INSERT 2 rows to 3,
  OVERWRITE 2 rows to 1, UPDATE value changed, DELETE 2 rows to 1).
- 10 cells refuse before any write: `CREATE TABLE` onto an existing `z` fails
  with `AnalysisException`, condition `TABLE_OR_VIEW_ALREADY_EXISTS`, SQLSTATE
  42P07. The existence check runs before the body.
- 6 cells refuse the MERGE body before any write:
  `UnsupportedOperationException`, no condition or SQLSTATE, `This feature is
  not implemented: Unsupported SQL statement: MERGE INTO ...`. The MERGE text
  answers standalone on main, so the refusal is the carrier path's, not the
  body's.

`z` never moves in any of the 40 cells. Format versions 2 and 3 are identical
cell for cell (outcomes and after-states equal after table-name normalization).
The view is [main-cells.md](../../wo/write-body-carriers-1/evidence/main-cells.md).

## 3. Main's wrapper cells (C-003)

Same session as §2. `EXPLAIN ANALYZE` of INSERT, INSERT OVERWRITE, UPDATE and
DELETE answers on both versions and commits the write (8 cells, `t` gains one
snapshot and the body's rows). `EXPLAIN ANALYZE` of MERGE refuses with the
same `UnsupportedOperationException` as the CTAS MERGE cells and writes nothing
(2 cells). `PREPARE <name> AS <write>` plus `EXECUTE <name>` answers both
halves for INSERT and INSERT OVERWRITE on both versions and commits the write
(4 cells). The card already rules that these verbs run their write; step 0 pins
the numbers, including the MERGE exception.

## 4. The seam

[seam.md](../../wo/write-body-carriers-1/evidence/seam.md), reading only, names
two product paths that reach a CTAS body. On the Spark door,
`router.rs:326`'s `Statement::CreateTable` arm re-renders any body (sqlparser
0.62 holds a write as `SetExpr::Insert`, `Update`, `Delete` or `Merge`) and
`execute_ctas` runs it at `ctas.rs:206` through the eager passthrough, which is
the commit §2 measures. One candidate refusal point: the arm at `router.rs:326`,
where the body's kind is known and which precedes every catalog read and every
file write. On the ANSI door, `create_table.rs:65` plans the body at :28-38 and
converts its schema at :127; DataFusion plans DML lazily and nothing collects
the frame before :127, so on this reading the ANSI door refuses before any
file is written. That reading is unverified by a run (§5).

## 5. What was not measured

No ANSI-door cell ran, on either engine. No DataFrame-door equivalent ran (the
DataFrame writers carry no SQL body, so no equivalent cell exists). The
`REPLACE TABLE ... AS <write>` spelling was read (it rewrites to the same arm)
but not run. Bodies inside CTEs, subqueries, or set operations were not run.
All tables were unpartitioned copy-on-write defaults in UTC; zones, partition
layouts, and row-level modes were not varied. Spark has neither `EXPLAIN
ANALYZE` of a write nor `PREPARE` / `EXECUTE`, so the wrapper cells have no
Spark column, as the card rules.

## Q. Questions

- **Q1 (C-004, RULING).** On `CREATE TABLE <existing> AS <write>`, should the
  fix answer Spark's parse text or keep the already-exists refusal? *Premise:*
  Spark refuses all five such cells at parse (§1); main refuses already-exists
  before reaching the body (§2), because the existence check precedes it. A
  refusal placed after that check keeps the divergence on 10 of 40 cells.
  *Lean:* Spark's text at parse altitude, since the card rules the text follows
  Spark's measured answer. *Build round 1 (2026-10-14):* taken with the lean.
  *Owner, 2026-10-14:* (a), the parse refusal wins, as built.
- **Q2 (C-005, RULING).** Should the ANSI door take the same named refusal?
  *Premise:* the card's refusal is a Spark-parity refusal and the ANSI door
  carries no Spark claim; the reading of §4 says it already refuses before any
  file is written, unverified. *Lean:* run one ANSI cell in the fix step and
  refuse by name only if it commits; otherwise leave the door untouched.
  *Build round 1 (2026-10-14):* five ANSI cells ran on main and head; no cell
  commits, so the door is untouched, with the lean. *Owner, 2026-10-14:* (a),
  the door stays untouched, as built.
- **Q3 (C-004, OWNER, answered).** The refusal omits Spark's `(line, pos)`
  suffix. *Owner, 2026-10-14:* (a), accepted and recorded: class, condition,
  SQLSTATE and keyword match Spark; the position is omitted, because the parser
  does not report the keyword's position.

## 6. Build round 1 (2026-10-14)

Branch `fix/write-body-carriers-1` at `367b3df2` (D2; D1 is `7a7de903`).

The refusal sits at the ruled point: the `Statement::CreateTable` arm at
`crates/repark-spark/src/router.rs:326`, via `refuse_ctas_write_body` beside
the arm. The test is the body's statement kind — `SetExpr::Insert`, `Update`,
`Delete` or `Merge` — unwrapping any `SetExpr::Query` layers the parser
produces for a parenthesised body (single and double parens measured; a
parenthesised `SELECT` still answers). Top-level `SELECT` and `VALUES` bodies
pass through unchanged; set-operation bodies were not run. No text search and
no new exception type: the
refusal reuses `view_ddl::temp_parse::spark_parse_error` with
`spark_error::message(PARSE_SYNTAX_ERROR, near)`, so Python raises the existing
`ParseException` path with the bare message `[PARSE_SYNTAX_ERROR] Syntax error
at or near '<KEYWORD>'. SQLSTATE: 42601`. The `(line, pos)` suffix Spark
appends is omitted: the parser does not report the keyword position.

Head battery (`python/repark/tests/test_write_body_carriers_1.py` on the head
release module): 6 of 6 tests pass — the 40 CTAS cells, 5 `REPLACE TABLE`
cells and 5 parenthesised cells refuse with Spark's class, condition, SQLSTATE
and keyword with `t` and `z` unchanged, and the 14 wrapper cells, 5 ANSI cells
and 6 neighbour cells answer as on main. Rust battery
(`crates/repark-spark/src/tests/write_body_carriers.rs`): 5 passed, 0 failed.

C-004 is PROVEN. The five `CREATE TABLE <existing> AS <write>` cells refuse
with the parse text: the arm runs before `execute_ctas`, so the refusal wins
over the already-exists check by construction. `REPLACE TABLE <missing> AS
<write>` still answers the pre-existing `TABLE_OR_VIEW_NOT_FOUND` from
`refuse_missing_replace_target`
(`crates/repark-spark/src/normalize/replace_table.rs:31`), which runs before
the arm; the body never runs and `t` never moves. `CREATE TABLE IF NOT EXISTS
<existing> AS <write>` moves from ANSWERED (no-op) on main to the parse
refusal on head: Spark refuses at parse regardless of `IF NOT EXISTS`, so the
change is Spark-correct.

C-005 is PROVEN with the door declined: the five ANSI-door cells refuse
identically on main and head with `UnsupportedOperationException`, `Query
<WRITE> not implemented yet` (DataFusion plans a DML `SetExpr` as a query only
at plan time, and nothing collects before the refusal), with the target rows
unchanged. No ANSI product change.

Parenthesised bodies: on main `CREATE TABLE z AS (<write>)` refused with
`UnsupportedOperationException`, `Query <WRITE> not implemented yet`, writing
nothing; on head the same shape refuses with the ruled `PARSE_SYNTAX_ERROR`.
The wrapping found is `SetExpr::Query`, one layer per paren pair.

Remaining differences against Spark's measured cells: the omitted `(line,
pos)` suffix; native `ParseException.getMessageParameters()` returns `None`
(the facade attaches no parameter map; Spark carries `{'error': '<KEYWORD>',
'hint': ''}`). Class, condition, SQLSTATE and the first line match.

Not changed this round: `EXPLAIN ANALYZE CREATE TABLE z AS <write>` and
`PREPARE p AS CREATE TABLE z AS <write>` refuse on main with
`UnsupportedOperationException`, `Query <WRITE> not implemented yet`, writing
nothing (`PREPARE` stores nothing, so `EXECUTE` answers `Prepared statement
... does not exist`); the re-run on the head module answers identically. The
Explain and Prepare statements never match the refused arm. No registry row
exists for this shape (searched `docs/spark-sql-iceberg-parity.md`
2026-10-14); none added.

## Coverage attestation

```text
COVERAGE_ATTESTATION:
  pr_unit: write-body-carriers-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The brief cell list was checked against the committed fixtures: 20 Spark cells over creators, bodies and z states, 40 main CTAS cells over both format versions, 10 EXPLAIN ANALYZE and 4 PREPARE cells, each with the error or answer and the before and after counts.
      artifacts: [task/wo/write-body-carriers-1/evidence/spark-cells.json, task/wo/write-body-carriers-1/evidence/main-cells.json]
    - id: AT-2
      status: ATTACKED
      evidence: The domain ran in full: five write bodies, both creators, z absent and existing, both format versions; the already-exists and MERGE refusals are boundary answers, not gaps.
      artifacts: [task/wo/write-body-carriers-1/evidence/main-cells.json]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal cell carries its class, condition, SQLSTATE, first line and full text with before and after readbacks, so commit-before-fail is a counted 24 of 40 and not an impression.
      artifacts: [task/wo/write-body-carriers-1/evidence/main-cells.json]
    - id: AT-4
      status: N/A
      justification: The recorders run sequentially with fresh tables per cell and one engine call at a time; no shared mutable state and no concurrency surface.
    - id: AT-5
      status: N/A
      justification: Local scratch catalogs only, no credentials, no privileged action, nothing secret in the evidence.
    - id: AT-6
      status: N/A
      justification: No product code changes, so no stored format, migration, or compatibility surface moves; the integrity finding itself is the measured C-002.
    - id: AT-7
      status: N/A
      justification: One-shot recorders over small tables; no product hot path and nothing system-breaking.
    - id: AT-8
      status: ATTACKED
      evidence: Engine versions were read from the live sessions (Spark 4.1.2, repark 1.5.5) and the Iceberg runtime from the pinned GAV, never assumed; both provenance blocks record them.
      artifacts: [task/wo/write-body-carriers-1/evidence/spark-cells.json, task/wo/write-body-carriers-1/evidence/main-cells.json]
    - id: AT-9
      status: N/A
      justification: A failing recorder raises with its traceback and writes nothing; there is no standing failure path to alarm on.
    - id: AT-10
      status: N/A
      justification: Step 0 adds no product branches and no tests; its clauses are pinned by citation to the two fixtures, which the fix step's pins will read.
  complete: true
```
