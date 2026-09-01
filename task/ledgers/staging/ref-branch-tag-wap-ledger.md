# Charter ledger — REF · branch / tag operations + write-audit-publish (WAP)

**Date:** 2026-09-01 · **Branch:** `feat/ref-branch-tag-wap` · **Base:** `482c64d`
(integration branch `feat/v1-close-and-v07-open`) · **Roadmap:**
[release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) REF row
("branch / tag operations + write-audit-publish (WAP), needs fork F-6") · **Path:** STANDARD.
**risk_tier:** standard.

**Retires:** parks in `staging/` while the unit runs; retired post-merge by the archival chore
(`make ledger-archive` at pickup).

**Why now.** [STATUS.md](../../../STATUS.md) carried "REF waits on fork F-6", and F-6
(`SnapshotUpdate.to_branch`, fork [#244](https://github.com/TRO-Wolf/iceberg-rust/pull/244)) is
at the pin `33be9a0`. The registry's REF-1 row and the engine's write-to-branch refusal both
name a **stale** reason — fork pin `b009ac1`, "no `to_branch` / `with_branch` commit-target
API". That sentence is false at `33be9a0`. C-001 measures what actually changed, and the answer
moves the gap rather than closing it: F-6 landed `to_branch` on the seven *transaction actions*,
not on the *`iceberg-datafusion` write path* the engine's `INSERT` / `UPDATE` / `DELETE`
statements execute through. Fork pin `33be9a0`. Do not bump the pin.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-REF-CHARTER
  agent: Actor
  action: ACTOR_BUILD charter + ref/WAP surface audit (no product edit yet)
  charter_trace: C-001
  preconditions:
    - base 482c64d on feat/ref-branch-tag-wap: SATISFIED
    - fork pin 33be9a0: SATISFIED
    - live oracle PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, Java 17: SATISFIED
  success_condition: C-001 matrix in this ledger; every product edit below traces to a cell
  uncertainty: NONE on the measured cells; the write-to-branch disposition is a filed fork gap
  verdict: PROCEED
```

## Scope

| In | Out |
|---|---|
| Audit of every reachable ref door at fork pin `33be9a0` (C-001) | Fork pin bump, `[patch.crates-io]` edits |
| Spark-door `branch_<name>` / `tag_<name>` read selectors | Inventing an `iceberg-datafusion` commit-target API |
| The `RETAIN … WITH SNAPSHOT RETENTION n SNAPSHOTS m DAYS` grammar half the parser drops | A partial write-to-branch that works for MERGE and not for INSERT |
| Honest, re-measured refusals for write-to-branch, write-to-tag, and every WAP door | `docs/examples/`, `scripts/check_example_coverage*`, Python function registration |
| Registry rows + STATUS truth-up for what this unit changed | ANSI-door dotted ref selectors (that door's spelling is `FOR VERSION AS OF '<ref>'`, already delivered) |

## C-001 measurement matrix (2026-09-01)

Oracle: live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog under a
scratch warehouse, Java 17, ANSI on, UTC, `local[2]`. Engine cells measured on this tree at fork
pin `33be9a0` through the Spark door. Incidental controls are included, not only the headline
assertions.

### Branch doors

| Cell | Apache Spark 4.1.2 + Iceberg 1.11.0 | Engine today | Verdict |
|---|---|---|---|
| `ALTER TABLE t CREATE\|REPLACE\|DROP BRANCH` | works | works | equal |
| `CREATE BRANCH b` when `b` exists | `IllegalArgumentException: Ref b already exists` | `DataInvalid => Ref b already exists` | equal outcome |
| `CREATE BRANCH IF NOT EXISTS` / `DROP BRANCH IF EXISTS` | accepted | refuses loud (REF-2) | DECLARED, unchanged |
| `SELECT … FROM cat.ns.t.branch_b` | reads the branch head | `Error during planning: Unsupported compound identifier … Expected 1, 2 or 3 parts, got 4` | **gap — opaque, not loud** |
| `SELECT … FROM cat.ns.t VERSION AS OF 'b'` | reads the branch head | reads the branch head | equal |
| `VERSION AS OF '<missing ref>'` | `Cannot find matching snapshot ID or reference name for version nope` | `unknown Iceberg snapshot ref "nope": no branch or tag with that name` | equal outcome |
| `SELECT … FROM cat.ns.t.branch_<missing>` | `ValidationException: Cannot use branch (does not exist): nope` | the same opaque 4-part planning error | **gap** |
| `INSERT INTO cat.ns.t.branch_b` | commits onto `b`; `main` unmoved; new snapshot parents off `main`'s head | refuses loud (REF-1) | DECLARED — see disposition |
| `UPDATE` / `DELETE` on `cat.ns.t.branch_b` | commit onto `b`; `main` unmoved | refuse loud (REF-1) | DECLARED |
| `df.writeTo("cat.ns.t.branch_b").append()` | commits onto `b` | no facade surface | DECLARED |
| `df.writeTo("cat.ns.t").option("branch","b").append()` | **committed onto `main`, not `b`** (incidental control) | n/a | recorded so no pin claims the option |
| `CREATE BRANCH b RETAIN 5 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS` | accepted; refs row `max_reference_age_in_ms=432000000`, `min_snapshots_to_keep=3`, `max_snapshot_age_in_ms=604800000` | **refuses** — the parser takes `n SNAPSHOTS` and then chokes on the trailing `7` | **gap — real grammar half missing** |
| `t.refs` metadata table | name / type / snapshot_id / the three retention columns | present and queryable | equal |

### Tag doors

| Cell | Apache Spark 4.1.2 + Iceberg 1.11.0 | Engine today | Verdict |
|---|---|---|---|
| `CREATE\|REPLACE\|DROP TAG`, `AS OF VERSION`, `RETAIN n DAYS` | works; `RETAIN 10 DAYS` → `max_reference_age_in_ms=864000000` | works | equal |
| `CREATE TAG v WITH SNAPSHOT RETENTION …` | **Spark parse error** (`mismatched input 'WITH' expecting {<EOF>, 'AS', 'RETAIN'}`) | refuses loud | equal outcome, already pinned |
| `DROP TAG <missing>` | `IllegalArgumentException: Tag does not exist: v3` | `DataInvalid` naming the ref | equal outcome |
| `SELECT … FROM cat.ns.t.tag_v` | reads the tag snapshot | the opaque 4-part planning error | **gap** |
| `SELECT … FROM cat.ns.t VERSION AS OF 'v'` | reads the tag snapshot | reads the tag snapshot | equal |
| `INSERT INTO cat.ns.t.tag_v` | **refuses**: `IllegalArgumentException: Cannot write to table with time travel` | refuses (the write-to-branch message) | equal outcome, different diagnostic |
| `UPDATE cat.ns.t.tag_v` | **refuses**: `Cannot modify table with time travel` | refuses (same) | equal outcome |

Tags are immutable refs on both engines: no measured write door reaches a tag. Retention on a
tag is `RETAIN n <unit>` only — the snapshot-retention clause is a **branch** clause, and Spark's
own parser rejects it on a tag.

### WAP doors

| Cell | Apache Spark 4.1.2 + Iceberg 1.11.0 | Engine today | Verdict |
|---|---|---|---|
| `spark.wap.branch` + `write.wap.enabled=true` | plain `INSERT INTO t` stages onto the branch; a plain `SELECT … FROM t` in the same session reads the **branch**; `main` unmoved until publish | `SET spark.wap.branch = …` refuses: `Invalid or Unsupported Configuration: Could not find config namespace "spark"` | fail-closed, no silent divergence |
| `spark.wap.branch` **without** `write.wap.enabled` | the conf is **ignored** — the row landed on `main` (incidental control) | same refusal | recorded |
| `spark.wap.id` | stamps `wap.id` into the snapshot summary; the snapshot joins the log with `main`'s head as parent, and `main` does **not** advance | same refusal | fail-closed |
| `CALL sys.fast_forward(table, branch, to)` | returns `(branch_updated, previous_ref, updated_ref)`; moves `main` to the audit branch head | refuses loud, naming the seven supported procedures | **gap** |
| `CALL sys.publish_changes(table, wap_id)` | returns `(source_snapshot_id, current_snapshot_id)`; the staged `wap.id` snapshot becomes current | same loud refusal | **gap** |
| `CALL sys.cherrypick_snapshot(table, snapshot_id)` | replays a branch-only snapshot onto `main` | same loud refusal | **gap** |

The fork carries the publish primitives at the pin — `ManageSnapshots::fast_forward`,
`Transaction::cherry_pick` (`GAP_MATRIX` R98) — so the WAP gap is an **engine** surface gap, not
a fork one. It is declared here rather than built: a publish procedure is a commit-path surface,
and this unit's own write leg is still refused, so a publish with nothing engine-written to
publish would be surface without a user.

### What F-6 actually delivered, and where the write-to-branch gap moved

At `33be9a0`, `crates/iceberg/src/transaction/to_branch.rs` adds `to_branch` to all seven
snapshot-producing actions — `FastAppendAction`, `MergeAppendAction`, `OverwriteFilesAction`,
`ReplacePartitionsAction`, `RewriteFilesAction`, `RowDeltaAction`, `DeleteFilesAction` — with
branch-scoped conflict validation and byte-stable sibling refs.

The engine reaches those actions from two different places, and only one of them is RePark's:

- **RePark-owned commit construction** — `write/append.rs::commit_append` (CTAS, ANSI create),
  `write/overwrite.rs`, `write/partition_overwrite.rs`, `write/merge/mod.rs`. These could pass a
  branch today.
- **The fork's DataFusion write path** — plain `INSERT INTO`, `UPDATE` and `DELETE` fall through
  the Spark door (`router.rs`) to DataFusion and execute through `iceberg-datafusion`'s
  `IcebergTableProvider` and its commit exec, which calls `tx.fast_append()` with **no
  commit-target parameter**. `grep -n branch crates/integrations/datafusion/src/table/mod.rs`
  at the pin returns nothing.

So `INSERT`/`UPDATE`/`DELETE`-to-branch needs fork surface **beyond** F-6:
`IcebergTableProvider` (or its write/commit exec) must accept a commit target and hand it to
`to_branch`. Building the RePark-owned half alone would make `MERGE` and `INSERT OVERWRITE`
write to a branch while `INSERT INTO` on the same statement family refuses — a Spark-visible
split the registry does not settle, and a fail-open shape on the write path. **Disposition:
refuse the whole write leg, correct the reason, file the gap.** Never invent the fork API here.

## PROPOSITION LEDGER — REF — 2026-09-01

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Measure first.** Every reachable branch, tag and WAP door is measured at fork pin `33be9a0` against the live oracle — including incidental controls — and recorded above before any product edit. | The matrix above; every later clause cites a cell. | **OPEN** | This commit. |
| C-002 | Spark-door `cat.ns.t.branch_<name>` and `cat.ns.t.tag_<name>` **reads** resolve the ref and return the ref's rows, value and type, instead of the opaque 4-part planning error; a missing ref refuses loud naming the ref. | Red-first pins in `crates/repark-spark/src/tests/ref_ddl.rs`. | **OPEN** | Matrix rows "read `t.branch_b`" / "read `t.tag_v`". |
| C-003 | `CREATE BRANCH b RETAIN n <unit> WITH SNAPSHOT RETENTION m SNAPSHOTS k <unit>` parses **both** snapshot-retention halves and writes the oracle-measured `min_snapshots_to_keep` and `max_snapshot_age_ms`; the tag form still refuses (Spark parse-fails it). | Red-first pin asserting the three retention values. | **OPEN** | Matrix row `RETAIN 5 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS`. |
| C-004 | The write-to-branch refusal names the **true** gap at `33be9a0` — F-6 delivered `to_branch` on the transaction actions; the `iceberg-datafusion` write path the engine's `INSERT`/`UPDATE`/`DELETE` execute through carries no commit target — and the gap is filed against the fork row. Write-to-tag stays refused, which is Spark-equal. | Refusal-text pin; fork filing. | **OPEN** | "What F-6 actually delivered" above. |
| C-005 | WAP is DECLARED with measured evidence: the three publish procedures and the `spark.wap.*` session confs refuse loud (fail-closed, no silent write to `main`), and the registry says so. | Pins for the three `CALL` refusals and the conf refusal; registry row. | **OPEN** | WAP matrix. |
| C-006 | Documents match the pins: registry rows for everything this unit changed, STATUS truth-up for the F-6 wait only, maps in lockstep. | Registry, STATUS, `make check-map-sync`. | **OPEN** | — |

VERDICT: 0 PROVEN, 6 OPEN, 0 REJECTED — charter commit. Clauses close as their slices land.

## Sequence

1. This charter + the C-001 matrix (this commit).
2. C-003 — the retention grammar half, red-first against the oracle values.
3. C-002 — the `branch_` / `tag_` read selectors, red-first against the opaque error.
4. C-004 / C-005 — re-measured refusals, the fork filing, the WAP declaration.
5. C-006 — registry rows, STATUS truth-up, gates.
