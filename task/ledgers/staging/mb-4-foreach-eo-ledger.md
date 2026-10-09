# Unit ledger — MB-4-FOREACH-EO · `foreachBatch` exactly-once on the declared sink

**Date:** 2026-10-09 · **Branch:** `feat/mb-4-foreach-exactly-once` · **Base:** `feat/mb-4-facade`
`abead993` · **Model:** claude-opus-5-5 (Opus worker build lane) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: high** (the write and
commit path of a streaming sink).

**Retires when:** the branch merges into `feat/mb-4-facade`; the orchestrator then moves this file
to `completed/` with the MB-4 ledger.

**Scope.** Owner ruling 2026-10-09 ("FIX IT") on the S1 of the MB-4 Opus verify: the
`foreachBatch` door must not duplicate rows in its declared sink when the body has written and the
epoch then fails, or the process dies, before the driver's trailing stamp. The ruling overturns the
default MB-3 acted on ([mb-3-ledger.md](mb-3-ledger.md) D-2, C-005 b, c and e) and restores the
sketch's Q9 answer ([the sketch](../../wo/microbatch/mb-design-2026-10-06.md) Q9, §3.4, §3.5, §4
MBE-13 and MBE-14). The `toTable` door is out of scope and must not move.

## Halt checks — 2026-10-09

- **A fork (`iceberg-rust`) change:** not needed. The design reads `TableCommit::identifier` and
  `TableCommit::base_table` only, which the pinned rev already has (`AppendFence` uses both).
- **A new public option:** none. `repark.cdc.sink` already declares the sink.
- **A change to `toTable`'s commit path:** none in behaviour. One shared function on that path
  gains a fallback that the `toTable` door cannot reach: `SiteStamp::claim_with` reads the scope
  token from the commit's summary extras first, as today, and only when the extras carry none does
  it ask for the ambient body scope. The `toTable` door always carries the token in the extras of
  its private batch session (MB-3 D-3), and its batch never runs inside a body scope. This is
  recorded as a judgement for the orchestrator to overrule; C-011 holds the proof that the door's
  pins and kill scenarios are unchanged.

## Design note — 2026-10-09 (committed before any code)

### The guarantee

Exactly-once covers the **declared sink's main branch**: each epoch lands at most one snapshot from
the body in the sink, that snapshot carries the epoch's stamp (the six `repark.cdc.*` summary keys
and the `repark.cdc.offsets.<query-id>` property) in the same catalog commit as the rows, and a
restart resumes after the newest stamped epoch. Everything else the body does is outside the
guarantee and is described under "Side effects".

### How a write issued by the body reaches the scope

The driver already enters a `BatchScope` for the batch, keyed by `(sink TableUuid, ScopeToken)`,
and the three commit arms already claim a stamp when the commit's summary extras carry the token
(`commit_append_with_summary`, and the copy-on-write and merge-on-read arms in
`merge/snapshot_commit.rs`). What was missing on this door is the way for the token to reach those
arms. MB-3 D-3 ruled out the shared session config, because any concurrent write on that session
would then claim the batch stamp.

The token now travels as an **ambient body scope**: a Tokio task-local holding `(sink TableUuid,
ScopeToken)`, set by the driver around `BatchBody::run` on the `foreachBatch` door only. It is
visible on the driver's task and nowhere else.

- **A write from Python on the same session.** The adapter runs the callable under
  `block_in_place` on the driver's task, so the thread that runs the callable is the thread that
  polls the driver's future. A write from the callable enters Rust on that thread and is polled
  there by the binding's `block_on` (`Session::sql`'s future is not `Send`, MB-3 D-7), so the
  task-local is still set. This is the mechanism `query.stop()` from inside a body already relies
  on (`DRIVING`, pinned by `self_stop_tests.rs`). Two places read it:
  1. `SiteStamp::claim_with`, when the extras carry no token: a commit to the sink's main branch
     claims the batch stamp. The rest of the arm is unchanged: the epoch check, the stamp in the
     summary, the offsets property in the same transaction, the `AppendFence` on the append arm,
     and `record_commit`.
  2. `Session::sql`'s catalog gateway (`session/write_options.rs`, the one place both SQL doors
     take their catalog registry snapshot): inside a body scope, each catalog in the statement's
     snapshot is wrapped in a `BodySinkGuard`. The guard captures the scope when the statement
     starts, so it does not depend on the task-local at commit time.
- **A write from another session on the body's thread** (a second session object used inside the
  callable): the same as above. The scope belongs to the task, not to a session, and the claim is
  by the table's uuid.
- **A write from another thread, or from another process, inside the body.** It does not carry
  the body scope, so it commits as on main, unstamped (the MB-2a fold-1 ruling stands: a commit
  that does not carry the token never claims). It is a foreign write, outside the guarantee. Two
  backstops name it: when the body made no stamped commit and the sink's head moved by an unstamped
  snapshot during the batch, the query ends `RecoveryRequired(UnstampedSinkCommit)` instead of
  stamping over it; and the registry row says a sink write must come from the callable's own
  thread.

### Every write shape onto the declared sink, from the body

| shape | answer |
|---|---|
| `writeTo(sink).append()`, `INSERT INTO sink`, `df.write.insertInto` / `saveAsTable` in append mode | **Stamped.** The append arm claims the stamp; the commit is fenced by `AppendFence`. |
| `MERGE INTO sink`, copy-on-write and merge-on-read | **Stamped**, under `write.merge.isolation-level=serializable`; otherwise `MergeIsolationRefused` (MBE-15, unchanged). |
| `UPDATE sink` / `DELETE FROM sink` through the row-level arms (the same two commit functions) | **Stamped**, under the matching `serializable` isolation property; otherwise MBE-15. |
| A row-level statement that changes nothing (no matched row, no new file) | No commit, as today; the driver's trailing stamp-only commit closes the epoch. |
| A second commit to the sink in one epoch through a stamped arm | **Refused** `SinkCommittedTwice` (MBE-13). |
| `INSERT OVERWRITE`, `overwritePartitions()`, `overwrite(condition)`, `TRUNCATE`, a metadata-only `DELETE` | **Refused** `UnstampedSinkWrite` (MBE-19, new) before anything commits. |
| `CREATE OR REPLACE TABLE sink` / RTAS, `DROP TABLE sink`, `ALTER TABLE sink RENAME` | **Refused** MBE-19. |
| CTAS onto the sink | Fails as on main: the table exists. |
| `ALTER TABLE sink …` (schema, properties, partition spec, sort order), a write to a branch or a tag of the sink | **Refused** MBE-19: inside the body the sink takes the one stamped commit and nothing else. |
| Maintenance `CALL`s on the sink (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `rollback_to_snapshot`, `set_current_snapshot`, `cherrypick_snapshot`, `add_files`, …) | **Refused** MBE-19. |
| A sink write that does not go through `Session::sql` or a stamped arm (none is known from the Spark facade) | Not seen by the guard. If it moved the head and the body made no stamped commit, the query ends `RecoveryRequired(UnstampedSinkCommit)`. |
| Any write to another table, an external call | Untouched; a side effect (below). |

MBE-19 is one refusal for every shape that cannot carry the stamp. The guard cannot read the
commit's updates without consuming them (the fork's `TableCommit` has `take_updates` only), so it
does not sort shapes: it lets a commit to the sink through only while the scope holds a claimed,
not yet recorded stamp, which is exactly the window of the stamped arm's own commit.

### The body's outcomes

| the body | the sink | the query |
|---|---|---|
| returns, one stamped commit | rows and stamp in one snapshot | epoch durable, next epoch |
| returns, no sink commit, head unmoved | the driver's stamp-only snapshot (R-18, unchanged) | epoch durable, next epoch |
| returns, no stamped commit, an unstamped snapshot landed on the sink during the batch | no stamp is written | `RecoveryRequired(UnstampedSinkCommit { snapshot })`, durable record the previous epoch |
| raises before its sink commit | nothing | `Failed`, `BatchFailed` (MBE-16); the restart replays the epoch |
| raises after its stamped commit | rows and stamp | `Failed`, `BatchFailed`; the epoch **is durable**, the restart resumes at the next epoch |
| a second stamped write, or an MBE-19 shape, and the error leaves the callable | the first commit, if one was made | `Failed` with the typed refusal (`SinkCommittedTwice` or `UnstampedSinkWrite`), not `BatchFailed`; an epoch that committed is durable |
| the stamped commit loses to another run of the query | the winner's rows | `Failed`, `Fenced { winner }` (MBE-11), typed, read from the scope's latched refusal |
| the stamped commit's outcome is unknown | found by the C-008 walk: durable. Not found: nothing known | found: the epoch is durable and the body's error fails the query. Not found: `RecoveryRequired(CommitOutcomeUnknown)`, as the `toTable` door answers |

### Restart cases

- **The process died before the body's commit.** The sink has no trace of the epoch; the restart
  replays it once.
- **The process died after the body's commit** (the verify's `exit-after-write`, and every kill
  that lands between the commit and the next trigger). The stamp is in the lineage;
  `read_resume_point` returns it and the query resumes at the next epoch. The body does not run
  for the committed epoch.
- **The process died during the commit.** The catalog commit is atomic: the rows and the stamp
  are both there or neither is.
- **A body with no sink write died before the trailing stamp.** The epoch replays; the body's
  side effects run again (at-least-once, as today).
- **Skipped entirely, or run with the sink write suppressed?** Skipped entirely. The sketch says
  so twice: §3.5 "Each batch" step 2, "Read the resume point. If the epoch is already durable,
  skip it (D-4)", and Q9's "a replay sees the same `batch_id`", which describes only an epoch that
  is not yet durable. Flink's two-phase-commit sink does the same: a committer that finds its
  checkpoint id already committed skips the commit and the pre-commit work is not re-run. A
  suppressed-write replay would need the driver to keep the window of a committed epoch and to
  tell a body "write nothing", and neither exists.

### Side effects (everything that is not the declared sink)

A write to another table and an external call are not fenced and not stamped. The sink commit is
the epoch's commit point, so:

- a side effect the body runs **before** its sink write is at-least-once: an epoch that fails
  before the commit replays, and the side effect runs again;
- a side effect the body runs **after** its sink write is at-most-once on a failure: if the body
  raises, or the process dies, after the commit, the epoch is durable and the body does not run
  again for it.

The registry row states both halves and the recipe (write the sink last, or make the side effect
idempotent on the batch id). The brief's phrase "stay at-least-once" is true of the first half
only; the second half is the direct consequence of "skipped entirely" and is recorded as an
observation in the hand-back.

### The two-session race

Two runs of one query (two sessions, or two processes) both reach the same epoch. Each body's
append claims its own scope's stamp and commits through `AppendFence`, which refuses the commit
whose base no longer sits under this query's newest stamp. One commit lands; the loser's write
raises in its body, the scope latches `Fenced`, and the driver ends that query `Fenced { winner }`
without a stamp-only commit. The loser's staged files are orphans, as on the `toTable` door. Inside
one process the two bodies never overlap: `BatchScope::enter` holds one scope per sink.

### What the design does not close

1. **A sink write from another thread or process of the body**, then a crash before the batch
   ends: the restart replays and that write lands twice. Closing it inside one process needs a
   guard on every catalog handle of every session (a wrapper at `CatalogRegistry::insert`), which
   sits on `toTable`'s commit path and is therefore a halt item, not built here. Lean: do not
   build it; a helper thread's write is a foreign writer by Flink's model.
2. **A restart after `RecoveryRequired(UnstampedSinkCommit)`** cannot tell the unstamped snapshot
   from a foreign writer's, so it starts and replays the epoch. The ending carries the snapshot id
   so the operator can roll it back first.

## North Star four-line records — 2026-10-09

- **R-1. Is a committed epoch's body skipped on restart, or run with the sink write suppressed?**
  - **Flink:** skipped; the committer finds the checkpoint id in the sink and does not commit or
    re-run it.
  - **Spark:** `foreachBatch` re-runs the body (at-least-once); its own Iceberg sink skips a batch
    id at or below the newest committed one.
  - **Default acted on:** skipped entirely (sketch §3.5 step 2).
- **R-2. How does the body's sink write find the batch's stamp?**
  - **Flink:** the sink operator owns the commit; user code never commits to the sink.
  - **Spark:** no link; the body's writes carry no streaming keys (MB0-W4).
  - **Default acted on:** an ambient scope on the driver's task, read at the three stamped commit
    arms; never the shared session config (MB-3 D-3).
- **R-3. What happens to a sink write from another thread, session or process during the body?**
  - **Flink:** a foreign snapshot; the committer walks past it and the job keeps running.
  - **Spark:** it commits; nothing notices.
  - **Default acted on:** it commits unstamped (MB-2a fold 1, V1). If the body made no stamped
    commit and an unstamped snapshot moved the head, the query ends
    `RecoveryRequired(UnstampedSinkCommit)` (sketch Q9, third bullet). A stamped epoch tolerates
    a foreign snapshot beside it, as Flink does.
- **R-4. A write shape to the sink that cannot carry the stamp: stamp it, pass it, or refuse?**
  - **Flink:** impossible by construction; one committer.
  - **Spark:** it commits, at-least-once.
  - **Default acted on:** refuse before the commit with a dated registry row (MBE-19), the North
    Star's "else refuse" rule. The sketch names three commit sites and no other.
- **R-5. The body raises after its stamped commit: is the epoch durable?**
  - **Flink:** yes; a failure after the commit does not undo it, and recovery starts after it.
  - **Spark:** the batch is not in the commit log, so it replays (the source of the duplicate).
  - **Default acted on:** durable. The query fails with the body's error and the restart resumes
    at the next epoch.
- **R-6. The body's stamped commit has an unknown outcome.**
  - **Flink:** the committer retries idempotently on the checkpoint id.
  - **Spark:** replays.
  - **Default acted on:** the `toTable` door's rule (MB-3 C-007 d, e): the C-008 walk; found is
    durable, not found is `RecoveryRequired(CommitOutcomeUnknown)`; never a re-submit.

## PROPOSITION LEDGER — MB-4-FOREACH-EO — 2026-10-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A body's append to the declared sink lands as one snapshot that carries the epoch stamp and the offsets property, and the driver writes no trailing stamp-only snapshot for that epoch. | Driver-level pin plus the public-door pin. | **OPEN** | Closes when the red pins of step 2 are green. |
| C-002 | A body that writes the sink and then raises leaves the epoch durable; the restart resumes at the next epoch and the sink holds each source row once. | Driver-level pin, the public-door pin, the verify's `f_writeraise`. | **OPEN** | As C-001. |
| C-003 | A process that dies after the body's sink write, at a random moment, or at a sink commit, restarts with each source row once in the sink. | Subprocess pins on the public door; the verify's `f_exitafterwrite`, `f_kill`, `f_killcommit`. | **OPEN** | As C-001. |
| C-004 | A second stamped write to the sink in one epoch refuses `SinkCommittedTwice`, the query ends with that typed error, and the sink holds the first write once. | Driver-level pin plus the public-door pin. | **OPEN** | As C-001. |
| C-005 | `MERGE INTO sink` from a body is stamped in its own snapshot under serializable isolation. | Public-door pin. | **OPEN** | As C-001. |
| C-006 | An overwrite of the sink from a body refuses `UnstampedSinkWrite` before anything commits, and so does every other shape the design table refuses. | Public-door pins per shape; guard pins in `repark-iceberg`. | **OPEN** | As C-001. |
| C-007 | A body that never writes the sink still gets the driver's stamp-only snapshot, one per epoch. | Driver-level pin plus the public-door pin. | **OPEN** | As C-001. |
| C-008 | A body that writes a second table leaves that table unstamped and unfenced, and the sink exactly-once. | Public-door pin. | **OPEN** | As C-001. |
| C-009 | A body that made no stamped commit while an unstamped snapshot moved the sink's head ends `RecoveryRequired(UnstampedSinkCommit)` and writes no stamp. | Driver-level pin. | **OPEN** | As C-001. |
| C-010 | Two runs of one query racing through `foreachBatch` bodies land every row once; the loser ends `Fenced`. | Driver-level race pin, 5 runs. | **OPEN** | As C-001. |
| C-011 | The `toTable` door is unchanged: its pins, the race pins and its kill scenarios answer as at the base. | `table_door_tests.rs`, `race_tests.rs` 5 times, the verify's `t_*` scenarios. | **OPEN** | As C-001. |
| C-012 | The verify's 18 exactly-once scenarios through the public doors report 0 violations. | The verify's own scripts, run unchanged apart from the interpreter path. | **OPEN** | As C-001. |
| C-013 | Five hand mutants of the new code each turn at least one pin red. | The mutant table. | **OPEN** | As C-001. |
| C-014 | The registry carries the side-effect contract (both halves), MBE-13 and `UnstampedSinkCommit` as reachable through the public door, and MBE-19. | The registry rows and the sketch §4 row. | **OPEN** | As C-001. |
