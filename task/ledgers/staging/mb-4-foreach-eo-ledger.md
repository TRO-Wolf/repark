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

## Amendments to the design note — 2026-10-09 (found while building; the note above is as committed)

- **A-1. The ambient scope is a thread-local, not a Tokio task-local.** `repark-iceberg` has no
  Tokio dependency outside its tests and the brief forbids a dependency edit. `scope_body` wraps
  the body's future and sets the thread-local for each poll, restoring the outer value when the
  poll returns. The reach is the same as the note describes.
- **A-2. A third reader of the scope: the SQL doors' routing.** The first public-door run was
  red: a plain `INSERT INTO sink` from the body landed unstamped and unseen by the guard. Both
  SQL doors send a plain `INSERT`, `UPDATE` and `DELETE` to the fork's table provider unless a
  session snapshot property is set (`session_write_conf_is_set`), and only the RePark-owned arms
  claim a stamp. That is why the sketch put the token in the session config. Inside a body scope
  `session_write_conf_is_set` now answers true, so those statements take the owned arms, the same
  route any session with a snapshot property takes. Mutant M8 pins it.
- **A-3. `DROP TABLE sink` and a drop-and-recreate are not refused.** The guard does not cover
  `drop_table` or `rename_table`: the public pin
  `test_spark_stop_raises_first_recovery_after_releasing` already holds the MB-3 answer
  (C-018 d), and a narrow fix leaves it. A drop alone ends `STREAM_FAILED` (the sink no longer
  loads); a drop-and-recreate ends `RecoveryRequired(UnstampedSinkCommit)`. `ALTER TABLE …
  RENAME` fails on its own on the memory catalog and was not measured further.
- **A-4. Table replacement is refused through `publish_replace_table`.** `createOrReplace()`, RTAS
  and `saveAsTable` in overwrite mode commit through that catalog call, not `update_table`; the
  first measurement showed them landing and being caught only after the fact. The guard now
  refuses them before they land.
- **A-5. A failed body is not audited for unstamped snapshots.** The outcome table's row "raises
  before its sink commit" stands as written; a body that raised after a write that slipped the
  scope ends `BatchFailed`, not `UnstampedSinkCommit`. The check would cost a sink load on every
  failure and would replace the body's own error text. The slipped write is named on the first
  batch that returns.
- **A-6. Two `DELETE` shapes refuse MBE-19.** A `DELETE` with no predicate and a `DELETE` that
  matches no row commit outside the two row-level arms, so the guard refuses them. A row-level
  `DELETE` that rewrites a file is stamped. The no-match refusal depends on the data; it is safe
  (nothing lands) and it is recorded as an observation for the owner.
- **A-7. The typed refusal wins over the body's own error.** When the scope holds a refusal and
  the body raised, the query ends with the refusal even if the body caught it and later raised
  something else. The scope cannot tell the two apart.
- **A-8. A `MERGE` with only `WHEN NOT MATCHED THEN INSERT`** commits through the append arm and
  needs no isolation property (measured: `append`, epoch 0).

## Measured write shapes — 2026-10-09 (public door, one body each, sink seeded with two snapshots)

| shape | measured answer |
|---|---|
| `writeTo(sink).append()`, `INSERT INTO … SELECT`, `INSERT INTO … VALUES`, `df.write.insertInto`, `saveAsTable` (append) | stamped `append` |
| `MERGE INTO` with a matched clause, serializable | stamped `overwrite` |
| `MERGE INTO`, insert-only, default isolation | stamped `append` |
| `UPDATE`, row-level `DELETE`, serializable | stamped `overwrite` |
| second append in one epoch | `SinkCommittedTwice`; the first lands once |
| `INSERT OVERWRITE`, `overwritePartitions()`, `overwrite(condition)`, `createOrReplace()`, RTAS, `saveAsTable` (overwrite), `TRUNCATE`, `DELETE` with no predicate, `DELETE` matching no row, `ALTER TABLE … SET TBLPROPERTIES`, `ADD COLUMN`, `CREATE BRANCH`, `INSERT INTO sink.branch_x`, `expire_snapshots`, `rewrite_data_files`, `rewrite_manifests`, `rollback_to_snapshot` | `UnstampedSinkWrite`; sink rows, snapshots and properties unchanged |
| CTAS onto the sink | `TABLE_OR_VIEW_ALREADY_EXISTS`, as on main |
| `DROP TABLE sink` | `STREAM_FAILED`, the sink does not load (unchanged) |
| a sink write from another Python thread, no stamped commit | `RecoveryRequiredException`, `UnstampedSinkCommit` |

## Hand mutants — 2026-10-09

| id | mutant | red pins |
|---|---|---|
| M1 | the claim ignores the ambient scope (`ambient_token` filtered out) | 4 in `sink_offsets` body-scope pins, 11 in `repark-core` microbatch (the stamp pin, the durable-epoch pin, the second-write pin, the foreach race, both unknown-outcome pins and five more) |
| M2 | the guard admits a sink commit whenever nothing is committed yet | 2 guard pins, 1 driver pin (`an_unstampable_commit_to_the_sink_refuses_before_it_lands`) |
| M3 | a body that fails after its commit does not record the epoch durable | 2 driver pins |
| M4 | the driver never runs the unstamped-head check | 2 driver pins |
| M5 | the driver ignores the scope's typed refusal | 3 driver pins, the foreach race among them (the loser no longer names the winner) |
| M6 | the body scope is not restored after a poll | 2 `sink_offsets` pins, 2 driver pins |
| M7 | the guard's replacement check is inverted | 1 guard pin (added after the mutant first survived at the Rust level) |
| M8 | the SQL routing ignores the body scope (native build) | 9 of 25 public-door pins |

M1 is also the red-first evidence for the Rust pins: it restores the base's claim behaviour. The
public-door battery was run red on a native build of the base tree before the fix: 14 of its
first 15 pins failed, each on the defect (unstamped snapshots, duplicated ids, no refusal), and
the no-sink-write pin passed.

## Exactly-once through the public doors — 2026-10-09

The verify's scripts were run from a scratch copy with two edits and no other change: the
interpreter and script directory point at this branch's build, and each session calls the private
`_native._streaming_tests_allow_local_catalog` seam, because fold 1 made the public start doors
refuse a memory catalog (MBE-8) after the verify ran. The verdict does not list its 18 scenarios
by name; the 12 choreographies of its driver script and the 9 cases of its multi-query script were
run.

| scenario | source rows | sink rows | duplicates | lost | epochs |
|---|---|---|---|---|---|
| `f_writeraise` | 24 | 24 | 0 | 0 | 6, no gap |
| `f_exitafterwrite` | 24 | 24 | 0 | 0 | 6, no gap |
| `f_kill` (10 random kills) | 120 | 120 | 0 | 0 | 30, no gap |
| `f_killcommit` (8 kills at a commit) | 80 | 80 | 0 | 0 | 20, no gap |
| `f_raise`, `f_exitbody` | 24 | 24 | 0 | 0 | 6, no gap |
| `f_stop`, `t_stop` | 136 | 136 | 0 | 0 | 34, no gap |
| `f_sparkstop`, `t_sparkstop` | 100 | 100 | 0 | 0 | 25, no gap |
| `t_kill` | 120 | 120 | 0 | 0 | 30, no gap |
| `t_killcommit` | 80 | 80 | 0 | 0 | 20, no gap |

The four that the verify counted as violations (4, 4, 12 and 16 duplicates) are exact, and each
sink now holds one snapshot per epoch (6, 6, 30, 20) where it held two. The six kill and
write-then-raise scenarios were run again on the final build with the same counts. The verify's
one-process repro ends `sink 1, 2, 3`, one snapshot `append / epoch 0 / 3 rows`.

The multi-query script's nine cases answer as its own text expects (refusals `INPUTS_CHANGED`,
`SINK_BUSY`, the duplicate-name refusal; the two-door and batch-insert cases exact), with one
artefact: the case "same name, same checkpoint, source swapped" showed sink `0, 1, 2, 3, 3, 4`.
Run alone it shows `0, 1, 2, 3, 4`. The extra row comes from the script: an earlier case leaves a
`toTable` query running when its second start refuses, and the script then drops and re-creates
the sink under it. See "Observed, out of scope".

## Observed, out of scope — 2026-10-09

- **A running `toTable` query keeps writing after its sink is dropped and re-created under the
  same name.** The driver loads the sink by name for each batch and does not compare its uuid with
  the one the query registered on, so the next batch lands in the new table. The `foreachBatch`
  door has had this check since MB-3 C-018 (d). Not touched: the `toTable` door is out of scope.
- **The no-match `DELETE` refusal** (A-6) would go away if the metadata-delete commit claimed the
  stamp as a fourth site; the sketch names three.
- **The scratch clone the verify's scripts name** now holds another lane's branch. One probe file
  was copied into its `tests` directory by mistake during the red run, failed at import, and was
  removed at once; `git status` there was clean before and after.

## Fold 2 — the lineage invariant — 2026-10-09 (after the re-verify; branch `feat/mb-4-facade` at `2877da20`)

The re-verify (a different Opus session) closed the first verdict's S1 and four S2 in fact
(343 kills, no duplicate) and failed the build on two S1 that are one defect: the refusal set
was a block-list by route. A sink write that passed neither the guarded registry nor a stamped
arm landed unstamped and unrefused, and beside the body's own stamped write the query ran on;
one raise or kill between the two then duplicated rows silently. Its routes were `EXPLAIN
ANALYZE` of a DML on the body's own thread and any write from another thread.

**Ruling (orchestrator, 2026-10-09).** Enforce the sketch's rule by construction, wherever the
write came from: a sink head that moved without the stamp ends
`RecoveryRequired(UnstampedSinkCommit)`. This fold builds that. Where it differs from the
sections above, this section holds; the earlier text stays as the record of what was built
first.

### What this fold overturns in the sections above

- **A-5** ("a failed body is not audited") and the outcome table's row "raises before its sink
  commit": every body is audited, and a violation wins over the body's own error.
- **R-3's default** ("a stamped epoch tolerates a foreign snapshot beside it") and the pin
  `a_foreign_snapshot_beside_a_stamped_epoch_is_tolerated`: an unstamped snapshot beside the
  stamped commit ends the query.
- **"What the design does not close", limit 1** (a write from another thread, then a crash):
  closed for every epoch after the first stamped one. **Limit 2** (a restart after
  `UnstampedSinkCommit` replays): closed the same way. Epoch 0 keeps a limit, below.
- **A-3** claimed a drop-and-recreate ends `RecoveryRequired` and that the door checks the
  sink's uuid. That held only inside a batch whose body made no stamped commit. It holds now
  for every batch: the table under the sink's name is compared with the one the query
  registered on before every body and after it.
- **A-6 and the measured-shapes table** listed two refused `DELETE` shapes. There are three:
  a `DELETE` whose predicate covers whole data files refuses as well (measured by the
  re-verify). The registry row says so.
- **Hand-back Q2** ("should a sink write from another thread be closed inside the process"):
  answered by the invariant, with no per-catalog guard.

### The invariant, as built

From the first start of a query on a sink, every snapshot on the sink's `main` above the
query's newest stamped epoch is the running epoch's one stamped snapshot, or the query ends.

- **(a) Before any body** (`Run::refuse_moved_sink`): at the start of the trigger loop, so at
  every restart, and again on the sink each batch loads. The table must be the one the query
  registered on. Walking `main` from the head to the query's newest stamp, no snapshot may be
  unstamped. For a query with no stamp, the walk ends at the head it found at start.
- **(b) After every body, returned or raised, before the epoch is recorded durable**
  (`SinkMark`): nothing on the sink may differ from what the batch found, except the one
  stamped commit. The differences it names: another table uuid; a new unstamped snapshot
  anywhere in the table (a branch write and a staged snapshot count); a removed snapshot;
  `main` moved to an older snapshot; a table property other than an offsets key; a schema,
  partition-spec or sort-order id; a branch or tag.
- **The ending** is `RecoveryRequired` with `UnstampedSinkCommit { snapshot, operation }` or
  the new `UnstampedSinkChange { what }`, and the durable record the handle reports is the last
  epoch that passed the audit.
- **It is detection after landing.** The rows are in the sink. The query stops and does not
  replay over them.

### Two readings of the ruling I made, for the orchestrator to overrule

1. **A snapshot stamped by a different streaming query is not a violation.** The ruling's
   words are "either this epoch's one stamped snapshot or a violation". A stray write of this
   body cannot carry another query's stamp, the error's own name is "unstamped", and the
   merged pin `two_sessions_on_one_sink_wait_for_the_scope` runs two queries on one sink
   through this door. The offsets properties of other queries are passed over for the same
   reason. A snapshot stamped by this query under another run is left to the epoch check and
   the fence (`Fenced`), as before.
2. **Check (a) is on snapshots only.** A property-only change between two runs does not
   refuse a restart; "property-only commits and ref changes" are the ruling's words for
   changes "during an epoch", and they are check (b)'s.

### Where the invariant stops

- **Epoch 0.** A query that never committed its first batch has no stamp. The ruling names
  "the registered starting head for epoch 0" as its baseline, and that head is in memory.
  After a refused or killed batch 0 a restart takes the head it finds as the baseline, runs
  the body again, and check (b) stops it again if the stray write recurs. So at epoch 0 a
  stray write can land once per start; it is never silent (the pin
  `at_epoch_zero_a_restart_replays_and_is_stopped_again`, and the re-verify's
  `explain_only_raise`). Closing it needs a durable mark of the starting head (a table
  property written once at the first start), which the sketch's state rule (NS-2: the summary
  and the offsets property, nothing else) does not allow without a ruling. It is the
  hand-back's question.
- **A stray write below the stamped commit, then a kill before the audit.** The restart finds
  the stamp at the head and resumes after it. Nothing is duplicated: the epoch is durable and
  the stray landed once. It is not reported.
- **`DROP TABLE` alone** still ends `STREAM_FAILED` (the sink does not load), and a restart
  after a drop-and-recreate is a new query: the query id comes from the table's uuid.
- **The sink is single-writer while the query lives.** A batch `INSERT`, a compaction or any
  other maintenance on the sink, between runs included, refuses the next start. Recovery is a
  rollback to the newest stamped snapshot, or a new `queryName`. This is the ruling's letter
  and it is a real restriction (a `foreachBatch` sink cannot be compacted under a live query
  name); it is in the registry row and in the hand-back.

### Refused before landing (defence in depth)

- **The `EXPLAIN ANALYZE` route.** The statement head is `EXPLAIN`, so both SQL doors hand it
  to DataFusion, whose registered table provider holds the unguarded catalog and commits with
  no stamped arm. The same is true of every statement the doors do not own. The one place
  such a plan runs is `PreExecute::execute` (the Spark door's passthrough now calls it too).
  Inside a body it finds every `Dml` node the statement will execute and refuses
  `UnstampedSinkWrite` when the target is the sink. A plain `EXPLAIN` executes nothing and
  passes.
- **The claimed state (the re-verify's first S2).** A stamped write that failed left the
  scope claimed: the guard then admitted every commit, and a retry got MBE-13's text although
  nothing was stamped. A `SiteStamp` now releases its claim when its commit fails for a known
  reason or is never attempted. A retry is the epoch's one stamped commit. Beside an unknown
  outcome a second claim is refused with its own text. MBE-13 is shown only for a commit that
  landed, or for a claim that is held with a known outcome.

### Recorded, not changed (the re-verify's S3)

- Statements on other tables answer differently inside a body (39 of 120 compared): the
  routing switch sends `INSERT`, `UPDATE` and `DELETE` to the engine's own arms, so a plain
  `INSERT` returns one empty row, an `INSERT` into a missing table words its error
  differently, and a branch `UPDATE` or `DELETE` gains `engine.operation-id`. Rows, snapshot
  operations and counts are equal. Registry row `MB-4-FOREACH-SINK-SHAPES-1`.
- The data-dependent `DELETE` refusals and the untyped in-body refusals: the same row. Giving
  the in-body refusal its type is not one site: the error crosses the generic engine-error
  mapper as a DataFusion external error, and the mapper's arm for it belongs to every door.
- A second catalog entry for the sink's metadata takes the stamp: the same row.
- The helper-thread crash in pyarrow's allocator: card THREADED-COLLECT-SEGV-1, extended.
- `toTable` writing on after a drop-and-recreate: card STREAM-SURFACE-RESIDUE-1, a line.
- **The "three trigger strings" regression is not one.** `'  bogus'`, `'bogus  '` and
  `' 1 month '` differ from the oracle's `parsed` record only through the private
  `_native.check_trigger_interval`, which has trimmed its input since the parser landed
  (`7c31d0cf`, before the first verdict's head); its own Rust pin of that date expects the
  trimmed echo. The public `trigger(processingTime=...)` strips first, as PySpark's does, and
  equals the oracle's `trigger` record. The only later commit to the parser file (fold 1's
  mask, `983f5632`) leaves these texts alone. Recorded on the residue card; no code change.

### Fold-2 proof — 2026-10-09

**Red first.** The re-verify measured the red on `2877da20` and its outputs are the record
(one-process repros: sink `1, 1, 2, 2, 3, 3, 100`, run 2 returns with no exception; `special`
`explain_raise`, `explain_kill`, `thread_raise`, `thread_kill`: source 32, sink 36, 4 duplicates
each; the claimed-state door cases). The pins of this fold were written from those repros and
not run again on the old build. Mutants N1 to N3 below restore the old behaviour of each check
and are the pins' own red evidence.

**The re-verify's scripts, re-run.** From a copy with only the path prefix changed.

| script | at `2877da20` (the verdict) | now |
|---|---|---|
| one-process repro, `explain` | run 2 silent, sink `1, 1, 2, 2, 3, 3, 100` | both runs end `STREAM_FAILED` MBE-19; nothing lands |
| one-process repro, `thread` | run 2 silent, sink `1, 1, 2, 2, 3, 3, 100` | both runs end `RecoveryRequiredException` naming the snapshot; this is epoch 0, so run 2 replays and lands the thread's rows again (the epoch-0 limit) |
| `special explain_raise`, `explain_kill`, `explain_only_raise` | 4 duplicates, silent | 0 duplicates; refused before landing, sink empty |
| `special thread_raise`, `thread_kill` | 4 duplicates, silent | 0 duplicates; `RecoveryRequired`, and once a stray sits above a stamp every restart refuses |
| `special swallow_twice`, `alt_raise_30`, `diff_body`, `ckpt` | exact | exact (32, 160, 36, 36 rows, 0 duplicates) |
| `special two_names` | exact | exact |
| `special rollback`, `rollback_all`, `expire_plain`, `expire_foreign`, `foreign_between` | refuse or stop as recorded | 0 duplicates in each; `foreign_between` now refuses the restart (one unstamped snapshot above the stamp) |
| `special conflict_append_0`, `_1`, `conflict_update_0`, `_1` | finished or stalled through restarts, retries refused MBE-13 | 0 duplicates; the hammer thread's rows are unstamped, so each run ends `RecoveryRequired` |
| `special conflict_mergeupd_0` | 123 duplicate rows of the hammer thread, no epoch ever stamped | 132, the same shape: the body's `MERGE` never wins the validation, no stamp exists, and at epoch 0 each of the 40 restarts runs the hammer thread again. Every run ends `RecoveryRequired`. This is the epoch-0 limit on the verifier's own script |
| door table, 166 shapes by 3 placements (498 cases), by the verifier's summariser | the `EXPLAIN ANALYZE` and thread shapes landed unstamped with the query running on in placements B and C (the verdict's route table) | per placement: 39 stamped, 118 no commit, 8 unstamped ending `RecoveryRequired` (the five thread routes, drop-and-create twice, the private ref), 1 `DROP TABLE` ending `STREAM_FAILED`; no unstamped case with the query running on |
| `scope` (120 statements, three arms) | 42 body differences, 3 thread | the same 45 lines |
| `leak` | no leak | no leak; its cross-writing queries now end `RecoveryRequired` |
| `droprecreate foreach` / `table` | both write on | `foreach` ends `RecoveryRequired` (replaced table); `table` writes on, unchanged |

**The kill set.** 14 scenarios with the verifier's bodies and kill mix and my seeds: 286 kills
over nine `foreachBatch` bodies on both triggers and 51 over three `toTable` runs (the kill
count is what landed: 23, 22 and 6 of 30, 30 and 14 attempts). 13 exact on the script's own
check with 0 duplicates and 0 lost. The processing-time `MERGE` scenario ended one batch short in the full run (the
final drain timed out under three concurrent proof runs; 0 duplicates) and exact when run
alone.

**The first verdict's choreographies.** 12 of 12 exact with the counts of the fold-1 table
above; its nine multi-query cases answer as before; its one-process repro ends `sink 1, 2, 3`.

**Hand mutants, fold 2.** Each applied in place, the `sink_offsets` pins of `repark-iceberg`
(100) and the microbatch pins of `repark-core` (131) run, the file restored.

| id | mutant | red pins |
|---|---|---|
| B (re-verify survivor) | the guard treats a commit with no base table as not the sink | 1: `the_guard_loads_the_table_when_the_commit_brings_no_base` |
| E (re-verify survivor) | the ambient scope is asked before the token in the extras | 1: `a_token_in_the_extras_decides_before_the_ambient_scope` |
| F2 (re-verify survivor, restated for the new walk) | check (a) walks the whole lineage and does not stop at the query's newest stamp | 4: two lineage pins, `rows_already_in_the_sink_and_a_seeded_restart_are_not_violations`, `a_stray_sink_write_beside_the_stamped_commit_ends_recovery_required`. A first wording of this mutant was equivalent to the code and survived; it was restated |
| N1 | check (a) never refuses | 3 driver pins |
| N2 | check (b) is skipped when the body raised (fold 1's A-5) | 2 driver pins |
| N3 | check (b) is skipped when the body made its stamped commit (fold 1's tolerance) | 2 driver pins |
| N4 | a failed stamped attempt releases its claim only on an unknown outcome | 2 claim pins |
| N5 | the planned-DML walk stops at `Analyze` and not at `Explain` | 1 driver pin |
| N6 | the mark ignores table properties | 1 mark pin, 1 driver pin |
| N7 | the epoch-0 baseline is applied to a query that has a stamp | 3 driver pins |
| N8 | the mark sees an unstamped snapshot only at the head | 1 mark pin, 1 driver pin |

**The full facade suite, per test, against base.** Head (this build, 16 workers): 16,604
cases, 15,958 passed, 645 skipped, 1 failed (`test_pg_acceptance`, which fails on base the same
way: its fixture file is absent). Base is the re-verify's recorded run of `40fc916f` (16,405
cases). Same-named tests whose status differs: 8, all of them not passing on base and passing
on head (six `test_deep_subquery_expression_1` errors, two fixture pins skipped). No same-named
test passes on base and fails on head. At `2877da20` the three `test_dfcore_1_exports` tests
did.

**Race pins.** `race_tests` 5 of 5 (3 pins each). **Gates** are in the hand-back.

**What ran on which build.** The kill set, the re-verify's scripts, the first verdict's
scenarios and the full facade suite ran on the fold-2 build before three later edits that
change no behaviour (two clippy findings and the lazy snapshot set in the mark). The four Rust
test gates, clippy, the panic ban, the seven MB-4 batteries and the export pin (209 passed) ran
on the final build.

## Fold 3 — the starting mark, the remedy text, the side-effect contract — 2026-10-10

Owner rulings of 2026-10-10 (Frontier D2 to D5) on fold 2's four questions. Main was merged
first (`ca5a062a..9b230aed`, the R-007 nested gate and the ENC-1 guard): one card conflicted
(main's closed copy kept, its duplicate map row dropped), and both sides had added the same
test helper to `microbatch/provider.rs`, which stopped the iceberg test crate compiling; one
copy was removed inside the merge commit.

### D2, the starting mark: can the offsets property carry it? Yes.

The halt check the ruling asked for, made before any code.

- **Readers of the offsets property.** One: `sink_offsets.rs::property_record`, called only by
  `read_resume_point`. Every consumer goes through that function: the driver's resume on both
  doors, the epoch check at each stamped arm, the append fence, the unknown-outcome walk and
  `stop`'s re-read of the durable record. No Python code and no other crate parses the value.
- **What a pending value would do unhandled.** `read_resume_point` would see a property with no
  stamped snapshot and answer `RecoveryRequired(StampedSnapshotExpired)`: a misread. So the
  reader is taught the mark: a value whose JSON carries `pending-epoch` is a mark, and
  `property_record` answers "no record" for it. `read_resume_point` then answers "nothing
  durable", which is the truth, on every door.
- **The value.** `{"format-version":1,"pending-epoch":0,"starting-head":<snapshot id or null>}`
  under the existing key `repark.cdc.offsets.<query-id>`. It is the offsets property's first
  value and nothing else is added: the sink still holds the summary stamp and the offsets
  property per query (NS-2). The first stamped commit sets the same key to the offsets record
  in the transaction that lands its rows, so the mark and the record never coexist.
- **A build older than this fold** that meets a mark reads it as a corrupt offsets record and
  refuses the query by name; it does not resume from it. That matters only for a downgrade
  between a query's first start and its first stamp.
- **`toTable`.** Writes no mark. A `toTable` query with the same name on a sink that holds a
  `foreachBatch` mark starts at epoch 0, and its stamped append replaces the mark.

No halt: the mark fits inside the offsets property.

### What was built

- **`Run::mark_the_start`** (`foreachBatch` door, only while the query has no stamp): read
  the mark and use its head as check (a)'s baseline; if there is none, take the head found now
  and write the mark.
- **Write-once.** The fork retries a property commit on a refreshed base and no table
  requirement can assert a property, so a driver that loaded the sink before a racing driver's
  first stamp would overwrite that stamp's offsets record with its mark. The first run of the
  fix turned both race pins red on exactly that. The mark commit goes through the per-commit
  fence with a rule of its own: refuse when the refreshed table already holds this query's
  offsets property or stamp; the driver then reloads and reads what is there.
- **A markless checkpoint** (a query that died at batch 0 before this fold: no stamp, no
  mark) is treated as fold 2 treated it, from the head found at this start, and gets its mark
  then. This is the brief's lean; it means rows a pre-fold run left behind are not findings.
- **D3.** The two unstamped reasons carry both remedies; `UnstampedSinkCommit` without an
  operation, which only the `toTable` door's own check raises, keeps its text byte for byte.
  Card MB-SINK-MAINTENANCE-PATH-1 is filed (a must before MB-5 closes).
- **D4.** The contract is in the `Notes:` of `DataStreamWriter.foreachBatch` and in the
  streaming package's map. The ruling's sentence "it matches Spark's `foreachBatch`" holds for
  the recipe (keying on the batch id is what Spark documents) and for the at-least-once half.
  It does not hold for the other half: Spark replays the whole callable after a failure (cell
  MB0-W6), so an effect after the sink write is at-least-once there and at-most-once here. The
  documents say which half differs; the hand-back raises it.
- **D5.** `task/wo/microbatch/mb4_lineage_timing.py` is the quiet-box measurement; it was not
  run here.

### What fold 3 overturns in fold 2

"Where the invariant stops", first bullet (epoch 0), and the pin
`at_epoch_zero_a_restart_replays_and_is_stopped_again`: a restart now refuses from the mark.
C-015's four questions are ruled.

### Fold-3 proof — 2026-10-10

**Red first.** The pins were committed before the fix (`ce2591d1`) and run on that tree: five
driver pins red of 136 and four public-door pins red of 57, each on the missing mark or the
missing remedies; the public-door kill-before-first-commit pin and the `toTable` pin were green
there, as pins of kept behaviour. The two iceberg unit pins of that commit were not run red:
the merge had left the iceberg test crate uncompilable (the duplicated helper) and the fix
followed before a second lock hold. Three unit pins that need the new functions came with the
fix.

**Found while building.**

- **The mark must be write-once.** The first run of the fix turned both race pins red: a
  racing driver's late mark overwrote the winner's offsets record. The fence rule closes it;
  mutant P4 then survived the race pins, so a deterministic pin was added
  (`the_mark_is_written_once_and_never_over_a_record`).
- **Main's ENC-1 gate** allows `impl Catalog for` in three files. Fold 1's body guard was a
  fourth. It became a rule of the one allowed per-commit wrapper; the gate's list is not
  edited.
- **The remedy text was first inaccurate.** It said the query "will not run past" the stray.
  The re-run of the verifier's update-conflict scenario showed the other case: a stray that
  lands below the batch's own stamped commit leaves that batch durable, the query ends
  `RecoveryRequired` at the end of the batch, and the restart resumes after it. The text now
  says a restart refuses while an unstamped snapshot sits above the newest stamped batch.
- **Load-counting tests moved** for the one extra commit at a `foreachBatch` query's first
  start (the list is in the core microbatch map).

**The scenario matrix, on the fold-3 build** (the same scripts as fold 2, from the same copy).

| set | scenarios | result |
|---|---|---|
| first verdict's choreographies | 12 | exact, the counts of the fold-1 table |
| first verdict's multi-query cases and repro | 9 + 1 | as before; the repro ends `sink 1, 2, 3` |
| re-verify's one-process repros | 2 | `explain`: refused before landing. `thread` (epoch 0): run 1 ends `RecoveryRequired`, run 2 now refuses from the mark |
| re-verify's special scenarios | 20 | 0 duplicates in every one. `conflict_mergeupd_0`, which fold 2 left at 132 duplicated hammer rows over 40 restarts, now lands one burst (8 unstamped snapshots) and every restart refuses. `conflict_update_1` now reaches all 10 epochs through the in-body retry after a live `CatalogCommitConflicts` (the retry is the stamped commit); each run still ends `RecoveryRequired` for the hammer thread's rows below the stamp |
| drop-and-recreate | 2 | `foreachBatch` ends `RecoveryRequired`; `toTable` writes on, unchanged |
| kill scenarios | 14 | 340 kills landed (286 `foreachBatch`, 54 `toTable`); 14 of 14 exact on the script's own check, 0 duplicates, 0 lost |
| door table | 498 | per placement 39 stamped, 118 no commit (157 after the body's append), 8 unstamped ending `RecoveryRequired`, 1 `DROP TABLE` ending `STREAM_FAILED`; none with the query running on |

558 scenarios, 340 kills, 0 silent duplicates. The loud duplicates fold 2 recorded at epoch 0
are gone.

**Hand mutants, fold 3.** Each applied in place, the microbatch and `sink_offsets` pins of
`repark-iceberg` and the microbatch pins of `repark-core` run, the file restored.

| id | mutant | red pins |
|---|---|---|
| P1 | `property_record` treats a record as a mark and a mark as a record | 5 + 51 in `repark-iceberg`, 63 in `repark-core` |
| P2 | the first start writes no mark | 12 driver pins |
| P3 | a mark that is found is ignored; the baseline is the head found now | 1 driver pin (`at_epoch_zero_the_restart_reads_the_mark_and_refuses_before_any_body`) |
| P4 | the write-once fence refuses only when the query is both marked and stamped | 1 (`the_mark_is_written_once_and_never_over_a_record`), after it first survived |
| P5 | the remedies are dropped from the text | 1 text pin |
| P6 | the mark records no head | 1 unit pin, 4 driver pins |

**Gates** are in the hand-back. The full `repark-core`, `repark-sql` and `repark-spark` lib
suites and the API freeze ran after the mark and before the two later iceberg-only edits (the
wrapper fold and the text); the full `repark-iceberg` suite, the core microbatch pins, the
binding's suite, clippy, the panic ban, the build and the batteries ran on the final tree.
The race pins ran 5 of 5 twice, the second time after the wrapper fold.

## Fold 4 — the third verify's three S1 and two S2 — 2026-10-10

The third verify of #1011 (head `dffca617`) returned FAIL: three S1, two S2, seven S3. The
orchestrator ruled on each the same day; this fold builds the rulings. Main (`ba71b593`, the
v1.5.4 release commit and the cluster-feature CI job) was merged first, with no conflict and
no overlap with the micro-batch files.

### Item 1: a stray under the batch's own stamp (S1)

**What failed.** Fold 2's restart check stopped at the query's newest stamp. A snapshot
between the two newest stamps was looked at only by the check after a body, in the process
that ran the body. A body that wrote its batch from a helper thread, made its stamped append
and then died before that check left the sink at source 32 rows, sink 36, and both runs
silent.

**The ruling, as built.** At every start, and before every body, `stray_on_main` walks the
main lineage above the newest stamp (as before) and then from the newest stamp down to the
previous stamp of the same query. Any unstamped snapshot in either stretch ends the query
`RecoveryRequired` before a body runs. This replaces fold 3's "loud once, then resumes": a
stray under a stamp refuses every start until the sink is resolved.

**The lower bound under a first stamp.** A first stamp has no previous stamp, and the mark
that held the starting head is gone once the first stamp replaces it in the offsets property.
Without a bound, a row that was in the sink before the query started cannot be told from a
stray. The first stamped commit therefore carries the head as one more summary key,
`repark.cdc.starting-head`. It is written by the claim: a claim made while the offsets
property still holds the mark copies the mark's head into the summary. NS-2 still counts two
durable items; the key is part of the stamp and lands in the same commit. A first stamp that
has no such key (a query begun through the `toTable` door, which writes no mark, or begun by
the fold-3 build) has no lower bound and the walk does not go under it.

**The cost per start (the brief's halt condition).** The walk reads snapshots already in the
table metadata the start has loaded. It makes no catalog call and reads no file. It is lazy:
it stops at the newest stamp when the sink is clean above it (one step for a healthy sink),
and under it at the previous stamp or the recorded head (one more step for a healthy sink).
Only a first stamp without a recorded head makes it read to the end of the lineage, in
memory. The walk is bounded; no halt.

**What it does not see.** A stray between two older stamps is not looked at: once a query has
stamped epoch N+1 over a clean stretch, the stretch under epoch N is not read again. The
driver cannot get there by itself, because it refuses before epoch N+1 can be stamped; a
`toTable` run under the same name (that door holds no lineage rule) could.

### Items 1 and 2: the remedies (S1)

**What failed.** Fold 3 printed one sentence for every case: roll the sink back to its newest
stamped snapshot, or start the query under a new name. The first half does not remove a stray
that sits under that stamp. The second half, followed as printed, starts a new query at the
beginning of the source: 16 of 32 rows duplicated, `awaitTermination` true, three runs.

**As built.** The remedy is data (`StrayRemedy`), computed from what the walk found, and its
text prints only what the driver can prove.

| where the stray sits | discard its rows | keep its rows |
|---|---|---|
| above the newest stamp | roll back to the newest stamped snapshot (named), start again under the same name | new name with `repark.cdc.start-after-snapshot-id` = the source position of the newest stamp |
| above, nothing stamped yet | roll back to the head the query started on (named), start again | new name, no option (nothing was delivered) |
| above, sink started empty | not offered: there is no snapshot to roll back to | new name, no option |
| under the newest stamp | roll back to the previous stamped snapshot (named), then a new name with the previous stamp's source position; the batch is delivered again once | new name with the newest stamp's source position |
| under the first stamp | roll back to the head the query started on, then a new name with no option | new name with the first stamp's source position |
| under the first stamp, sink started empty | not offered | new name with the first stamp's source position |
| another query's stamps share the stretch | not offered (they would leave with the rollback) | as above |
| the stamp's batch ends inside a source snapshot | under a stamp: not offered | not offered: no start-after position continues exactly |

The old name stays refused after a rollback under its newest stamp: the offsets property is
ahead of the lineage, which MB-2's resume check has always refused. That is why the under-a-
stamp recipe names a new query name. The source position is taken from the stamp's own record
and offered only when `WindowPlanner::ends_snapshot` confirms the record consumed every added
file of that snapshot; a batch capped inside a snapshot would lose the rest of it under a
start-after option.

**Pinned by following the text.** `test_mb_4_streaming_remedies.py` parses the snapshot id and
the option value out of the refusal and does what it says, then counts the sink: ten pins,
one per row of the table that prints a recipe, plus the capped-batch case that prints no new
name. Scope: "every refusal text that prints one" is read as every text of the stray refusal.
The other refusals that name a new query (`NonAppendSnapshot`, `StampNotInLineage`,
`SnapshotNotInLineage`, `GenerationMismatch` and the like) are MB-1 to MB-3 texts on main and
are a question in the hand-back.

### Item 3: session settings (S1)

**Where it is reachable.** The defect is on main, in `WeakSessionState::snapshot`
(`time_travel/microbatch_source.rs`, MB-1 to MB-3). No door on main reaches it: main has the
driver and no public streaming surface, so only a Rust caller of `StreamingQueryManager`
would see it. This PR's facade is the first public door.

**The cause.** The helper cloned the session's state behind its lock. DataFusion's
`SessionContext::state()` does one more thing, `mark_start_execution`, which copies the
current option set into the execution properties and stamps the start time. A plan's
functions read their options from those properties at run time. Without the call the
properties were the ones captured when the session was built: the session zone, ANSI mode
and every other option set afterwards did not reach a streaming plan, and
`current_timestamp()` had no start time to fold to.

**The fix** is that one call, in the driver's own helper. Nothing changes in how sessions
are built outside the driver; no halt.

| setting | before the fix | after |
|---|---|---|
| `spark.sql.session.timeZone` (builder or `conf.set`) | stale: UTC-computed values in both doors and in the frame a body collects | bound; equal to the batch write, read from the Parquet files, two zones |
| `spark.sql.ansi.enabled` set after the session started | stale: an `INT` overflow raised where the batch write wrapped | bound |
| `current_timestamp()` | `Internal error: invoke should not be called on a simplified now()` | the batch's own time |
| `spark.sql.caseSensitive` | bound (read at plan time from the live state) | bound; measured green before and after |
| the write options a sink commit reads (snapshot properties, codec, the scope token) | bound (read from the live state by `batch_session`) | bound |

Every option that lives in the session's option set is bound at once, because the call
copies the whole set.

### Item 4: the two gates (S2)

- **Nested `timestamp_ns`.** The `toTable` door stages files below the Spark router, where
  R-007's gate lives. `Run::refuse_unwritable_sink` asks the same function
  (`refuse_nested_ns_supply`, the `Unreadable` question a DataFrame write asks) at the start
  and before every batch, with the batch doors' label. The query ends with the batch doors'
  sentence and no file is written.
- **Reserved summary keys.** `summary_with_extras` is where every snapshot summary takes its
  caller extras; it refuses a `repark.cdc.*` key by name. The stamp no longer passes through
  it as an extra: `SiteStamp::summary` adds the claimed stamp after the caller's summary is
  built. Measured on seven statements and five writer shapes, each refused with the table
  unchanged. A pin from MB-2 that an unscoped commit keeps a caller's `repark.cdc.note` is
  reversed by the ruling.

### Item 5: CI (S2)

`docs/examples/session/connect_only_and_declared.py` expected `session.readStream` and
`session.streams` to refuse. The two names left it; `streaming_entry_points.py` covers them
(a fresh `DataStreamReader` per read, the `StreamingQueryManager` with an empty `active`).
`scripts/check_example_coverage.py --require-execute` is green on the built module, and so is
`make check-example-coverage`.

### Item 6: the seven S3

| the verify's S3 | disposition |
|---|---|
| A batch write that sets `repark.cdc.query-id` passes the exclusive-sink check | **Fixed here** (item 4, C-036). Another streaming query's stamps still pass, by design. The maintenance card now says no maintenance path exists against a live sink. |
| The contract sentence is wider than the check | **Fixed here, in the text.** The docstring, the streaming map and the registry row say what is checked (snapshots on the main branch) and what is not (a property, a schema change, a branch, a tag, a staged or branch write, statistics, between batches). `UnstampedSinkChange` no longer promises a refused restart or a rollback. The check itself is unchanged: **accepted**, the ruling's invariant is on rows. |
| Remedy 1 cannot be followed on a sink that started empty | **Fixed here**: the text says there is no snapshot to roll back to and offers only what works (two pins on an empty sink). |
| On a keyed sink the `foreachBatch` door writes its mark before it refuses | **Fixed here** (C-037): refused before the mark, with the `toTable` door's text, no body run. |
| A pre-fold build that meets a mark refuses with a text that names neither | **Accepted**: it fails closed, the build that wrote the mark runs on exactly, and no such build is released. Recorded on card STREAM-SURFACE-RESIDUE-1. |
| Still open from the earlier verdicts (three trigger strings; 39 of 120 statements in a body answer differently; six `readStream` shorthands are a bare `AttributeError`; `toTable` writes into a sink dropped and re-created) | **Carded**, STREAM-SURFACE-RESIDUE-1: the first and the last were there since fold 2, the two others are added in this fold. |
| Two of seven fresh mutants survive | **Fixed here** (C-039): `only_epoch_zero_can_be_pending` for N2 and `a_stamp_alone_refuses_the_mark_even_without_the_offsets_property` for N4. Both mutants are red below. |

### What fold 4 overturns

- Fold 3, "the remedy text was first inaccurate": the corrected sentence was still one
  sentence for every case. It is gone.
- Fold 3's ending for a stray under the batch's own stamp (loud once, the restart resumes):
  every start refuses.
- C-030 (the text names "both remedies") is restated by C-033.
- Fold 2, "Where the invariant stops": the stretch under the newest stamp is now inside it.

### Fold-4 proof — 2026-10-10

**Red first.** The public-door pins were committed before the fix (`af07ebef`) and run on the
fold-3 build: `test_mb_4_streaming_remedies.py` 10 red of 10;
`test_mb_4_streaming_session_settings.py` 6 red of 8 (four zone cases, ANSI off,
`current_timestamp()`), the two case-sensitivity cases green as pins of what was already
bound; `test_mb_4_streaming_sink_gates.py` 16 red of 16; the kill pin in
`test_mb_4_streaming_foreach_eo.py` 2 red of 2 (the restart exited 0 with the duplicated rows
in the sink). The Rust pins need the new types and came with the fix; each is shown red by a
mutant below instead.

**Found while building.**

- **The first stamp has to carry the starting head** (above). The first version of the walk
  had no bound under a first stamp and would have refused every sink that held rows before
  the query started; mutant Q3 is that version, red on four pins.
- **A rollback under the newest stamp leaves the old name refused** (the offsets property is
  ahead of the lineage). The recipe therefore names a new query name; a pin holds that the
  old name answers `summary epoch 0 disagrees with property epoch 1` after the rollback.
- **A start-after position is exact only on a snapshot boundary.** A batch capped inside a
  source snapshot records a file position; `repark.cdc.start-after-snapshot-id` would skip
  the rest of that snapshot. The driver checks the boundary and prints no new name without
  it (one public-door pin, one source pin, mutant Q6).
- **`current_timestamp()` was not usable in a streaming plan at all.** Found while measuring
  which settings were stale; closed by the same one call.
- **Writer options arrive lower-cased**, so a reserved key written in upper case is refused
  under its lower-case name.
- **The walk was made lazy** after the first green run: it had collected the whole lineage on
  every batch. It now stops at the previous stamp.

**The scenario matrix, on the fold-4 build** (the fold-3 scripts from the same copy, then the
third verify's scripts copied into the lane's scratch with their paths changed; the copy of
`foreign.py` follows the remedy the refusal prints instead of a fixed recipe).

| set | scenarios | result |
|---|---|---|
| first verdict's choreographies | 12 | exact, the counts of the fold-1 table; run twice (the first pass overlapped a reinstall of the module), identical |
| first verdict's multi-query cases and repro | 9 + 1 | as before; the repro ends `sink 1, 2, 3`. One line answers `name a is already active` in three runs of four and `SINK_BUSY` in one: the script leaves a `toTable` query running and then drops and re-creates its sink, and which refusal the next start meets depends on whether that query's 100 ms tick hit the gap. Both refusals are fold-1 texts |
| re-verify's one-process repros | 2 | `explain`: refused before landing. `thread`: `RecoveryRequired`, the text now the empty-start one |
| re-verify's special scenarios | 20 | 0 duplicates in every one. `conflict_update_1` no longer reaches its ten epochs: its hammer writes land under the first stamp and every restart refuses there |
| drop-and-recreate | 2 | `foreachBatch` ends `RecoveryRequired`; `toTable` writes on, unchanged (carded) |
| kill scenarios | 14 | 340 kills landed (286 `foreachBatch`, 54 `toTable`); 14 of 14 exact, 0 duplicates, 0 lost |
| door table | 498 | 78 stamped, 393 no commit, 24 unstamped ending `RecoveryRequired`, 3 `DROP TABLE` ending `STREAM_FAILED`; every case classified as in fold 3 |
| third verify's repros and fold-3 scenarios (`special3.py`) | 10 | `q2_once_kill_batchrows` (the S1): the restart ends `RecoveryRequired` naming the snapshot under the stamp; sink 16 rows for 3 stamped epochs, the 4 duplicated rows the named ones, and the query does not run on. `q2_once_kill`: the same with the foreign row. `q2_every`: twelve runs, twelve refusals, one stamped epoch. `remedyB_start_after`: exact. `empty_sink_stray_after_mark`: the text offers no rollback; the new name ends exact with the stray kept. The four door-switch and dead-mark scenarios: exact at each step (52 of 52 rows after four switches); the reader inventory unchanged |
| mark-window kills | 40 | 104 kills; 40 of 40 first kills landed on the mark; all 40 end exact, the seeded sinks included (the seed is under the recorded head and is not reported) |
| foreign-commit matrix | 83 | 25 runs refused and 58 accepted, case for case as in the verify. Refused with a rollback printed (21): the printed snapshot is the one the verifier computed, and following it is exact in all 21. Refused and kept under a new name as printed (4): 0 duplicated, 0 lost, the stray's rows kept (fold 3: 16 duplicated in each). The two forged-stamp kinds are now refused at the write, in every placement |
| session zone | 4 | both doors and the body's frame equal the batch write in every run |
| nested `timestamp_ns` | 9 + 2 | `toTable` refuses with the batch doors' text, no file; the top-level column is unchanged |
| keyed sink | 17 | every `foreachBatch` trial on a keyed sink: the ENC-1 text, no file written |
| in-body changes | 5 | each ends `RecoveryRequired` once and the restart runs on, as the text now says |
| audit timing | 2 | both placements loud |

730 scenarios, 444 kills in the two kill matrices (the foreign and special sets add their own
set-up kills), 0 silent duplicates, 0 silent losses, 0 duplicates that are loud once and then
accepted. The three verifier repros: the stray under a stamp is loud on every start; the
second remedy is exact as printed; the zone values are equal.

**Hand mutants, fold 4.** Each applied in place, the microbatch and `sink_offsets` pins of
`repark-iceberg` and the microbatch pins of `repark-core` run, the file restored (`git status`
after the run lists no source file).

| id | mutant | red pins |
|---|---|---|
| Q1 | the walk does not go under the newest stamp | 3 lineage pins, 1 driver pin |
| Q2 | the first stamp does not record the starting head | 3 (`the_first_stamp_records_the_starting_head_and_bounds_the_walk_under_it` and two more) |
| Q3 | the walk under a first stamp does not stop at the recorded head | 1 lineage pin, 3 driver pins (rows already in the sink are reported) |
| Q4 | a stray under a stamp is told to start again under the same name | 1 (`the_remedy_offers_only_what_the_walk_can_prove`) |
| Q5 | a new name is printed without its start-after position | 1 lineage pin, 1 driver pin |
| Q6 | an offset inside a snapshot is taken as a start-after position | 1 (`only_an_offset_that_ends_its_snapshot_is_a_start_after_position`) |
| Q7 | the batch state does not mark its start | 1 (`a_state_snapshot_carries_the_settings_and_the_start_time_of_its_moment`) |
| Q8 | a reserved summary key is not refused | 3 |
| Q9 | a keyed sink is not refused before the mark | 1 |
| Q10 | the start does not walk the sink, only a batch does | 1 (the restart with no batch to run) |
| Q11 | the audit keeps its own reason and prints no remedy | 4 driver pins |
| Q12 | another query's stamps in the stretch do not withhold the rollback | 1 |
| N2 | any pending epoch is a mark (the third verify's survivor) | 1 (`only_epoch_zero_can_be_pending`) |
| N4 | a stamp alone does not refuse the mark (the third verify's survivor) | 1 (`a_stamp_alone_refuses_the_mark_even_without_the_offsets_property`) |

The nested-gate rule on the `toTable` door has no Rust pin and no mutant: the driver's test
fixture has no format-version 3 sink. Its evidence is the public-door pin, red before the fix.

**Gates** are in the hand-back. Clippy, the panic ban, the four lib suites and the build ran
on the final source tree; the batteries, the matrix and the mutants ran on that build.

## Fold 5 — the fourth verify's S1 — 2026-10-10

The fourth verify (release build of `58a76557`) closed all five findings of the third and
returned FAIL on one S1 and six S3. Owner ruling of the same day: the summary key
`repark.cdc.starting-head` is accepted on one condition, that the walk's arm where the first
stamp has been expired is measured and is never silent. The leans of fold 4's Q1, Q3 and Q4
stand.

### The S1

A stray under a stamp, hidden by a process death before the driver's check, was accepted
silently by the next `foreachBatch` start in two cases: (a) an expiry had removed the
previous stamp (source 32, sink 36); (b) one `toTable` run under the same name had stamped
over it (source 40, sink 44).

### Item A: the walk with no lower bound

**What failed.** `stray_on_main` ended "nothing found" when the stretch under the newest
stamp had neither a previous stamp nor a recorded head, without looking at what it had
collected. Fold 4 wrote that arm for a first stamp with no recorded head; an expiry produces
the same shape for any stamp.

**As built.** The walk stops at the first stamped snapshot of this query, the recorded head,
or the end of the retained lineage, whichever comes first, and reports an unstamped snapshot
it met on the way. What makes this sound without a bound: everything between a stamp of
epoch 1 or later and its predecessor was committed after the query started, so an unstamped
snapshot there is a stray whether or not the predecessor is still in the table.

| what the stretch under the newest stamp ends on | reported | rollback printed |
|---|---|---|
| the previous stamp | yes | to that stamp (fold 4) |
| the recorded head, still on the lineage | yes | to that head (fold 4) |
| the end of the lineage; the newest stamp is of epoch 1 or later | yes | no: "the stamped batch before it is no longer in the table" |
| the end of the lineage; a recorded head that is not on it | yes | no: "the snapshot the query started on is no longer on the sink's main branch" (an expiry removed it, or a rollback took the branch under it; in the second case the snapshot is still in the table) |
| the end of the lineage; a `foreachBatch` first stamp that records no head (a build before fold 4) | yes | no: "the query's first stamp does not record the head it started on" |
| the end of the lineage; a `toTable` first stamp that records no head | no | (rows under it were in the sink before the query; that door takes other writers) |

The same check on the head applies above the newest stamp while nothing is stamped: a mark
whose head is not on the lineage prints no rollback. The new name with its start-after
position is still printed in every row, where the stamp ends on a source snapshot boundary.

**Why reporting with no floor is not a false positive (owner ruling D1, measured).** The
sentence to prove or refute: an expiry removes an older-first suffix of the main branch, so
when the starting head is gone everything older than it is gone too, and every surviving
unstamped snapshot under the newest stamp is post-start. It holds for what the walk reads.
The walk follows parent pointers from the newest stamp, so "surviving" has to be read as
"still reachable from the stamp". Measured on the release build (`tmp` scratch, 26 cells):

| expiry shape | healthy sink | a stray hidden under a stamp first |
|---|---|---|
| `retain_last` 1 and 2, `older_than`, with a TAG on a snapshot older than the starting head | 5 cells: the tagged snapshot stays in the table (2 or 3 snapshots, 1 or 2 on the chain) and is not reported; both doors run on exactly | 4 cells: refused; the rollback is printed only where the head is still on the chain, and followed it is exact |
| the same with a BRANCH on that old snapshot | 4 cells: the same | 3 cells: the same |
| `older_than` alone (keeps one snapshot) over a hidden stray | | 2 cells: silent, 4 rows duplicated. This is the expire-to-one residue, not a false positive |
| a rollback under the stamps, then an expiry | 4 cells: refused by MB-2's resume check (the summary's epoch disagrees with the property's) before the walk runs | |
| a rollback under a pending mark (no stamp yet), with and without an expiry | 2 cells with one seed (the rollback is a no-op): run on exactly | 2 cells with an older seed: refused, "the snapshot the query started on is no longer on the sink's main branch", no rollback printed; the new name the text offers ends exact |

No case refutes it. One neighbour is recorded: with no stamp yet, a rollback of the sink
under the mark's head makes the first start refuse and name a snapshot that predates the
query. That is the rule of the check above the stamp (the head must be the recorded
starting head), not the no-floor report, and it is loud.

**A healthy sink is not refused.** With nothing unstamped under the newest stamp the walk
answers "clean" with or without a bound. Pinned through the public doors with one, two and
three snapshots retained, on both doors, green before and after the change.

**Halt condition, checked.** No catalog call and no file read was added. The walk is one
pass over snapshots in the metadata the start has loaded. On a healthy sink it takes one
step past the newest stamp. It reads to the end of the retained lineage only when no bound
is retained, which after an expiry is a short lineage.

### Item B: `toTable` under a name that has run through `foreachBatch`

`Run::refuse_moved_sink` now runs on both doors. At a `toTable` start the driver asks
`carried_by_foreach` once: the name holds a starting mark, or any stamp of the name on the
main branch is a `foreachBatch` stamp. Such a name is walked at the start and before every
batch with the `foreachBatch` rules, for as long as that stamp is retained, also after later
clean `toTable` batches (owner ruling D2: a name that ever carried a `foreachBatch` stamp or
mark). A `foreachBatch` stamp is told from a `toTable` one by the two Spark keys the
`toTable` door adds to its summary; no key was added. A name with only `toTable` stamps, or
with neither stamp nor mark, is not walked: a foreign commit above or between its stamps
passes on that door as before (pinned).

"Ever" is as far as the table records it: once every `foreachBatch` stamp of the name is
expired and no mark is left, nothing on the sink says the name ran through that door.

### Item D: the S3

| the verify's S3 | disposition |
|---|---|
| The printed discard names an expired starting head | **Fixed here** (item A): a head is printed only when the walk reached it. |
| `current_timestamp()` is not fixed for a micro-batch on the `foreachBatch` door | **Fixed here** (C-043). Spark 4.1.2 measured: two `collect()` calls 1.3 s apart and the write inside one batch give one value. Here each action on the frame folded its own time. The frame is now optimized once before it goes to the body, with the state the batch was planned under (`fixed_at_the_batch_start`, one call), so the time is a constant in the plan. |
| The offsets property is writable by `ALTER TABLE` | **Carded** on STREAM-SURFACE-RESIDUE-1 with the repro. |
| A pre-fold first stamp has no recorded head | **Closed by item A** and pinned (`a_stray_under_the_newest_stamp_is_reported_when_no_lower_bound_is_left`, the `HEAD_UNRECORDED` case). A healthy checkpoint of such a build whose only stamp is its first, over a sink that held rows before, is refused too: the driver cannot tell the two apart. No such build is released. |
| The red CI job `Rust test (cluster feature)` | Not this lane's (a timing flake in `cluster_two_executors.rs`, fixed on main in its own PR). |
| Earlier residue | Carded, unchanged. |

### Item C and owner ruling D3: the expected-parent sketch (written before any code for it)

**The residue.** A stray under the newest stamp, a process death before the driver's check,
then an expiry that keeps only the newest snapshot. Measured on the build before the
prevention rule below (`0ab8266f`): the restart runs on, source 32 rows, sink 36 batch rows,
no signal. It is silent. After the expiry the
table holds one snapshot, the stamp. One trace survives, measured: the stamp's
`parent-snapshot-id` still names the stray's snapshot id, which is no longer in the table.
Nothing in the table says what the parent should have been.

**The candidate (D3).** Every stamped commit records the snapshot id the batch expected as
its parent. On a first stamp that is the starting head (`none` for an empty sink).

**Condition 1: is the expected parent known on every commit arm? Yes, after one signature
change.** Today it is not: `BatchScope::enter` takes the sink's uuid and the stamp, and the
claim records `base`, the head when the body's write begins. The driver holds the right
value: `Run::enter_scope` loads the sink when the batch begins. Passed into the scope, it
reaches every arm through the claim.

| arm | where the value comes from |
|---|---|
| first stamp | the scope; equal to the starting head, because the check before the body has just held the head to it |
| retry after a refresh (the transaction's own retry) | unchanged: the summary is built once from the claim, before the first attempt |
| retry by the body after a failed stamped write | the same scope entry; the released claim is taken again with the same value |
| unknown-outcome recovery | writes nothing; it looks for the stamp that already carries the key |
| `toTable` | the same scope, entered by the same `Run::enter_scope` |
| stamp-only | claimed with the batch's token, so the same scope entry |

**Condition 2: is there a legal interleaving that mismatches without a stray? Yes, one. The
condition fails.** Another streaming query's stamped batch that lands on the same sink
between this batch's start and its stamped commit. The lineage rule has taken another
query's stamps as legal since fold 2 ("a snapshot stamped by a different streaming query is
not a finding"), and the check after a body passes them. Inside one process the scope is
exclusive per sink (`SINK_BUSY`), so it takes a second process on the same sink; nothing
refuses that today, and the commit is retried on the refreshed table as any optimistic
commit is. Measured with two queries taking turns on one sink: a stamp's parent is the other
query's stamp, not this query's previous stamp, so the expectation can only be "the head the
driver read when the batch began", and the other query's commit inside the batch then makes
actual differ from expected with no stray anywhere. The two-process race itself is not
measured: a memory catalog is per process.
Telling that case from a stray needs the snapshots between the two ids, which is the walk,
and after an expiry they are gone: the driver would have to refuse a legal state or accept
an illegal one. What does not mismatch, measured: a property-only commit, a schema change, a
branch and a tag add no snapshot to the main branch and leave the parent alone.

The condition would hold under one more rule that is the owner's to make, not this lane's:
that a second process's stamped batch inside this batch's window is not legal (the in-process
rule already says one at a time per sink), enforced by refusing a stamped commit whose
parent is not the snapshot its claim verified. That turns today's transparent retry into a
failed write for two processes sharing a sink.

**Outcome under D3.** Condition 2 fails, so expected-parent is not built. Fold 5 ships D1 and
D2, the registry row and the streaming map state the expire-to-one residue as a limit, and
card MB-SINK-MAINTENANCE-PATH-1 gains the MUST: expiry on a sink with a live `foreachBatch`
query retains the newest two stamps and everything between.

**For the record, what the key would and would not do.** It would replace
`repark.cdc.starting-head` (the first stamp's expected parent is that head) and make the
healthy check a comparison of two ids inside the newest stamp. It would not replace the
walk where the ids differ, cannot name or roll back a stray whose snapshot is expired, and
covers no stamp written before it exists. Two durable items either way; one more key on
every stamp.

### Prevention: the arm that let a stray land under a batch's own stamp

**It is not a re-base that skips the fence.** Every attempt of a stamped commit, the first
and each retry on a refreshed table, goes through `AppendFence::update_table`, which runs
`refuse_stamp` against the table the commit is about to be applied to. The fence runs every
time. Two things in it and in the claim let the stray through:

1. **The claim takes the head it finds as its base.** `BatchScope::claim_checked` sets
   `base` to the current snapshot of the table the body's write has just loaded. In the third
   verify's repro the helper thread's append has landed by then, so the base is already the
   stray (measured: the stamp of epoch 2 has the stray as its parent, and the stray has the
   stamp of epoch 1 as its parent). The claim never compares that head with the head the
   driver read when the batch began.
2. **The fence looks only for this query's own stamps.** `breach` walks from the refreshed
   head down to the claim's base and refuses a snapshot stamped by the same query or a base
   that left the main branch. An unstamped snapshot in that stretch passes. So a stray that
   lands between the claim and the commit also ends under the stamp, through an ordinary
   retry, with the fence run and satisfied.

**The fix, built in this fold with a red pin first.** The driver enters the scope with
`BatchScope::enter_on`, which records the head it read when the batch began. On the `foreachBatch` door the claim refuses when an unstamped
snapshot sits between the head it finds and that head, and the fence refuses when one sits
between the refreshed head and the claim's base. Another query's stamp in either stretch
still passes. The body's sink write then fails before it lands, the batch's check finds the
stray above the newest stamp, and every start refuses there, where no expiry can remove it:
an expiry keeps the head. A stamp is no longer committed over a stray that landed after the
batch began, so the state the residue needs cannot be produced by this build on a catalog
that checks a commit's requirements. The walk under the newest stamp stays for stamps
already on disk. The `toTable` door is not changed: it takes other writers.

### Fold-5 proof — 2026-10-10

**Red first.** Three sets of public-door pins were committed before their fixes and run on
the build they were written against. `9becf2d1`, on the fold-4 release build: the expiry
route, the two `toTable` routes, the stray above a `foreachBatch` stamp on the `toTable`
door and the expired-head text red (5), the healthy-expiry controls green (8). The
`current_timestamp()` pin, written with its fix, was run red on that build first (the stored
row 0.8 s later than the collected one). `01e9e07b`, on the build of `0ab8266f`: the
prevention pin red on both routes (the stamped write landed over the stray). The Rust pins
came with the fixes; each is shown red by a mutant below.

**Found while building.**

- **The first fix of this fold (`0ab8266f`) made the walk report without a lower bound and
  gave the `toTable` door an entry keyed on the name's newest stamp.** The owner's rulings of
  the same day then widened the entry to a name that ever carried a `foreachBatch` stamp or
  mark, and asked for the arm to be named and closed. The prevention rule followed.
- **The prevention rule makes the state under a stamp unreachable through the doors.** Every
  public-door pin that set up a stray under a stamp (fold 4's kill pin, its five thread
  routes, four of the ten remedy pins, the first fold-5 pins) now sets up a refused stamped
  write and a stray above the stamp, and asserts that. The texts for a stray under a stamp
  remain for stamps already on disk and are pinned at the walk and the remedy.
- **An MB-2 pin said a foreign writer inside a scope is re-based over and the batch commit
  lands.** That is the arm. The pin is kept for the `toTable` door and reversed for the
  `foreachBatch` door (`a_foreign_writer_inside_a_foreach_scope_fails_the_batch_commit_at_the_fence`).
- **A base that left the main branch is still reported as that**, before the stray check: on
  such a table the stretch to walk has no floor.
- **"No longer in the table" was false after a rollback under a mark**; the text says "no
  longer on the sink's main branch".
- **One clippy finding** (`Option<Option<_>>` on the first walk entry); the entry became an
  enum and then went with the ever-carried rule.

**The scenario matrix, on a RELEASE build of the final source** (`maturin develop --release`;
the fold-3, fold-4 and fourth-verify scripts from the lane's scratch copies).

| set | scenarios | result |
|---|---|---|
| the fold-4 matrix, every set | 730 | as in fold 4, case for case where the state is still reachable: 12 choreographies exact; 14 kill scenarios with 339 kills landed (286 `foreachBatch`, 53 `toTable`), 14 of 14 exact; 40 mark-window scenarios with 104 kills, 40 of 40 exact; the foreign-commit matrix 25 refused and 58 accepted as before, 21 printed rollbacks exact, 4 printed new names 0 duplicated 0 lost, no body run in a refused restart; the door table 498 cases classified exactly as in fold 3. Changed by the prevention rule: the third verify's repro `q2_once_kill_batchrows` now leaves the batch's rows in the sink once (12 rows for 2 stamped epochs and the stray, 0 duplicated) and both restarts refuse |
| fourth verify's walk scenarios | 14 | every one loud or exact, 0 duplicated rows in all 14. `expire_retain2` and `expire_retain3`: refused, the rollback printed. `expire_retain1` and `first_expire1`, the two that were the silent residue: refused (`stamped snapshot expired`; "the snapshot the query started on is no longer on the sink's main branch"). `door_switch`, `door_switch_first`: the `toTable` start refuses and so does the `foreachBatch` start after it. `totable_foreign_between`: refused on the `foreachBatch` door under the newest `toTable` stamp, the printed discard followed, exact |
| fourth verify's remedy jobs | 64 | 50 print a remedy and following it literally is exact by the script's own verdict (28 rollback under the same name, 14 new name with the position, 8 new name); 14 print none and say why (10 batches that end inside a source snapshot, 4 empty starts) |
| the owner's condition on the first stamp (`reverify4b`), measured by the lane | 77 | the first stamp removed by `retain_last` (with one, two and many later stamps), by `older_than`, after a tag held it and was dropped; the starting head expired with the first stamp kept; the head `none`; a rollback and a `set_current_snapshot` across the first stamp; a compaction; each with no stray, a stray written above the newest stamp, and the helper-thread body (whose stamped write is now refused), followed by an expiry in some cells, on both doors. 18 healthy cells run on exactly through the restart and the next batch. 44 refused with a rollback printed, followed, exact. 9 refused with `stamped snapshot expired` (a stray above, then an expiry to one). 6 refused by MB-2 (`stamp not in lineage`, the rollbacks across the stamp). 0 silent |
| owner ruling D1, on the final build | 26 | 0 silent; 11 healthy cells exact with a tag or a branch on a snapshot older than the starting head. The 26 cells of the table under item A were run on the build of `0ab8266f`, where a stray can still be hidden under a stamp |
| session settings (`sess.py`) | 4 | zone, ANSI and case sensitivity equal on both doors, changed between epochs and between runs; `current_timestamp()` on the `foreachBatch` door: the stored row equals the collected one |
| parent pointers (`c_measure.py`) | 3 | the measurements quoted in the sketch |

918 scenarios, 443 kills in the two kill matrices, 0 silent duplicates, 0 silent losses, 0
duplicates that are loud once and then accepted. The two routes of the S1: (a) an expiry
after the kill refuses every start, with one, two or three snapshots retained; (b) a
`toTable` start under the same name refuses.

**Hand mutants, fold 5.** Each applied in place, the `sink_offsets` pins of `repark-iceberg`
and the microbatch pins of `repark-core` run, the file restored.

| id | mutant | red pins |
|---|---|---|
| R1 | a stray under the newest stamp is not reported once the previous stamp is gone | 1 |
| R2 | a starting head that left the lineage is still printed as the rollback target | 1 |
| R3 | a `foreachBatch` first stamp with no recorded head is not walked under | 2 |
| R4 | a `toTable`-begun first stamp is walked under like a `foreachBatch` one | 1 |
| R5 | the `toTable` door never walks | 2 driver pins |
| R6 | a `foreachBatch` stamp does not make a name walked on the `toTable` door | 1 lineage pin, 3 driver pins |
| R7 | a mark does not make a name walked on the `toTable` door | 1 |
| R8 | a `toTable` stamp makes a name walked on the `toTable` door | 2 lineage pins, 1 driver pin |
| R9 | the walk under the newest stamp stops at the first snapshot it meets | 3 |
| P1 | the claim does not look for a stray since the batch began | 1 scope pin, 1 driver pin |
| P2 | the fence does not look for a stray since the claim | 2 |
| P3 | the `toTable` door's stamped commit is refused over a foreign commit too | 6 (the MB-2 re-base pins) |
| P4 | another query's stamp since the batch began counts as a stray | 1 |
| P5 | the driver enters the scope without the head it read | 1 driver pin |

`fixed_at_the_batch_start` has no Rust pin and no mutant: the difference shows only when an
action re-homes the frame onto the live session, which the Python door does. Its evidence is
the public-door pin, red before the fix.

**Gates** are in the hand-back; all ran on the final source, the batteries and the matrix on
its release build.

## PROPOSITION LEDGER — MB-4-FOREACH-EO — 2026-10-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A body's append to the declared sink lands as one snapshot that carries the epoch stamp and the offsets property, and the driver writes no trailing stamp-only snapshot for that epoch. | Driver-level pin plus the public-door pin. | **PROVEN** | `foreach_tests.rs::the_foreach_batch_door_stamps_the_body_s_own_commit` (`stamp 0 with 3 rows, stamp 1 with 2 rows`, no Spark key, no token key, no token in the shared session); `sink_offsets_body_scope_tests.rs::a_commit_inside_the_body_scope_claims_without_a_token_in_its_extras`; public door `test_body_append_carries_the_epoch_stamp_in_its_own_snapshot` and `test_body_insert_into_carries_the_epoch_stamp` (`append / 0 / 2`, `append / 1 / 1`). Mutants M1, M8. pins: mb-4-foreach-eo/C-001 |
| C-002 | A body that writes the sink and then raises leaves the epoch durable; the restart resumes at the next epoch and the sink holds each source row once. | Driver-level pin, the public-door pin, the verify's `f_writeraise`. | **PROVEN** | `foreach_tests.rs::a_body_that_fails_after_its_sink_write_leaves_the_epoch_durable` (`BatchFailed`, durable epoch 0, the restart sees only epoch 1, ids `1..5` once); `test_write_then_raise_restarts_without_a_duplicate`; `f_writeraise` 24 of 24, 0 duplicates. Mutant M3. pins: mb-4-foreach-eo/C-002 |
| C-003 | A process that dies after the body's sink write, at a random moment, or at a sink commit, restarts with each source row once in the sink. | Subprocess pins on the public door; the verify's `f_exitafterwrite`, `f_kill`, `f_killcommit`. | **PROVEN** | `test_exit_after_the_sink_write_restarts_without_a_duplicate` (24 rows), `test_kills_at_sink_commits_restart_without_a_duplicate` (5 kills, 48 rows), `test_random_kills_restart_without_a_duplicate` (6 kills, 64 rows); the three verify scenarios exact (table above). pins: mb-4-foreach-eo/C-003 |
| C-004 | A second stamped write to the sink in one epoch refuses `SinkCommittedTwice`, the query ends with that typed error, and the sink holds the first write once. | Driver-level pin plus the public-door pin. | **PROVEN** | `exactly_once_tests.rs::a_second_sink_write_in_one_epoch_refuses_and_the_first_stays_once` (the typed error, durable epoch 0, one snapshot, ids `1, 2, 3`); `test_second_append_in_one_epoch_refuses_mbe13` (`STREAM_FAILED`, `[REPARK_MICROBATCH.SINK_COMMITTED_TWICE]`); `test_swallowed_second_append_leaves_one_commit_per_epoch`. Mutants M1, M3, M5. pins: mb-4-foreach-eo/C-004 |
| C-005 | `MERGE INTO sink` from a body is stamped in its own snapshot under serializable isolation. | Public-door pin. | **PROVEN** | `test_merge_body_is_stamped_under_serializable_isolation` (a raise after the merge at epoch 1, the restart runs no body, ids `1, 2, 3`, epochs `0, 1`); `test_row_level_statement_is_stamped_under_serializable_isolation` for `UPDATE` and `DELETE` (`append / none`, `overwrite / 0`, `overwrite / 1`). pins: mb-4-foreach-eo/C-005 |
| C-006 | An overwrite of the sink from a body refuses `UnstampedSinkWrite` before anything commits, and so does every other shape the measured table refuses. | Public-door pins per shape; guard pins in `repark-iceberg`. | **PROVEN** | `test_unstampable_sink_write_refuses_before_it_commits` over eleven shapes (rows, snapshot log and properties unchanged each time); the six further shapes of the measured table by probe; `sink_offsets_body_scope_tests.rs::the_guard_refuses_an_unstampable_sink_commit_and_passes_another_table` and `::the_guard_admits_the_stamped_commit_and_refuses_what_follows_it`; `exactly_once_tests.rs::an_unstampable_commit_to_the_sink_refuses_before_it_lands`. Mutants M2, M7. A-3 records the two shapes the design note listed as refused and that are not. pins: mb-4-foreach-eo/C-006 |
| C-007 | A body that never writes the sink still gets the driver's stamp-only snapshot, one per epoch. | Driver-level pin plus the public-door pin. | **PROVEN** | `exactly_once_tests.rs::a_body_without_a_sink_write_gets_one_stamp_only_snapshot_per_epoch`; `test_body_without_a_sink_write_gets_the_stamp_only_commit` (green on the base build too: the behaviour is kept). pins: mb-4-foreach-eo/C-007 |
| C-008 | A body that writes a second table leaves that table unstamped and unfenced, and the sink exactly-once. | Public-door pin. | **PROVEN** | `test_second_table_is_a_side_output_and_the_sink_stays_exact`: the side write runs before the sink write, the body raises after both at epoch 0, and after the restart the side table holds `1, 2, 3` in two unstamped snapshots and the sink `1, 2, 3` in epochs `0, 1`. The side table shows no duplicate here because the failed epoch had committed; the registry row states the at-least-once half, which the failed-before-commit pin (`a_failed_body_fails_the_query_and_the_restart_replays_its_epoch`) shows as a replayed body. pins: mb-4-foreach-eo/C-008 |
| C-009 | A body that made no stamped commit while an unstamped snapshot moved the sink's head ends `RecoveryRequired(UnstampedSinkCommit)` and writes no stamp. | Driver-level pin. | **PROVEN** | `exactly_once_tests.rs::a_sink_write_outside_the_body_scope_with_no_stamped_commit_ends_recovery_required` (the reason names the unstamped snapshot, no stamp, durable none) and `::a_sink_write_the_guard_cannot_see_ends_recovery_required`; `::a_foreign_snapshot_beside_a_stamped_epoch_is_tolerated` for the other half of R-3; public door `test_sink_write_from_another_thread_ends_recovery_required`. Mutants M4, M6. pins: mb-4-foreach-eo/C-009 |
| C-010 | Two runs of one query racing through `foreachBatch` bodies land every row once; the loser ends `Fenced`. | Driver-level race pin, 5 runs. | **PROVEN** | `race_tests.rs::two_sessions_racing_foreach_bodies_land_every_row_exactly_once`: 50 seeded iterations, every row once, every epoch stamped once by one run, the loser drained or naming the winner; 43 of 50 iterations refused a staged write (floor 10). 5 of 5 runs green. Mutants M1, M5. pins: mb-4-foreach-eo/C-010 |
| C-011 | The `toTable` door is unchanged: its pins, the race pins and its kill scenarios answer as at the base. | `table_door_tests.rs`, `race_tests.rs` 5 times, the verify's `t_*` scenarios. | **PROVEN** | `table_door_tests.rs`, `fence_tests.rs` and the `toTable` race pin are unedited in their assertions and green (the only edits: two constants made visible to a sibling test module, and `race_once` takes the door); `race_tests` 5 of 5; `t_stop`, `t_kill`, `t_killcommit`, `t_sparkstop` exact with the base's row and epoch counts; the 137 pins of the five existing MB-4 batteries green with two foreach pins re-read. Nothing in `Run::append` changed. pins: mb-4-foreach-eo/C-011 |
| C-012 | The verify's exactly-once scenarios through the public doors report 0 violations. | The verify's own scripts, run unchanged apart from the interpreter path and the test seam. | **PROVEN** | 12 of 12 choreographies exit 0 on their own check with 0 duplicates, 0 lost, 0 extra, no epoch gap (table above); the nine multi-query cases as described there. pins: mb-4-foreach-eo/C-012 |
| C-013 | Five hand mutants of the new code each turn at least one pin red. | The mutant table. | **PROVEN** | Eight mutants, eight red (table above); M7 survived the Rust pins first and got a pin. pins: mb-4-foreach-eo/C-013 |
| C-014 | The registry carries the side-effect contract (both halves), MBE-13 and `UnstampedSinkCommit` as reachable through the public door, and MBE-19. | The registry rows and the sketch §4 row. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` rows `MB-4-FOREACH-EO-1`, `MB-4-FOREACH-SIDE-EFFECTS-1`, `MB-4-FOREACH-SINK-SHAPES-1`, each dated 2026-10-09 with its pins; the sketch's §4 gains MBE-19; `streaming_errors.rs` renders it (`sink_committed_twice_renders_stream_failed`). pins: mb-4-foreach-eo/C-014 |
| C-016 | Check (a): on the `foreachBatch` door a start, a restart and every batch refuse `RecoveryRequired` before any body runs when an unstamped snapshot sits above the query's newest stamp or the table under the sink's name was replaced. | Driver pins, public-door pins. | **PROVEN** | `exactly_once_tests.rs::a_failed_body_is_audited_and_the_restart_refuses_before_any_body` (two restarts refuse, no body runs, the sink is unchanged), `::a_foreign_snapshot_between_runs_refuses_the_restart`, `::a_stray_sink_write_beside_the_stamped_commit_ends_recovery_required` (the restart refuses when the stray is above the stamp and resumes when the stamp is the head), `::rows_already_in_the_sink_and_a_seeded_restart_are_not_violations`, `::at_epoch_zero_a_restart_replays_and_is_stopped_again` (the limit); `lifecycle_tests.rs::a_sink_replaced_under_the_body_ends_recovery_required`; `sink_offsets_lineage_tests.rs::the_walk_stops_at_this_query_s_newest_stamp_and_ignores_what_lies_below`; public door `test_foreign_insert_between_runs_refuses_the_restart` (and the rollback that recovers it). Mutants F2, N1, N7. pins: mb-4-foreach-eo/C-016 |
| C-017 | Check (b): after every body, returned or raised, any change to the sink other than the epoch's one stamped commit ends `RecoveryRequired` naming the snapshot and its operation, or the change, before the epoch is recorded durable. | Unit pins on the mark, driver pins, public-door pins. | **PROVEN** | The mark's seven unit pins in `sink_offsets_lineage_tests.rs`; driver pins for a stray with no stamped commit, before and after the stamped commit, beside a raise, and a property change beside the stamped commit; public door `test_thread_route_beside_the_stamped_write_ends_recovery_required`, `test_main_thread_statement_while_a_body_runs_ends_recovery_required`, `test_sink_dropped_and_recreated_after_the_stamped_write_ends_recovery_required`. The handle's durable record stays at the last audited epoch. Mutants N2, N3, N6, N8. pins: mb-4-foreach-eo/C-017 |
| C-018 | A DML the engine planned through DataFusion against the sink from a body (`EXPLAIN ANALYZE` of an `INSERT`, `UPDATE` or `DELETE`, a bare `INSERT`) refuses MBE-19 before it lands; a plain `EXPLAIN` passes. | Driver pin, public-door pins. | **PROVEN** | `exactly_once_tests.rs::a_planned_write_to_the_sink_refuses_before_it_lands_and_a_plain_explain_passes`; public door `test_explain_analyze_of_a_sink_write_refuses_before_it_lands` (four statements, raised and swallowed: the sink's rows and snapshots unchanged) and `test_plain_explain_of_a_sink_write_passes`; the re-verify's `explain_raise`, `explain_kill`, `explain_only_raise`: 0 duplicates, sink empty. Mutant N5. pins: mb-4-foreach-eo/C-018 |
| C-019 | The re-verify's thread routes (five, and a main-thread statement while a body runs) each end `RecoveryRequired` naming the unstamped snapshot, and a restart after a raise refuses without running a body. | Public-door pins. | **PROVEN** | `test_thread_route_then_raise_ends_recovery_required_and_the_restart_refuses` over the five routes (the snapshot id and its operation in the text, the body's error as `__cause__`, two restarts refused with the body never called and the sink unchanged) and the main-thread pin; the re-verify's door table: the five thread shapes end `RecoveryRequired` in all three placements. pins: mb-4-foreach-eo/C-019 |
| C-020 | A stamped write that failed releases the claim: the guard refuses unstamped commits again, a retry lands stamped once, and MBE-13's text is not shown for a write that did not land. | Unit pins, public-door pins. | **PROVEN** | `sink_offsets_body_scope_tests.rs::a_failed_stamped_attempt_releases_the_claim_and_the_retry_is_the_stamped_commit`, `::a_stamped_attempt_that_never_reached_its_commit_releases_the_claim`, `::a_second_claim_beside_an_unknown_outcome_refuses_without_the_twice_text`; public door `test_retry_after_a_failed_stamped_write_is_the_stamped_commit` (the read-only-metadata repro: the retry lands stamped once) and `test_unstampable_commit_after_a_failed_stamped_write_still_refuses` (`ALTER` and `INSERT OVERWRITE`). The commit-conflict retry is pinned at the Rust level by an injected failing commit, not by a live conflict. Mutant N4. pins: mb-4-foreach-eo/C-020 |
| C-021 | `tests/test_dfcore_1_exports.py` is green with the `map_bridge` delta declared, and no same-named test of the facade suite passes on base and fails on head. | The pin; the per-test suite comparison. | **PROVEN** | `test_dfcore_1_exports.py` 10 of 10 with the delta declared in its header and tables; the full facade suite per test against the re-verify's base run: no same-named test passes on base and fails on head (table above). pins: mb-4-foreach-eo/C-021 |
| C-022 | MBE-16: the body's Python exception is the `__cause__` of the query's exception on every raising door. | Public-door pin. | **PROVEN** | `test_body_exception_is_the_cause_of_the_query_exception`: `awaitTermination`, `exception()` and `awaitAnyTermination` raise with `__cause__` being the body's own exception object; the thread pins show it on a `RecoveryRequiredException` too. pins: mb-4-foreach-eo/C-022 |
| C-023 | Ten hand mutants, the re-verify's three survivors among them, each turn a pin red. | The fold-2 mutant table. | **PROVEN** | Eleven mutants, eleven red (table above), the re-verify's B, E and F2 among them, each with a pin added in this fold. pins: mb-4-foreach-eo/C-023 |
| C-024 | The re-verify's scripts and kill set, and the first verdict's choreographies, show no silent duplicate. | The scripts, run from a copy with only their paths changed. | **PROVEN** | The re-run table above: no silent duplicate in any script. Every violation ends `RecoveryRequired` or is refused before landing. A restart refuses wherever a stamp exists below the stray; at epoch 0 it replays and is stopped again (two of the verifier's scripts show it: the one-process `thread` repro and `conflict_mergeupd_0`). 337 kills: 0 duplicates, 0 lost. The first verdict's 21 scenarios as before. pins: mb-4-foreach-eo/C-024 |
| C-025 | The `toTable` door is unchanged by fold 2. | Its pins, its kill scenarios, the race pins 5 times. | **PROVEN** | `table_door_tests.rs` and the `toTable` halves of `fence_tests.rs` and `race_tests.rs` unedited and green; `race_tests` 5 of 5; the three `toTable` kill scenarios and the first verdict's four `t_*` choreographies exact; the re-verify's `droprecreate table` and its `leak` probes (a `toTable` query on a sink with a foreign snapshot resumes) answer as before. Two shared pieces on its path changed with no effect on the door: the append arm's `SiteStamp` releases a failed claim (the door ends its query on a failed commit), and `UnstampedSinkCommit` gained an optional operation the door leaves empty, so its text is unchanged. pins: mb-4-foreach-eo/C-025 |
| C-026 | The audit's cost on a 200-epoch `availableNow` run is measured before and after. | The timing table. | **PROVEN** | 200 epochs, `availableNow`, one file per batch, memory catalog, medians of three runs, `2877da20` then this fold, one after the other under one build-lock hold on a shared box. Body appends to the sink: 60.619 s to 61.266 s, +1.1 %. `toTable` (code unchanged): 55.842 s to 57.318 s, +2.6 %, which is the noise of the pair. Body writes nothing: 7.813 s to 8.659 s, **+10.8 %**, about 4 ms an epoch: over the 5 % line, reported as a finding. The mark's snapshot set was then made lazy and the final build measured alone twenty minutes later: 63.354 s (one run 71.2), 57.228 s and 6.541 s. The no-write reading did not reproduce (16 % under the "before" figure), and the writing body read 4.5 % over it while the unchanged `toTable` read 2.5 % over. The box does not resolve a difference under about 5 %; no reading puts the audit above 5 % twice. A quiet-box measurement is owed. pins: mb-4-foreach-eo/C-026 |
| C-027 | None of the seven lines the re-verify listed under "the contract as documented could mislead" is true any more. | The registry rows, the MBE-13 and MBE-19 texts. | **PROVEN** | Registry rows `MB-4-FOREACH-EO-1`, `MB-4-FOREACH-SIDE-EFFECTS-1` and `MB-4-FOREACH-SINK-SHAPES-1` rewritten; MBE-19's text names the `DELETE` that refuses; MBE-13 is not shown for a write that did not land. The seven lines, in the re-verify's order: the data-dependent `DELETE` is stated; MBE-13 after a failed write is gone; the at-most-once half and its recipe are in the row a user reads; a helper thread's write beside the body's own now ends the query, and a restart refuses above a stamp; the token order has a pin (mutant E); drop-and-recreate ends `RecoveryRequired` in every batch; the checkpoint paragraph says it holds no state and that clearing it resets nothing. pins: mb-4-foreach-eo/C-027 |
| C-028 | At a `foreachBatch` query's first start the offsets property's first value is the starting mark (the sink's head, epoch 0 pending); it is written once, read as no durable record by every reader, and replaced by the first stamped commit. No third durable item exists. | Unit pins, driver pins, public-door pins. | **PROVEN** | `sink_offsets_lineage_tests.rs`: a pending mark reads as no durable record and the first stamp replaces it; the mark round-trips its head per query; it is written once and never over a record; a corrupt value is neither. Driver: `the_first_start_writes_the_mark_and_the_first_stamp_replaces_it` (the exact value on an empty and a seeded sink, a restart that finds it writing nothing, one offsets value per query afterwards). Public door: `test_first_start_writes_the_mark_into_the_offsets_property`. Mutants P1, P2, P4, P6. pins: mb-4-foreach-eo/C-028 |
| C-029 | A stray write between the mark and the first stamp refuses the first restart, naming the snapshot, with no body run; a kill before batch 0 commits restarts with no duplicate; two query names on one sink keep their own marks; a markless checkpoint starts from the head found; `toTable` writes no mark. | Driver pins, public-door pins, the scenario matrix. | **PROVEN** | `at_epoch_zero_the_restart_reads_the_mark_and_refuses_before_any_body` (two restarts, the first stray named, no body, the sink unchanged); `a_start_with_no_mark_and_no_stamp_takes_the_head_it_finds_and_marks_it`; `two_query_names_on_one_sink_each_keep_their_own_mark_and_stamps`; `the_table_door_writes_no_mark`. Public door: `test_stray_at_the_first_batch_refuses_the_first_restart_from_the_mark` (two routes, and the rollback to the starting head that recovers it), `test_stray_then_kill_at_the_first_batch_refuses_the_first_restart`, `test_kill_before_the_first_commit_restarts_from_the_mark_without_a_duplicate`, `test_table_door_writes_no_mark`. The scenario matrix above. Mutant P3. pins: mb-4-foreach-eo/C-029 |
| C-030 | The `RecoveryRequired` text of a stray on the sink's main branch names the stray and what resolves it, and card MB-SINK-MAINTENANCE-PATH-1 is filed. (Fold 3 proved "both remedies" as one sentence; fold 4 replaced the sentence, and what the remedies must be is C-033.) | Text pins; the card and its map row. | **PROVEN** | `error.rs::the_unstamped_reasons_name_the_stray_and_only_a_stray_on_main_prints_remedies` (the stray reason with its remedy, the off-main and the `toTable` texts byte for byte, and the change reason); the public-door pin asserts the text on the first ending and on both refused restarts; card `mb-sink-maintenance-path-1-card-2026-10-10.md` with its map row. pins: mb-4-foreach-eo/C-030 |
| C-031 | The side-effect contract is documented where the facade's `foreachBatch` contract lives, in markdown and in the docstring. | The docstring and the streaming map. | **PROVEN** | The `Notes:` section of `DataStreamWriter.foreachBatch` (`readers.py`) and the section "The `foreachBatch` contract" of the streaming package's map; registry row `MB-4-FOREACH-SIDE-EFFECTS-1` points at both. pins: mb-4-foreach-eo/C-031 |
| C-032 | At every start, and before every body, the driver walks the sink's main lineage from this query's newest stamp down to its previous stamp (or to the head the first stamp records), and ends `RecoveryRequired` naming any unstamped snapshot it finds there; a stray under a stamp is loud on every start, not once. | Walk pins in the iceberg and core crates; the public-door kill pin. | **PROVEN** | `sink_offsets_lineage_tests.rs::a_stray_under_the_newest_stamp_is_found_down_to_the_previous_stamp`, `::the_first_stamp_records_the_starting_head_and_bounds_the_walk_under_it`, `::an_empty_start_a_markless_first_stamp_and_a_shared_stretch_are_told_apart`; `exactly_once_tests.rs::a_stray_sink_write_beside_the_stamped_commit_refuses_every_restart_above_or_under_the_stamp` (two restarts, the first with no batch to run); the public-door kill pin `test_kill_between_a_twice_written_batch_and_the_audit_refuses_every_restart` at epochs 0 and 2, red on the fold-3 build (the restart exited 0); the five thread routes in `test_mb_4_streaming_foreach_lineage.py`. The third verify's repro `q2_once_kill_batchrows` now refuses both restarts. Mutants Q1, Q2, Q3, Q10. pins: mb-4-foreach-eo/C-032 |
| C-033 | Every remedy a stray refusal prints is exact when followed as printed: the snapshot to roll back to is named, a new query name comes with its `repark.cdc.start-after-snapshot-id` position or is not offered, and a stray under a stamp is never said to leave with a rollback to the newest stamp. | Text pins in `error.rs`; ten public-door pins that follow the printed text. | **PROVEN** | `stray_remedy.rs` (three text pins); `error.rs::the_unstamped_reasons_name_the_stray_and_only_a_stray_on_main_prints_remedies`; `sink_offsets_lineage_tests.rs::the_remedy_offers_only_what_the_walk_can_prove`; `microbatch_source_fold2_tests.rs::only_an_offset_that_ends_its_snapshot_is_a_start_after_position`; `test_mb_4_streaming_remedies.py`, ten pins that parse the refusal and follow it with exact row counts, ten of ten red on the fold-3 build. In the matrix, 21 printed rollbacks and 4 printed new names followed: 0 duplicated, 0 lost. Mutants Q4, Q5, Q6, Q11, Q12. pins: mb-4-foreach-eo/C-033 |
| C-034 | A streaming plan runs under the session settings of the moment its batch starts: the session time zone, ANSI mode and the batch's own start time reach both doors and the frame handed to a body, and the values stored equal the same statement run as a batch write. | Core pin on the snapshot helper; public-door pins reading the Parquet files in two zones. | **PROVEN** | `microbatch_source_fold2_tests.rs::a_state_snapshot_carries_the_settings_and_the_start_time_of_its_moment`; `test_mb_4_streaming_session_settings.py`: two zones by two ways of setting them, values read from the Parquet files of both doors equal to the batch write, the body's collected frame equal to the batch read; ANSI off; `current_timestamp()`; case sensitivity (green before the fix). Six red on the fold-3 build. Mutant Q7. pins: mb-4-foreach-eo/C-034 |
| C-035 | The `toTable` door asks R-007's nested gate about the sink's type and refuses a nested naive `timestamp_ns` leaf with the batch doors' text before any file is written. | Core pin; public-door pin comparing the two texts and the sink's files. | **PROVEN** | `test_mb_4_streaming_sink_gates.py::test_table_door_refuses_a_nested_timestamp_ns_leaf_as_the_batch_write_does` (the batch write's sentence in the query's error, the sink's files unchanged, no snapshot), red on the fold-3 build. No Rust pin: the driver fixture has no format-version 3 sink. pins: mb-4-foreach-eo/C-035 |
| C-036 | A write that brings a `repark.cdc.*` snapshot summary key from user options or the session conf is refused by name where every summary is assembled, so no batch write can forge a stamp. | Iceberg pin on the choke point; public-door pins over seven statements and five writer shapes. | **PROVEN** | `sink_offsets_tests.rs::a_site_without_a_claim_keeps_the_caller_extras_and_refuses_a_reserved_key_by_name`, `sink_offsets_scope_tests.rs::a_claimed_site_adds_the_stamp_and_the_starting_head_and_drops_the_token`, `::caller_extras_cannot_displace_the_stamp`; thirteen public-door cases in `test_mb_4_streaming_sink_gates.py` (seven statements, five writer shapes, the restart after a refused forgery), all red on the fold-3 build. Mutant Q8. pins: mb-4-foreach-eo/C-036 |
| C-037 | The `foreachBatch` door refuses a keyed sink (ENC-1) before it writes its starting mark, with the text the `toTable` door prints. | Core pin; public-door pin on the sink's files. | **PROVEN** | `exactly_once_tests.rs::a_keyed_sink_refuses_the_foreach_door_before_the_mark_and_any_body`; `test_mb_4_streaming_sink_gates.py::test_foreach_door_refuses_a_keyed_sink_before_it_writes_its_mark` (two bodies; no file, no mark, no call). In the matrix all six `foreachBatch` trials on a sink keyed before the start write nothing. Mutant Q9. pins: mb-4-foreach-eo/C-037 |
| C-038 | The session example follows the surface: `SparkSession.readStream` and `SparkSession.streams` answer, and the example-coverage job that executes every example is green. | The example; the gate with `--require-execute`. | **PROVEN** | `docs/examples/session/streaming_entry_points.py` and the edited `connect_only_and_declared.py`; `scripts/check_example_coverage.py --require-execute` exit 0 on the built module (252 examples); `python/repark-parity/tests/test_ex_0_example_coverage.py` green. pins: mb-4-foreach-eo/C-038 |
| C-039 | The two mark rules the third verify's mutants N2 and N4 found unpinned are pinned: only epoch 0 can be pending in a mark, and a stamp alone refuses a late mark. | One pin each; both mutants red. | **PROVEN** | `starting_mark.rs::only_epoch_zero_can_be_pending` (mutant N2 red) and `sink_offsets_lineage_tests.rs::a_stamp_alone_refuses_the_mark_even_without_the_offsets_property` (mutant N4 red). pins: mb-4-foreach-eo/C-039 |
| C-040 | An unstamped snapshot under the query's newest stamp is reported at every start even when the walk finds no lower bound (the previous stamp expired, the first stamp expired, the recorded starting head expired, a first stamp that records no head), and no rollback is printed that cannot be followed. | Walk pins; public-door kill-then-expire pins. | **PROVEN** | `sink_offsets_lineage_tests.rs::a_stray_under_the_newest_stamp_is_reported_when_no_lower_bound_is_left` (the three lost bounds, above and under a stamp, each with no rollback), `::a_sink_with_nothing_unstamped_under_its_newest_stamp_is_healthy_without_a_bound`; `stray_remedy.rs` (the text over all five reasons); `test_mb_4_streaming_foreach_eo.py::test_a_stray_before_any_stamp_prints_no_rollback_once_the_starting_head_is_expired`. Owner ruling D1 measured in 26 cells, no case against it. In the matrix `expire_retain2`, `expire_retain3`, `first_expire1` and `first_expire2` refuse. Mutants R1, R2, R3, R9. pins: mb-4-foreach-eo/C-040 |
| C-041 | A `toTable` start under a name that ever carried a `foreachBatch` stamp or mark still on the sink runs the same walk and refuses the same way; a name that only ran through `toTable` is unchanged. | Driver pins; public-door pins on both placements of the stray. | **PROVEN** | `sink_offsets_lineage_tests.rs::a_name_is_walked_on_the_table_door_once_it_ever_carried_a_foreach_stamp_or_mark`; `exactly_once_tests.rs::a_table_door_start_under_a_foreach_name_runs_the_walk_and_refuses_the_same_way`, `::a_name_that_ever_ran_through_foreach_stays_walked_on_the_table_door`, `::a_name_that_only_ran_through_the_table_door_takes_a_foreign_commit_as_before`; `test_mb_4_streaming_foreach_eo.py::test_a_table_door_start_under_the_same_name_does_not_run_past_a_stray` (epochs 0 and 2), `::test_a_stray_above_the_newest_foreach_stamp_refuses_a_table_door_start`, `::test_a_foreign_commit_refuses_a_table_door_start_once_the_name_ran_through_foreach`. In the matrix `door_switch` and `door_switch_first` refuse on both doors. Mutants R4, R5, R6, R7, R8. pins: mb-4-foreach-eo/C-041 |
| C-042 | A healthy sink is not refused after an ordinary expiry: with one, two or three snapshots retained, both doors run on exactly, and a query begun through `toTable` runs through `foreachBatch` after an expiry. | Public-door pins, green before and after. | **PROVEN** | `test_mb_4_streaming_foreach_eo.py::test_a_healthy_sink_runs_on_after_an_ordinary_expiry` (three retentions by two doors) and `::test_a_table_begun_query_runs_through_the_foreach_door_after_an_expiry`, green before and after; in the matrix 18 healthy cells of the owner's condition and 11 of ruling D1 run on exactly. pins: mb-4-foreach-eo/C-042 |
| C-043 | On the `foreachBatch` door `current_timestamp()` is one value for the whole micro-batch, as on Spark 4.1.2: every action on the frame and the sink write see the same value. | Spark measurement; public-door pin. | **PROVEN** | Spark 4.1.2 measured (`foreachBatch`, two `collect()` calls 1.3 s apart and a write: one value). `test_mb_4_streaming_session_settings.py::test_current_timestamp_is_one_value_for_the_whole_micro_batch_on_the_foreach_door`, red on the fold-4 build. No Rust pin. pins: mb-4-foreach-eo/C-043 |
| C-044 | On the `foreachBatch` door a stamped commit is refused before it lands when an unstamped snapshot sits between its parent and the head the driver read when the batch began: at the claim, and again at the fence on every attempt. Another query's stamp there passes. A stamp is never committed over a stray that landed after the batch began. | Scope and fence pins; public-door pin. | **PROVEN** | `sink_offsets_lineage_tests.rs::a_stamped_commit_is_refused_over_a_stray_that_landed_after_the_batch_began`, `::the_fence_refuses_a_stray_that_landed_between_the_claim_and_the_commit`, `::another_query_s_stamp_since_the_batch_began_does_not_refuse_the_stamped_commit`; `sink_offsets_scope_tests.rs::a_foreign_writer_inside_a_foreach_scope_fails_the_batch_commit_at_the_fence`; `exactly_once_tests.rs::a_stray_sink_write_beside_the_stamped_commit_never_ends_under_the_stamp`; `test_mb_4_streaming_foreach_lineage.py::test_a_stamped_write_over_a_stray_that_landed_in_the_batch_is_refused_before_it_lands` (five routes), red before the fix; `test_mb_4_streaming_foreach_eo.py::test_a_twice_written_batch_is_refused_at_its_stamped_write_and_a_kill_hides_nothing`, `::test_no_expiry_hides_a_stray_that_landed_in_a_killed_batch`. Mutants P1 to P5. pins: mb-4-foreach-eo/C-044 |
| C-015 | The questions of the fold-2 hand-back are ruled (owner rulings D2 to D5, 2026-10-10: the mark, the exclusive sink kept with a maintenance card, the side-effect contract, the re-verify and timing gate). What stays open is D5 itself: one Opus re-verify of the whole PR, then the quiet-box 200-epoch measurement with `task/wo/microbatch/mb4_lineage_timing.py`, median of 3, at most 1.05 times the no-audit driver. | The re-verify's verdict and the measurement. | **OPEN** | Closes on D5. The coverage attestation is the Critic's and is filed then. |
