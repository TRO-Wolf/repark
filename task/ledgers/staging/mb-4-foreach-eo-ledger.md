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
| C-015 | The open questions of the hand-back are ruled: the epoch-0 baseline (a durable mark of the starting head, or the limit as built), the single-writer consequence for maintenance, the reading that another query's stamped snapshot is not a violation, and the side-effect contract (owner Q1). | A ruling on the hand-back's questions. | **OPEN** | Closes on the ruling. The coverage attestation is the Critic's and is filed when this clause closes. |
