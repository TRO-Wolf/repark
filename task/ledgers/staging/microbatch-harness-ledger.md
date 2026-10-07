# Unit ledger — HARNESS · the micro-batch crash harness, red before MB-2c

**Date:** 2026-10-07 · **Branch:** `test/microbatch-harness` · **Base:** `origin/main`
`2791c142` (MB-1 #979 and MB-2a #981 merged) · **Model:** claude-opus-5-5 (opus-worker build
lane) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Test-only.

**Scope.** The [harness order](../../wo/microbatch/harness.md) under its correction in the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §6 (the `harness.md` row: Rust, not
Python; pins 1 and 5 green, pins 2–4 red) and §5 (the five pins). Files:
`crates/repark-iceberg/src/microbatch/crash_tests.rs` (pins 2–5 beside MB-2a's pin 1) and its
`microbatch/map.md` row. No product file is touched. `microbatch/mod.rs` already declares
`#[cfg(test)] mod crash_tests;` (MB-2a), so it is not edited.

## Halt checks — 2026-10-07

- **MB-1 and MB-2a merged** (order halt rule 3 not met): `6a48443c` and `2791c142` are on the
  base.
- **Every §5 row names its setup, kill point and assertion** (order halt rule 1 not met).
- **No red pin passes** (order halt rule 2, which the §6 correction scopes to pins 2–4): all three
  fail at an assertion on the base. The texts are recorded under Gates.

## PROPOSITION LEDGER — HARNESS — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `crash_tests.rs` holds the five §5 pins under their §5 names. Pins 1 and 5 run. Pins 2, 3 and 4 carry `#[ignore = "red until MB-2c: <scenario>"]`, and MB-2c turns them on by deleting those three lines. | `cargo test -p repark-iceberg --lib microbatch::crash_tests` shows 2 passed and 3 ignored; the `--ignored` run shows 3 failed. | **PROVEN** | Both runs under Gates. pins: microbatch-harness/C-001 |
| C-002 | Pin 2, `test_microbatch_duplicate_delivery_skips_1`. Kill point (b): files staged for epoch 0 and then dropped are in no snapshot after epoch 0 replays. Epoch 0 re-delivered with its original `CommitStamp` against the pre-commit `Table`, through the stamped append arm, must fail, add no snapshot, and leave rows `[1, 2]` and epoch 0 stamped once. A claim of that stamp on the reloaded sink must return `AlreadyCommitted { query, epoch: 0 }`. | Red on the base at the re-delivery assertion; the (b) half green on the base. | **PROVEN** | The re-delivery commits on the base: `the stale re-delivery of epoch 0 must not commit a second time`. With that assertion removed in a probe, the next one reads 2 snapshots against 1, so the append re-bases and duplicates as §5 says. pins: microbatch-harness/C-002 |
| C-003 | Pin 3, `test_microbatch_two_drivers_one_sink_1`. Runs A and B share a `QueryId` and hold distinct `RunId`s. Both resume at epoch 0 and plan epoch 1 over one window. `FaultCatalog::Race` commits A's stamped epoch 1 inside B's `update_table` (kill point e). Variant (i) is B on `commit_append_with_summary`, variant (ii) B on a copy-on-write `execute_merge` (`MERGE … WHEN NOT MATCHED THEN INSERT *`). For each, B's commit must fail, epoch 1 must be stamped once, the rows must be `[1, 2, 3]`, the property must name run A, and B's claim on the reload must be `Fenced { query, epoch: 1, winner: A }`. | Red on the base on both variants. | **PROVEN** | (i) `Append: run B's commit of epoch 1 must fail at its base once run A has committed epoch 1`. (ii), measured in a reordered probe: B already fails at its pinned base on the base commit, the rows, epochs and property hold, and the pin is red at `CopyOnWrite: run B's claim of epoch 1 must be Fenced by run A, got Ok(true)`. pins: microbatch-harness/C-003 |
| C-004 | Pin 4, `test_microbatch_unknown_outcome_reconciles_1`, on the `foreachBatch` door: an unstamped body write, then `commit_stamp_only` for epoch 1 through `FaultCatalog`, with `commit.status-check.num-retries=0`. Kill point (d), the landed variant: the result must be `Ok` with the landed snapshot, which carries `engine.operation-id`. There must be exactly one `update_table`, every snapshot an `append`, and resume reads epoch 1. The unlanded variant: no snapshot is added and there is one `update_table`. The refusal must be `RecoveryRequired { epoch: 1, durable: Some(epoch 0), reason: CommitOutcomeUnknown }`, and resume still reads epoch 0. | Red on the base on both variants. | **PROVEN** | Landed: `the landed stamp must resolve to its snapshot <id>, got Err(RecoveryRequired { …, epoch: Epoch(1), durable: None, reason: CommitOutcomeUnknown { operation_id: Some("<uuid>") } })`. Unlanded, measured in a reordered probe: `an unlanded stamp must refuse CommitOutcomeUnknown with epoch 0 durable, got Err(RecoveryRequired { …, durable: None, … })`. pins: microbatch-harness/C-004 |
| C-005 | Pin 5, `test_microbatch_bronze_overwrite_refuses_1`, a green guard over MB-1. Epoch 0 commits Bronze rows 1 and 2, then Bronze takes a mutation and an append of row 4. R2 `INSERT OVERWRITE` and R5 `DELETE`: planning the next window refuses `NonAppendSnapshot` naming the mutation's snapshot and operation, and the sink's snapshots, rows and resume point are unchanged. R8 `replace`: it is skipped, and epoch 1 streams row 4 into a sink of `[1, 2, 4]`. | Green on the base; a mutation of the walk turns it red. | **PROVEN** | Green under Gates. Mutation M1 skips an overwrite like a replace (`window.rs` walk): red at `Overwrite: the window must refuse NonAppendSnapshot, got Ok(Some(WindowPlan { … }))`. M2 refuses a replace: red at the R8 epoch with `NonAppendSnapshot { …, operation: Replace, … }`. Each was restored from a backup and confirmed by `git diff --quiet`. pins: microbatch-harness/C-005 |
| C-006 | Every pin enters a `BatchScope` and carries the guard's token through the session snapshot property `spark.sql.iceberg.snapshot-property.repark.cdc.scope-token`. The append arm gets it from `resolve_empty_session_write`, and `execute_merge` reads it from its `SessionContext`. That is MB-3's seam (MB-2a D-10). No pin passes a raw `summary_entry`, and no product file changes. | `session_extra` asserts the token is present; `git diff --stat origin/main` names only the test file, its map row and this ledger. | **PROVEN** | `batch_session` and `session_extra` in `crash_tests.rs`; pin 1 keeps its body byte for byte and stays green through the session path. pins: microbatch-harness/C-006 |

## Dated decision rows

- **D-1 (2026-10-07). Pins 2 and 3 read the typed outcome from a claim on the reloaded sink,
  not from the arm's error.** MB-2c may edit only `write/sink_offsets.rs` (its order §2). The
  arms surface a commit failure through `write_options.rs` / `merge/snapshot_commit.rs`
  (`commit_result`), and the fork's `TransactionAction` trait is `pub(crate)`
  (`fork:crates/iceberg/src/transaction/action.rs:69`). So nothing in `sink_offsets.rs` can put a
  typed `MicroBatchError` inside the arm's error when the Q8 fence fails the commit. The §3.4
  epoch check reads `read_resume_point` on the `Table` it is given, with no IO, so a claim on the
  stale pre-commit view can never see epoch 0. Each pin therefore asserts two things. The
  re-delivered or losing commit fails (any cause) and lands nothing. Then a claim of the same
  stamp on the reloaded sink, the driver's next step, returns `AlreadyCommitted` or
  `Fenced { winner: A }`.
  - The question: where a red pin reads the typed epoch-check outcome.
  - Flink: a committer that finds its checkpoint committed skips it; a fenced producer fails.
  - Spark: `foreachBatch` replays the batch at least once, and the user deduplicates on `batch_id`.
  - The NS default: refuse loud with the typed variant at the first read MB-2c owns, which is the
    claim.

- **D-2 (2026-10-07). Pin 4 drives `commit_stamp_only`.** `resolve_unknown_outcome` does not
  exist at MB-2a, so a pin calling it would not compile and the gate's 2 passed / 3 ignored could
  not hold. The append arm's unknown-outcome branch lives in `write_options.rs`, outside MB-2c's
  file list. `commit_stamp_only` is the one stamped site whose `CommitStateUnknown` branch sits in
  `sink_offsets.rs`. It is also the `foreachBatch` door's trailing stamp in OQ-2a-2's lean. MB-2c
  turns the pin green by sending that branch through the C-008 walk.
  - The question: which site carries the reconcile pin.
  - Flink: the committer re-checks the sink for its checkpoint id before re-committing.
  - Spark: n/a; the batch fails and replays.
  - The NS default: the site whose unknown branch the reconcile slice owns, so the pin can turn
    green without moving.

- **D-3 (2026-10-07). Pin 4 sets `commit.status-check.num-retries=0` on the sink.** The fork's
  strict status check retries a failed reload up to `num-retries + 1` times, default 3
  (`fork:…/transaction/commit_status.rs:215`). At the default, a reload that fails once is
  followed by one that succeeds, the fork reconciles on its own, and the landed variant would
  pass on the base for the wrong reason. At 0, failing the reconcile reload once leaves the
  outcome unknown, which is the §5 setup. A probe at the default is not needed; the loop bound is
  in the source.

- **D-4 (2026-10-07). Pin 3's run A is a raw stamped commit on the inner catalog.** The
  `BatchScope` registry allows one scope per sink in a process (`SinkBusy`), so run A acts as
  another process: a `fast_append` that carries A's `ClaimedStamp` summary entries and property,
  the probe tests' `race_stamped` shape. The variants differ in B's door only. Variant (ii) B
  already fails at its pinned base on the base commit (`validate_from_snapshot(H)` under the
  default serializable isolation), so its red assertion is the `Fenced` claim alone. Variant (i)
  stays red until Q8 branch A or B is green, as §5 says.

- **D-5 (2026-10-07). Pin 5 commits epoch 0 before the mutation.** §5's "the sink is unchanged"
  then compares a non-empty sink. The overwrite replaces both files with row 9, the delete removes
  row 1's file, and the replace rewrites row 1's file in place. An `Unbounded` plan refuses at the
  first overwrite or delete in the window (MB-1 fold 2, G1), so the append of row 4 after the
  mutation never streams on R2 or R5.

- **D-6 (2026-10-07). The shared helpers changed shape; pin 1's body did not.** `run_epoch` now
  builds its stamp through `stamp_for` and commits through `deliver`, which reads the token from
  the batch session (C-006). `bronze_append` returns the files it wrote, for pin 5's mutations.
  `try_plan_window` hands back the window's `Result`. The file is 974 lines, under the default
  ceiling, so the fault catalog was not split into a sibling.

## Gates — 2026-10-07

All exit 0 unless noted, on the commit tree:

- `cargo test -p repark-iceberg --lib microbatch::crash_tests`: **2 passed, 0 failed, 3 ignored**.
- `cargo test -p repark-iceberg --lib microbatch::crash_tests -- --ignored`: exit 101, **3
  failed**, each at its assertion:
  - pin 2 at `crash_tests.rs:666`: `the stale re-delivery of epoch 0 must not commit a second time`;
  - pin 3 at `crash_tests.rs:735`: `Append: run B's commit of epoch 1 must fail at its base once run A has committed epoch 1`;
  - pin 4 at `crash_tests.rs:866`: `the landed stamp must resolve to its snapshot <id>, got Err(RecoveryRequired { query: QueryId(…), epoch: Epoch(1), durable: None, reason: CommitOutcomeUnknown { operation_id: Some("…") } })`.
- `make rust-clippy`, `cargo fmt --check`, `make rust-panic-ban`,
  `python3 scripts/check_rust_file_size.py`, `./scripts/check_lib_rs.sh`,
  `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main`,
  `python3 scripts/check_ledger_grammar.py`, and the comment-ban probe (0 hits).

## Notes for MB-2c

- MB-2c's gate is `cargo test -p repark-iceberg --lib microbatch::crash_tests` with five passed,
  after deleting the three `#[ignore]` lines and nothing else.
- Pin 2 needs the append-door fence (Q8 branch A or B), so the stale re-delivery fails at its
  base `H`. Here `H` is the empty pre-commit sink. It also needs the epoch check in `claim`.
- Pin 3 (i) needs the same fence. Pin 3 (ii) needs only the epoch check, since the
  copy-on-write arm already fails at its base.
- Pin 4 needs `commit_stamp_only`'s `CommitStateUnknown` branch to run the walk: it returns the
  found snapshot, and otherwise refuses with the durable record read from the reloaded sink.

```
COVERAGE_ATTESTATION:
  pr_unit: microbatch-harness
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each §5 row walked against its pin. Setup, kill point and assertion are mapped in C-002…C-005, and the four places the pin shape departs from §5's literal wording are dated (D-1 claim on the reload, D-2 the stamp-only site, D-3 the status-check bound, D-4 run A as another process).
      artifacts: [task/ledgers/staging/microbatch-harness-ledger.md, crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Negative paths pinned are the stale re-delivery, the losing driver on two doors, a landed and an unlanded unknown outcome, and an overwrite and a delete inside a window. Each red pin was run and fails at its assertion, not in setup. The variants that later assertions hide were measured in reordered probes and restored.
      artifacts: [crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The red pins match the typed MicroBatchError variants (AlreadyCommitted, Fenced, RecoveryRequired with CommitOutcomeUnknown) and pin 5 matches NonAppendSnapshot, never the text.
      artifacts: [crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Every harness owns a fresh memory catalog and fresh sink uuid, so the process-wide scope registry never shares an entry between parallel tests; each guard drops before the next scope on the same sink; FaultCatalog's state is atomics and a mutex never held across an await.
      artifacts: [crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The scope token reaches the arms only through the session snapshot property (C-006); the pins never place it in a raw summary entry.
      artifacts: [crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Test-only. No product file changes, and pin 1 keeps its body and stays green through the session path.
      artifacts: [crates/repark-iceberg/src/microbatch/crash_tests.rs, crates/repark-iceberg/src/microbatch/map.md]
    - id: AT-7
      status: N/A
      justification: Test-only unit; no runtime path changes.
    - id: AT-8
      status: ATTACKED
      evidence: The gate roster of the order ran green (Gates). crash_tests.rs is 974 lines, under the default ceiling, and microbatch/map.md is updated in the same commit.
      artifacts: [task/ledgers/staging/microbatch-harness-ledger.md, crates/repark-iceberg/src/microbatch/map.md]
    - id: AT-9
      status: N/A
      justification: No log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The three red pins are red on the base, which is their mutation proof against MB-2c's absence. For green guard pin 5, M1 (overwrite skipped like a replace) and M2 (replace refused) each turn it red, and each was restored and checked with git diff --quiet.
      artifacts: [task/ledgers/staging/microbatch-harness-ledger.md]
```
