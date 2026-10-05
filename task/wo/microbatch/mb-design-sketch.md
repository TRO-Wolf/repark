# MB-DESIGN — the micro-batch design sketch from the packet                       grade: B   engine band: Opus 5.5

In the ATTR-ID-1 design-order shape (`task/wo/attr-id-1/attr-id-1-design.md`):
the sketch rules from measurement, names every type and signature, and hands
each slice a buildable contract. The owner reads the sketch before any
executor opens.

## 0. Why a sketch and not a first slice

The plan (§4) rules out one-lane-one-shot: four to five units the size of
ICE-CHANGELOG-1, one scoped verifier per product PR, every design question
answered from recorded cells, exactly-once as a proof, the fork pin cycle,
and §7 already ruled. The sketch turns MB-0's 24 cells into the contracts —
Q1…Q11 closed, types fixed, error classes taken from the cells — so MB-1,
MB-2a, the harness, MB-2c, MB-3, MB-4 and MB-5 each run in fewer rounds.

## 1. Inputs, and what is out of scope

Inputs: [packet.md](packet.md) (§1 D-1…D-8, §2 O-1…O-9a, §4 fork asks, §5
Q1…Q11), the MB-0 oracle JSON with its SHA-256
([mb-0-oracle.md](mb-0-oracle.md)), the two fork cards (PR #953), CC-1/CC-6/CC-9.
Measured on `origin/main`; every `path:line` the sketch cites is at its base
sha. Out of scope, named: any product code, any fork code, the *Identity*
slice's kernel design, the acceptance cell.

## 2. What the sketch must close

One ruled paragraph per open question, each citing the MB-0 cell that
decides it:

- Q1 `checkpointLocation` on an Iceberg sink: accept-and-ignore or refuse
  (from MB0-W8); the error class if refused.
- Q2 the `Once` trigger (from MB0-T1); the error class if refused.
- Q3 `complete` output mode on an Iceberg sink (from MB0-W3); the error
  class if refused.
- Q4 a `replace` snapshot inside a window: fail loud or skip (from MB0-R8,
  MB0-R9); how the answer meets O-5.
- Q5 Spark-checkpoint migration: read a Spark checkpoint directory or start
  from the sink offsets alone (from MB0-R13); the refusal if none.
- Q6 the exact snapshot-summary key names, the table-property key, and the
  offset encoding, single input and vector form (from MB0-R11…R13, MB0-W7).
- Q7 the `StreamingQuery` states, the progress shape, and the
  `awaitTermination` / `stop` semantics under CC-1's four rules (from
  MB0-T3).
- Q8 the same-key property-race protection: fork-side typed conflict or
  RePark-side generation fencing (pending the F-COMMIT measurement (3)).
- Q9 the `foreachBatch` callable contract: arguments, failure and retry
  semantics, per-batch observables (from MB0-W4…W6).
- Q10 which maintenance hooks ship with the slices and which stay later.
- Q11 file and rate sources: product surface or test-only harness helpers.

A question MB-0's cells cannot decide is a HALT with the missing cell, not
an answer from documentation.

## 3. Types and signatures the sketch must name

Every name below leaves the sketch fixed: the Rust path or the Python
qualified name, the signature, and the module it lives in.

- `MicroBatchSource`: the batch-source type over the incremental append
  scan — next window, caps, fail-loud, offset arithmetic.
- The offset: the single-input encoding and the vector form for multi-input
  Silver, with position-within-snapshot semantics.
- The summary keys and the table-property key (Q6's answer, as identifiers).
- The driver task: the Session-owned task, its handle, both triggers, the
  four shutdown outcomes.
- `StreamingQuery`: the facade type with `start`, `awaitTermination`,
  `stop`, `lastProgress`.
- `foreachBatch`'s callable signature (Q9's answer, as a signature).

## 4. Error classes

One registry row per refusal, each taken from an MB-0 cell: the Bronze
overwrite/delete in a window (O-5), the keyless table (O-6), the local
filesystem catalog (O-3), the `Once` / `complete` / `checkpointLocation`
refusals if Q1…Q3 refuse, and the recovery-required outcome (CC-1/CC-9).
Spark wording where the semantics match and the cell was measured (CC-4);
RePark-owned with a dated row otherwise. The sketch lists the class, the
cell, and which slice flips each SES-DECL row.

## 5. The crash harness's five scenarios, as test names

The sketch specifies the five harness scenarios as test names with their
setup, kill point and assertion each:

1. kill between sink commit and the next trigger, then resume: no duplicate
   batch;
2. duplicate delivery of one batch's rows: dedup on the id;
3. two drivers on one sink: the second loses at `validate_from_snapshot`,
   every batch applies once;
4. unknown commit outcome: the C-008 walk reconciles, never a replace retry;
5. a Bronze overwrite inside a window: the query refuses (O-5).

Names follow the C-0 harness shape
(`python/repark-parity/tests/live_db/test_c0_cdc_scenarios.py`): one
`test_microbatch_<scenario>_1` per scenario, red until MB-2c.

## 6. Slices the sketch feeds

The sketch confirms or corrects the file assignments in
[mb-1-source.md](mb-1-source.md), [mb-2a-sink-offsets.md](mb-2a-sink-offsets.md),
[harness.md](harness.md), [mb-2c-replay-reconcile.md](mb-2c-replay-reconcile.md),
[mb-3-driver.md](mb-3-driver.md), [mb-4-facade.md](mb-4-facade.md) and
[mb-5-multi-source.md](mb-5-multi-source.md); a correction names the file,
the reason and the cell. No two slices edit one file.

## 7. Halt rules

1. A Q1…Q11 question has no deciding MB-0 cell.
2. Two MB-0 cells disagree on one question.
3. A required type cannot live in its assigned module without breaking the
   crate DAG or a file ceiling.
4. The F-COMMIT measurement (3) is still open when Q8 must close: the sketch
   rules both branches and names the trigger that picks one.

## 8. Sizes (judgement, not measured)

One Opus 5.5 round for the sketch; the owner reads it before MB-1 and MB-2a
open. The sketch itself is prose plus signatures, about the size of the
ATTR-ID-1 design §§1–5.

## 9. Hand-back

The sketch as a dated file beside this order, plus
`{"status":"DONE|HALT","closed":["Q1",…],"open":["Q…"],"corrections":[{"file":"…","reason":"…"}],"questions":[]}`.
