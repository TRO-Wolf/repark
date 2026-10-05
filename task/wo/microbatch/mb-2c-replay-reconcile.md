# MB-2c — replay and reconcile                       grade: B   engine band: Opus 5.5, same lane as MB-2a   release: 1.7

## 0. Why and what is out of scope

D-4's proof: the epoch check before commit, the C-008 walk on
`CommitStateUnknown`, generation fencing. Turns the harness green. Out of
scope, named: the driver (MB-3), row-level dedup (D-5, the *Identity*
slice).

## 1. Rulings already made

- D-4, CC-9 (a checkpoint written by one producer generation is never
  advanced by another), the sketch's Q8 answer, the F-COMMIT measurement
  (3), the harness's five pins.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `crates/repark-iceberg/src/write/sink_offsets.rs` | edited: the MB-2a module gains the epoch check, the C-008 walk, fencing | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-iceberg/src/write/map.md` | edited only if the module row needs it | lockstep |

No other file is edited. No new module: MB-2c extends the MB-2a module in
place.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the epoch-check read path, the reconcile walk bounds,
the fencing condition (Q8's branch), and the recovery-required outcome
shape under CC-1 rule 3.

## 4. Steps

Step 1. The epoch check: a batch whose epoch already appears in the sink's
summary history skips its commit. Step 2. The C-008 walk on unknown
outcome: resolve by walking snapshots for the run key, never retrying a
replace. Step 3. Generation fencing per Q8. Step 4. The harness goes green.
Step 5. Commit.

## 5. Gates and their expected output

- `CARGO_BUILD_JOBS=6 cargo test -p repark-iceberg --lib` — all green.
- `pytest python/repark/tests/test_microbatch_crash_1.py` — five passed.
- `cargo clippy -p repark-iceberg --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- One scoped Opus verifier pass on the replay logic; mutations: dropping
  the epoch check turns the duplicate pin red; retrying a replace turns the
  reconcile pin red.

## 6. Halt rules

1. The sketch's Q8 answer is missing, or both branches are still open.
2. A harness scenario stays red for a reason outside this module: halt with
   the scenario, do not move the pin.
3. A source redelivering rows across different batches is found to need
   row-level dedup here: that is D-5's job — halt, do not build it.
4. Any neighbour cell (the MB-2a stamps) changes its answer.

## 7. Hand-back

`{"status":"DONE|HALT","harness":"5 green","verifier":"pass|fail","questions":[]}`
plus the commit sha.
