# MB-3 — the Session-owned driver                       grade: B   engine band: Opus 5.5   release: 1.7

## 0. Why and what is out of scope

D-3's loop: the tokio task, one batch in flight per query, `availableNow`
and `processingTime` triggers, the four shutdown rules, progress reporting.
The driver's only soft state is the in-flight batch, rebuilt from the sink
on restart. Out of scope, named: the facade (MB-4), multi-source vectors
(MB-5), the maintenance schedule beyond Q10's in-scope hooks.

## 1. Rulings already made

- D-3, CC-1's four rules (registering does not start; explicit async
  shutdown waits and reports the durable offset; timeout or ambiguous
  commit yields recovery-required; dropping a Python object is never the
  only shutdown), the sketch's Q2 (`Once`) and Q7 (states, progress,
  shutdown semantics) answers, MB-2c merged.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `crates/repark-core/src/microbatch/mod.rs` | new: the driver module root | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-core/src/microbatch/driver.rs` | new: the task, triggers, shutdown, progress | default 1000 |
| `crates/repark-core/src/microbatch/map.md` | new: one row per file | lockstep |
| `crates/repark-core/src/lib.rs` | edited: one `pub mod` line (153 lines, headroom) | default 1000 |

`crates/repark-core/src/session.rs` (1000/1000, zero headroom) is NOT
edited; Session ownership goes through the existing extension seam or a
path the sketch names that does not grow `session.rs`.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the driver-task handle and its registration path, the
trigger representations, the four shutdown outcomes as types, the progress
struct fields (Q7), and the Q10 in-scope maintenance hooks.

## 4. Steps

Step 1. The task and both triggers over one Bronze input. Step 2. The four
shutdown rules, pinned one by one. Step 3. Progress reporting. Step 4. The
Q10 hooks, if any. Step 5. Commit.

## 5. Gates and their expected output

- `CARGO_BUILD_JOBS=6 cargo test -p repark-core --lib` — all green.
- `cargo clippy -p repark-core --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- Pins: available-now drains and stops; processing-time ticks; shutdown
  reports the durable offset; drop does not stop; timeout yields
  recovery-required. One scoped Opus verifier pass.

## 6. Halt rules

1. The sketch's Q7 answer is missing or its MB-0 cell disagrees.
2. MB-2c has not merged.
3. Session ownership needs a `session.rs` edit: halt with the seam, do not
  grow the file.
4. Any neighbour cell changes its answer.

## 7. Hand-back

`{"status":"DONE|HALT","pins":"<count> green","verifier":"pass|fail","questions":[]}`
plus the commit sha.
