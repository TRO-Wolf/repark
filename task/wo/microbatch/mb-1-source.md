# MB-1 — the batch source over the incremental append scan                       grade: B   engine band: Muse at max   release: 1.7

## 0. Why and what is out of scope

D-2's source: next window, caps, fail-loud, offset arithmetic, over the
shipped incremental append scan (ICE-CHANGELOG-1 C-001…C-008). Out of scope,
named: the sink (MB-2a), the driver (MB-3), the facade (MB-4), the identity
kernel (*Identity*).

## 1. Rulings already made

- D-2, O-5 ("No deletes in bronze": an overwrite or delete snapshot inside
  a window refuses; no opt-in skip), the sketch's Q4 (`replace`) and Q6
  (offset encoding) answers, the MB-0 R-cells it cites.
- The F-APPEND-WINDOW-FAILLOUD-1 pin has merged before this slice opens.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `crates/repark-core/src/time_travel/microbatch_source.rs` | new: `MicroBatchSource`, window splitting, offset arithmetic | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-core/src/time_travel.rs` | edited: one `pub mod` line (426 lines, headroom) | default 1000 |
| `crates/repark-core/src/time_travel/incremental.rs` | read-only; edited only if the sketch names a line (288 lines) | default 1000 |
| `crates/repark-core/src/time_travel/map.md` | edited: the new module row | lockstep |

No other file is edited. In particular `session.rs` (1000/1000, zero
headroom) is not touched.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the `MicroBatchSource` signature, the offset encoding
(snapshot id plus position within the snapshot's added files), the
max-files / max-rows split rule, the fail-loud error class from MB0-R2/R5,
and the `replace` behaviour from MB0-R8/R9.

## 4. Steps

Step 1. The window arithmetic: next window over `(from, to]`, split by file
count and row count per the sketch. Step 2. The fail-loud path through the
fork's opt-in on non-append snapshots. Step 3. Pins over the
ICE-CHANGELOG-1 fixtures: window arithmetic, split by files and rows,
refusal on an overwrite inside the window, the `replace` pin per Q4. Step 4.
Commit.

## 5. Gates and their expected output

- `CARGO_BUILD_JOBS=6 cargo test -p repark-core --lib` — all green.
- `cargo clippy -p repark-core --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- A mutation dropping the fail-loud check turns the refusal pin red.

## 6. Halt rules

1. The sketch's Q4 or Q6 answer is missing or its MB-0 cell disagrees.
2. The F-APPEND pin has not merged.
3. A batch behaviour the sketch requires is unreachable without editing an
   excepted or at-ceiling file.
4. Any neighbour cell (the ICE-CHANGELOG-1 pins) changes its answer.

## 7. Hand-back

`{"status":"DONE|HALT","pins":"<count> green","mutation":"red|green","questions":[]}`
plus the commit sha.
