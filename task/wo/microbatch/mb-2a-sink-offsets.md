# MB-2a — summary stamping and the offset table property                       grade: B   engine band: Muse at max   release: 1.7

## 0. Why and what is out of scope

D-1's sink half: every micro-batch commit into Silver stamps the query id,
the batch epoch and the source snapshot vector into the snapshot summary
and writes the same vector as a table property in the same commit, so
resume is one read. Out of scope, named: the epoch check and reconcile
(MB-2c), the driver (MB-3), the source (MB-1).

## 1. Rulings already made

- D-1, O-1 ("Go with recommendations"), the sketch's Q6 (summary keys,
  property key, offset encoding) answer and the MB-0 W-cells it cites.
- The F-COMMIT combined-commit proof is the mechanism this slice stands on.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `crates/repark-iceberg/src/write/sink_offsets.rs` | new: stamping, the property write, resume-from-sink | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-iceberg/src/write/mod.rs` | edited: one `pub mod` line (174 lines, headroom) | default 1000 |
| `crates/repark-iceberg/src/write/write_options.rs` | edited only if the sketch extends `summary_with_extras` (632 lines, headroom) | default 1000 |
| `crates/repark-iceberg/src/write/merge/snapshot_commit.rs` | edited only if the sketch names a line (450 lines, headroom) | default 1000 |
| `crates/repark-iceberg/src/write/map.md` | edited: the new module row | lockstep |

`crates/repark-iceberg/src/write/merge/mod.rs` (exception baseline 1569,
exact) is NOT edited: the new module lives beside `merge/`, not inside it.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the summary key names, the table-property key, the
single and vector offset encodings, which commit arms stamp (MERGE
copy-on-write and merge-on-read, append, overwrite), and the
resume-from-sink read path.

## 4. Steps

Step 1. Stamp the summary and the property in one commit on every arm the
sketch names. Step 2. Resume-from-sink: one read returns the last offset.
Step 3. Pins: the stamps round-trip; resume finds the last offset in one
read; a concurrent unrelated append still commits both halves exactly once.
Step 4. Commit.

## 5. Gates and their expected output

- `CARGO_BUILD_JOBS=6 cargo test -p repark-iceberg --lib` — all green.
- `cargo clippy -p repark-iceberg --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- A mutation dropping the property write turns the resume pin red.

## 6. Halt rules

1. The sketch's Q6 answer is missing or its MB-0 cell disagrees.
2. The F-COMMIT proof shows the combined commit fails and no fix has
   merged.
3. A required stamp is unreachable without editing `merge/mod.rs`.
4. Any neighbour cell (the MERGE and write-options pins) changes its
   answer.

## 7. Hand-back

`{"status":"DONE|HALT","pins":"<count> green","mutation":"red|green","questions":[]}`
plus the commit sha.
