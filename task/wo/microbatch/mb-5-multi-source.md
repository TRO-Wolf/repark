# MB-5 — multi-source vectors and the SilverPlan hook                       grade: B   engine band: Opus 5.5   release: 1.7

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why and what is out of scope

D-7's second half: a Silver table fed by several Bronze tables carries a
vector offset; inside a batch the changed rows of one input join the other
inputs pinned at their end snapshots, and late arrivals re-process through
`MERGE` idempotency; the hook for S-2's declarative door. Out of scope,
named: the S-2 compiler itself, acceptance.

## 1. Rulings already made

- D-7, O-2 (identity enters the contract now; late arrivals dedup on the
  event id), the sketch's Q6 vector-form answer, MB-3 and MB-4 merged.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `crates/repark-core/src/microbatch/vector.rs` | new: the vector offset, per-input pinning | default 1000 (`scripts/check_rust_file_size.py`) |
| `crates/repark-core/src/microbatch/driver.rs` | edited: the MB-3 driver gains multi-input batches | default 1000 |
| `crates/repark-core/src/microbatch/map.md` | edited: the new module row | lockstep |

No other file is edited.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the vector offset encoding (Q6), the pinning rule for
non-advancing inputs, the S-2 hook signature, and the late-arrival
re-process path through `MERGE`.

## 4. Steps

Step 1. The vector offset and per-input end-snapshot pinning. Step 2. Late
arrivals through `MERGE` idempotency. Step 3. The S-2 hook. Step 4. Pins: a
two-input fact with late arrival on one input converges. Step 5. Commit.

## 5. Gates and their expected output

- `CARGO_BUILD_JOBS=6 cargo test -p repark-core --lib` — all green.
- `cargo clippy -p repark-core --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- One scoped Opus verifier pass; the release-roadmap rows and `STATUS.md`
  updated in the PR that ships MB-5, naming the carve-out from IPI-47
  closed on the three cells.

## 6. Halt rules

1. The sketch's Q6 vector answer is missing.
2. MB-3 or MB-4 has not merged.
3. A late arrival needs row-level dedup beyond the event id: that is the
  *Identity* slice — halt, do not build it.
4. Any neighbour cell (the single-input driver pins) changes its answer.

## 7. Hand-back

`{"status":"DONE|HALT","pins":"<count> green","verifier":"pass|fail","questions":[]}`
plus the commit sha.
