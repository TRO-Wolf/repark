# WRITE-SPINE-1 steps (1) and (3) — shared kernel names, a fail-closed lookup, and the door matrix enumerated from the code     grade: B, with C-grade files, skeletons and gate lines where the code already fixes them   engine band: Claude Opus 5.5 build (lane A); Opus 5.5 and Muse MAX verifiers   release: 1.5.6

**Status:** order, written 2026-10-24 by Claude (claude-fable-5-1), orchestrator, on the owner's
"let's do it" of the same day, from the card
[WRITE-SPINE-1](../../roadmap/mid-term/write-spine-1-card-2026-10-22.md) (option D adopted
2026-10-22) and the door table of [the crate-boundary evaluation](crate-boundary-eval-2026-10-22.md)
§3, which the owner's adoption makes the sketch the card asked for. Read on main `e4f8d36b`;
nothing was run. Dispatches Mon 2026-10-26 on lane A. Step (2) has its own order, written after
this one merges and the step-0 fixtures exist; step (4) follows the v2 label.

## 0. Why, and what is out of scope

Six of the eighteen Iceberg write doors build their own store projection or SQL text (D3, D4, D7,
D8, D10, D21) and each carries its own copy of the zone, floor and pairing rules; every fix round
of October was a copy one door had missed. Step (2) replaces the copies with one plan-time conform
stage. Before that can be built safely, two things must exist: one set of kernel names both crates
compile against, with a lookup that refuses instead of silently storing a wall as UTC when a kernel
is missing (step 1); and a test that finds every door from the code and pushes every shape through
it, so a door that skips the stage is a failing test and not a verifier finding (step 3).

**Out of scope, by name:** any behaviour change on any door except the fail-closed refusal of
step 1; the conform stage itself (step 2); removing gate arms (GATE-RETIRE-1); the ZS-9 refusal,
R-015 and the nanosecond create-door rule (untouched); `repark-core`'s `zone_localiser` copy;
the Postgres, path and text writers; a new crate or a new crate edge (option D forbids both).

## 1. Rulings already made (nothing is decided inside the round)

- Option D (owner 2026-10-22): no new crate, no new edge; step (1) is "no behaviour change";
  step (3) is "the door matrix generated from the router arms".
- Nanosecond ruling (owner 2026-10-23): the floor and the nanosecond-leaf refusal are rules of
  step (2)'s stage; this order does not touch them; S1-3 keeps refusing.
- ZS-5: the native door (`repark.sql()`) keeps native-door semantics; D18 is a control.
- ZS-7 and ZS-9 stand; the seam's refusal text is not changed here.
- Ruling 70: a statement main answers that the head refuses, or answers differently, outside the
  fail-closed case of step 1 is a halt.
- Ruling 76(a): no hint or refusal names a data-deleting action.
- Owner 2026-10-22: every write-path order carries a step-0 door-by-shape census measured
  against Spark; the hand-back ends with "doors I did not touch, and why".
- Owner 2026-10-23: two verifiers, Opus 5.5 and Muse MAX, same brief, same head; merge on the union.
- Owner 2026-10-24: both Opus lanes open from 10-26, never both compiling; the v2 label stays
  11-10; step (2) slips behind the label if not merged by 11-05, this order does not.
- Standing: no code comments from Anthropic models (Rust `///` and `//!` count; relocated code
  sheds its comments; a `pins:` citation lives in a `map.md`); `map.md` lockstep per commit;
  file ceilings from `scripts/check_rust_file_size.py` (default 1000); the scratch clone, never
  the live worktree; `taskset -c 0-25`, `CARGO_BUILD_JOBS=8`; one trailer,
  `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`; never `Co-Authored-By`.

## 2. Files (exact, with ceilings; the builder corrects a line number, never a file)

**Step 1, the names (new file, ceiling 1000, comment ceiling 0):**

| Path | Change |
|---|---|
| `crates/repark-common/src/udf_names.rs` | new: the string constants both crates name today, one `pub const` each: `__repark_cast_timestamp_ntz__`, `__repark_cast_timestamp_ns__`, `__repark_cast_timestamptz_ns__`, `__repark_cast_map__`, `__repark_zone_seam__`; plus the iceberg-owned `__repark_store_int_guard__` and `__repark_store_pair__` so the registry has one home. No logic. |
| `crates/repark-common/src/lib.rs` | `pub mod udf_names;` |
| `crates/repark-functions/src/timestamp_ntz_cast.rs:20`, `timestamp_ns_cast.rs:33-34`, `cast_map.rs:26`, `zone_seam.rs:21` | each `pub const … NAME` becomes `pub use repark_common::udf_names::… as …;` keeping the existing identifier, so no call site changes. |
| `crates/repark-iceberg/src/write/ntz_store.rs:15-17`, `store_cast.rs:26`, `struct_pairing.rs:19` | the same re-export shape. |
| `crates/repark-common/src/map.md`, `crates/repark-functions/src/map.md`, `crates/repark-iceberg/src/write/map.md` | lockstep rows. |

**Step 1, the fail-closed lookup:**

| Path | Change |
|---|---|
| `crates/repark-iceberg/src/write/ntz_store.rs` (392 lines; ceiling 1000) | the closure at `:218-221` computes `wall` as `Option<ScalarUDF>` and `None` falls to the plain-cast arms. Split the two meanings: a target that `wall_kernel_reads` for and whose kernel `store_kernel_udf_name` names but `ctx.udf(name)` does not hold is an **error**, not `None`. The map collects into `Result<Vec<Expr>>`; `zone_stores` returns the error. Error class: the session's internal-invariant class with the kernel name in the text (the exact `ReparkError` constructor is the one `store_overflow.rs:833` uses for its own refusals; reuse it, do not add one). Spark has no equivalent statement, so the text names RePark's invariant, not a Spark condition. |
| `crates/repark-spark/tests/store_kernel_registry.rs` | one new test: a `SessionContext` built without the NTZ wall kernel registered, an `INSERT … BY NAME` of a wall into a `TIMESTAMP` column refuses with that class and text; on main the same statement stores the wall as UTC. Red on main, green on head. |
| `crates/repark-spark/tests/map.md` | the row for the test with `pins: write-spine-1/C-00N` (the ledger row below). |

**Step 3, the door matrix enumerated from the code:**

| Path | Change |
|---|---|
| `crates/repark-spark/src/write_door.rs` | new (ceiling 1000, comment ceiling 0): `pub enum WriteDoor` with one variant per row of the evaluation's table, D1–D10 and D14–D21 (eighteen), `impl WriteDoor { pub const ALL: [WriteDoor; 18]; pub fn id(self) -> &'static str; pub fn is_spark_door(self) -> bool; }`, and `pub fn door_of(statement: &Statement, options: &StatementWriteOptions) -> Option<WriteDoor>` as an exhaustive `match` over the statement shapes `router.rs:284` dispatches (the arms that write values return `Some`; DELETE, CALL, TRUNCATE and reads return `None`). A statement variant added later without an arm is a compile error; a variant added to `WriteDoor` without a spelling fails the test below. |
| `crates/repark-spark/src/router.rs` (968 lines; ceiling 1000) | `execute_inner` calls `door_of` once and carries the `Option<WriteDoor>` into the existing trace/statement record so the test can read which door a statement took. If the file would cross 1000, the call lives in a new `router/door_tag.rs` and `router.rs` loses nothing else. |
| `crates/repark-spark/src/lib.rs` | `pub mod write_door;` |
| `crates/repark-python/src/…` | one `#[pyfunction] write_doors() -> Vec<(String, bool)>` returning `WriteDoor::ALL` ids and whether each is a Spark-door; registered beside the existing session functions. |
| `python/repark/tests/timestamps/_write_spine_1_doors.py` | the spelling table: for each `WriteDoor` id at least one SQL or DataFrame spelling, built from the U-8 probe's 28 spellings (`task/wo/ice-ntz-nested-refuse-1/evidence/probe.py:44-72`) plus one for each door the probe lacks (D2 options, D6 CTAS family, D14–D20 through `repark.sql()`, D18 through the Rust append API, D21 through the micro-batch sink). The test fails if any `write_doors()` id has no spelling or any spelling names an id that does not exist. |
| `python/repark/tests/timestamps/_write_spine_1_shapes.py` | the shape axis: sources (instant literal, wall literal, string, date, column of each) × targets (leaf `TIMESTAMP`, leaf `TIMESTAMP_NTZ`, struct, array, map, struct-in-array holding each) × zones (`UTC`, `America/New_York`, `Asia/Kolkata`), the same axes and spellings as the ZONE-ORACLE-1 corpus (`_zone_oracle_1_corpus.py`) so the two share cell ids. |
| `python/repark/tests/timestamps/_record_write_spine_1.py` | the recorder, modeled on `_record_zone_oracle_1.py`: writes each cell on Spark 4.1.2 + Iceberg 1.11.0 (Hadoop catalog, `local[1]`, ANSI on) and reads it back; `--main --base <sha>` records main's answers the same way. Provenance block as the oracle's. |
| `python/repark/tests/timestamps/write_spine_1_spark_oracle.json`, `write_spine_1_main_<sha>.json` | the fixtures (step 0 output). |
| `python/repark/tests/timestamps/test_write_spine_1_doors.py` | the matrix: every door × every shape × every zone written on the head, read back, classified against the Spark fixture (EQUAL / DIFFERENT / REFUSED / ERROR) and against the main fixture (never worse). Marked `slow`. |
| `python/repark/tests/timestamps/map.md`, `crates/repark-spark/src/map.md`, `crates/repark-python/src/map.md` | lockstep rows; the test rows cite `pins: write-spine-1/C-00N`. |
| `task/ledgers/staging/write-spine-1-ledger.md`, `task/ledgers/staging/map.md` | the unit ledger, opened here (rows below). |

## 3. Skeletons (the builder fills bodies; signatures are fixed)

```rust
pub enum WriteDoor { D1Insert, D2InsertOptions, D3InsertByName, D4InsertOverwrite, D5ReplaceWhere, D6Ctas,
    D7MergeInsert, D8MergeUpdateStruct, D9MergeUpdateField, D10Update,
    D14NativeInsert, D15NativeInsertConf, D16NativeOverwritePartition, D17NativeCtas, D18NativeAppendApi,
    D19NativeMerge, D20NativePredicateDml, D21MicroBatchSink }
impl WriteDoor { pub const ALL: [WriteDoor; 18]; pub fn id(self) -> &'static str; pub fn is_spark_door(self) -> bool; }
pub fn door_of(statement: &Statement, options: &StatementWriteOptions) -> Option<WriteDoor>;
```

```rust
pub fn zone_stores(ctx: &SessionContext, frame: DataFrame, targets: &[Option<DataType>]) -> Result<DataFrame>;
```
(the signature is unchanged; the body's `wall` lookup becomes `Result<Option<ScalarUDF>>`.)

Tests named before their bodies exist: `missing_wall_kernel_refuses_instead_of_storing_utc`
(Rust), `every_write_door_has_a_spelling`, `every_spelling_names_a_door`,
`door_matrix_matches_main_and_classifies_against_spark` (Python).

## 4. Steps (one commit each; the commit is its own step)

0. **Census (lane B, before step 3b opens; may run in parallel with steps 1 and 3a):** record
   `write_spine_1_spark_oracle.json` on the box's Spark 4.1.2 and `write_spine_1_main_<sha>.json`
   on main; file the census table (door × shape × zone, Spark's answer, main's answer) under
   `task/wo/write-spine-1/census-2026-10-2N.md`. Every cell where main differs from Spark is
   listed by id: that list is step (2)'s red set, not this order's.
1. **Step 1a, the names:** `udf_names.rs`, the re-exports, the maps. `cargo build --workspace`
   and `make rust-clippy` green; no test changes. Commit.
2. **Step 1b, fail closed:** the `ntz_store.rs` change and `store_kernel_registry.rs` test. The test
   is red on main (stores UTC) and green on head. Commit.
3. **Step 3a, the enum:** `write_door.rs`, the `router.rs` tag, the Python `write_doors()`
   function, `_write_spine_1_doors.py`, the two enumeration tests green. Commit.
4. **Step 3b, the matrix:** `_write_spine_1_shapes.py`, the recorder, the matrix test against the
   step-0 fixtures. Expected result on this head: **every cell equals main's recorded answer**
   (steps 1 and 3 change no value) and the Spark classification matches the census table. Commit.
5. **Mutants and revert check:** (a) `door_of` returning `None` for one writing arm reds
   `every_write_door_has_a_spelling` or the matrix; (b) a `WriteDoor` variant removed from `ALL`
   reds the enumeration test; (c) the fail-closed check reverted reds the registry test;
   (d) one spelling deleted reds `every_write_door_has_a_spelling`. Recorded in the ledger. Commit.
6. **Records:** the unit ledger rows PROVEN with `pins:` in the maps; the WRITE-SPINE-1 card gains
   a dated "steps (1) and (3) merged" line; `task/wo/write-spine-1/map.md`; the v1.5.6 notes
   draft gains one line. Commit.
7. **Push, CI green, hand back.** Both verifiers from this order at the head.

## 5. Gates and their green lines

| Gate | Command | Green |
|---|---|---|
| comment gate (before every push) | `git diff origin/main...HEAD -- '*.rs' '*.py' '*.sh' '*.toml' '*.yml' '*.yaml' \| grep -E '^[+-]\s*(///\|//!\|//\|#)' \| grep -vE '^[+-]\s*#(\[\|!)'` | prints nothing |
| comment density | `make check-comment-density` | exit 0 |
| clippy and panic ban | `make rust-clippy rust-panic-ban` | exit 0, no warnings |
| file size and crate DAG | `python3 scripts/check_rust_file_size.py && scripts/check_crate_dag.sh` | `clean` lines |
| map lockstep and links | `scripts/check_map_md.sh && python3 scripts/sync_map_md.py --check` | `maps clean` |
| ledger grammar | `make check-ledger-grammar` | `ledger-grammar: … clean` |
| Rust tests | `make rust-test` (one Rust suite per clone, inside the 26-core cap) | all pass |
| the new Rust pin alone | `cargo nextest run -p repark-spark --test store_kernel_registry` | pass on head, fail on main |
| the matrix | `uv run pytest python/repark/tests/timestamps/test_write_spine_1_doors.py -q` | pass; zero cells differ from the main fixture |
| the fast gate | `make ci` | exit 0 |
| CI on the branch | `gh pr checks <n>` | every required check SUCCESS before a verifier is spawned |

## 6. Halts (the round ends at the first; the hand-back names it)

- A door in the evaluation's table that cannot be reached by any spelling from the Python facade
  or `repark.sql()` without a public signature change (record it as "not reachable from a test"
  and continue only if the owner's line in this file says so; otherwise halt).
- A statement shape `router.rs:284` dispatches that the evaluation's table does not list (a
  nineteenth door): halt and name it; do not add a variant on your own.
- Any cell on head whose answer differs from main's recorded answer, other than the fail-closed
  refusal of step 1b in a session with an unregistered kernel.
- A ceiling that would rise; a new crate edge; a change to `repark-core` beyond the maps.
- A Spark answer the step-0 fixture does not hold (record the cell as `UNRECORDED`, do not guess).
- The hour budget: 6 h for steps 1a–1b, 10 h for 3a, 10 h for 3b; past a budget, commit what is
  green and hand back.

## 7. Hand-back

The §7 JSON of [TEMPLATE.md](../TEMPLATE.md) (`status`, `summary`, `commits`, `gates`,
`guards_relaxed`, `questions`, `out_of_scope_observed`) plus: the door count the enum holds and
the table's count (both 18, or the halt that says otherwise); the matrix counts on head against
main (must be all EQUAL) and against Spark (the census numbers, unchanged); the mutant table;
**"doors I did not touch, and why"**, by D-id.

## Ledger rows this order opens (`task/ledgers/staging/write-spine-1-ledger.md`)

| id | claim | evidence | status |
|---|---|---|---|
| C-001 | Every UDF name both `repark-functions` and `repark-iceberg` name is one constant in `repark_common::udf_names`, and no call site changed. | `grep -rn '"__repark_' crates/` shows each string once, in `udf_names.rs`. | OPEN |
| C-002 | A session whose NTZ wall kernel is not registered refuses an `INSERT … BY NAME` of a wall into a `TIMESTAMP` column, where main stores the wall as UTC. | `store_kernel_registry.rs`, red on main, green on head. | OPEN |
| C-003 | `WriteDoor::ALL` holds exactly the eighteen value-writing doors of the evaluation's table, and `door_of` is an exhaustive match over the dispatched statement shapes. | `write_door.rs`; the enumeration tests; a removed variant reds them. | OPEN |
| C-004 | Every door has at least one spelling and every spelling names a door. | `every_write_door_has_a_spelling`, `every_spelling_names_a_door`. | OPEN |
| C-005 | The door matrix on this head equals main's recorded answers on every cell, and classifies against the Spark fixture as the step-0 census table says. | `test_write_spine_1_doors.py` against the two fixtures. | OPEN |
| C-006 | The four mutants of step 5 each red a named test. | the ledger's mutant table. | OPEN |

## What the two verifiers do (same brief, same head, fresh clones)

Rebuild head; run the matrix themselves against the checked-in fixtures; run the four mutants and
one of their own; read `door_of` against `router.rs:284` for an arm that writes values and returns
`None`; read the fail-closed change for a path that still falls to a plain cast; check the
comment gate and the ceilings; check that no value changed (any cell on head not equal to main's
fixture is S1). Verdict PASS or FAIL with S1/S2 lines. The orchestrator reproduces every finding
before it becomes a fix round.
