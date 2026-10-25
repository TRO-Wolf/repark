# WRITE-SPINE-1 step (2) — one plan-time conform stage for every Iceberg write door

grade: B (step 0 is a sketch handed back before any product edit) · engine: Claude Opus 5.5 (`claude-opus-5-5`) only · release: 1.5.6 (lands before the v2 label of 2026-11-10; the ZS-9 retirement is step 4, after it) · base: `origin/main` after GATE-RETIRE-1 merges

Parent records: card `task/roadmap/mid-term/write-spine-1-card-2026-10-22.md` (the ask, the 2026-10-23 ns ruling, the
2026-10-24 `add_months` fold); evaluation `task/wo/write-spine-1/crate-boundary-eval-2026-10-22.md` §2a–2c (the
straddling rules, site by site) and option D; orders `task/wo/write-spine-1/order-steps-1-3.md` and
`order-step-3b.md`; ledger `task/ledgers/staging/write-spine-1-ledger.md` (C-007, C-008, C-016 are this step's
inputs); folded card `task/roadmap/mid-term/write-ntz-to-ltz-nested-1-card-2026-10-22.md` (sites A, B, C).

## 0. Why, and what is out of scope

Six doors build their own store projection, so the three rules that straddle plan time and store time (the
session-zone conversion, the nanosecond floor, the struct pairing) live at eight sites in two crates, and a feed
shape one copy missed is the recurrence behind R-009, R-011, R-017, R-019, R-020, R-024 and ZS-9. This step turns
`zone_stores` plus `wrap_store_outputs` into one plan-time conform function that every value-writing door reaches
with `ctx` in hand, carrying the zone rule (nested leaves through `ZONE_SEAM_NAME`), the ns floor, the ns-leaf
refusal and the pairing rule as one stage, and proves by the census that no cell leaves EQUAL.

Out of scope, by name: retiring the ZS-9 refusal (`refuse_naive_into_instant`) and R-011 arm A1 (step 4, after the
v2 label); D18 (no session; native-door semantics; stays a control through site A); the DST-edge instant kernel
(ZONE-HORIZON-INSTANT-1); the Postgres door's `SessionZoneLocaliser`; any new crate or crate edge (`check_crate_dag`
must not change); the read-side; the fork; U-2 and R-024 (1.5.7); re-recording the step-0 main fixture (orchestrator
ruling on the hand-back).

## 1. Rulings already made

- **WS2-1 (eval option D, owner-adopted 2026-10-22):** no new crate, no new edge. The stage is a function in
  `repark-iceberg` that takes the `SessionContext` and a planned frame and returns the conformed frame; it names
  the `repark-functions` kernels only through `repark_common::udf_names`, and a kernel that is not registered
  refuses (step 1's fail-closed lookup extends to every rule; C-007 and C-008's fail-open returns close).
- **WS2-2 (from C-016, 2026-10-24/25):** the stage sits at plan time inside each door's own write function, at or
  after the point where the door is recorded today (the `zone_stores` / `wrap_store_outputs` call sites), never at
  `router.rs:143` or any point ahead of the door branch. The refusals that fire ahead of the doors are not moved
  in this step: ZS-9 stays where it is (step 4 removes it); the store-assignment refusal of the D10 row stays.
  Consequence the sketch must state: the 976 ZS-9 cells remain REFUSED on this head.
- **WS2-3 (owner 2026-10-23, "rule it"):** the ns floor and the ns-leaf refusal are rules of this stage. The floor is
  one kernel (`store_micros` through `TIMESTAMP_NS_CAST_NAME`) reached by every door including D21;
  `refuse_coarser_timestamp` at the sink stays as the fail-closed guard and must become unreachable for routed
  doors (a test proves the guard never fires on them). The ns-leaf refusal (R-015, `refuse_nested_ns_supply`)
  keeps refusing on every door; no door loses it; the stage does not duplicate it.
- **WS2-4 (card fold, owner 2026-10-24):** a plain `UPDATE` setting a `TIMESTAMP_NTZ` field from `add_months(...)`
  stores session midnight (`1769922000000000` in New York, Spark's value), not UTC midnight; the pin is in scope.
- **WS2-5 (eval §2c):** the MERGE/UPDATE staging sink's unpartitioned and lineage branches
  (`merge/session_staging.rs`) stop skipping the pairing refusal: they pass the stage like the partitioned branch.
- **WS2-6 (card ask 4, mechanical):** a test that enumerates the doors from the code (`WriteDoor::ALL` less D18)
  and fails when a door skips the stage. The mechanism is the sketch's proposal (a stage record readable from a
  test beside `last_write_doors`, or a plan-walk for the stage's marker); the requirement is fixed.
- **WS2-7 (sizes):** `store_overflow.rs` (939) and `store_cast.rs` (994) are at their ceilings; the stage is a new
  file `crates/repark-iceberg/src/write/conform_stage.rs` (ceiling 1000, no comments), and code it replaces is
  deleted from `ntz_store.rs`, `store_overflow.rs` and `update_cast.rs` in the same commit, shedding comments.
  No ceiling is raised.
- **WS2-8 (oracle):** the step-0 census is the oracle: `_record_write_spine_1.py --main` on the head against
  `write_spine_1_main_7a789fc5.json` and `write_spine_1_spark_oracle.json`. Allowed moves: any cell that ends EQUAL
  to Spark's value. Halt: any EQUAL cell that leaves, any move that ends DIFFERENT or ERROR, any ZS-9 cell that
  stops refusing (WS2-2). The hand-back tables every moved cell.
- **WS2-9 (step-4 evidence, measured not shipped):** after the build, one uncommitted mutant build with
  `refuse_naive_into_instant` returning `Ok(())` replays the 976 ZS-9 cells; the table (cell, Spark value, head
  value, EQUAL or not) goes in the hand-back as step 4's input. Zero non-EQUAL is the aim; a non-EQUAL cell is a
  finding, not a halt.
- **WS2-10 (clerk tier, owner 2026-10-24):** the builder commits code and tests only; maps, ledger rows, the card
  and the parity rows are the clerk's commit from `handback-notes.md`.
- **WS2-11 (verification):** Muse MAX critic on the PR; Opus 5.5 verifier on this PR because it moves stored
  values (owner standing rule for product PRs), with a brief that reproduces the census recount and the WS2-9
  table and measures the ZONE-SEAM-1 perf set (10-run medians, 1.05x, the New York nested INSERT cost cell
  excepted until step 4). The builder measures no perf.

## 2. Files (the sketch corrects a line, never a file; ceilings from `scripts/check_rust_file_size.py`)

| file | lines | change |
|---|---|---|
| `crates/repark-iceberg/src/write/conform_stage.rs` | new | the stage: one public function, its door-facing helpers, the stage record if the sketch picks that mechanism |
| `crates/repark-iceberg/src/write/ntz_store.rs` | 410 | `zone_stores`, `zone_stores_by_name`, `zone_store_frame`, `registered_store_kernel` move into the stage or are deleted; the type helpers stay |
| `crates/repark-iceberg/src/write/store_overflow.rs` | 939 | `wrap_store_outputs` becomes a call into the stage or is deleted; nothing added |
| `crates/repark-iceberg/src/write/update_cast.rs` | 478 | `nested_leaf_cast_sql` emits `ZONE_SEAM_NAME` for a nested zone pair instead of bare `arrow_cast` (site B) |
| `crates/repark-iceberg/src/write/conform.rs` | — | `entry_store_cast` (site A) becomes the identity for routed doors; D18 keeps it |
| `crates/repark-iceberg/src/write/merge/insert.rs`, `merge/session_staging.rs`, `predicate_dml.rs`, `predicate_dml/lineage.rs` | — | call the stage (sites C and the D9/D10 plans; WS2-5) |
| `crates/repark-spark/src/insert_by_name.rs` (:218), `insert_overwrite.rs` (:226, :433), `insert_overwrite/store.rs` (:35) | — | D3, D4 call the stage |
| `crates/repark-core/src/microbatch/run.rs` (:479) | — | D21 calls the stage with its batch session |
| `crates/repark-spark/tests/ntz_into_ltz_nested.rs`, `crates/repark-sql/tests/write_door_native.rs`, `crates/repark-iceberg/src/write/conform_stage/tests.rs` | — | pins (§3) |
| `python/repark/tests/timestamps/test_write_spine_1_doors.py` | — | the C-016 rows gain `measured` entries only if a count changes; the `DIVERGENCES` rows stay |

## 3. The pins (names fixed; bodies by the builder)

1. `every_value_writing_door_passes_the_stage` — WS2-6; one write per door in `WriteDoor::ALL` less D18; red when
   any door's call is removed (the mutant table names one door per site).
2. `a_missing_kernel_refuses_on_every_routed_door` — WS2-1; a session without the wall kernel refuses on D3, D4,
   D7, D9, D10, D21 (today only D3 is pinned, `store_kernel_registry.rs`).
3. `the_sink_guard_never_fires_on_a_routed_door` — WS2-3; `refuse_coarser_timestamp` unreachable for routed
   doors; a nanosecond value floors on D21 (today it refuses with an internal error).
4. `a_plain_update_from_add_months_stores_session_midnight` — WS2-4, New York, Spark's `1769922000000000`.
5. `the_staging_sink_pairs_on_every_branch` — WS2-5.
6. `a_nested_zone_pair_in_an_assignment_names_the_seam` — site B text: `nested_leaf_cast_sql` output contains
   `__repark_zone_seam__` and no `arrow_cast` for a nested zone pair; unchanged for other pairs.
7. The existing `ntz_into_ltz_nested.rs` refusal pins stay green (WS2-2): the ZS-9 cells still refuse.
8. The three `test_write_spine_1_*.py` files and `test_zone_seam_1.py`, `test_zone_oracle_1.py` stay green.

## 4. Steps

0. **Sketch, then halt by design.** Read: this order; eval §2a–2d and §3's door table; the card's ask; C-007,
   C-008, C-016; the files of §2 at the lines named (correct them). Write `task/wo/write-spine-1/sketch-step-2.md`
   (committed, your one commit of this round): (a) every door of `WriteDoor::ALL` with the point where its rows
   become the sink's batch and which of the three rules it applies today and where (card ask 1); (b) the stage's
   signature and where each door calls it; (c) the WS2-6 mechanism; (d) the 1,512 store-assignment marker cells
   classified from the census fixtures: how many are EQUAL (Spark refuses too) and how many REFUSED (Spark
   stores), by door; (e) the cells you expect to move under WS2-8, by row and count, from the fixtures; (f) the
   lines of `ntz_store.rs`, `store_overflow.rs`, `update_cast.rs` that are deleted; (g) questions as premise +
   lean. Hand back. The orchestrator rules; the build continues in this lane on the rulings.
1. Build once (native module and `cargo build --tests` for `repark-iceberg`, `repark-spark`, `repark-sql`,
   `repark-core`).
2. Slice 1, the zone rule: `conform_stage.rs` with the leaf and nested zone conversion; route D3, D4, D7, D21;
   site B text; delete the replaced code. Pins 1 (for these doors), 2, 6, 7. Commit.
3. Slice 2, the floor: the ns kernel through the stage for every routed door; D9, D10 plans through the stage.
   Pins 1 (complete), 3. Commit.
4. Slice 3, pairing and the fold: WS2-5, WS2-4. Pins 4, 5. Commit.
5. Census replay on the head in the background (`--main --base <sha> --out <scratch>/census-head.json`), diff
   against the main fixture per WS2-8; then the WS2-9 mutant replay of the 976 cells (`--only` the ZS-9 ids),
   mutant reverted. Gates §5. Pin 8.
6. `handback-notes.md` (one line per touched file; the measurement sentences for the ledger rows of §7; the
   sentence for R-011's and R-015's dated status lines; the card's step-2 line) and `handback.json`.

## 5. Gates and the line that means green

comment grep empty · `make rust-fmt-check rust-clippy rust-panic-ban` exit 0 · `python3 scripts/check_rust_file_size.py`
clean, no ceiling changed · `scripts/check_crate_dag.sh` clean, no new edge · `cargo nextest run -p repark-iceberg -p
repark-sql` all passed · `cargo nextest run -p repark-spark -E 'binary(ntz_into_ltz_nested) | binary(ntz_nested_refuse) |
test(nested_ns) | test(store_kernel_registry) | test(insert_overwrite) | test(merge)'` all passed · pytest pin 8 files
passed (live tier skipped) · census diff: every moved cell ends EQUAL, zero EQUAL→other, zero ZS-9 cell moved ·
WS2-9 table present.

## 6. Halts

H1 model id not `claude-opus-5-5` · H2 a door the stage cannot reach without a public signature change outside the
workspace (card: parks the unit for the owner) · H3 a census move that does not end EQUAL, or an EQUAL cell that
leaves, or a ZS-9 cell that stops refusing · H4 a ceiling that would have to rise · H5 a rule that needs a second
site after the stage exists (an architecture finding: hand back, do not build around it) · H6 the hour budget.

## 7. Hand-back and the ledger rows this step opens (clerk writes them)

`handback.json` per `COMMON.md` with the move table, the WS2-9 table, the mutant table (one per site, one per
rule) and `doors_not_touched` (D18 and why; D14 if the bare session has no Iceberg sink; any other).
Rows: C-017 one stage, every routed door, fail closed (pins 1, 2); C-018 the floor through the stage and the sink
guard unreachable (pin 3); C-019 site B names the seam (pin 6); C-020 the fold (pin 4); C-021 the staging sink
pairs on every branch (pin 5); C-022 never worse: the census recount; C-023 step-4 evidence: the WS2-9 table,
OPEN until step 4; C-007 and C-008 close (fail-open paths gone); C-016 closes (the stage sits after the record
points; the two rows stand as measured, unchanged by design).

## Outcome (2026-10-25)

Built in three slices on `feat/write-spine-1-step-2`: `0db0c3af` (slice 1, the stage and the zone seam), `18baab29` (slice 2, the nanosecond floor through the stage) and `d45eb72d` (slice 3, the staging sink pairs on every branch). The census recount on `d45eb72d` moves 288 cells, every one DIFFERENT to EQUAL and all in family F5, not the sketch's 328: findings F-2 and F-3 keep the 40 nested-literal cells DIFFERENT, as on main. Rulings WS2-25 (the seam rewrites only a column-reading `arrow_cast`; NESTED-LITERAL-SEAM-1 folds before step 4) and WS2-26 (step 4 takes the mutant A+B table, 726 of 976 EQUAL, as its input) are recorded in the step (2) section of the [write-spine-1 ledger](../../ledgers/staging/write-spine-1-ledger.md), with rows C-017 to C-024. Status: pending verification.

Fix round 1 (2026-10-25, commit `c3f22afb`): PR #1189's CI red on the D4 PARTITION path (113 facade cells and one Rust pin, outside the census) is closed by restoring the ice-struct-pairing-1 C-031 carve-out on that site, the partition path's own refusal text and the arity-first order (WS2-27 to WS2-29); the 106 carve-out cells stored Spark's rows before the fix and are carded as PARTITION-PAIRING-CARVE-OUT-CLOSE-1.

Verification round 1 (2026-10-25, Muse MAX critic on head `5ce6524c`): FAIL with one S1 (a date into a MAP TIMESTAMP key, C-026, carded NESTED-MAP-KEY-ZONE-1) and two S2 (the P25 partitioned UPDATE under F-3, and the D19/D20 refusal class, C-027), all pre-existing; ruling WS2-30 leaves the step unchanged and records and cards the gaps.
