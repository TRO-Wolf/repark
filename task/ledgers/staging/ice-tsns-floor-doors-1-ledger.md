# Unit ledger — ICE-TSNS-FLOOR-DOORS-1 · one floor on every door into a microsecond column

**Date:** 2026-10-14 · **Branch:** `fix/ice-tsns-floor-doors-1` · **Base:** `018d0efd` ·
**Model:** step 0 Muse Spark (muse-spark-1.3-contributor); the build Claude (claude-opus-5-5),
lane A · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: the next bug release (1.5.6).

**Order:** [../../../task/wo/ice-tsns-floor-doors-1.md](../../../task/wo/ice-tsns-floor-doors-1.md)
(U-3, R-009) from registry row ICE-TSNS-SQL-1-R-009 and ledger question Q2 of
`ice-tsns-merge-wall-1-ledger.md` (claim C-014). **This ledger closes when the unit's pull request merges.**
Step 0 changed no product file. The build (2026-10-14) runs on main by the owner's rulings
of that day, not on the type-planner head: one deciding function (`store_micros`), two
router arms, the fixture red first, the merge-wall re-pin and these records. The provisional
guard (lean A) is built and measured but held off this branch by the halt on a file outside the
sketch's list (Q6): it sits on the local branch `probe/floor-doors-guard`, with its revert-alone
run on `probe/floor-doors-guard-revert`, neither pushed. Built on the orchestrator's leans A to
C, pending the owner's word (§Q).

**Why.** Into a microsecond `TIMESTAMP` column, INSERT floors a nanosecond
source and reads a naive wall in the session zone, while the overwrite doors
truncate toward zero and read the wall as UTC; into `TIMESTAMP_NTZ` every
door truncates. Step 0 re-records the control cells on main's release module
with the rule-derived references beside each observation, and names the seam
where the narrowing is decided.

## PROPOSITION LEDGER — ICE-TSNS-FLOOR-DOORS-1 — 2026-10-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The door matrix on main is recorded: 216 combinations, 2376 ticks, with harness-derived floor and session-zone references beside each observation. | The recorder and its evidence files. | **PROVEN** | `python/repark/tests/_record_ice_tsns_floor_doors_1_main.py`; `task/wo/ice-tsns-floor-doors-1/evidence/main-cells.json` (216 cells) and `main-cells.md` (counts by door and target). pins: ice-tsns-floor-doors-1/C-001 |
| C-002 | On `TIMESTAMP` targets the append class equals INSERT's floor on every tick, and the four overwrite doors differ wherever truncation or the UTC reading differs. | The count table of `main-cells.md`. | **PROVEN** | `main-cells.md`: INSERT, MERGE update arm, UPDATE on v3, `writeTo().append()`, `insertInto` append 66 of 66 EQUAL per version; INSERT OVERWRITE, MERGE insert arm, `overwritePartitions`, `insertInto(overwrite=True)` 28 EQUAL, 16 truncated, 14 wall-as-UTC, 8 both per version. pins: ice-tsns-floor-doors-1/C-002 |
| C-003 | On `TIMESTAMP_NTZ` targets every door, INSERT included, truncates toward zero. | The count table of `main-cells.md`. | **PROVEN** | `main-cells.md`: every door except v2 UPDATE 42 EQUAL, 21 truncated, 3 other per version; the other trio is the Kolkata `timestamptz_ns` pre-epoch set (truncation before the zone shift). pins: ice-tsns-floor-doors-1/C-003 |
| C-004 | The registry row's 72 cells reproduce on main. | The collapsed overlap of `main-cells.md`. | **PROVEN** | `main-cells.md`: the 48 collapsed combinations (4 doors x 2 sources x 3 zones x 2 versions; the 3 MERGE spellings collapse to one arm) show INSERT all EQUAL and every overwrite door differing; the split holds. pins: ice-tsns-floor-doors-1/C-004 |
| C-005 | The seam list names each door's narrowing expression and wall-localisation site with file and line, cross-checked by EXPLAIN. | `seams.md` with the plan pastes. | **PROVEN** | `task/wo/ice-tsns-floor-doors-1/evidence/seams.md`: N1 the RePark floor-plus-zone kernel, N2 the write-side Arrow cast, N3 the store-assignment cast; INSERT-family EXPLAIN pasted; MERGE EXPLAIN refused by the router and DataFrame writers have no SQL EXPLAIN, so those doors rest on code plus values. pins: ice-tsns-floor-doors-1/C-005 |
| C-006 | There is one seam where every door's narrowing is decided, by type. | The sketch's one-chokepoint proposal and the build of C-009 and C-010. | **PROVEN** | Step 0's three narrowing points were three places that made a tick; the sketch traced every door to one of them and proposed one function body. The owner gave the go on 2026-10-14 (Q1 answered). On the head every fixture tick is made by `store_micros`: 0 red of 26928 (§6), and mutants M1 to M4 in that one function move cells on every door family (§7). pins: ice-tsns-floor-doors-1/C-006 |
| C-007 | Step 0 measures only the listed surface and changes no product file. | The diff and the not-measured list of §4. | **PROVEN** | §4; `git diff --stat origin/main HEAD -- 'crates/*/src' python/repark/src` prints nothing. pins: ice-tsns-floor-doors-1/C-007 |
| C-008 | The build's fixture derives every expected tick from the recorder's rule and is red first on main's release module. | `test_ice_tsns_floor_doors_1.py` run on main's module, with a tick count per door and target. | **PROVEN** | §5: 180 of 246 tests red on main; 4400 of 26928 ticks red, 1976 `TIMESTAMP` and 2424 `TIMESTAMP_NTZ`, every one from a nanosecond source; the step 0 subset is red on 712 ticks (304 and 408), as the sketch counted. pins: ice-tsns-floor-doors-1/C-008 |
| C-009 | One function, `store_micros` in `timestamp_ns_cast.rs`, makes the tick of every narrowing into a microsecond `TIMESTAMP` or `TIMESTAMP_NTZ`, by source type, target type and session zone; its callers are the LTZ kernel and `ntz_single`. | The seam commit, the fixture's NTZ slice on it, and the mutants of §7. | **PROVEN** | §6: on the seam commit every `TIMESTAMP_NTZ` cell that reaches `ntz_single` and the NTZ read are green; §7: mutants M1 to M4 die on named fixture tests. pins: ice-tsns-floor-doors-1/C-009 |
| C-010 | The two router predicates in `ntz_store.rs` send every staged door's nanosecond source to `store_micros` by type: `needs_ltz_instant_cast` accepts `Timestamp(Nanosecond, _)`, `wall_kernel_reads` a naive nanosecond source for a `TIMESTAMP_NTZ` target. | The routers commit, the whole fixture on it, and the mutants of §7. | **PROVEN** | §6: 0 red ticks of 26928 on the routers commit; no microsecond-source cell and no INSERT value into `TIMESTAMP` moved from main (H3 not hit); §7: mutants M7 and M8 die by value. pins: ice-tsns-floor-doors-1/C-010 |
| C-011 | The merge-wall control cells of a nanosecond source into a microsecond target move from main's recording to the rule; no other control cell moves. | The re-pin commit; the unchanged control test run on the routers commit. | **PROVEN** | §6: unchanged, the control test fails 47 tests (94 cells) on the routers commit; re-pinned, all 988 tests of the fixture and the three neighbours pass. 94 of main's 152 nanosecond control cells moved (24 `TIMESTAMP`, 70 `TIMESTAMP_NTZ`), 46 held the rule, 12 are refusals; the 384 microsecond-source control cells are byte-equal to a fresh recording on main. pins: ice-tsns-floor-doors-1/C-011 |
| C-012 | Write cost stays within 1.05 of main. | Ratios of five interleaved set medians on the write set, same build profile both sides. | **PROVEN** | §8: every ratio at or below 1.038; the overwrite doors fell to 0.75 to 0.79 of main. pins: ice-tsns-floor-doors-1/C-012 |
| C-013 | The 1.5.6 release note carries one line per stored-value change, for a reader who will not open the registry, and registry row R-009 is FIXED with its pins. | The notes draft and the registry row. | **PROVEN** | `task/roadmap/mid-term/v1-5-6-release-notes-draft.md` (three lines: the overwrite doors' wall, the floor on every door, the NTZ read; tables already written keep their values); `docs/spark-sql-iceberg-parity.md` R-009 FIXED 2026-10-14 with the door count restated (ten `TIMESTAMP` doors of the fixture's twenty, all twenty into `TIMESTAMP_NTZ`). pins: ice-tsns-floor-doors-1/C-013 |

## 1. The matrix (C-001)

Zones UTC, America/New_York, Asia/Kolkata. Eleven ticks: -1, -1001, -999, 0,
1, 999, 1001 ns; the New York repeated-hour and skipped-hour walls read as
UTC nanoseconds; int64 minimum and maximum nanosecond instants. Sources
`timestamp_ns` and `timestamptz_ns` from a version-3 source table; targets
`TIMESTAMP` and `TIMESTAMP_NTZ` on versions 2 and 3; nine doors (INSERT,
INSERT OVERWRITE, MERGE insert arm, MERGE update arm, UPDATE from a sibling
column, `overwritePartitions`, `insertInto(overwrite=True)`,
`writeTo().append()`, `insertInto` append). References are derived by the
recorder from the rule (floor `tick // 1000`, cross-domain conversion through
`zoneinfo`); the oracle is ruling Q-21c-8, since Spark cannot read the source
types. Recorded on the release module of main at `018d0efd`.

## 2. The counts (C-002, C-003, C-004)

TIMESTAMP, both versions identically: the five append-class doors 66 of 66
EQUAL (UPDATE on v2 refuses instead, §4); the four overwrite doors 28 EQUAL,
16 truncated, 14 wall-as-UTC, 8 both. TIMESTAMP_NTZ, both versions
identically: every answering door 42 EQUAL, 21 truncated, 3 other. The 72
registry cells reproduce as the 48 collapsed combinations with the split
holding on all of them.

## 3. The seams (C-005, C-006)

Three distinct narrowing points: the RePark kernel (floor plus session zone),
the write-side Arrow cast (toward zero, no zone step), and the
store-assignment DataFusion cast (toward zero, no zone step). The overwrite
doors display the kernel in EXPLAIN and store the cast values, so the seam
lies downstream of the displayed projection. The sketch must say more than
one seam, and the unit halts for the owner under the order's section 6.
Resolved by the sketch and the owner's go (C-006): the three points became one
function, and the doors reach it by type (§6).

## 4. What was not measured (C-007)

No product file changed; no existing test file changed. Not measured: nested
leaves (R-011, R-015); the frame door (R-019); `applyInPandas` (R-020); the
create door; nanosecond targets; MERGE spelling variants beyond the single
insert and update arms; the cost gate; any planner head (U-1 lands first and
the build re-measures the 72 control cells there).

## 5. The fixture, red first (C-008)

`python/repark/tests/test_ice_tsns_floor_doors_1.py`, committed failing. Every
expected tick is `rule_expected` of the step 0 recorder: the floor by `//`,
the naive wall localised in the session zone and the instant shifted to the
session wall through `zoneinfo`. A `TIMESTAMP_NTZ` or `TIMESTAMP` source runs
through the same rule on its nanosecond twin (`micros * 1000`). Run on main's
release module (built from `293896ce`; main has moved by docs and tests
only since):

| Group | Tests | Red tests | Ticks | Red ticks | Expected v2 refusals |
|---|---|---|---|---|---|
| Step 0 doors, nanosecond sources | 108 | 75 | 2376 | 712 (304 `TIMESTAMP`, 408 `TIMESTAMP_NTZ`) | 132 ticks |
| Added doors x four layouts, nanosecond sources | 132 | 102 | 11088 | 3688 (1672, 2016) | 0 |
| Microsecond sources, every door and layout | (same tests) | | 13464 | 0 | 0 |
| Read casts (`TIMESTAMP_NTZ` on its own name; `TIMESTAMP` control) | 6 | 3 (`TIMESTAMP_NTZ`) | | | |
| All | 246 | 180 | 26928 | 4400 | 132 |

Red ticks by door and target, nanosecond sources, every zone, version and layout:

| Door | `TIMESTAMP` | `TIMESTAMP_NTZ` |
|---|---|---|
| INSERT, `insertInto` append, `writeTo().append()`, MERGE update arm | 0 | 48 each |
| UPDATE from a sibling column (v3; v2 refuses) | 0 | 24 |
| INSERT OVERWRITE, MERGE insert arm, `overwritePartitions`, `insertInto(overwrite=True)` | 76 each | 48 each |
| INSERT BY NAME, INSERT OVERWRITE BY NAME, INSERT OVERWRITE (k, v, id), MERGE insert `*`, MERGE insert literal | 304 each | 192 each |
| INSERT OVERWRITE `PARTITION (k = 0)` (partitioned layouts only) | 152 | 96 |
| REPLACE WHERE, `overwrite(cond)`, `saveAsTable` append, MERGE update `*`, MERGE update literal | 0 | 192 each |

H1 restated for main: every door that step 0 recorded 66 of 66 on `TIMESTAMP`
(INSERT, MERGE update arm, UPDATE on v3, `writeTo().append()`, `insertInto`
append) is green here on every tick. Not hit.

## 6. The build (C-009, C-010, C-011)

**Seam commit.** `store_micros(array, target, zone)` replaces `narrow`: a nanosecond
source floors by `div_euclid`; a naive source into an instant target is localised in the
session zone; an instant source into a naive target goes through `unlocalize_to_walls`; the
same domain passes through; a source that is not nanoseconds keeps the Arrow cast it had.
The `Timestamp` arm of `ntz_single` calls it with the `TIMESTAMP_NTZ` type. On that commit's
release module: `TIMESTAMP_NTZ` red falls from 2424 ticks to 408, every one a `timestamp_ns`
source through an overwrite door; the NTZ read is green; `TIMESTAMP` and microsecond cells
do not move.

**Routers commit.** `needs_ltz_instant_cast` gains `Timestamp(Nanosecond, _)` and
`wall_kernel_reads` gains a naive nanosecond source for a `TIMESTAMP_NTZ` target. On that
commit's release module: 0 red ticks of 26928; 132 ticks are the expected v2 refusal; 0
microsecond-source cells and 0 INSERT cells into `TIMESTAMP` differ from main's observed
ticks. The unchanged merge-wall control test fails 47 tests there.

**Re-pin commit.** The control test takes a nanosecond source's ticks from the rule; a
count test pins the 94 moved cells. `ice_tsns_merge_wall_1_main.json` and its recorder do
not change: re-recorded on main's module into scratch, the 384 control cells are byte-equal,
and its 68 nanosecond-target cells that differ from today's main are the `40fc916f` history
`test_main_recorded_the_rule_wherever_it_answered_right` counts (Q3).

## 7. Mutants

Each mutant built as a release module in a scratch clone of the re-pin state and run
against the fixture and the merge-wall module (602 tests, `-n 8`).

| Mutant | Edit | Failed of 602 | Dies on (first named test) | How |
|---|---|---|---|---|
| M1 | `div_euclid` becomes `/` in `store_micros` | 310 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (102), the added-door test (132), both read casts (6), the merge-wall control test (70) | value |
| M2 | the naive-to-instant localisation is dropped | 115 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (34), the added-door test (44), `test_the_ltz_read_cast_of_a_nanosecond_source_floors` (2), the control test (35) | value |
| M3 | the instant-to-wall shift is dropped | 120 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (36), the added-door test (44), `test_the_ntz_read_cast_of_a_nanosecond_source_floors` (2), the control test (38) | value |
| M4 | `ntz_single` gets its two Arrow casts back | 155 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (51), the added-door test (66), `test_the_ntz_read_cast_of_a_nanosecond_source_floors` (3), the control test (35) | value |
| M7 | `Nanosecond` leaves `needs_ltz_instant_cast`, no guard | 72 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (24), the added-door test (36), the control test (12) | value |
| M8 | the naive arm leaves `wall_kernel_reads`, no guard | 48 | `test_every_step0_door_stores_the_floor_in_the_session_zone` (18), the added-door test (24), the control test (6) | value |

"Value" means every failure is a stored tick that differs from the rule; no failing test
raised the guard's internal error (none existed on this state). M5, M6 (the last two with
the guard) and M9 (the guard refuses a widening pair) run on the guard's commit (§10).

## 8. Cost (C-012)

Five interleaved sets (main, head, head, main, ...), each the median of five timed writes
after one warm run, 2,000,000 rows from a `timestamp_ns` source (and one `TIMESTAMP_NTZ`
control) in America/New_York, version 3; main's release module built from `293896ce`, the
head's from this records state, both `maturin develop --release` with 16 codegen units, the
same CPU set. Ratio is the median over the five sets of head over main.

| Write | main ms | head ms | ratio |
|---|---|---|---|
| INSERT into `TIMESTAMP` | 76.8 | 79.7 | 1.038 |
| INSERT OVERWRITE into `TIMESTAMP` | 332.8 | 251.1 | 0.754 |
| INSERT … BY NAME into `TIMESTAMP` | 331.0 | 254.2 | 0.768 |
| MERGE insert into `TIMESTAMP` | 374.3 | 295.3 | 0.789 |
| INSERT into `TIMESTAMP_NTZ` | 56.9 | 53.4 | 0.939 |
| INSERT OVERWRITE into `TIMESTAMP_NTZ` | 257.4 | 244.6 | 0.950 |
| INSERT OVERWRITE into `TIMESTAMP` from `TIMESTAMP_NTZ` | 254.7 | 250.4 | 0.983 |
| `overwritePartitions` into `TIMESTAMP` | 340.5 | 258.0 | 0.758 |

INSERT into `TIMESTAMP` runs the same plan on both sides; 1.038 is the noise of the set.

## 9. What was not measured

Nested leaves (R-011, R-015); the frame door (R-019); `applyInPandas` (R-020); the create
door; nanosecond targets beyond the merge-wall neighbours; streaming sinks; the ANSI door's
own overwrite staging (`crates/repark-sql/src/insert_overwrite.rs`, which never calls
`zone_stores`); format version 2 sources; zones beyond UTC, America/New_York and
Asia/Kolkata in the fixture; the write cost of the provisional guard's commit (a type
comparison per column per batch, not timed); any planner head (U-1 rebases over this unit).

## Gates

Gates run before the final hand-back per the brief; exit codes are recorded there, not here.

## Q. Questions for a ruling

- **Q1 (C-006, OWNER). ANSWERED 2026-10-14:** the owner gave the go on the sketch's
  one-chokepoint proposal (six narrowing functions, one deciding function); the build runs
  on main.
- **Q2 (OWNER, lean A).** Built as leaned, pending the owner's word: the closed batch casts
  raise an internal error when a top-level timestamp would get coarser (`strict_store_cast`).
  It is one commit on `probe/floor-doors-guard` over this branch, held by Q6; its own ledger
  section there carries mutants M5, M6, M9 and the revert-alone count. *Lean:* keep it.
- **Q3 (RULING).** The brief says the 72 control cells of `ice_tsns_merge_wall_1_main.json`
  move and the recorder file moves in the same commit. The JSON holds the New York controls
  only; 94 of its nanosecond control cells move (24 `TIMESTAMP`, 70 `TIMESTAMP_NTZ`); the 72
  is the registry's three-zone count of the six `TIMESTAMP` doors. The JSON and its recorder
  were left unchanged, because a fresh recording on main equals the control cells byte for
  byte and would rewrite 68 history cells another test counts. *Premise:* §6. *Lean:* accept
  the re-pin as built; no recorder change.
- **Q4 (OWNER, lean B).** The read `CAST(ns AS TIMESTAMP_NTZ)` moves with the seam
  (`-1 ns` reads `-1 µs`, main `0`), pinned on its own name
  `test_the_ntz_read_cast_of_a_nanosecond_source_floors` and given its own release-note
  line. *Lean:* keep it; it is the same function as the store.
- **Q5 (OWNER, lean C).** INSERT … BY NAME and INSERT OVERWRITE with a static `PARTITION`
  are in this unit: they truncated and read the wall as UTC on main, and the routers fix them
  with the rest. The registry row's door count is restated (ten of twenty). *Lean:* keep.
- **Q6 (RULING, halt).** The guard shrinks `crates/repark-iceberg/src/write/overwrite.rs` from
  1053 to 1049 lines (its local strict-cast options go), and the exact-baseline file-size gate
  then fails until `scripts/check_rust_file_size.py` and its mirror
  `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` read 1049. Neither file is in
  the sketch's section 3, which the brief makes a halt. *Premise:* `make check-rust-file-size`
  exits 2 with "shrank to 1049 lines below exact baseline 1053". *Lean:* allow the two-number
  ratchet (and the `scripts/` and parity-tests map rows) inside the guard's commit, so the
  guard and its ratchet revert together; then cherry-pick `probe/floor-doors-guard` as the last
  commit.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-tsns-floor-doors-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the order (sections 0, 1, 4 and 6), the sketch (sections 2 to 6), the owner's rulings of 2026-10-14 in the brief and registry row R-009; C-006 closes Q1 by the owner's go; the leans A to C are named as pending in Q2, Q4 and Q5.
      artifacts: [task/ledgers/staging/ice-tsns-floor-doors-1-ledger.md, task/wo/ice-tsns-floor-doors-1/sketch.md]
    - id: AT-2
      status: ATTACKED
      evidence: The fixture enumerates twenty doors, four layouts on the eleven added ones, four targets, three zones, four sources and eleven ticks (26928 ticks), counted per door and target on main, on the seam commit and on the routers commit.
      artifacts: [python/repark/tests/test_ice_tsns_floor_doors_1.py, task/ledgers/staging/ice-tsns-floor-doors-1-ledger.md]
    - id: AT-3
      status: ATTACKED
      evidence: store_micros and the router arms read for unwrap, panics, overflow and casts; the floor is div_euclid on i64 (no overflow), localisation and the wall shift return errors out of range as before; make rust-clippy, make rust-panic-ban and the touched crates' tests run before hand-back.
      artifacts: [crates/repark-functions/src/timestamp_ns_cast.rs, crates/repark-functions/src/timestamp_ltz_ntz.rs, crates/repark-iceberg/src/write/ntz_store.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, lock or async path is added; the change is pure array functions and two type predicates.
    - id: AT-5
      status: N/A
      justification: No credentials, network, user-supplied paths or secret-bearing output are touched.
    - id: AT-6
      status: ATTACKED
      evidence: The fixture was committed red on main (180 tests, 4400 ticks) before any product commit; the mutants of section 7 each die on a named test.
      artifacts: [python/repark/tests/test_ice_tsns_floor_doors_1.py, python/repark/tests/test_ice_tsns_merge_wall_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Release modules built inside the build lock for main, the seam commit, the routers commit, each mutant and the head; the fixture, the three neighbours and the facade suite run on the head; ruff, rustfmt, clippy, panic ban, file-size, map, docs, ledger and comment gates run before hand-back.
      artifacts: [python/repark/tests/test_ice_tsns_floor_doors_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The oracle is ruling Q-21c-8 (floor) and INSERT's session-zone wall; every expected tick is rule_expected of the step 0 recorder, never typed in; Spark 4.1.2 cannot read the source types.
      artifacts: [python/repark/tests/_record_ice_tsns_floor_doors_1_main.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry row R-009 moves to FIXED 2026-10-14 with its pins and the restated door count; the 1.5.6 notes draft carries one line per stored-value change.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/v1-5-6-release-notes-draft.md]
    - id: AT-10
      status: ATTACKED
      evidence: Behaviour-move guards run on the routers commit; 0 microsecond-source cells and 0 INSERT cells into TIMESTAMP moved from main (H3), and the merge-wall control cells that moved are counted (94) with the rest byte-equal to main.
      artifacts: [python/repark/tests/test_ice_tsns_merge_wall_1.py]
  complete: true
```

VERDICT: 13 clauses, 13 PROVEN, 0 OPEN, 0 REJECTED.
