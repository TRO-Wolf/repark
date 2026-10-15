# ICE-STRUCT-PAIRING-1 (U-6, R-016): one rule pairs struct fields on every write door     grade: A   engine: Claude Opus 5.5 high, self-directed   release: 1.5.6

**Status:** order, dispatched for the build on 2026-10-14 after step 0 and the owner's answers
of the same day (§1); build round 2 dispatched the same day after questions 47 and 48. Written by Claude (claude-opus-5-5), lane B of the Iceberg closeout, on
2026-10-13, from registry row ICE-TSNS-SQL-1-R-016 (audit item PA-07), ledger clause C-037 and
question Q8 of `task/ledgers/staging/ice-tsns-merge-wall-1-ledger.md`. Main at writing:
`018d0efd`. The sketch is part of this order: where the order says §n it means the sketch's
section n.

## 0. Why, and what is out of scope

Which source field feeds which field of a struct column depends on the cast that happens to
run on a door's route, so RePark differs from Spark 4.1.2 in 279 of 512 measured cells on each
format version and stores a NULL nobody wrote in 48 of them (§2). The owner ruled on
2026-10-13 that a silent NULL for a renamed field disqualifies the v2 label. This unit puts
one rule, `pair_store_value`, in front of every cast on the write path (§4).

Out of scope, by name: any leaf conversion (R-011, R-015, ICE-TSNS-NESTED-1 and its gate
`refuse_nested_ns_supply`, which stays first and unchanged); `UPDATE` with no `WHERE`
assigning a nested column (R-014; list L2 of §6); the `VALUES` spellings that fail before any
store (R-018; list L1); R-017 and ICE-TSNS-TYPE-PLANNER-1; whether a nested leaf pair is
store-assignable on the INSERT doors; `CREATE TABLE … AS SELECT` and `REPLACE TABLE … AS
SELECT`, which pair nothing; an explicit `CAST` of a struct in a query; the cost of the
`INSERT … BY NAME` route. No plan walk, no list of statement spellings, no marker function, no
reading of how a value was produced: the unit is refused at review if one appears.

**Scope change (owner, 2026-10-14, rulings 56(a) and 57(a) of §1).** Whether a nested leaf
pair is store-assignable is now in scope on every door: after pairing (by position or by
name) each leaf pair goes through the store-assignment judge flat columns use, at every depth
and inside arrays and maps, and a leaf Spark refuses refuses with Spark's condition. The judge
is reused, not written twice. `INSERT INTO t (cols) VALUES (…)` with a column list pairs by
position, as every other `INSERT` door does. The out-of-scope item "whether a nested leaf pair
is store-assignable on the INSERT doors" above is struck by 56(a) on 2026-10-14; the rest of
the list stands.

## 1. Rulings already made

- **Owner, 2026-10-13:** Iceberg is the priority until the v2 and v3 labels read PRODUCTION;
  a silent NULL for a renamed field disqualifies the v2 label.
- **Owner, 2026-10-13:** R-016 targets 1.5.6 if the sketch names one seam, else 1.5.7. The
  sketch names one; halts H2, H3, H4 and H7 of §8 are the events that undo that.
- **Owner, 2026-10-13:** where the rule cannot be met, the fallback is a refusal with a hint,
  never a store. One rule at one conform point, decided by the door's mode and the two struct
  types. Cost medians within 1.05 of main.
- **Spark's rule (§2, measured 2026-10-09 and 2026-10-13):** by position on `INSERT … VALUES`,
  `INSERT … SELECT`, `INSERT OVERWRITE`, `REPLACE WHERE`, `insertInto` with and without
  overwrite; by name on `INSERT … BY NAME`, every MERGE arm, `UPDATE`, `writeTo().append()`,
  `overwritePartitions()`, `saveAsTable` (append). The same at every depth and in every
  container. By name follows `spark.sql.caseSensitive`.
- **Lessons 2026-10-12 and 2026-10-13** (`task/lessons.md`): rounds are counted against the
  finding class; a census cell's expected category comes from the harness; a provisional
  decision ships with a revert-alone run and its pasted test count.
- **Sketch Q1, Q2, Q3 (owner, 2026-10-14, triage questions 13 to 15 and 36):** adopted at the
  leans. Q1: follow Spark with no switch; the 1.5.6 note carries one line for the doors that
  change what they store (`task/roadmap/mid-term/v1-5-6-release-notes-draft.md`). Q2: see the
  question-35 ruling below. Q3: the Rust `append` API and the streaming sink refuse an unpaired
  struct with hint H-1 in 1.5.6; a mode of their own only when a user asks.
- **Step 0 (2026-10-14, `step-0.md`):** H1, H2, H3, H5 not met; H4 and H7 met; the owner ruled
  both on the same day, so the unit goes on with one seam.
- **Owner, 2026-10-14, question 34 (H7, the static-partition overwrite route):** that route
  (`crates/repark-spark/src/insert_overwrite.rs`, `execute_partition_overwrite`) stages through
  `zone_stores` and then S3, with no `conform_types` and no `wrap_store_outputs` call. U-3
  (R-009, PR #1059) fixed its nanosecond floor inside `zone_stores`, a top-level timestamp
  projection; it did not route the door through the common store stage. So the guard ships:
  one check, the H-1 error, a hint that names the route and the workaround (write the struct in
  the table's field order), and a dated row in `docs/spark-sql-iceberg-parity.md`. No second
  pairing site on that route.
- **Owner, 2026-10-14, question 35 (H4):** register `StorePairKernel` as the store kernels are
  registered and add its name to R-013's row, on three conditions: one registration table holds
  every by-name store kernel; a session-build pin proves it; R-009's retirement sketch covers
  every name on R-013's row.
- **Owner, 2026-10-14, question 37:** R-022 (`mergeSchema` surplus struct field dropped; Spark
  answers `EXTRA_STRUCT_FIELDS`) is in this unit's class: one fixture cell on the head; if the
  seam closes it, the row closes here with its pin, otherwise the hand-back says so and it gets
  a card. R-021 (case-twin struct fields under BY NAME; Spark answers `AMBIGUOUS_COLUMN_NAME`)
  has its own card; if hint H-2 closes it, say so with the cell, but the unit does not grow for
  it.
- **Stacked on U-3 (2026-10-14):** the branch merges `fix/ice-tsns-floor-doors-1` at
  `fb325e57`. After U-3, S1 (`conform.rs`, `conform_batch_scoped`), S2 (`overwrite.rs`) and S3
  (`partition_overwrite.rs`) all go through `conform::strict_store_cast`, U-3's provisional
  guard (owner lean A, pending). The H-1 guard may then be one check there for all three casts.
  If the owner strikes lean A, U-3's revert restores the three casts and the guard goes back to
  the three places §2 lists. Merge `origin/main` once U-3 lands; never rebase.
- **Build round 1 (2026-10-14, Opus lane A) halted** on two §6 rules: 18 cells of list L2
  moved (refusal to refusal, none stores), and the ANSI door's struct MERGE refused
  `Invalid function '__repark_store_pair__'` where main stores, because the shared MERGE and
  UPDATE text names the kernel and that door does not register it. Round 2 continues from
  `6b59b5b2`; the round count for the class "a struct field paired by a rule other than
  Spark's" is build round 2, and the rest of §4 (steps 5 and 6) runs inside it.
- **Owner, 2026-10-14, question 47 (one registration table):** one table of the by-name store
  kernels (the pairing kernel and the integer store casts) in
  `crates/repark-iceberg/src/write/store_cast.rs`, called by both doors: the Spark door's
  `SparkExtension::register` and the ANSI door's `AnsiDialect::on_session_built`. The
  nanosecond wall kernels stay in `repark-functions` and are listed in one constant that the
  pin reads. The pin proves that every name on R-013's row resolves on both doors at session
  build; it is the gate for U-1's kernel retirement later. `repark-sql/src/dialect.rs` and
  `repark-spark/src/extension.rs` join §2. One table owning the wall kernels too is not a U-6
  matter.
- **Owner, 2026-10-14, question 48 (list L2):** L2 is re-pinned. The 14 cells that now give
  Spark's refusal pin it; the 4 that moved from one internal error to another are listed by
  name in R-014's row as still internal, so the row records that they are not refusals with a
  hint. R-014 stays open for the shapes Spark stores. The `is_flat` return of
  `refuse_unless_write_store_assignable` stays. The H-1 guard is one check inside
  `conform::strict_store_cast` for S1, S2 and S3.
- **Verifier 1 (2026-10-14, fresh Opus, head `e3fc32ab`) returned FAIL** with findings F-01 to
  F-09. The owner ruled the same night (2026-10-14), rulings 54 to 59 below. The round count
  for the class "a struct field paired by a rule other than Spark's" is build round 3, and
  there is no round 4: if the round-3 re-verifier is not green, U-6 parks.
- **Owner, 2026-10-14, ruling 54 (recorded only; no code):** the NS-ENCODED-NESTED-FLOOR-1
  card owns the MERGE refusals.
- **Owner, 2026-10-14, ruling 55 (a):** round 3 on lane A, scoped to F-03, F-04, F-05, F-07,
  F-09, then one scoped re-verifier.
  - **F-03:** "match Java's equalsIgnoreCase, per character, upper-case then lower-case
    compare, pinned with É/é and ẞ/ß. Not a full Unicode fold." Two names match when they have
    the same length in UTF-16 code units and, per position, the chars are equal, or their
    `Character.toUpperCase` are equal, or the `Character.toLowerCase` of those upper-cased
    chars are equal. One Rust function, used everywhere by-name struct pairing compares names
    under `spark.sql.caseSensitive=false`. É/é, ẞ/ß and one pair Java does not match (`ß`
    against `SS`) are measured on live Spark and pinned.
  - **F-04:** "refuse loudly if storing needs any new seam; otherwise store as Spark does. The
    in-order silent no-op on main gets its pin either way." `UPDATE t SET st = (SELECT st FROM
    src) WHERE …`, with the source struct reordered and in order: each either stores Spark's
    value or refuses loudly, and is never a success that writes nothing. Both pinned.
  - **F-05:** the UPDATE refusal reads the session's `spark.sql.caseSensitive` instead of a
    fixed `StructPairing::by_name(false)`; the verifier's 8 cells give Spark's condition.
  - **F-07:** Rust tests that kill mutants O1 (ambiguity check off) and O5 (passthrough drops
    caseSensitive), each shown red under its mutant.
  - **F-09:** the red-on-main count is re-measured on main's module and reported in the
    hand-back (566 by the verifier's count, against 548 in the PR body). The orchestrator owns
    the PR body.
- **Owner, 2026-10-14, ruling 56 (a):** "Every paired field passes the same store-assignment
  check flat columns use. The order records the scope change with today's date." After pairing
  (by position or by name), each leaf pair goes through the flat columns' store-assignment
  judge, so a leaf Spark refuses (`CANNOT_SAFELY_CAST` and the rest of Spark's
  store-assignment refusals) refuses with Spark's condition, at every depth and inside arrays
  and maps. This closes F-01's 4 regressions, its 24 and its 33. The judge is reused; no second
  one is written. The scope change is recorded in §0.
- **Owner, 2026-10-14, ruling 57 (a):** "Column-list VALUES pairs by position." `INSERT INTO t
  (st, id) VALUES (…)` takes the positional mode like every other INSERT door (F-02's three
  repros). The scope change is recorded in §0.

**Exception round (owner, 2026-10-15, rulings 73 and 74).** Ruling 73 (a): "as the one named
exception to ruling 55, on three conditions: the two failures are named in the hand-back with
their shape before the round opens; both are inside the pairing class, or the lane parks; Muse
xhigh builds, the Opus verifier re-runs those cells and the four mutants only. A FAIL from that
verifier parks U-6 with no further round and the v2 date is re-planned the same day." Ruling 74
(a): "An UPDATE that reports success and writes nothing is a silent failure. The whole family
stores Spark's value or refuses loudly, in-order shapes included." The scoped re-verifier of
build round 3 returned FAIL on `5b97a250` (2026-10-15 02:15); the two failures, re-measured
2026-10-15 on the head module byte-identical to `verify2/out/Q-*.json` and `R2-*.json`, are:
V2-01, `UPDATE t SET st = (SELECT st FROM t_src) WHERE true` (also `WHERE id IN (0, 3)`, no
`WHERE`, and `(SELECT st FROM t_src WHERE id = 1)`), reordered and in-order source: head
succeeds and writes nothing where Spark stores `{a:1,b:2}` in every row, main refusing the
reordered shapes loudly and the same silent success in order; V2-02, `INSERT INTO t VALUES (1,
named_struct('b', 2, 'a', 1)), (2, named_struct('x', 7, 'y', 8))` (also with the `(id, st)` and
`(st, id)` lists): head stores by position where Spark refuses
`[INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE]` with nothing written, main refusing.
Scope of the exception round: V2-01's family, V2-02, Rust pins that kill the four surviving
mutants of verdict finding V2-03 (`V2_L1`, `V2_N3`, `V2_N4`, `V2_N5`), the ruling 75 note and the
ruling 76 row edit.
- **Owner, 2026-10-14, ruling 58 (a), as a dated carve-out row:** "both PARTITION spellings, 38
  cells per route, refusal with a hint naming the workaround (fields in table order), closed by
  the routing unit from question 34(b)." The static-partition row
  (`ICE-STRUCT-PAIRING-1-STATIC-PARTITION`) in `docs/spark-sql-iceberg-parity.md` is rewritten
  as that carve-out, dated 2026-10-14: both spellings named, the 38 cells per route (v2 and v3)
  counted, the hint text quoted, the closing unit named. If the hint on either spelling does not
  name the workaround, fixing it is in scope.
- **Owner, 2026-10-14, ruling 59:** "Closes inside 56(a). Any of the 54 refusals still carrying
  raw Arrow text after the fold is listed by name in the hand-back."

## 2. Files

Ceilings are those of `scripts/check_rust_file_size.py` (default 1,000 lines; the script's
exception list for the rest). Every `.rs`, `.py` and `.toml` change updates its directory's
`map.md` in the same commit.

| Path | Change |
|---|---|
| `crates/repark-iceberg/src/write/struct_pairing.rs` | new: `StructPairing`, `pair_store_value`, `StorePairs`, `StorePairKernel`, the Spark texts, the in-module table test |
| `crates/repark-iceberg/src/write/store_overflow.rs` (949 lines) | `pairing` in `ConformStoreFlags`; `conform_one_store_expr` calls the kernel; `wrap_store_outputs` takes the mode; `transform_store_dml` reads the statement's mode. Split first if the ceiling is near |
| `crates/repark-iceberg/src/write/update_cast.rs` | `store_assignment_cast_sql` renders the kernel; the struct and map arms of `incompatible_nested_message` and `struct_member` deleted |
| `crates/repark-iceberg/src/write/store_assign.rs` | `struct_fields_store_assignable`, `map_entries_store_assignable` and the `is_flat` return replaced by the seam |
| `crates/repark-iceberg/src/write/conform.rs`, `overwrite.rs`, `partition_overwrite.rs` | the guard of hint H-1 before the three casts |
| `crates/repark-iceberg/src/write/merge/insert.rs`, `predicate_dml.rs` | pass by name to `wrap_store_outputs` |
| `crates/repark-iceberg/src/write/store_cast.rs` | the one table of by-name store kernels and the function that registers it (question 47) |
| `crates/repark-sql/src/dialect.rs`, `crates/repark-spark/src/extension.rs` | each door calls the table's registration at session build (question 47) |
| `crates/repark-spark/src/spark_ast.rs`, `router.rs`, `insert_by_name.rs`, `insert_overwrite.rs`, `insert_overwrite/store.rs`, `append_with_options.rs`, `router/insert_positional/replace_where.rs` | carry the door's mode to the conform |
| `crates/repark-spark/src/merge/nested_assign.rs`, `merge/nested_assign/expand.rs`, `merge/nested_assign/render.rs` | deletions of §5 |
| `crates/repark-spark/src/insert_by_name/nested.rs` | deleted |
| `python/repark/src/repark/spark/dataframe/` (the writers that call `run_through_temp_view`) | `source_by_name=True` for `append`, `overwritePartitions`, `saveAsTable` (append); its own commit |
| `python/repark/tests/_record_ice_struct_pairing_1.py`, `ice_struct_pairing_1_spark_oracle.json`, `ice_struct_pairing_1_main.json`, `test_ice_struct_pairing_1.py` | new: the fixture of §7 |
| `crates/repark-spark/tests/struct_pairing_doors.rs` | new |
| `docs/spark-sql-iceberg-parity.md`, `task/ledgers/staging/ice-struct-pairing-1-ledger.md`, `STATUS.md` | row R-016 closed with its pins; the unit ledger; the status line |

## 3. Skeletons

Grade A: none beyond the signatures of §4, which are binding.

## 4. Steps

0. **Measure, no product code.** Commit as `step-0.md` beside this order:
   (a) the route table: for each of the 16 doors of §2 plus `overwrite(condition)`, the
   functions from the door's entry to the batch cast it ends in (S1, S2 or S3), and which of
   the two call sites of §4 it passes; (b) the eight cast-holding files §3 did not read, each
   cleared or reported; (c) the probes of H2, H3, H4, H5 and H7 with their printed results;
   (d) on both engines, the cells §11 owes: `overwrite(condition)`, a write with
   `mergeSchema`, two source fields that differ only in case, a required struct field;
   (e) whether `refuse_insert_source_types` holds both types for list L1; (f) the write-cost
   medians of main on the write set under `docs/perf/`, as the bar for step 5. Any halt of §8
   ends the round here.
1. **Fixture, red.** The recorder, the two JSON files, `test_ice_struct_pairing_1.py` and
   `struct_pairing_doors.rs`, with step 0's added cells. Expected red on main: 460 cells plus
   the added ones; paste the count. Commit.
2. **The seam, no caller.** `struct_pairing.rs` with its table test. The fixture count does
   not move. Commit.
3. **The callers and the deletions.** The two call sites, the three guards, the mode from
   each door, the deletions of §5, the gate change of S8. The fixture goes green outside L1
   and L2; L1, L2 and the 233 unchanged cells answer as recorded. Commit.
4. **The facade flag.** `source_by_name=True` on the three by-name DataFrame doors. Commit.
5. **Proof.** The nine mutant families of §9, each with the cell it turns red. The two
   revert-alone runs of §9 on throwaway branches, with pasted test counts. The cost medians
   against step 0's bar. The `EXPLAIN` byte-compare of H6.
6. **Records.** Row R-016 rewritten as closed with its pins; rows R-014 and R-018 gain the
   cells of L2 and L1; the unit ledger with one clause per sentence of the rule of §4, each
   pinned; `STATUS.md`; the map rows. Commit.

## 5. Gates and their expected output

| Command | Green means |
|---|---|
| `make verify` | exit 0 |
| `make preflight` | exit 0; the facade suite includes `test_ice_struct_pairing_1.py` |
| `cargo test -p repark-iceberg struct_pairing` and `cargo test -p repark-spark --test struct_pairing_doors` | every test passes |
| `python3 scripts/check_rust_file_size.py` | `files clean` |
| `make check-docs-links check-docs-compaction check-ledger-grammar`, `make spell-check` | exit 0 |
| `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| the 842-cell Spark and Iceberg matrix, live; the parity-live job on the branch | no cell moved outside §2's doors |
| write-cost medians on the write set | each within 1.05 of step 0's bar |

One cargo build at a time, `CARGO_BUILD_JOBS=6`, inside `repark.slice`; probes under
`ulimit -v 67108864`, in the foreground. CI green on the branch before any verifier. Then one
fresh Opus 5.5 high verifier, scoped to the fixture, the mutants and the gates above. A FAIL
that names a route the guard refused is one fold; a FAIL that shows a value stored by a rule
other than §4's parks the unit, and the owner hears first.

## 6. Halt rules

Stop, commit what is green, and hand back with the evidence on any of: H1 to H7 of §8; a
fixture cell whose Spark answer differs between two recordings; a need for a plan walk or a
list of spellings to make a cell green; a control cell (the 233, L1, L2) that moves; a change
needed in a product file §2 does not list; a second seam.
A decision the order does not make is a halt with the question, its premise and a lean, never
a choice made in the round.

## 7. Hand-back

One JSON object: `status` (`CONCLUDED`, `HALT` or `BLOCKED`); `head`; `commits` (full sha and
subject, in order); `fixture` (`red_on_main`, `green_on_head`, `l1`, `l2`, `controls`);
`mutants` (family, the cell that died); `revert_alone` (the two test counts); `cost` (each
median over its bar); `gates` (command and exit code); `halts` (letter and evidence);
`questions` (id, class, question, premise, lean); `out_of_scope_observed`.

Standing rules inherited from [TEMPLATE.md](../TEMPLATE.md): no code comments from Anthropic
models and relocated code sheds its comments; map.md lockstep; the scratch clone, never the
live worktree; never push, never open a pull request; the TRO-Wolf identity and exactly one
trailer, `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`.
