# Unit ledger — ICE-COLUMN-REORDER-1 · `ALTER COLUMN … FIRST/AFTER` column moves on Iceberg tables

## Round 1 (2026-09-17) — run 20b

**Date:** 2026-09-17 · **Branch:** `fix/ice-column-reorder-1` · **Base:** `chore/fork-pin-ice-19b` (RePark PR #665) ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `ICE-COLUMN-REORDER-1` **OPEN** (target FIXED 2026-09-17).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Rating row V2-10b (measured 2026-09-16): `ALTER TABLE t ALTER COLUMN b FIRST`
(and `ALTER COLUMN b AFTER id`) on an Iceberg table refuses on RePark with
`ALTER COLUMN … FIRST/AFTER (column MOVE) without ADD is not supported yet — use ADD COLUMN … FIRST|AFTER for new columns (I6)`,
with no registry row. Spark 4.1.2 + Iceberg 1.11.0 moves the column (Java
`UpdateSchema.moveFirst/moveAfter/moveBefore`: same field ids, new order, a new schema id).
Reading and writing a Spark-reordered table already answer Spark; that stays true.

**Not in this unit:** the fork pin (moves only in its own PR); `STATUS.md`; `briefs/next-sequence.md`;
anything outside the column-move surface.

**Fork position (measured 2026-09-17).** The pinned fork (`75da2b58`) already exposes standalone
`UpdateSchema::move_first/move_before/move_after` pushing `SchemaOp::MoveFirst/MoveBefore/MoveAfter`
(`crates/iceberg/src/transaction/update_schema.rs`), the same builders the positioned `ADD COLUMN`
path chains after `add_column_to`. No table-format API is missing, so no fork work and no HALT on
that ground: the move is expressible in RePark as a schema update with unchanged field ids.

**Writable paths:** `task/ledgers/staging/ice-column-reorder-1-ledger.md`,
`task/ledgers/staging/map.md`, `crates/repark-iceberg/src/write/{alter.rs,map.md}`,
`crates/repark-spark/src/{alter.rs,router.rs,map.md}`, `crates/repark-spark/map.md`,
`python/repark/tests/{test_ice_column_reorder_1.py,test_ice_column_reorder_1_truth.json,map.md}`,
`python/repark/tests/_oracle_pins.py` (only if the shared helper needs a move-aware hook),
`docs/spark-sql-iceberg-parity.md`, `docs/map.md`.

## Plan

- [ ] S0: ledger skeleton, OPEN clauses (this section), COMMIT.
- [ ] S1: clause matrix — Spark's answer and RePark's per clause, both doors.
- [ ] S2: reproduce the refusal on the release native; paste verbatim.
- [ ] S3: Spark oracle recorder + truth JSON, COMMIT.
- [ ] S4: red-first pins `test_ice_column_reorder_1.py`, failures pasted, COMMIT.
- [ ] S5: Rust implementation + release-native rebuild.
- [ ] S6: registry row ICE-COLUMN-REORDER-1; I6 refusal text removed/rewritten; maps lockstep, COMMIT.
- [ ] S7: gates on the release native, counts in the ledger.
- [ ] S8: final COMMIT, log/status/comment-count print.

## PROPOSITION LEDGER — ICE-COLUMN-REORDER-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `ALTER COLUMN c FIRST` moves the column to the front with field ids unchanged and a new schema id, matching Spark 4.1.2 + Iceberg 1.11.0 (DESCRIBE order and `SELECT *` order, both doors). | Truth JSON from live Spark; offline pins vs the truth JSON; live tier replaying Spark. Red-first on the release native. | **PROVEN** | `test_move_first_matches_oracle` green offline (ids `[(3,b),(1,id),(2,a)]`, schema 0 → 1, rows + `table().columns`) and in the live replay. |
| C-002 | `ALTER COLUMN c AFTER x` moves the column to immediately after `x`, ids unchanged, new schema id, matching Spark on both doors. | Same harness as C-001. | **PROVEN** | `test_move_after_matches_oracle` green offline and live (`[(1,id),(3,b),(2,a)]`, schema 0 → 1); e2e `alter_column_move_first_and_after_reorder` in `tests/column_move.rs`. |
| C-003 | Moving a column to where it already is (e.g. first column `FIRST`, `AFTER` its current predecessor) matches Spark's answer — same order, and Spark's schema-id behaviour recorded verbatim. | Truth JSON records whether Spark mints a new schema id on a no-op move; pins assert RePark's commit matches. | **PROVEN** | Spark commits nothing (truth stays v2/schema 0). Round 2: the fork reuses the identical schema by content, so RePark keeps the order and schema id too (`test_noop_moves_keep_schema_id`, ANSI e2e order-stable, Rust round-trip `assert_eq`). The one delta is the metadata file: RePark still writes one where Spark writes none — split into strict-xfail `test_noop_moves_write_no_metadata_file` and OPEN residue ICE-COLUMN-REORDER-1-R-001. |
| C-004 | Moving the first column after the last matches Spark on both doors. | Same harness as C-001. | **PROVEN** | `test_first_after_last_matches_oracle` green offline and live (`[(2,a),(3,b),(1,id)]`, schema 0 → 1). |
| C-005 | `AFTER` naming the moved column itself matches Spark's answer (Spark's error class and message prefix, or its resulting order, recorded verbatim). | Truth JSON records Spark's exact behaviour; pins assert RePark answers the same. | **PROVEN** | Spark refuses (`SparkException: Unsupported table change: Cannot move b after itself`, table untouched). `test_self_move_refuses` asserts `PySparkException` + `Cannot move b after itself` with no new metadata; the Rust round-trip asserts the fork's Java message. Diagnostic delta recorded in ICE-COLUMN-REORDER-1. |
| C-006 | `AFTER` an unknown column refuses loud with Spark's error class and message prefix on both doors. | Truth JSON records Spark's error; pins assert RePark's typed refusal matches the class and prefix. | **PROVEN** | Spark raises `AnalysisException [UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703`. `test_after_unknown_column_refuses` asserts the same class, tag, name and SQLSTATE with no new metadata file. |
| C-007 | Moving an unknown column refuses loud with Spark's error class and message prefix on both doors. | Same harness as C-006. | **PROVEN** | Same shape as C-006 via `test_move_unknown_column_refuses`; ANSI e2e covers the native door's refusal. |
| C-008 | Moving a nested struct field (`ALTER COLUMN s.b FIRST`) matches Spark, or — if not small — refuses with a typed Spark-shaped refusal plus a dated DECLARED registry row. | Small: same harness as C-001 on a struct shape. Not small: typed refusal pin + registry row. Decision recorded here with the measured fork behaviour. | **PROVEN** | Small: the fork validates dotted paths. `test_nested_field_move_matches_oracle` (struct `b:4,a:3`, ids intact, schema 0 → 1) and `test_struct_top_move_matches_oracle` green offline and live; no DECLARED row needed. Round 2 measured three more nested cells against live Spark: short sibling `s.b AFTER a` succeeds (sibling scope, struct back to `a,b`, schema id restored to 0 — the fork reuses it, as Spark does); dotted `s.b AFTER s.a` refuses `[PARSE_SYNTAX_ERROR] … 42601`; cross-struct `s.b AFTER id` refuses `[UNRESOLVED_COLUMN]` naming `` `s`.`id` `` with top-level suggestions. All three pinned offline and in the live tier. |
| C-009 | After the move, `INSERT INTO t VALUES (...)` in the NEW positional order lands rows readable by both engines, and `SELECT *` reads agree in both directions (Spark reads RePark's moved table; RePark reads Spark's). | Cross-read pins in the live tier; offline pins assert RePark's positional insert + read round-trip against the truth JSON. | **PROVEN** | `test_positional_insert_and_select_after_move` green; `test_live_cross_read_moved_tables` green both directions (`(b1,1,a1),(b2,2,a2)`). |
| C-010 | The move works on format-v2 and format-v3 tables. | Truth + pins parametrised over both versions. | **PROVEN** | `test_move_on_v3_matches_oracle` green offline; `first_v3` green in the live replay. |
| C-011 | The move works on a partitioned table where the moved column is a partition source, and reads/writes still answer Spark. | Partitioned shape in the truth JSON; pins cover post-move read + insert. | **PROVEN** | `test_move_partition_source_matches_oracle` green offline; `part_v2` green in the live replay; positional insert lands `(b3,3,a3)`. |
| C-012 | DataFrame door: `spark.table(t).columns` order and `df.writeTo(t).append()` by name after the move match Spark. | Live-tier pins through the facade DataFrame API. | **PROVEN** | `test_dataframe_door_columns_and_append` green offline (`columns == [id,b,a]`, append lands `(9,b9,a9)`); `test_live_dataframe_door_matches_truth` green live. |
| C-013 | Registry row ICE-COLUMN-REORDER-1 (FIXED 2026-09-17, plus DECLARED rows for anything refused) exists, the I6 refusal text no longer fires on supported moves, and every touched `map.md` is current. | `docs/spark-sql-iceberg-parity.md` diff; grep for the old I6 text; `make check-map-sync`. | **PROVEN** | Row landed after DBT-COLCOMMENT-1 (no DECLARED rows needed — nested moves work); I6 move arm deleted (`ALTER COLUMN … COMMENT` refusal stays); maps current (`check-map-sync` clean in the S6 hook). Round 2: row text updated to the Q-20b-5 shape (sibling scope, dotted refusal, no-op file residue) and OPEN residue row ICE-COLUMN-REORDER-1-R-001 filed with fork trigger F-UPDATE-SCHEMA-SAME-1. |
| C-014 | Gates green on the release native: new file offline + live, `cargo test -p repark-spark --lib`, `cargo test -p repark-iceberg --lib`, `make verify`, whole facade suite, whole parity suite. | Counts pasted below in §7. | **PROVEN** | §7 counts, all green. Round-2 counts appended in §7; the pin file docstring now cites C-013 and C-014. |

## 1. Clause matrix (S1) — Spark 4.1.2 + Iceberg 1.11.0, measured 2026-09-17

Base shape `(id INT, a STRING, b STRING)`, ids 1/2/3, seed `(1, 'a1', 'b1')`, fresh table per
case (CREATE = v1/schema 0, seed INSERT = v2). Full record:
`python/repark/tests/test_ice_column_reorder_1_truth.json` via
`python/repark/tests/_record_ice_column_reorder_1.py`.

| Case | Spark's answer |
|---|---|
| `b FIRST` | Moves. Order `[(3,b),(1,id),(2,a)]`, schema 0 → 1 (v3 metadata). DESCRIBE and `SELECT *` both `(b,id,a)`; row `('b1',1,'a1')`. |
| `b AFTER id` | Moves. Order `[(1,id),(3,b),(2,a)]`, schema 0 → 1. |
| `id FIRST` (no-op) | Commits NOTHING: stays v2 metadata, schema 0, order unchanged. |
| `a AFTER id` (no-op) | Commits NOTHING: stays v2 metadata, schema 0. |
| `id AFTER b` (first after last) | Moves. Order `[(2,a),(3,b),(1,id)]`, schema 0 → 1; row `('a1','b1',1)`. |
| `b AFTER b` (self) | Refuses, table untouched (v2/schema 0). `Py4JJavaError` wrapping `org.apache.spark.SparkException: Unsupported table change: Cannot move b after itself`. |
| `a AFTER nope` | Refuses, table untouched. `AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name \`nope\` cannot be resolved. Did you mean one of the following? [\`id\`, \`a\`, \`b\`]. SQLSTATE: 42703`. |
| `nope FIRST` | Same `UNRESOLVED_COLUMN` refusal as the unknown reference. |
| Positional INSERT after move | `INSERT ... VALUES ('b2', 2, 'a2')` in the NEW order lands `('b2',2,'a2')`; schema stays 1 (v4 = new snapshot, same schema). |
| Nested `s.b FIRST` on `(id, s STRUCT<a:3,b:4>)` | Moves inside the struct: `struct<b:4,a:3>`, nested ids unchanged, schema 0 → 1. |
| Whole-struct `s FIRST` | Moves. Order `[(2,s),(1,id)]`, schema 0 → 1. |
| v3 table, `b FIRST` | Moves, schema 0 → 1, same as v2. |
| `PARTITIONED BY (b)`, `b FIRST` | Moves, schema 0 → 1; positional INSERT lands `('b3',3,'a3')`; DESCRIBE appends the partition-info tail. |
| DataFrame door | `spark.table(t).columns == ['id','b','a']` after the move; `createDataFrame(...).writeTo(t).append()` by name lands `(9,'b9','a9')`. |

RePark's pre-fix answer: every move shape refuses on the facade with the I6 text (§2); the
native door fails to parse (`Expected: SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after
ALTER COLUMN, found: FIRST`). Post-fix RePark answers are pinned per clause in S4.

Error-shape design (matches Spark's class + prefix on the facade): unknown moved column and
unknown `AFTER` reference resolve at analysis time, so RePark pre-validates names against the
loaded schema and raises `DataFusionError::Plan` with Spark's `[UNRESOLVED_COLUMN...] SQLSTATE:
42703` framing (→ `AnalysisException`, as Spark). Move *legality* (self-move, cross-struct) is
table-format semantics owned by the fork, which already refuses with Java's messages (`Cannot
move b after itself` → `Error::Iceberg` → `PySparkException`, the diagnostic delta recorded in
the registry row). A move that changes nothing commits nothing (Spark mints no schema on
no-ops), so RePark filters no-op moves before the commit in `apply_schema_changes`, shared by
both doors.

## 2. Refusal reproduction (S2) — release native at fork pin `75da2b58`, 2026-09-17

Facade door (`spark.sql`):

```text
ERR  facade: ALTER TABLE rp.ns.t ALTER COLUMN b FIRST
     UnsupportedOperationException: This feature is not implemented: ALTER COLUMN … FIRST/AFTER (column MOVE) without ADD is not supported yet — use ADD COLUMN … FIRST|AFTER for new columns (I6)
ERR  facade: ALTER TABLE rp.ns.t ALTER COLUMN b AFTER id
     UnsupportedOperationException: This feature is not implemented: ALTER COLUMN … FIRST/AFTER (column MOVE) without ADD is not supported yet — use ADD COLUMN … FIRST|AFTER for new columns (I6)
```

Native door (`repark.sql`):

```text
ERR  native: ParseException: SQL error: ParserError("Expected: SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN, found: FIRST at Line: 1, Column: 36")
```

## 3. Oracle record (S3)

Recorder `python/repark/tests/_record_ice_column_reorder_1.py` (GAV from
`python/repark/tests/_oracle_pins.py`, same session config as `_live_parity.py`'s Iceberg
engine; local builder only because `/tmp/sparkenv` has no pyarrow) + truth
`python/repark/tests/test_ice_column_reorder_1_truth.json` (15 cases, provenance Spark 4.1.2 /
Iceberg 1.11.0). Ran 2026-09-17 under `jb-jvm.sh`, exit 0.

## 4. Red-first pins (S4) — 2026-09-17, unfixed release native

`python/repark/tests/test_ice_column_reorder_1.py` (26 tests: 12 offline, 14 live):

```text
FAILED test_move_first_matches_oracle
FAILED test_move_after_matches_oracle
FAILED test_noop_moves_commit_nothing
FAILED test_first_after_last_matches_oracle
FAILED test_self_move_refuses
FAILED test_after_unknown_column_refuses
FAILED test_move_unknown_column_refuses
FAILED test_nested_field_move_matches_oracle
FAILED test_positional_insert_and_select_after_move
FAILED test_move_on_v3_matches_oracle
FAILED test_move_partition_source_matches_oracle
FAILED test_dataframe_door_columns_and_append
12 failed, 14 deselected in 1.46s
```

Head failure (all twelve refuse at the same gate):

```text
repark.errors.UnsupportedOperationException: This feature is not implemented:
ALTER COLUMN … FIRST/AFTER (column MOVE) without ADD is not supported yet —
use ADD COLUMN … FIRST|AFTER for new columns (I6)
```

Every pin fails on the I6 refusal (facade) or the native parse error — none passes
vacuously. Live tier (14 tests) skips without `REPARK_PARITY_LIVE=1`; replayed in S7.

## 5. Implementation (S5) — 2026-09-17

`crates/repark-iceberg/src/write/alter.rs`: new `SchemaChange::MoveColumn { name, position }`
reusing `ColumnPosition`; `apply_schema_changes` maps it to the fork's standalone
`move_first` / `move_after` (the builders the positioned ADD already chains — no new
table-format API, no fork work). `check_column_move` (new, pure, shared by both doors):
resolves the dotted path case-insensitively, returns `Ok(changed)` by simulating the order,
or `Err` with Spark's `[UNRESOLVED_COLUMN…] SQLSTATE: 42703` framing. `apply_schema_changes`
filters no-op moves before the commit, so a no-op-only batch returns `Ok` without minting a
schema (Spark commits nothing on no-ops). Move *legality* (self-move, cross-struct) stays
fork-owned: the fork refuses with Java's messages.

Facade (`crates/repark-spark/src/alter.rs` + `router.rs`): `try_parse_column_move_ddl` /
`execute_column_move_ddl` in the I7 token style, wired ahead of the residual refusal; the I6
move-refusal arm is deleted (the COMMENT arm stays). Unknown names refuse via
`DataFusionError::Plan` (→ `AnalysisException`); no-ops return empty with no commit and no
reregister.

ANSI door (`crates/repark-sql/src/alter.rs` + `router.rs`): `try_parse_column_move` /
`execute_column_move` in the same shape, wired in the pre-parse stage; `resolve_target` +
`invalidate` reused. The `unsupported_operation` list names the new form.

Tests with the code: `check_column_move_names_noops_and_nested` (pure) and
`schema_move_column_reorders_ids_stable_and_noop_commits_nothing` (round-trip incl. the
self-move Java message) in repark-iceberg; `parse_column_move_first_after_and_nested` +
`parse_column_move_leaves_other_alter_forms_alone` in repark-spark (incl. the residual-refusal
bypass pin); `column_move_first_after_and_nested_parse` +
`column_move_leaves_other_alter_forms_alone` in repark-sql; the existing
`alter_unsupported_comment_move_and_after_missing_refuse` becomes
`alter_comment_refuses_and_column_move_lands` (COMMENT still refuses; the move lands and
`SELECT *` leads with the moved column);
`alter_column_move_reorders_and_noop_writes_no_metadata` in
`crates/repark-sql/tests/alter_column_move.rs` pins the ANSI door end to end (reorder,
no-op writes no metadata, `UNRESOLVED_COLUMN`; consolidated out of `src/tests.rs`).

No `//` or `#` comment line was added to any source file in this unit (round ban; the
`///` lines the diff touches are byte-identical context). New public item `check_column_move`
and the `MoveColumn` variant carry no doc comments for the same reason; the reason lives here.

Native-door finding (S5): the Python `repark.sql` session (`DataFusionDialect`, stock parse)
cannot address Iceberg tables — it is catalog-isolated (measured: `table 'mem.ns.t' not
found` from a facade-seeded table) and exposes no catalog registration, and stock sqlparser
models no position-change op. Wiring it through the ANSI router would reroute every native
statement, a semantic-adjacent rewrite out of this fix's scope ("Fixes stay narrow"). The
ANSI door is therefore implemented and pinned at the Rust level (`AnsiDialect`, reachable by
construction); Python pins cover the facade SQL door and the DataFrame door — every door that
can express the statement. The registry row states this split.

## 6. Registry (S6) — 2026-09-17

Row `ICE-COLUMN-REORDER-1` FIXED 2026-09-17 in `docs/spark-sql-iceberg-parity.md` after
DBT-COLCOMMENT-1 (the sibling `ALTER COLUMN` row), with the pins, the oracle path, the
self-move diagnostic delta, and the native-door split. The I6 move-refusal text is deleted
from `refuse_unsupported_alter_sql`; the `ALTER COLUMN … COMMENT` refusal is untouched.
Maps in lockstep: `python/repark/tests/map.md`, `crates/repark-spark/src/map.md`,
`crates/repark-iceberg/src/write/map.md`, `crates/repark-sql/src/map.md`,
`crates/repark-sql/src/alter/map.md`, `docs/map.md`.

## 7. Gates (S7)

Measured 2026-09-17 on the release native after the S5 rebuild.

- New file offline: 13/13 green (`test_ice_column_reorder_1.py`, built native).
- New file live: 14/14 green under `jb-jvm.sh` with `REPARK_PARITY_LIVE=1`.
- `cargo test -p repark-spark --lib`: green (inside `make verify`, exit 0).
- `cargo test -p repark-iceberg --lib`: green (inside `make verify`, exit 0).
- `make verify`: exit 0 (`/tmp/reorder_verify_out2.txt`).
- Whole facade suite: 9334 passed, 386 skipped, 26 xfailed, exit 0
  (`/tmp/oc-worker/jb-reorder/facade.log`, 3307.70 s).
- Whole parity suite: `make py-test` (uv isolated env) cannot spawn `pytest`
  on this box, so the identical tree ran under the repo venv instead:
  `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q` → 757 passed, 2 skipped, 12 xfailed, exit 0
  (`/tmp/oc-worker/jb-reorder/parity.log`, 1031.78 s).

C-014 is PROVEN. No gate is red.

## 8. Open questions

None. The fork exposes the move API, so the brief's HALT ground does not trigger.

## Round 2 (2026-09-17) — run 20b

**Base:** `origin/main` `444323f2` (RP-22 fork pin `96fc9f1f`, rebased clean, no conflicts).
**Model:** muse-spark-1.3-contributor. Reviews:
`/tmp/oc-worker/jb-rv/reviews/ro-logic-report.md` (L-001…L-006),
`/tmp/oc-worker/jb-rv/reviews/ro-rustperf-report.md` (R-01…R-03).

**Ruling Q-20b-5 (orchestrator, 2026-09-17).** `check_column_move` computed the sibling
order and the no-op decision in RePark, which fork rule 3 forbids. Removed the local
simulation and the early no-op return on both doors and in `apply_schema_changes`: every
move goes through the fork's `UpdateSchema::move_first / move_after` inside the
transaction. Name resolution is the fork's own `field_by_name_case_insensitive` (the same
function the fork's `find_field` uses), plus Spark's analyzer rule that a bare `AFTER`
reference resolves in the mover's struct — the fork cannot resolve bare nested shorts, so
RePark qualifies them (`s.b AFTER a` → `move_after("s.b", "s.a")`) and hands the dotted
form to the fork. Order, self/cross-struct legality and the commit are the fork's alone.
A dotted `AFTER` reference never reaches the fork: Spark parse-refuses it, so both
parsers refuse it with `[PARSE_SYNTAX_ERROR] … SQLSTATE: 42601` (a `DataFusionError::SQL`
that classifies to `ParseException`).

**Measurement falsified two review claims and narrowed the third.** The report said Spark
accepts dotted `AFTER s.a`: live Spark 4.1.2 parse-refuses it (`ParseException
[PARSE_SYNTAX_ERROR] … pos 66`). The report said the fork always mints a schema on a
no-op: at pin `96fc9f1f` `TableMetadataBuilder` reuses schemas by content
(`reuse_or_create_new_schema_id` over `is_same_schema`), so a no-op keeps the schema id
and a round-trip move restores schema 0 — both matching Spark. The remaining delta is one
file: RePark still writes a metadata document on a no-op where Java skips the commit.
That narrowed residue is ICE-COLUMN-REORDER-1-R-001 (fork trigger F-UPDATE-SCHEMA-SAME-1),
pinned by strict-xfail `test_noop_moves_write_no_metadata_file`, not by a schema-id pin.

**Red-first evidence (unfixed tree, before the fix).** Rust
(`cargo test -p repark-iceberg --lib`): `schema_add_then_move_batch_applies_in_order`,
`schema_nested_short_after_resolves` and `schema_noop_move_mints_new_schema_id` FAILED
(the last asserted the ruling's predicted mint; the fork dedup then corrected the
expectation to id stability), `schema_stale_move_rebases_to_java_order` passed (reliance
pin, green before and after). Python vs the round-1 native:
`test_nested_short_after_matches_oracle`, `test_nested_cross_after_refuses` and
`test_nested_dotted_after_refuses` FAILED; `test_noop_moves_write_no_metadata_file`
XPASSed under strict xfail (the old filter wrote nothing) — the sensitivity proof for
the xfail pin.

**Live-tier failures found and fixed in this round.** (a) The replay's
`test_move_partition_source_matches_oracle (0, 1)` and this round's
`test_nested_short_after_matches_oracle (1, 0)` share one harness root cause:
`_current_order_ids` picked the newest metadata document by file mtime, and the memory
catalog writes `0000N-uuid.metadata.json` names — a probe showed two consecutive commits
with identical `st_mtime_ns`, so `max` picked arbitrarily. Fixed by ordering on the
sequence prefix (mtime only breaks ties). (b) The dotted-refusal live replay compared
Spark's recorded `(line 1, pos 66)` against the replay's `(line 1, pos 78)` — table names
differ in length. Fixed with a position-insensitive needle. Full live file after both
fixes: 33 passed, 1 xfailed, 0 skipped.

**Findings dispositions.** L-001 REMEDIATED (early no-op return deleted on both doors
and in `apply_schema_changes`; `schema_stale_move_rebases_to_java_order` pins the fork
rebase to Java's `[id, b, a]`). L-002 REMEDIATED (sibling scope + three truth cells +
pins on both tiers). L-003 REMEDIATED (the simulation is deleted; `column_move.rs` keeps
only fork-index resolution, the `ALTER` prefix scan and the message framing).
L-004 REMEDIATED (`schema_add_then_move_batch_applies_in_order` pins `[id, z, a, b]`).
L-005 REMEDIATED (suggestions are the top-level fields; cross-struct names render
Spark-style `` `s`.`id` ``). L-006 ACCEPTED_FLAGGED (multi-clause `ALTER COLUMN …
FIRST, ADD COLUMN …` still refuses as trailing tokens — fail-loud, unchanged surface,
no silent order). R-01 REMEDIATED (`starts_with_alter` zero-alloc prefix scan gates both
`try_parse` fns before any tokenize). R-02 REMEDIATED (door-side load deleted; doors
resolve on one load and commit through `apply_schema_changes_on_table` — one
`load_table` per move). R-03 REMEDIATED (no order vectors are built anymore).

## 7b. Gates (round 2, release native rebuilt from this tree)

- New file offline: 16 passed, 1 xfailed (`test_noop_moves_write_no_metadata_file`,
  R-001), 0 failed.
- New file live (step-6 command): 33 passed, 1 xfailed, 0 skipped, exit 0
  (`/tmp/reorder_live_r3.log`).
- `cargo test -p repark-sql`: full crate run in the final gate set.
- `cargo test -p repark-spark --lib column_move alter`: move + alter rows green.
- `cargo test -p repark-iceberg --lib`: full crate green.
- `uvx ruff@0.15.22 check .`: clean. `ruff format --check` on both touched pin files:
  already formatted.
- `make verify`: NOT run in round (clock; the live tier plus rebuild filled the window).
  The three touched Rust crates ran full (`repark-sql`, `repark-iceberg --lib`) or
  row-filtered (`repark-spark --lib column_move alter`), all exit 0; whole suites are the
  orchestrator's runs.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-column-reorder-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001..C-014 walked against the recorded oracle cells, not a paraphrase — 18 truth cases (15 round-1 plus nested_after_short/dotted/cross) across plain, nested, v3, partitioned, insert and DataFrame shapes on the facade SQL door, the Rust ANSI door end to end, and the live replay that re-derives Spark per run.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, python/repark/tests/test_ice_column_reorder_1_truth.json, crates/repark-sql/tests/alter_column_move.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — already-first and already-after no-ops, first-after-last, self-move, unknown mover, unknown reference flat and nested, short/dotted/cross-struct nested references, empty-reference trailing tokens, non-ALTER statements through the intercept, v2 and v3, partitioned source moves.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, crates/repark-spark/src/column_move.rs, crates/repark-sql/src/alter/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is loud and typed — UNRESOLVED_COLUMN with SQLSTATE 42703 for unknown names, PARSE_SYNTAX_ERROR with 42601 for dotted references, the fork Java message for self and cross-struct moves — and every refusal pin asserts no new metadata file; the no-op file delta is a strict xfail tied to OPEN residue R-001, never absorbed.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: ATTACKED
      evidence: The stale no-op race is closed by deleting the load-time decision — every move commits through the fork transaction, whose do_commit rebases onto the refreshed table; schema_stale_move_rebases_to_java_order pins Java final order [id, b, a]; ADD+MOVE batch order pinned against the pre-batch schema hole.
      artifacts: [crates/repark-iceberg/src/write/alter.rs, crates/repark-iceberg/src/write/column_move.rs]
    - id: AT-5
      status: N/A
      justification: No auth, injection, secret, or deserialization surface — fixed ALTER spellings and fixed table names over local test warehouses; the ALTER-prefix scan only reads bytes, and no user input reaches SQL text beyond the statement itself.
    - id: AT-6
      status: ATTACKED
      evidence: Schema ids are never reassigned by a move (fork reuses by content, round-trip restores 0 as Spark does); Spark-written moved tables adopted through register_table and cross-read both directions live; positional INSERT after the move lands Spark-equal rows.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, crates/repark-iceberg/src/write/alter.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The intercept fast path (starts_with_alter) and the single-load doors answer R-01/R-02; no other resource shape changed — moves are metadata-only commits, and the torture/spill suites are the orchestrator's whole-suite runs, not this unit's.
      artifacts: [crates/repark-iceberg/src/write/column_move.rs, crates/repark-spark/src/column_move.rs, crates/repark-sql/src/alter.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Upstream contracts honored, not presumed — fork move/find_field/dedup behavior read at pin 96fc9f1f and pinned (stale rebase, batch order, sibling scope, id reuse); the Spark GAV read from _oracle_pins; the recorder re-derives every cell from live Spark; the live tier replays Spark per run.
      artifacts: [python/repark/tests/_record_ice_column_reorder_1.py, python/repark/tests/_oracle_pins.py, crates/repark-spark/src/tests/column_move.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure mode carries its Spark-shaped message and SQLSTATE through the engine classifier (Plan to AnalysisException, SQL to ParseException, fork Java text verbatim), and the two live-tier harness defects (mtime ordering, pos-sensitive needle) were diagnosed from the assertion values and pinned by the fixed helpers.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, crates/repark-core/src/error_map.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Pins-first held — the L-002/L-004 pins failed on the unfixed tree and the R-001 xfail XPASSed there, so each fails without the change; every branch the diff adds (prefix scan, sibling qualify, dotted refusal, batch-added cover) has a nameable input in the suite; the full unit file plus the three touched Rust crates ran green on the release native.
      artifacts: [python/repark/tests/test_ice_column_reorder_1.py, crates/repark-iceberg/src/write/alter.rs, crates/repark-spark/src/column_move.rs, crates/repark-sql/src/alter/tests.rs]
  complete: true
```

```yaml
FINDING:
  id: F-ICE-COLUMN-REORDER-L-001
  severity: S2
  category: AT-4
  clause: C-003
  disposition: REMEDIATED
  title: Stale load-time no-op filter skipped a move Java applies after a concurrent commit
  evidence: The Ok(false) early return is deleted on both doors and in apply_schema_changes; every move commits through the fork transaction with OCC rebase. schema_stale_move_rebases_to_java_order commits a real move between load and commit of a would-be no-op and asserts Java final order [id, b, a].
FINDING:
  id: F-ICE-COLUMN-REORDER-L-002
  severity: S2
  category: AT-1
  clause: C-008
  disposition: REMEDIATED
  title: Nested AFTER a short sibling name refused; Spark accepts it in the mover struct scope
  evidence: The resolver qualifies bare references into the mover struct (s.b AFTER a becomes move_after s.b/s.a) through the fork index. Truth cells nested_after_short/dotted/cross recorded live; pins offline and live on both tiers.
FINDING:
  id: F-ICE-COLUMN-REORDER-L-003
  severity: S2
  category: AT-8
  clause: C-001, C-002, C-003, C-004
  disposition: REMEDIATED
  title: Local Iceberg order/no-op simulation duplicated fork semantics against fork rule 3
  evidence: check_column_move is deleted. column_move.rs keeps only fork-index name resolution, the ALTER prefix scan and Spark message framing; order, legality and commit are the fork UpdateSchema. Q-20b-5 recorded above.
FINDING:
  id: F-ICE-COLUMN-REORDER-L-004
  severity: S2
  category: AT-4
  clause: C-001
  disposition: REMEDIATED
  title: ADD+MOVE in one apply_schema_changes batch dropped the move as a pre-batch no-op
  evidence: The pre-batch filter is deleted; the whole batch applies in order on one UpdateSchema. schema_add_then_move_batch_applies_in_order pins Spark order [id, z, a, b]; it failed on the unfixed tree.
FINDING:
  id: F-ICE-COLUMN-REORDER-L-005
  severity: S3
  category: AT-3
  clause: C-006
  disposition: REMEDIATED
  title: Unknown AFTER on a nested mover suggested nested siblings, not Spark table columns
  evidence: Suggestions are the top-level fields and multipart blame renders Spark-style (`s`.`id`), matching the recorded Spark refusal. test_nested_cross_after_refuses pins the list; it failed on the unfixed tree.
FINDING:
  id: F-ICE-COLUMN-REORDER-L-006
  severity: S3
  category: AT-1
  clause: C-001
  disposition: ACCEPTED_FLAGGED
  title: Multi-clause ALTER COLUMN FIRST plus ADD COLUMN refuses as trailing tokens
  evidence: Both parsers require the statement to end at FIRST/AFTER, so Spark one-transaction form stays fail-loud, never silently reordered. No behavior change in this round; flagged here so the gap is not re-litigated as a surprise.
FINDING:
  id: F-ICE-COLUMN-REORDER-R-01
  severity: S2
  category: AT-7
  clause: C-014
  disposition: REMEDIATED
  title: Every non-ALTER statement paid a full tokenize through the new intercept
  evidence: starts_with_alter (whitespace/comment-skipping byte scan, no allocation) gates both try_parse fns before tokenizing; parse_column_move_skips_non_alter_without_tokenizing pins the fast path.
FINDING:
  id: F-ICE-COLUMN-REORDER-R-02
  severity: S2
  category: AT-7
  clause: C-014
  disposition: REMEDIATED
  title: A successful move paid two catalog loads and two schema walks
  evidence: The door-side load-then-check is deleted; doors resolve on one load and commit through apply_schema_changes_on_table. The second check is pure and runs once per path (door pre-resolve for the Plan class, batch resolve in the wrapper).
FINDING:
  id: F-ICE-COLUMN-REORDER-R-03
  severity: S3
  category: AT-7
  clause: C-014
  disposition: REMEDIATED
  title: The After path allocated sibling-id vectors and lowercased every sibling name
  evidence: The order simulation is deleted with L-003; resolution is fork-index lookups with no per-sibling allocation in RePark.
```
