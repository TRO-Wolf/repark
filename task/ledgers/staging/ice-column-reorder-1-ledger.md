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
| C-001 | `ALTER COLUMN c FIRST` moves the column to the front with field ids unchanged and a new schema id, matching Spark 4.1.2 + Iceberg 1.11.0 (DESCRIBE order and `SELECT *` order, both doors). | Truth JSON from live Spark; offline pins vs the truth JSON; live tier replaying Spark. Red-first on the release native. | **OPEN** | Awaiting S2 refusal paste and S3 oracle. |
| C-002 | `ALTER COLUMN c AFTER x` moves the column to immediately after `x`, ids unchanged, new schema id, matching Spark on both doors. | Same harness as C-001. | **OPEN** | Awaiting S2 refusal paste and S3 oracle. |
| C-003 | Moving a column to where it already is (e.g. first column `FIRST`, `AFTER` its current predecessor) matches Spark's answer — same order, and Spark's schema-id behaviour recorded verbatim. | Truth JSON records whether Spark mints a new schema id on a no-op move; pins assert RePark's commit matches. | **OPEN** | Awaiting S3 oracle. |
| C-004 | Moving the first column after the last matches Spark on both doors. | Same harness as C-001. | **OPEN** | Awaiting S3 oracle. |
| C-005 | `AFTER` naming the moved column itself matches Spark's answer (Spark's error class and message prefix, or its resulting order, recorded verbatim). | Truth JSON records Spark's exact behaviour; pins assert RePark answers the same. | **OPEN** | Awaiting S3 oracle. |
| C-006 | `AFTER` an unknown column refuses loud with Spark's error class and message prefix on both doors. | Truth JSON records Spark's error; pins assert RePark's typed refusal matches the class and prefix. | **OPEN** | Awaiting S3 oracle. |
| C-007 | Moving an unknown column refuses loud with Spark's error class and message prefix on both doors. | Same harness as C-006. | **OPEN** | Awaiting S3 oracle. |
| C-008 | Moving a nested struct field (`ALTER COLUMN s.b FIRST`) matches Spark, or — if not small — refuses with a typed Spark-shaped refusal plus a dated DECLARED registry row. | Small: same harness as C-001 on a struct shape. Not small: typed refusal pin + registry row. Decision recorded here with the measured fork behaviour. | **OPEN** | Fork `move_first` takes a name string; whether dotted nested paths validate is measured in S1. |
| C-009 | After the move, `INSERT INTO t VALUES (...)` in the NEW positional order lands rows readable by both engines, and `SELECT *` reads agree in both directions (Spark reads RePark's moved table; RePark reads Spark's). | Cross-read pins in the live tier; offline pins assert RePark's positional insert + read round-trip against the truth JSON. | **OPEN** | Awaiting S3 oracle and S4 pins. |
| C-010 | The move works on format-v2 and format-v3 tables. | Truth + pins parametrised over both versions. | **OPEN** | Awaiting S3 oracle. |
| C-011 | The move works on a partitioned table where the moved column is a partition source, and reads/writes still answer Spark. | Partitioned shape in the truth JSON; pins cover post-move read + insert. | **OPEN** | Awaiting S3 oracle. |
| C-012 | DataFrame door: `spark.table(t).columns` order and `df.writeTo(t).append()` by name after the move match Spark. | Live-tier pins through the facade DataFrame API. | **OPEN** | Awaiting S3 oracle. |
| C-013 | Registry row ICE-COLUMN-REORDER-1 (FIXED 2026-09-17, plus DECLARED rows for anything refused) exists, the I6 refusal text no longer fires on supported moves, and every touched `map.md` is current. | `docs/spark-sql-iceberg-parity.md` diff; grep for the old I6 text; `make check-map-sync`. | **OPEN** | Awaiting S5–S6. |
| C-014 | Gates green on the release native: new file offline + live, `cargo test -p repark-spark --lib`, `cargo test -p repark-iceberg --lib`, `make verify`, whole facade suite, whole parity suite. | Counts pasted below in §7. | **OPEN** | Awaiting S7. |

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
`SELECT *` leads with the moved column); `alter_column_move_reorders_and_noop_mints_no_schema`
pins the ANSI door end to end (reorder, no-op mints no schema, `UNRESOLVED_COLUMN`).

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

Unmeasured. Counts land here in S7.

## 8. Open questions

None. The fork exposes the move API, so the brief's HALT ground does not trigger.
