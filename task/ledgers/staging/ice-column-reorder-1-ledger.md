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

## 1. Clause matrix (S1)

Unmeasured. Filled in S1–S3.

## 2. Refusal reproduction (S2)

Unmeasured. Verbatim paste lands here in S2.

## 3. Oracle record (S3)

Unmeasured. Recorder script + truth JSON land here in S3.

## 4. Red-first pins (S4)

Unmeasured. Failing output lands here in S4.

## 5. Implementation (S5)

Design: new `SchemaChange::MoveColumn { name, position: ColumnPosition }` in
`crates/repark-iceberg/src/write/alter.rs` mapped to the fork's standalone
`move_first` / `move_after`; a token-level `ALTER TABLE … ALTER COLUMN … FIRST|AFTER …`
parse in `crates/repark-spark/src/alter.rs` in the I7 style (stock sqlparser models no
position-change op), wired ahead of the I6 refusal in `router.rs`; the I6 refusal arm
narrows to whatever stays unsupported (nested moves only if C-008 declares them).
Simplest correct approach: reuse the existing `ColumnPosition` and the fork move builders
the ADD path already chains.

## 6. Registry (S6)

Unmeasured. Row ICE-COLUMN-REORDER-1 lands here in S6.

## 7. Gates (S7)

Unmeasured. Counts land here in S7.

## 8. Open questions

None. The fork exposes the move API, so the brief's HALT ground does not trigger.
