# Card MAP-KEY-REFUSAL-TEXTS-1: refusal texts and parse refusals around map-key fields that differ from Spark's (leads L4 to L10)

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from leads L4 to L10 (clauses C-012 to C-018) of [MAP-KEY-ALTER-1](../../ledgers/completed/map-key-alter-1-ledger.md).

**Status:** filed, not dispatched. Verifier class S3. In every cell below the statement is refused and nothing is committed; the cards are about the text or the parse path, not about a wrong read or write.

**Cells:**

- L4 (C-012): a COMMENT on a map-key field that repeats the current doc is refused with the map-key text; Java's source reads an equal doc as a no-op. Spark's answer is not measured.
- L5 (C-013): `ALTER TABLE t RENAME COLUMN mk.key.a TO b` (an exact-case sibling) answers the fork's `Invalid schema: multiple fields for name mk.key.b`; Spark answers FIELD_ALREADY_EXISTS. Plain structs behave the same way, so this predates the unit.
- L6 (C-014): `DROP COLUMN mk.key` and `RENAME COLUMN mk.key TO k2` answer the fork's terse `Cannot delete map keys: map` and `Cannot update map keys: map`; Java renders the full map type.
- L7 (C-015): dotted `DROP NOT NULL`, `SET NOT NULL`, `SET DEFAULT`, `DROP DEFAULT` and `CHANGE COLUMN` refuse at parse on both doors. For an optional key field, DROP NOT NULL is a Java no-op that Spark accepts.
- L8 (C-016): on the native door, a dotted `ALTER COLUMN mk.key.a TYPE BIGINT` (or SET DATA TYPE) refuses with a parse error where Spark says `Cannot alter map keys`; every mapkey INSERT with `CAST(NULL AS MAP<STRUCT<...>, ...>)` refuses at parse.
- L9 (C-017): under `spark.sql.caseSensitive=true`, `ALTER COLUMN mk.key.B AFTER b` answers `Cannot move mk.key.B after itself`; Spark reports an unresolved column.
- L10 (C-018): `RENAME COLUMN mk.key.a TO a` answers the map-key text; Spark answers FIELD_ALREADY_EXISTS (derived from Spark's analyzer, not measured).

**Fix shape:** the texts first (L5, L6, L9, L10), through the fork's rename and duplicate-name checks and the move's column resolution; then the parser for L7 and L8 (dotted nullability and default changes, dotted TYPE, and the MAP-of-STRUCT cast on the ANSI door).

**Retires:** when each cell's text or parse outcome matches the measured Spark column, or the owner declines it with a dated ruling on this card.

**Relation:** [MAP-KEY-ALTER-1](map-key-alter-1-card-2026-11-04.md) (leads C-012 to C-018); [F-MAP-KEY-DROP-ALTER-1](f-map-key-drop-alter-1-card-2026-10-31.md).

Up: [map.md](map.md).
