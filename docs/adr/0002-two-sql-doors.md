# ADR 0002 — Two honest SQL doors, no blended parser

- **Status:** Accepted (2026-08-06)
- **Deciders:** project owner + Claude
- **Related:** [../../PROJECT.md](../../PROJECT.md) "What RePark is",
  [../testing.md](../testing.md) (the entry-point matrix), [../../AGENTS.md](../../AGENTS.md).

## Context

RePark V2 is a new engine with its own identity, not "v1 plus features." v1 was Spark-shaped: its
only SQL surface was the Spark dialect. V2 demotes the PySpark facade from front door to one
supported door among several — but existing production Spark SQL must keep running unchanged, and a
native engine deserves a native dialect that isn't burdened by Spark-isms. A single parser that
"accepts both" would have to guess which dialect a given string meant; dialect guessing produces
silent wrong answers on the strings where the dialects disagree.

## Decision

1. **Two doors, each declaring its dialect.**
   - Native `repark.sql()` speaks **ANSI, Trino-style**: catalog-determines-format CTAS (no
     `USING iceberg` clause), `WITH (…)` table properties, `FOR VERSION AS OF` /
     `FOR TIMESTAMP AS OF` time travel, maintenance as callable ops. Where DataFusion has no
     opinion, copy Trino's Iceberg SQL.
   - The Spark facade's `.sql()` keeps the **Spark dialect** unchanged — existing production SQL
     runs on day one.
2. **No blended parser.** Guessing which dialect a string meant is banned. A string enters through
   exactly one door and is parsed by that door's dialect, full stop.
3. **Shared Iceberg machinery beneath both doors.** Commit semantics, MERGE, snapshots, and
   evolution live once (`repark-iceberg` + the owned fork); the dialect layers are thin
   translators.
4. **Dual-spelling rule for new surface.** The toll is paid only when NEW SQL surface lands (e.g.
   branching DDL): pick the native spelling, match the Spark spelling, and land **one test row per
   door** in the entry-point matrix.
5. **The native dialect's Iceberg DDL gets one deliberate design pass before the first public
   commit of that surface** (port phase 2) — that surface is permanent once published.

## Consequences

- **Positive:** each door is honest — no dialect guessing, no silent divergence; the facade keeps
  its near-drop-in promise; the native dialect can be clean ANSI/Trino without Spark baggage.
  Shared machinery means a fix beneath the doors fixes both.
- **Cost:** every new SQL surface is specified and tested twice (once per door). The entry-point
  matrix in [../testing.md](../testing.md) makes this mechanical rather than optional.
- **Guard:** any proposal to "just accept both dialects in one parser" contradicts this ADR and
  needs a superseding ADR, not a code change.
