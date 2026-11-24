# Card INSERT-OVERWRITE-CONST-CAST-1: an empty `INSERT OVERWRITE` with an invalid constant cast wipes the table under ANSI

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review, measured here.
**Severity:** S2 (owner, 2026-11-24). **Status:** filed, not dispatched.

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2 with Iceberg 1.11.0, on a hadoop catalog. The table
holds one row first.

| Statement | ANSI | Spark 4.1.2 | RePark |
|---|---|---|---|
| `INSERT OVERWRITE t SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0` | on | `[CAST_INVALID_INPUT]`; the table keeps its row | **succeeds; 0 rows** |
| the same, `CAST(s AS TIMESTAMP)` | on | `[CAST_INVALID_INPUT]`; the table keeps its row | **succeeds; 0 rows** |
| either statement | off | succeeds; 0 rows | succeeds; 0 rows |

The source is empty, so both engines agree on what the overwrite writes. They differ on whether it runs. Spark folds
the constant cast and refuses before writing. RePark writes the empty overwrite, so the table's data is gone.

## Not yet known

- Whether any shape with a **non-empty** source writes rows that Spark refuses. That would make this S1.
- The outside review named four more casts on the same path (to `TIMESTAMP_NTZ`, and decimal from double). Not
  measured here.

Up: [map.md](map.md).
