# Card MAP-COLUMN-PANIC-1: `map(<key>, <column>)` over a column value panics in DataFusion's map constructor on every door

**Date:** 2026-10-20. **Filed by:** Claude (claude-fable-5-1), orchestrator, from the second
hostile verification of PR #1124 (ICE-NTZ-NESTED-REFUSE-1, U-8) on main `c516c54a`.

**Status:** fixed on branch `fix/map-column-panic-1` (2026-10-20, builder Claude Sonnet 5.5; the pull request
awaits the owner's merge). The analyzer rule `SparkMapRewrite` sends every non-constant `map(...)` to the Spark
per-row kernel on every door; the evidence is under `task/wo/map-column-panic-1/evidence/`. An engine defect
outside U-8; U-8's row R-011 misnamed it as a container cast of a map, and its fix round 2 corrects the row
to point here.

**Retires:** when every statement below answers Spark's value, with a pin per door, and the
Python boundary no longer reports a caught Rust panic for any `map()` over a column.

## What fails

Session `America/New_York` (the zone is incidental; the panic is the same in UTC). Source
`ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ)`, Iceberg through the catalog. Main
`c516c54a` and PR #1124's head `c20c3e25` answer alike.

| Statement | RePark |
|---|---|
| `SELECT map('a', id) FROM ice.ns.src` | panic `list array` |
| `INSERT INTO ice.ns.m SELECT id, map('a', id) FROM ice.ns.src` (`m` has `MAP<STRING, INT>`) | panic `list array` |
| `INSERT INTO ice.ns.m3 SELECT id, map('a', w) FROM ice.ns.src` (`MAP<STRING, TIMESTAMP_NTZ>`, a wall) | panic `list array` |
| `INSERT INTO ice.ns.m2 SELECT id, map('a', CAST(x AS TIMESTAMP_NTZ)) FROM ice.ns.src` | panic `list array` |
| control `INSERT … SELECT 1, map('a', 1)` and `map('a', TIMESTAMP_NTZ '…')` (literals) | stores |
| control `SELECT CAST(map('a', TIMESTAMP '…+00:00') AS MAP<STRING, TIMESTAMP_NTZ>)['a']` | answers (the UTC wall; CONTAINER-CAST-ZONE-1) |

Panic text: `thread '<unnamed>' panicked at arrow-array-58.4.0/src/cast.rs:929:28: list array`.
Frames, innermost first: `datafusion_common::utils::list_to_arrays`,
`<datafusion_functions_nested::map::MapFunc as ScalarUDFImpl>::invoke_with_args`,
`ScalarFunctionExpr::evaluate`, `CastExpr::evaluate`, `Projector::project_batch`,
`DataFrame::collect`, `repark_spark::spark_ast::execute_passthrough_inner`,
`repark_spark::router::execute_insert_routed`. The Python boundary catches it and raises
"repark internal error in PyReparkSession.sql: list array (a Rust panic was caught at the
Python boundary …)". Spark 4.1.2 answers every row.

**Wider than a column (ZONE-ORACLE-1, PR #1128, 2026-10-20):** `map('k', <function result>)` panics
the same way for any non-literal value, sixty ERROR cells of the 1,640-cell corpus on main
`754fb6e5`, in every zone. The one-row view the corpus reads from is `FROM range(1)`; a view of
bare literals is folded and the panic does not show.

## Where to look

- `MapFunc` builds its key and value arrays with `list_to_arrays`, which expects a list array;
  a column value arrives as a plain array under RePark's lowering of `map(...)` (the literal
  case arrives as a list and works). The fix is in RePark's lowering of `map` over non-literal
  arguments, or a wrapping of the arguments into the list shape `MapFunc` expects; a panic
  must become a planning error at worst (`make rust-panic-ban` governs the crate).
- The U-8 verifier's residue probe and its jsonl and stderr captures for main, head and Spark
  are the measurement corpus; when this unit is scheduled they move under
  `task/wo/map-column-panic-1/evidence/` with a `map.md`.

## Out of scope

The UTC wall a container cast of a map literal returns (CONTAINER-CAST-ZONE-1); U-8.
