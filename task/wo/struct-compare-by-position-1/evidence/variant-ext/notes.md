# Notes: comparison unit, variant axis (step-0 extension)

## Status

Measurement only. Nothing committed, no clone edited or built, the live repository untouched. Both legs ran to exit 0 (`logs/spark.exit`, `logs/base.exit`). Raw results are in `raw/spark.jsonl` (227 records) and `raw/base.jsonl` (246 records); the rendered table is `table.md` (473 cells in total, inside the 400 to 700 target, so no prune was proposed).

## Cell plan

Base cell B: list/list kinds, plain encoding, top level, forward order (L `a,b`, R `b,a`), struct payload, SQL temp-view door, pandas source. Seats: `=`, `IN` list, join key. Each axis is crossed one factor at a time around B.

| plan | axis | cells per leg | legs | notes |
|---|---|---|---|---|
| P30 | base B | 3 | spark, base | twin for the encoding and door rows |
| P31 | orientation: reverse of all 36 kind pairs (L `b,a`, R `a,b`) | 108 | spark, base | the reversed list/list cell is the twin of the reversed rows |
| P32 | encoding: RunEndEncoded and Dictionary, encoded–encoded, encoded–plain, plain–encoded | 18 | spark, base | pandas source; Dictionary refused at ingest |
| P33 | depth: inside a struct; inside a map value (standard and renamed entries, value reordered) | 54 | spark, base | six reps × three depths × three seats |
| P34 | names: same-order control (L `a,b`, R `a,b`) | 18 | spark, base | six reps |
| P35 | door: DataFrame typed (`==`, `isin`, join) | 18 | spark, base | six reps |
| P36 | door: native `repark.sql` with `arrow_cast` | 18 | base only | six reps; Spark twin is the SQL-door Spark result |
| P37 | native Dictionary encoded–encoded | 1 | base only | one row, the ruled refusal row |
| P38 | DML: UPDATE and DELETE on a fresh v2 copy-on-write Iceberg table, list and large kinds, reordered and same-order predicate | 8 | spark, base | derived oracle on Spark where Spark raises |

Six reps: `list/list`, `large/list`, `list/fsl1`, `fsl1/fsl2`, `largeview/largeview`, `view/list`.

Not re-run: the forward 36 kind pairs in seat C (already measured in the variant table, P10). Not crossed: the list-of-struct payload on the new axes (the variant table measured it for the forward pairs). The head leg is not in the brief's leg list.

## Deviations and judgement calls

1. Native `IN`. The first native `IN` form, `l.s IN (SELECT r.s FROM r)`, was refused by the physical planner (`InSubquery` not implemented) on every row, so it would have measured the harness form. P36 uses `EXISTS (SELECT 1 FROM r WHERE r.s = l.s)` instead. With a one-row right side it has the same meaning as `IN`.
2. Map depth, corrected before the final run. The first run compared whole maps (`map<string, array<struct>>`). Spark refuses map-typed `=`, `IN` and join (`INVALID_ORDERING_TYPE`), so all 36 map cells were Spark refusals and told nothing about the value. The final run compares the map value `s['k']`, which is how the step-0 `map_value_struct` cells are built. Only the final run is in `raw/` and `table.md`.
3. Spark key. Spark has one array type, so its twin key ignores kind and encoding. Names are also ignored for map entries, because Spark has no entry names. Door matters only where the DataFrame door is used (P35), which gets its own Spark run.
4. Struct depth shape. The "inside a struct" depth is `struct<x: array<struct<...>>>`. It is not the step-0 `struct_in_struct` shape (a struct nested in a struct), which is where Spark refuses reordered `=` and `IN` in step-0. That shape is not crossed here, so the Spark refusal question for nested structs stays open.
5. DML `large` kind. Iceberg has no LargeList column, so the table column stays `ARRAY<STRUCT>` on both legs. On base the predicate literal is `arrow_cast(array(...), 'LargeList(Struct(...))')`. On Spark the predicate is the plain-list twin, which is the logical shape.
6. DML tables. Created with `format-version=2`, `write.delete.mode=copy-on-write` and `write.update.mode=copy-on-write` as table properties. The table properties were not read back from metadata, and the create succeeded on both legs.
7. DML derived oracle. Spark raises `Cannot create expression literal` only for DELETE. Spark UPDATE with the struct-literal predicate runs and matches nothing for the reordered form. So DERIVED-* rows exist only for DELETE, and Spark's own UPDATE answer is used directly. The derived oracle is Spark's `SELECT id ... WHERE s = <same predicate>` on a fresh copy of the fixture, and the expected read-back is computed from it (DELETE removes matched ids; UPDATE sets matched ids to 9).
8. Labels added. `EQUAL` for DML rows whose read-back and snapshot delta match the oracle, and `REFUSED-WHERE-SPARK-ANSWERED` for DML where the base refused and Spark wrote. Neither is in the brief's list, but both are needed to label the rows.
9. Arrow types. The types the engine saw on each side are recorded for seat C cells (`types` columns). DML rows record no types.
10. Kind-invariance twin. The twin of a cell is found by a key that strips kind and encoding, and keeps leg, seat, payload, shape, depth, orientation, operation, door, source, DML statement and predicate order. Both refused counts as `INVARIANT`.

## Findings

Seat C (comparisons), base against Spark, from `table.md`:

- Base 1.5.6 answers by name wherever it accepts a reordered pair. P30 base: `=`, `IN` and join all answer `true` or a row where Spark gives `false` or no rows (3 WRONG-ANSWER).
- P31 (all 36 reversed kind pairs): 108 cells. Base refuses 54 (every pair that has a `view` or `largeview` side, plus `fsl`/`large`/`list` against those), all of which Spark answers (`REFUSED-WHERE-SPARK-ANSWERED`). Of the 54 base answers, 36 are WRONG-ANSWER and 18 are EQUAL. Spark answers all 108.
- P32 (encodings, list/list): all 9 RunEndEncoded cells are WRONG-ANSWER (reordered by name). All 9 Dictionary cells are refused at ingest on base (the facade refuses pandas Dictionary), and Spark answers them, so they are `REFUSED-WHERE-SPARK-ANSWERED`.
- P33 (depths): base is WRONG-ANSWER on 36 cells across the struct and map-value depths, EQUAL on 9, and refused on 9 (the `view`/`list` rep at each depth, where Spark answers). Spark answers all 54.
- P34 (same-order control): base EQUAL on 12, which is the control working. The 3 `view`/`list` cells are refusals where Spark answers.
- P35 (DataFrame typed): base EQUAL on 3, WRONG-ANSWER on 12, and 3 refusals (`view`/`list`).
- P36 (native `arrow_cast`): base EQUAL on 3, WRONG-ANSWER on 12, and 3 refusals (`view`/`list`). The list/list native twin agrees with the SQL-door result, so the native door does not change the answer.
- P37 (native Dictionary): refused at ingest (`Unsupported output type for dictionary packing`), as the variant table recorded.
- Kind invariance (against the plain-List twin): 73 of 108 reversed cells are VARIANT-DIFFERS on base; P32 has 9, P33 has 18, and P34, P35 and P36 have 6 each; P37 has 1. Spark has no VARIANT-DIFFERS in any plan.
- Spark refused zero comparison cells in this run. The only Spark refusals are the four DELETE cells (see DML).

DML (P38), base against the Spark oracle:

- Reordered predicate, UPDATE (list and large): base WRONG-WRITE. Base updates row 1 by name (read-back `[2, 9]`). Spark, which matches positionally, leaves the rows unchanged (`[1, 2]`).
- Reordered predicate, DELETE (list and large): DERIVED-WRONG-WRITE. Base deletes row 1 (read-back `[2]`). The derived oracle, `SELECT ... WHERE` on Spark, matches nothing, so the expected read-back is `[1, 2]`.
- Same-order predicate, UPDATE: EQUAL (read-back `[2, 9]` on both legs).
- Same-order predicate, DELETE: DERIVED-EQUAL (read-back `[2]` on base, derived expected `[2]`).
- Same-order control result: the reordered-versus-same-order contrast is the predicate form itself, not the table. The Spark struct-literal error appears only on DELETE with the reordered and same-order predicate alike, so it is not caused by reordering.

Comparison with the step-0 table, for the Spark-refusal question (hand-back item 6):

- step-0 `array_of_struct|reordered`, `=` and `IN` on the boolean `select` seats: Spark answers `rows=1 [[false]]`. P30 Spark answers `false` for `=` and `IN`. These agree. The other step-0 seats return ids from a multi-row fixture and are not comparable to P30. The step-0 join answers are multi-row fixtures and are not directly comparable to the single-row P30 join.
- step-0 `struct_in_struct|reordered` (nested structs): Spark refuses `=` and `IN` on most seats with `DATATYPE_MISMATCH`. This shape is not in this run (see deviation 4), so no comparison is possible.
- step-0 `map_value_struct|reordered` compares `mp['k']` (a struct), and Spark answers. This run's map-value cells compare `s['k']` (an array of struct), and Spark also answers them. The two are different shapes and are not compared.
- No cell in this run has Spark refusing a pair that step-0 says Spark answers, or the reverse.
