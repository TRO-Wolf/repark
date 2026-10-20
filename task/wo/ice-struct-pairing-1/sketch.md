# Sketch ICE-STRUCT-PAIRING-1: one rule pairs struct fields on every write door

**Date:** 2026-10-13. **Author:** Claude (Opus 5.5), lane B of the Iceberg closeout, unit U-6.
**Status:** sketch, halts before code; builds only on the owner's adopt. **Parity row:**
[ICE-TSNS-SQL-1-R-016](../../../docs/spark-sql-iceberg-parity.md) (audit item PA-07), ledger
clause C-037 and question Q8 of
[ice-tsns-merge-wall-1-ledger.md](../../ledgers/staging/ice-tsns-merge-wall-1-ledger.md).
**Order:** [order.md](order.md). **Target:** 1.5.6 (§10). No product code and no test changed
in this unit of work.

## 1. The defect in one paragraph

A struct value written into a struct column has to decide which source field feeds which
target field. Spark decides by the door: by position on the positional doors, by name on the
by-name doors. RePark decides by whichever cast happens to run on that door's route, and three
different casts run: DataFusion's struct cast (by name, case-sensitive, a target field with no
source field becomes NULL), Arrow's struct cast (by name when every target name is present,
else by position, a surplus source field is dropped) and the MERGE fold's rebuild in SQL text
(by name, Spark's refusals). No gate judges a nested pair on the INSERT doors
(`refuse_unless_write_store_assignable` returns `Ok` for every type that is not flat), so
whatever the cast does is stored. A renamed field stores NULL and keeps the row on six doors.

## 2. The measured matrix

**Method.** One script, both engines, the same statements: 16 doors × 32 shapes = 512 cells,
run on format version 2 and on format version 3 (1,024 cells per engine), plus 192 cells with
`spark.sql.caseSensitive=true`. Target `struct<a: INT, b: INT>` at the top level, inside a
struct (`struct<o: struct<a, b>, n: INT>`), inside an array and as a map value; the seed row is
`(10, 20)` and the written value is `(1, 2)`, so a cell shows which source field reached which
target field. Main is the release module of `293896ce` (v1.5.5; the branch head `018d0efd`
differs from it in documents only). Spark is 4.1.2 with the Iceberg 1.11.0 runtime, a Hadoop
catalog, session zone UTC, defaults otherwise. The scripts (`pair.py`, `final.py`, `cost.py`,
`stage.py`) and their outputs stay in the lane's scratch directory `closeout-1013/u6`.

**Format versions.** Main answers the same in all 512 cells on v2 and v3. Spark answers the
same in all 512 cells on v2 and v3. The tables below are one format version.

**Spark's rule, confirmed.** The registry row's Spark cells (80, on `TIMESTAMP_NTZ` leaves,
measured 2026-10-09) and tonight's 1,024 agree on every door the row records. Measured live
tonight for doors the row does not record: `REPLACE WHERE` and `insertInto` (append) pair by
position; `saveAsTable` (append), `MERGE … INSERT *`, `MERGE … UPDATE SET *` and `UPDATE` with
no `WHERE` pair by name. Spark's answer does not depend on the container: for every door and
field shape the top-level, nested, array and map cells are the same (0 of 128 door-shape pairs
vary). With `spark.sql.caseSensitive=true` the by-position doors store as before and every
by-name door refuses a recased name with `CANNOT_FIND_DATA` (192 cells).

| Spark mode | Doors | Source |
|---|---|---|
| by position | `INSERT … VALUES`, `INSERT … SELECT`, `INSERT OVERWRITE`, `insertInto(overwrite=True)` | registry row; re-measured tonight |
| by position | `INSERT … REPLACE WHERE`, `insertInto` (append) | measured live tonight |
| by name | `INSERT … BY NAME`, MERGE insert and update arms, `UPDATE … WHERE`, `writeTo().append()`, `overwritePartitions()` | registry row; re-measured tonight |
| by name | `saveAsTable` (append), `MERGE … INSERT *`, `MERGE … UPDATE SET *`, `UPDATE` with no `WHERE` | measured live tonight |

By position, names are ignored: a surplus source field refuses `EXTRA_STRUCT_FIELDS`, a short
source refuses `STRUCT_MISSING_FIELDS`. By name, the session's case rule matches names: a
target field with no source field refuses `CANNOT_FIND_DATA` with the dotted path
(`` `st`.`o`.`b` ``, `` `st`.`element`.`b` ``, `` `st`.`value`.`b` ``), a surplus source field
refuses `EXTRA_STRUCT_FIELDS`. No Spark cell stores a NULL the statement did not write.

**Reading a cell.** `1,2` is the stored `(a, b)`; `∅` is a stored NULL. A plain cell is the
same on main and on Spark. A bold cell is main's answer, then Spark's after the arrow.
Spark's refusals: `CFD` = `CANNOT_FIND_DATA`, `ESF` = `EXTRA_STRUCT_FIELDS`, `SMF` =
`STRUCT_MISSING_FIELDS`, all under `INCOMPATIBLE_DATA_FOR_TABLE`. Main's other refusals:
`R1` = `MERGE … cannot store-assign column` (the MERGE gate); `R2` = DataFusion
`Unsupported CAST from Struct`; `R3` = DataFusion `Cannot automatically convert`; `R4` = Arrow
`arguments need to have the same data type`; `R5` = Arrow `Incorrect number of arrays for
StructArray fields`; `R6` = DataFusion `type mismatch and can't cast`; `R7` = DataFusion
`Incompatible inputs for Union`. Shapes: `renamed` renames the second field, `renamed_first`
the first; `case` writes `A, B`; `case_reordered` writes `B, A`.

**Doors Spark pairs by position.**

| shape | `insert` | `insert_values` | `insert_overwrite` | `replace_where` | `df_insert_into` | `df_insert_into_overwrite` |
|---|---|---|---|---|---|---|
| top / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| top / reordered | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 |
| top / renamed | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 |
| top / renamed_first | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 |
| top / extra | **1,2** → ESF | **R6** → ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF |
| top / missing | **1,∅** → SMF | **R6** → SMF | **R5** → SMF | **1,∅** → SMF | **1,∅** → SMF | **R5** → SMF |
| top / case | **R2** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 | **R2** → 1,2 | 1,2 |
| top / case_reordered | **R2** → 2,1 | **R2** → 2,1 | 2,1 | **R2** → 2,1 | **R2** → 2,1 | 2,1 |
| nested / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| nested / reordered | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 |
| nested / renamed | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 |
| nested / renamed_first | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 |
| nested / extra | **1,2** → ESF | **R6** → ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF |
| nested / missing | **1,∅** → SMF | **R6** → SMF | **R5** → SMF | **1,∅** → SMF | **1,∅** → SMF | **R5** → SMF |
| nested / case | **R2** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 | **R2** → 1,2 | 1,2 |
| nested / case_reordered | **R2** → 2,1 | **R2** → 2,1 | 2,1 | **R2** → 2,1 | **R2** → 2,1 | 2,1 |
| array / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| array / reordered | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 |
| array / renamed | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 | **1,∅** → 1,2 | **1,∅** → 1,2 | 1,2 |
| array / renamed_first | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 | **∅,2** → 1,2 | **∅,2** → 1,2 | 1,2 |
| array / extra | **R3** → ESF | **R6** → ESF | **1,2** → ESF | **R3** → ESF | **R3** → ESF | **1,2** → ESF |
| array / missing | **R3** → SMF | **R6** → SMF | **R5** → SMF | **R3** → SMF | **R3** → SMF | **R5** → SMF |
| array / case | **R2** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 | **R2** → 1,2 | 1,2 |
| array / case_reordered | **R2** → 2,1 | **R2** → 2,1 | 2,1 | **R2** → 2,1 | **R2** → 2,1 | 2,1 |
| map / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| map / reordered | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 | **1,2** → 2,1 |
| map / renamed | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| map / renamed_first | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| map / extra | **R3** → ESF | **R6** → ESF | **1,2** → ESF | **R3** → ESF | **R3** → ESF | **1,2** → ESF |
| map / missing | **R3** → SMF | **R6** → SMF | **R5** → SMF | **R3** → SMF | **R3** → SMF | **R5** → SMF |
| map / case | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 |
| map / case_reordered | 2,1 | 2,1 | 2,1 | 2,1 | 2,1 | 2,1 |

**Doors Spark pairs by name.**

| shape | `insert_by_name` | `merge_insert` | `merge_insert_star` | `merge_update` | `merge_update_star` | `update` | `update_no_where` | `df_append` | `df_overwrite_partitions` | `df_save_append` |
|---|---|---|---|---|---|---|---|---|---|---|
| top / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| top / reordered | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| top / renamed | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,∅** → CFD | **1,2** → CFD | **1,∅** → CFD |
| top / renamed_first | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **∅,2** → CFD | **1,2** → CFD | **∅,2** → CFD |
| top / extra | **1,2** → ESF | ESF | ESF | ESF | ESF | ESF | ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF |
| top / missing | **R5** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,∅** → CFD | **R5** → CFD | **1,∅** → CFD |
| top / case | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 |
| top / case_reordered | **2,1** → 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | **R2** → 1,2 | **2,1** → 1,2 | **R2** → 1,2 |
| nested / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| nested / reordered | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| nested / renamed | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,∅** → CFD | **1,2** → CFD | **1,∅** → CFD |
| nested / renamed_first | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **∅,2** → CFD | **1,2** → CFD | **∅,2** → CFD |
| nested / extra | **1,2** → ESF | ESF | ESF | ESF | ESF | ESF | ESF | **1,2** → ESF | **1,2** → ESF | **1,2** → ESF |
| nested / missing | **R5** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,∅** → CFD | **R5** → CFD | **1,∅** → CFD |
| nested / case | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 |
| nested / case_reordered | **2,1** → 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | **R2** → 1,2 | **2,1** → 1,2 | **R2** → 1,2 |
| array / same | 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| array / reordered | 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| array / renamed | **1,2** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **1,∅** → CFD | **R4** → CFD | **1,∅** → CFD | **1,2** → CFD | **1,∅** → CFD |
| array / renamed_first | **1,2** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **∅,2** → CFD | **R4** → CFD | **∅,2** → CFD | **1,2** → CFD | **∅,2** → CFD |
| array / extra | **1,2** → ESF | **R1** → ESF | **R1** → ESF | **R1** → ESF | **R1** → ESF | **R7** → ESF | **R3** → ESF | **R3** → ESF | **1,2** → ESF | **R3** → ESF |
| array / missing | **R5** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **R1** → CFD | **R7** → CFD | **R3** → CFD | **R3** → CFD | **R5** → CFD | **R3** → CFD |
| array / case | 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R2** → 1,2 | **R2** → 1,2 | **R2** → 1,2 | 1,2 | **R2** → 1,2 |
| array / case_reordered | **2,1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R2** → 1,2 | **R2** → 1,2 | **R2** → 1,2 | **2,1** → 1,2 | **R2** → 1,2 |
| map / same | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| map / reordered | 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| map / renamed | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,2** → CFD | **1,2** → CFD | **1,2** → CFD |
| map / renamed_first | **1,2** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **1,2** → CFD | **1,2** → CFD | **1,2** → CFD |
| map / extra | **1,2** → ESF | **R1** → ESF | **R1** → ESF | **R1** → ESF | **R1** → ESF | **R7** → ESF | **R3** → ESF | **R3** → ESF | **1,2** → ESF | **R3** → ESF |
| map / missing | **R5** → CFD | CFD | CFD | CFD | CFD | CFD | CFD | **R3** → CFD | **R5** → CFD | **R3** → CFD |
| map / case | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | 1,2 | **R4** → 1,2 | 1,2 | 1,2 | 1,2 |
| map / case_reordered | **2,1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **R1** → 1,2 | **2,1** → 1,2 | **R4** → 1,2 | **2,1** → 1,2 | **2,1** → 1,2 | **2,1** → 1,2 |
**Counts, per format version (512 cells; double them for both versions).**

| Main against Spark | All 16 doors | The 11 doors of the brief (352 cells) |
|---|---|---|
| same answer | 233 | 164 |
| **differs** | **279** | **188** |
| stores a NULL the statement did not write (silent NULL) | 48 | 34 |
| stores the other field's value (position against name) | 35 | 27 |
| stores and drops a surplus source field Spark refuses | 26 | 24 |
| stores by position where Spark refuses the name | 20 | 20 |
| refuses where Spark stores | 78 | 38 |
| both refuse, main with a raw text | 72 | 45 |

The 48 silent NULLs: 38 for a renamed field (Spark stores both fields in 24 of them and
refuses `CANNOT_FIND_DATA` in 14) and 10 for a missing field (Spark refuses). They sit on
`INSERT … VALUES`, `INSERT … SELECT`, `REPLACE WHERE`, `writeTo().append()`, `insertInto`,
`saveAsTable`, and on `UPDATE … WHERE` under an array (2 cells). The MERGE doors store no
NULL: at the top level and in a nested struct they already answer as Spark does, and under an
array or a reordered map value they refuse with the gate's own text (`R1`, 44 cells, 24 of
them where Spark stores).

Differing cells by door: `insert` 24, `insert_values` 24, `replace_where` 24,
`df_insert_into` 24, `df_append` 23, `df_save_append` 23, `update_no_where` 21,
`insert_by_name` 20, `df_overwrite_partitions` 20, `insert_overwrite` 12,
`df_insert_into_overwrite` 12, each of the four MERGE doors 11, `update` 8.

## 3. Every place on main where a struct source is paired to a struct target on a write

Read at `018d0efd`. Eleven places in three kinds.

**A cast that re-pairs the value.**

| # | Function | File:line | Rule it applies | Doors that reach it |
|---|---|---|---|---|
| S1 | `conform_batch_scoped` | `crates/repark-iceberg/src/write/conform.rs:142` (cast at 176) | Arrow's struct cast | every staged append; the Rust `append` API; the streaming sink |
| S2 | `positional_map_all_columns`, `positional_map_column_list` | `crates/repark-iceberg/src/write/overwrite.rs:144` (cast at 171), `:183` (cast at 228) | Arrow's struct cast | `INSERT OVERWRITE`, `overwritePartitions`, `insertInto(overwrite=True)`, `REPLACE WHERE`, the owned append |
| S3 | `store_assign_source_column` | `crates/repark-iceberg/src/write/partition_overwrite.rs:91` (cast at 101) | Arrow's struct cast | static partition overwrite |
| S4 | DataFusion's `insert_to_plan` conform cast, planned through `execute_passthrough_inner` | `crates/repark-spark/src/spark_ast.rs:152` (state), `:224` (analysis); read by `refuse_unassignable_insert` `crates/repark-iceberg/src/write/insert_gate.rs:45` and `conform_store_exprs` `crates/repark-iceberg/src/write/store_overflow.rs:230` | DataFusion's struct cast | plain `INSERT`, the owned append, `REPLACE WHERE`, `writeTo().append()`, `insertInto`, `saveAsTable` |
| S5 | `nested_leaf_cast_sql`, through `store_assignment_cast_sql` | `crates/repark-iceberg/src/write/update_cast.rs:20`, `:12`; callers `write/merge/insert.rs:146`, `:376`, `crates/repark-spark/src/merge/nested_assign.rs:391`, `:393` | `arrow_cast` text, DataFusion's struct cast | MERGE arms, `UPDATE` |

**A rebuild in SQL text.**

| # | Function | File:line | Rule | Doors |
|---|---|---|---|---|
| S6 | `value_sql_for` with `source_field`, `names_match_exactly` | `crates/repark-spark/src/merge/nested_assign.rs:412`, `:126`, `:397` | by name, the session's case rule, Spark's refusals; structs in structs only | MERGE `UPDATE SET`, `UPDATE` |
| S7 | `needs_rebuild` and the star expansion | `crates/repark-spark/src/merge/nested_assign/expand.rs:25`, `:121`, `:205` | by name | `MERGE … INSERT *`, `UPDATE SET *` |

**A check that accepts or refuses a pairing.**

| # | Function | File:line | Rule | Doors |
|---|---|---|---|---|
| S8 | `struct_fields_store_assignable`, `map_entries_store_assignable` under `ansi_store_assignable` | `crates/repark-iceberg/src/write/store_assign.rs:88`, `:99`, `:25` | names equal in order ignoring case; no list arm | the MERGE gate (`write/merge/insert.rs:217`, `:286`) |
| S9 | `incompatible_nested_message` with `struct_member` | `crates/repark-iceberg/src/write/update_cast.rs:97`, `:146` | by name | MERGE, `UPDATE` |
| S10 | `check_type` under `refuse_nested_mismatch` | `crates/repark-spark/src/insert_by_name/nested.rs:54`, `:8` | by name | the by-name source of `REPLACE WHERE` and schema evolution |
| S11 | `refuse_unless_write_store_assignable` with `is_flat` | `crates/repark-iceberg/src/write/store_assign.rs:157`, `:172` | none: a nested pair is not judged | every INSERT door |

A grep of `crates/repark-iceberg/src/write` and `crates/repark-spark/src` finds a cast in 21
files. Six are in the tables above. Seven were read and cast only to `Utf8`, `Int64` or a
partition literal (`merge/copy_on_write.rs`, `merge/merge_on_read.rs`, `merge/mod.rs`,
`merge/row_lineage.rs`, `static_value.rs`, `distribution.rs`,
`router/insert_positional/partition_append.rs`). The other eight were not read tonight and
are step 0's to clear (halt H1). `refuse_nested_ns_supply`
(`crates/repark-iceberg/src/write/nested_ns_gate.rs:79`) refuses a nested `timestamp_ns` leaf
before any of these and pairs nothing; it stays as it is.

## 4. The seam

**One seam: yes.** One function decides every pairing from the door's mode and the two types,
and one kernel applies its answer. They live in one new file,
`crates/repark-iceberg/src/write/struct_pairing.rs`.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StructPairing {
    ByPosition,
    ByName { case_sensitive: bool },
}

pub fn pair_store_value(
    pairing: StructPairing,
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Result<StorePairs>;

impl StorePairs {
    pub fn is_identity(&self) -> bool;
    pub fn paired_type(&self) -> DataType;
    pub fn apply(&self, value: &ArrayRef) -> Result<ArrayRef>;
}
```

**The rule `pair_store_value` holds, all of it.** Walk source and target together. Two structs
are paired by the mode: by position, the i-th source field feeds the i-th target field, a
longer source refuses `EXTRA_STRUCT_FIELDS` and a shorter one `STRUCT_MISSING_FIELDS`; by
name, each target field takes the one source field of that name under the mode's case rule, a
target field without one refuses `CANNOT_FIND_DATA` and a source field left over refuses
`EXTRA_STRUCT_FIELDS`. A list pairs its element (path `element`), a map its key and value
(paths `key`, `value`), with the same mode at every depth. A dictionary or run-end-encoded
value is paired as its values. Anything else is a leaf and is not looked at. The texts are
Spark's, with the table label the door prints (measured: the INSERT doors and the DataFrame
writers name the table; MERGE and `UPDATE` print an empty pair of backticks). There is no arm
that fills a NULL and no arm that drops a field.

**What `apply` does.** It returns the value with the target's field names in the target's
order and the source's leaf types: struct children are re-ordered without copying a buffer,
the validity of every level is kept. It converts no leaf. The cast that already follows on
each route still converts leaves, and it now sees a source whose names are the target's in
order, where Arrow's cast, DataFusion's cast and an exact match are the same pairing. The
pairing those casts did on their own has nothing left to decide. Pairing an already paired
value is the identity in either mode.

**How every door reaches it.** The kernel is one scalar function, `StorePairKernel`: its
planning hook (`return_field_from_args`) calls `pair_store_value` with the argument's type, so
a refusal is raised at plan time and no file is written; its `invoke` calls `apply`. It is
placed at the two places where a store value still has its source type:

| Call site | File:line on main | Routes | Mode |
|---|---|---|---|
| `conform_one_store_expr`, `pairing` added to `ConformStoreFlags` | `crates/repark-iceberg/src/write/store_overflow.rs:286`, `:280` | the DataFusion-planned INSERT (S4) through `conform_store_exprs` `:230`; every staged route through `wrap_store_outputs` `:603` (`insert_by_name.rs:225`, `insert_overwrite/store.rs:33`, `write/merge/insert.rs:184`, `:194`, `:256`, `write/predicate_dml.rs:412`) | the caller's argument |
| `store_assignment_cast_sql` | `crates/repark-iceberg/src/write/update_cast.rs:12` | an assignment value inside the MERGE and `UPDATE` rewrite text (S5), where the value sits in a `CASE` beside the old value | by name, the session's case rule |

`conform_one_store_expr` is already the one function every store output passes for the
overflow conform of CAST-OVERFLOW-INSERT-1; the unit adds the pairing to it and does not add a
second conform. An identity pairing leaves the expression untouched, so a write with no
struct, or with the target's names in order, plans exactly as on main.

**Where the mode comes from.** The door states it once.

- SQL text states it: `INSERT … BY NAME`, MERGE and `UPDATE` are by name; every other `INSERT`
  is by position.
- The DataFrame doors state it through the flag that exists,
  `StatementWriteOptions.source_by_name` (`crates/repark-spark/src/write_options.rs:25`, set
  by the facade's `run_through_temp_view`,
  `python/repark/src/repark/spark/dataframe/writer_layout.py:95`): true for
  `writeTo().append()`, `overwritePartitions()`, `saveAsTable` (append); false for
  `insertInto`. Today only `overwrite(condition)` sets it.
- On the DataFusion-planned INSERT the analyzer rule has no door. The statement's mode rides
  on the statement's own state snapshot: `execute_passthrough_inner` takes `ctx.state()` by
  value at `spark_ast.rs:152` and analyzes the plan with that snapshot at `:224`, so a value
  set on the snapshot's configuration is seen by `StoreOverflowCast` for this statement and by
  no other. Absent, the mode is by position for `WriteOp::Insert` and by name for
  `WriteOp::Update`.

**Why the by-name DataFrame doors are not simply routed to the by-name executor.** Measured
tonight on main, 4,000,000 rows, median of seven runs, twice: a flat table takes 0.088 s
through the plain route and 0.47 s through `INSERT … BY NAME` (5.3 times); with one struct
column 0.094 s against 0.66 s (7.2 times). The reroute fails the 1.05 cost gate before it is
written, so the mode travels to the conform instead.

**The three batch casts stop pairing.** S1, S2 and S3 keep their leaf cast and gain one
guard: a struct that reaches them with names that are not the target's in order is refused
(§6, hint H-1). A route nobody listed therefore refuses; it cannot store by a rule of its own.
This is what frees the design from an enumeration of routes: the route table of the order's
step 0 is a list of what must not refuse, and a hole in it shows as a red fixture cell, not
as a wrong stored value.

**Why this is not the R-011 and R-017 mistake.** Those designs read how a value was produced
(a plan walk, a list of spellings) and each verifier found a production nobody listed. The
seam reads two types and a mode. A view, a cached frame, a subquery, a `UNION` or an array
element changes how a value is produced and does not change its type, so none of them is a
case.

## 5. What gets deleted

- S6: the struct arm of `value_sql_for`, with `names_match_exactly` and `source_field`; a
  whole struct value goes to `store_assignment_cast_sql` unchanged. Field assignment
  (`SET t.st.a = …`, `Aligner`) is another feature and stays.
- S7: `needs_rebuild` and both rebuild arms of the star expansion.
- S8: `struct_fields_store_assignable` and `map_entries_store_assignable`; the MERGE gate
  asks `pair_store_value` (by name) and judges the leaf pairs it returns with the flat matrix,
  through lists too; the 44 `R1` cells then take Spark's answer (24 store, 20 refuse with
  Spark's class).
- S9: the struct and map arms of `incompatible_nested_message`, and `struct_member`.
- S10: `insert_by_name/nested.rs`, whole file.
- S11: the `is_flat` return; a nested pair is paired first, then its leaves are judged as on
  main (not at all on the INSERT doors; that question is not this unit's).
- The Spark texts for `CANNOT_FIND_DATA` and `EXTRA_STRUCT_FIELDS`
  (`merge/nested_assign/render.rs:210`, `:218`) move to the seam, their one home.
- Nothing is deleted from S1 to S5: their casts stay for the leaves.

## 6. Cells that become refusals, with their hint text

Every refusal Spark makes is made with Spark's class and text. Three refusals are RePark's
own.

| Hint | When | Text |
|---|---|---|
| H-1 | a struct reaches a batch cast (S1, S2, S3) unpaired: the Rust `append` API, the streaming sink, or a route the step 0 table missed | ``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table {table}: the struct {path} has the fields {source names} and the table has {target names}. This entry does not pair struct fields; give the value the table's field names in the table's order.`` |
| H-2 | by name, case-insensitive, two source fields match one target field | ``[INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME] Cannot write incompatible data for the table {table}: Ambiguous column name in the input data {path}.`` (a placeholder: step 0 measures what Spark answers and the text follows it) |
| H-3 | a `Union` or another layout that holds a struct and is not in the rule | the H-1 text with ``the layout {arrow type} is not written into a struct column`` in place of the field lists |

H-1 changes main for the two entries below the SQL router, which today pair by Arrow's rule
(question Q3). No cell of the 512 takes a hint: every measured cell either stores Spark's
value or refuses with Spark's class, except the two lists that follow.

**Cells that stay as they are, named before any code.**

- L1, 28 cells per format version, both engines refuse, main with a raw text: a list or a map
  whose struct has a different number of fields on the DataFusion-planned INSERT (`R3`, 20
  cells: DataFusion's logical planner raises before any rule runs) and a `VALUES` row with a
  surplus or a missing field (`R6`, 8 cells; parity row R-018). If step 0 finds that
  `void_type::refuse_insert_source_types` (called before planning by every INSERT route,
  `router/insert_positional.rs:28`) already holds both types for them, it calls
  `pair_store_value` for the refusal and L1 is dropped.
- L2, 32 cells per format version: `UPDATE` with no `WHERE`. It cannot assign any nested
  column on main, the same shape included (`R4`; parity row R-014). The door is by name on
  Spark and is given that mode, and its cells are pinned as main answers until R-014 is fixed.

## 7. Fixture

Committed red before the first product commit.

- `python/repark/tests/writes/_record_ice_struct_pairing_1.py`: the lane's `pair.py`, as the
  recorder; `ice_struct_pairing_1_spark_oracle.json` (1,024 cells and the 192 case-sensitive
  cells from Spark 4.1.2) and `ice_struct_pairing_1_main.json` (main's 1,024, for the
  controls).
- `python/repark/tests/writes/test_ice_struct_pairing_1.py`: the expected answer of a cell is
  computed by the harness from the rule of §4 (mode of the door, field shape), never written
  per cell. Test one: the rule reproduces Spark's oracle in every cell. Test two: the engine
  answers the rule in every cell outside L1 and L2. Test three: L1 refuses and writes no file;
  L2 and the 233 unchanged cells answer what `ice_struct_pairing_1_main.json` holds. Test four:
  for one cell per door the stored fields are read from the Parquet file, not through a scan.
- Red on main: 230 cells per format version (279 less the 21 differing cells of L2 and the 28
  of L1), 460 in all.
- `crates/repark-iceberg/src/write/struct_pairing.rs`, in-module table test: 2 modes × 2 case
  rules × the 8 field shapes × every layout of the rule (`Struct`, `List`, `LargeList`,
  `ListView`, `LargeListView`, `FixedSizeList`, `Map` key and value, `Dictionary`,
  `RunEndEncoded`), NULL struct values and NULL elements included; and the property that
  pairing a paired value is the identity in both modes.
- `crates/repark-spark/tests/struct_pairing_doors.rs`: the 16 doors at the Rust door for the
  shapes `reordered`, `renamed`, `extra`, `missing`.

## 8. Halt criteria (step 0 measures them; the unit stops and reports, no code)

- H1: a struct-capable cast or pairing place on the write path that §3 does not list.
- H2: the statement's mode does not reach `conform_store_exprs` through the statement's own
  state snapshot (probe: set by name on the snapshot, `INSERT … SELECT` a reordered struct,
  the conform must be handed by name; a second statement on the same session must be handed
  by position). There is no fallback inside the cost gate (§4).
- H3: DataFusion raises on its own conform cast before `StoreOverflowCast` runs, for a pair
  the rule stores. Measured tonight: the recased struct raises at physical planning, after
  the rule (the logical plan holds the cast); only pairs both engines refuse raise earlier.
- H4: the kernel cannot be named in the MERGE and `UPDATE` rewrite text without being visible
  to user SQL (question Q2).
- H5: `source_by_name` has a reader that moves a statement to another route, or changes a
  cost, once the facade sets it for `append`, `overwritePartitions` and `saveAsTable`. Its
  readers today: `router/nested_ns.rs:28` and `router/insert_positional/replace_where.rs:294`.
- H6: a write-cost median above 1.05 of main, or an `EXPLAIN` of a write with no struct that
  differs from main's by one byte.
- H7: a route from a door to S1, S2 or S3 that passes neither call site of §4 and cannot be
  given the `wrap_store_outputs` call (the partition overwrite route of
  `insert_overwrite.rs:224` is the known candidate: it stages without `conform_types`).

## 9. Mutants and the revert-alone test

Each mutant must turn at least one named fixture cell red.

1. By position pairs by name.
2. By name ignores the case rule.
3. A missing field is filled with NULL instead of refused.
4. A surplus field is dropped instead of refused.
5. The walk stops at a list; at a map value; at a map key; at a dictionary.
6. The guard of S1 is removed (repeat for S2 and S3): the batch cast pairs again.
7. One door at a time is handed the other mode (16 mutants).
8. The default of `WriteOp::Insert` is by name.
9. `is_identity` answers true for a reordered struct.

**Revert-alone.** The product lands in three commits: the fixture (red), the seam file with
its table test and no caller (the fixture stays red, everything else green), the callers with
the deletions (green). Reverting the third commit alone on a throwaway branch must leave the
tree compiling and the suite at exactly the fixture's 460 red cells and nothing else red; the
hand-back pastes the test counts. The facade's three `source_by_name` flags are a fourth
commit with its own revert-alone run, so the by-name DataFrame doors can go back to main's
behaviour without touching the seam.

## 10. Cost risk and the target release

- An identity pairing adds no node to the plan, so every write without a struct, and every
  struct write with the target's names in order, plans as main does (H6 proves it by
  `EXPLAIN`). The decision costs one walk of two types per store column at plan time.
- A non-identity pairing adds one kernel call per batch that re-orders child arrays and copies
  no buffer, in place of the pairing the following cast did.
- The risk that is not zero is the mode's path on the DataFusion-planned INSERT (H2) and the
  flag on the DataFrame doors (H5). Both are measured in step 0, before code.
- Measured tonight for the record: the plain route and `writeTo().append()` cost the same on
  main (0.088 s and 0.091 s); `INSERT … BY NAME` costs 5.3 to 7.2 times the plain route.

**Target: 1.5.6.** The owner's rule gives 1.5.6 to a sketch that names one seam, and this one
does: `pair_store_value`, applied by one kernel at two call sites, with three guards that
refuse. If step 0 meets H2, H3, H4 or H7 the seam is no longer one, and by the same rule the
unit moves to 1.5.7 and the v2 label waits on it.

## 11. Not measured tonight

`writeTo().overwrite(condition)`; a write with `mergeSchema` or `accept-any-schema`, where a
surplus field may evolve the table instead of refusing; the streaming sink and the Rust
`append` API on Spark's side (Spark has no twin of the second); two source fields that differ
only in case; a required (`NOT NULL`) struct field; `spark.sql.storeAssignmentPolicy` other
than the default; leaves other than `INT`. Step 0 of the order measures the first, second,
fifth and sixth on both engines and adds their cells to the fixture.

## 12. Questions for the owner

- Q1 (ruling): three DataFrame doors change from by-name-with-NULL to Spark's by name
  (`append`, `saveAsTable`) and two SQL doors from by name to by position (`INSERT`,
  `REPLACE WHERE`). A reordered struct that stored `a` from the field named `a` yesterday will
  store `a` from the first field on `INSERT … SELECT`, as Spark does. Lean: follow Spark with
  no switch, and say so in the 1.5.6 release note; 35 measured cells per format version store
  a different value after the unit.
- Q2 (ruling): the MERGE and `UPDATE` rewrites are SQL text, so the kernel is named there, as
  the wall kernels are today through `store_kernel_sql`. Parity row R-013 calls such a name
  visible to user SQL a defect, and ICE-TSNS-TYPE-PLANNER-1 retires the by-name wall kernels.
  Lean: register the kernel as the store kernels are registered on main and add its name to
  R-013's row; the alternative is a planning state private to the rewrite.
- Q3 (owner): H-1 refuses an unpaired struct on the Rust `append` API and on the streaming
  sink, which pair by Arrow's rule today. Lean: refuse with the hint in 1.5.6; give either
  entry a mode of its own only when a user asks.
