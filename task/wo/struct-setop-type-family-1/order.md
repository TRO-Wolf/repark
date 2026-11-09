# STRUCT-SETOP-TYPE-FAMILY-1: one shared struct-type walker, and the set-operation check across type variants

**Grade:** B (guided). **Builder:** Muse (`muse-spark-1.3-contributor`), effort max. **Review:** one scoped review, cap
one round.
**Ruling (owner, 2026-11-09, verbatim "defaults", adopting the Unit C park memo):**
- the unit runs before `STRUCT-COMPARE-BY-POSITION-1`'s build;
- the variant table is measured first, and it is the clause;
- the scoped-review rule binds: a FAIL cards what it found, and the lane moves on.

**Closes:** ledger C-117 (F-I), S1, which blocks v1.5.7. Card:
[struct-setop-type-family-1-card-2026-11-09.md](../../roadmap/mid-term/struct-setop-type-family-1-card-2026-11-09.md).

## Clause

On both doors, the set-operation name check of `STRUCT-SETOP-REORDER-REFUSAL-1` is **kind-invariant**. A cell whose
struct operands sit inside any Arrow representation of a list or an encoded value gets the same outcome as its plain
`List` twin. Concretely:
- `LargeList`, `FixedSizeList` of any size, `ListView`, `LargeListView`, `RunEndEncoded` and `Dictionary`;
- at any depth and in either orientation;
- under `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` and `unionByName`.

A reordered or renamed element struct refuses with the unit's `[INCOMPATIBLE_COLUMN_TYPE]` class, and a conforming
one answers. The table is the clause: `evidence/variant-table.md`, which has 2,177 cells on Spark 4.1.2, RePark 1.5.6
and the unit C head `b8ad7a9c`.

## Design

**New module:** `crates/repark-core/src/session/df_guards/struct_walk.rs`. It is public, so that
`STRUCT-COMPARE-BY-POSITION-1` in `repark-spark` calls the same walker.

1. **`peel_encoding(&DataType) -> &DataType`.**
   - `Dictionary(_, V)` returns `V`.
   - `RunEndEncoded(_, values)` returns the values field's type.
   - It repeats until the type is neither.
2. **`list_element(&DataType) -> Option<&FieldRef>`.** It returns the element field of `List`, `LargeList`,
   `FixedSizeList`, `ListView` or `LargeListView`, and `None` for anything else.
3. **`holds_struct(&DataType) -> bool`.** True when a `Struct` occurs at any depth, after peeling and through list
   elements, map keys and values, and struct fields.
4. **`first_struct_name_mismatch(left, right)`** moves into the walker, made public, with this order on each pair
   after `peel_encoding` on both sides:
   1. if either side is `Null`, there is no mismatch: the NULL skip stays first;
   2. `Struct` with `Struct`: compare names exactly, then recurse field by field. This is unchanged;
   3. both sides in the list family, kind and size free: recurse into the elements, with the path suffix `[]`;
   4. `Map` with `Map`: recurse into key and value, as today. If either entries type is not a two-field struct, fall
      through to step 5;
   5. **fallthrough:** if `holds_struct(left) && holds_struct(right)`, return a mismatch that names both types at that
      path (a new variant, "unpaired"). Otherwise return no mismatch.

The `Union` arm, the INTERSECT/EXCEPT join arm and the `RecursiveQuery` arm in `struct_setop.rs` call the walker. No arm
keeps its own match.

- **Refusal text for the unpaired variant:** the existing `[INCOMPATIBLE_COLUMN_TYPE] <OP> can only be performed ...`
  form, naming the path and both Arrow types. The name-list variant's text is unchanged.
- **Export:**
  - `pub mod struct_walk` in `df_guards.rs`;
  - re-exported through the existing `pub use df_guards::{...}` list in `crates/repark-core/src/session.rs`;
  - re-exported through the existing session re-export list in `crates/repark-core/src/lib.rs`, with no new `pub use`
    line. Hold `lib.rs`'s exact ceiling by reflowing the list.

## Type-variant inventory (the 2026-11-09 rule)

1. **Representations that reach the seat, by source:**

   | Source | Representations |
   |---|---|
   | SQL `array(...)`, `make_array` | `List` |
   | an Iceberg read | `List` |
   | polars | `List` arrives as `LargeList`; `Array` arrives as `FixedSizeList` |
   | pandas Arrow dtypes | any list kind, and `RunEndEncoded` |
   | facade `createDataFrame` | the kind as given; it refuses a pandas Arrow `Dictionary` dtype and a pyarrow `Table` |
   | native door | `arrow_cast` to any kind; DataFusion refuses an `arrow_cast` to `Dictionary(List(struct))` |

2. **Pairs DataFusion 54.1.0's coercion unifies at this seat:**
   - `type_union_coercion` and `comparison_coercion` (`datafusion-expr-common-54.1.0/src/type_coercion/binary.rs`):
     - name-blind equality: same kind, nested names may differ (`List`, `LargeList`, `ListView`, `LargeListView`,
       `FixedSizeList` of one size, `Struct`, `Map`, `Dictionary`, `RunEndEncoded`, `Union`);
     - `dictionary_coercion` and `ree_coercion`: encoded against encoded, or against its value type;
     - `list_coercion`: `List`, `LargeList` and `FixedSizeList` in any mix, including two `FixedSizeList` sizes. It has
       no arm for the views, so a view against a plain list already fails closed;
     - `struct_coercion` and `map_coercion`.
   - The fold is `coerce_union_schema_with_schema` (`datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs`).
3. **The fallthrough** refuses whenever both sides hold a struct. After steps 1 to 4, no pair that DataFusion 54.1.0
   unifies reaches it with a struct on both sides, except the Arrow `Union` type, which Spark does not have. So it
   costs no over-refusal on the measured table, and it turns any future variant into a refusal instead of a wrong
   write.

## Insertion inventory

Unchanged from `STRUCT-SETOP-REORDER-REFUSAL-1`: this unit changes the comparison function, not the seats.
- the session analyzer list, and `build_expr_context`, both immediately after `type_coercion`;
- native `CREATE VIEW` at read;
- the `PREPARE` belt in `PreExecute::guard`;
- the `RecursiveQuery` arm.

Each is already pinned by unit C's tests, which must stay green.

## Pins (tests in the same commit as the code they pin)

1. **Rust unit tests of the walker**, on hand-built types:
   - every ordered list-kind pair;
   - two `FixedSizeList` sizes;
   - `Dictionary` and `RunEndEncoded` on either side and both sides;
   - `Map` with renamed entry fields, value struct same and reordered;
   - a `Union` type holding structs on both sides (the fallthrough refuses);
   - a non-struct fallthrough (no refusal);
   - the NULL skip;
   - three-deep nesting.
2. **Native-door Rust pins** (`crates/repark-sql/tests/`): the variant table's `arrow_cast` cells, read and insert,
   including the `RecursiveQuery` row.
3. **Facade pins** (`python/repark/tests/sql/`): the variant table's polars, pandas-dtype, Iceberg-read-against-polars
   and temp-view cells, across union, subtract and intersect, read and insert.
   - Every reordered variant cell refuses with the unit's class, as its `List` twin does.
   - Every conforming variant cell answers as its Spark twin.
4. **F-K** (accepted, owner, 2026-11-09): `SELECT named_struct('a', 1, 'b', 2) AS s UNION ALL SELECT CAST(NULL AS STRUCT<b: INT, a: INT>) AS s`
   answers `[NULL, {a:1,b:2}]` on both doors, equal to Spark. The typed-NULL-first twin refuses.

## Acceptance

- The variant table re-run on the new head:
  - every seat-S cell that is `VARIANT-DIFFERS` on `b8ad7a9c` becomes `INVARIANT`;
  - the 152 hidden cells refuse;
  - no head cell that answered equal to Spark changes.
- Unit C's 200-cell domain table, 252-cell controls and delta-2 pins are unchanged.

## Halts (hand back, do not work around)

- A head cell that answered equal to Spark on `b8ad7a9c` now refuses.
- Any of unit C's pins changes outcome.
- A file outside the list in the build brief needs a change.
- The design above contradicts the code.

## Not in this unit

- The comparison rule (`STRUCT-COMPARE-BY-POSITION-1`), which consumes the walker later.
- Facade ingest normalisation (`FACADE-INGEST-ARROW-NORMALIZE-1`).
- Explicit `CAST` pairing (`STRUCT-USER-CAST-POSITIONAL-1`).
- `unionByName` nested pre-alignment.
- The native door's Trino reference, which is unmeasured.
