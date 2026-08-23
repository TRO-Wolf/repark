# Unit ledger — WI-1 ANSI store-assignment gate on the non-MERGE write paths

**Unit:** WI-1 · **Date:** 2026-08-15 · **Lane:** repark ·
**Branch:** `fable/wi1-insert-store-gate` · **Base (FROZEN):** `8cbde88` (conductor-14 closeout, #137)

**Charter:** close the S1-class durable-corruption finding recorded in
`planning/hardening/G63-DATE-INT-DESIGN.md` §1.4 — the store-assignment gate that #111 (MERGE
INSERT) and #135 (MERGE UPDATE SET) shipped had exactly **two** call sites in the whole tree, both
under `write/merge/`. Every plain insert door persisted a `Date32 → Int32` reinterpretation
(`18262`, days since 1970-01-01) into a committed Iceberg data file. This ledger does **not** edit
`docs/spark-sql-iceberg-parity.md` or `STATUS.md`.

---

## 0. What was measured, first

Re-measured on this base (`8cbde88`, workspace `0.2.0`) with a fresh `maturin develop`, memory
catalog, `t_int(k INT, v INT)` fed from `t_date(k INT, v DATE)`, values read back after each write.
This reproduces O-7's §1.4 column 2 on a tree that already carries #111 **and** #135:

| # | Door | Spark 4.1.2 ANSI (O-7 oracle) | repark @ `8cbde88` | repark @ this unit |
|---|---|---|---|---|
| 1 | `INSERT INTO t SELECT k, v FROM t_date` | refuse `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` | **writes `18262`** | **writes `18262`** — NOT gated, §4 |
| 2 | `INSERT INTO t VALUES (2, DATE '…')` | refuse, same class | **writes `18262`** | **writes `18262`** — NOT gated, §4 |
| 3 | `INSERT OVERWRITE t SELECT …` | refuse, same class | **writes `18262`** | **REFUSES** |
| 4 | `df.writeTo("t").append()` | refuse | **writes `18262`** | **writes `18262`** — NOT gated, §4 |
| 5 | `df.write.mode("append").insertInto("t")` | refuse | **writes `18262`** | **writes `18262`** — NOT gated, §4 |
| 5b | `df.write.mode("overwrite").insertInto("t")` | refuse | writes `18262` | **REFUSES** |

Refusal text now emitted on the gated doors, verbatim:

```
INSERT OVERWRITE cannot store-assign column `v`: source type Date32 is not ANSI-store-assignable
to target type Int32 (Spark INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST; add an explicit CAST
only if the reinterpretation is intended semantics)
```

That is the red-then-green evidence: the same five-door script, same session shape, before and
after the diff — one door flipped from a silent `18262` to a refusal, four did not, for the
structural reason in §4.

---

## 1. Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | `ansi_store_assignable` / `normalize_for_assignment` have exactly two call sites, both in `merge/insert.rs`; nothing outside `merge/` reads them. | PROVEN — grep over `crates/**` returns only `insert.rs` |
| C-002 | The hoist therefore needs **no** `merge/mod.rs` edit (`mod.rs` imports `insert_projection` / `insert_stream_checked` / `store_assignment_then_sql` / `update_stream_checked`, none of the matrix). | PROVEN — `merge/mod.rs` untouched in the diff |
| C-003 | MERGE error text stays byte-identical: `merge/insert.rs` keeps a `MERGE `-prefixed path-label wrapper and passes the coarse `INCOMPATIBLE_DATA_FOR_TABLE` class. | PROVEN — `test_merge_store_assign.py` 10/10 green untouched; `insert_gate_tests` unchanged |
| C-004 | The matrix is **not** a CAST-legality matrix and must not be wired to one (Spark's store-assignment matrix permits `Date32 → Timestamp` and refuses `Timestamp → Int64`; the cast matrix does the reverse). | PROVEN — stated in the `store_assign.rs` module docs, per G6-3 design §3.3 |
| C-005 | Nested pairs must NOT gain a new refusal on the append/overwrite conform paths — the v1 matrix judges them by identity, and `List<Utf8View>` → `List<Utf8>` conforms correctly today. | PROVEN — `refuse_unless_write_store_assignable` excuses nested pairs; `nested_pairs_are_excused_by_the_write_gate` |
| C-006 | Gate runs BEFORE any `cast_with_options`, i.e. before a single byte is written to a data file. | PROVEN — call sites precede the kernel in `conform_batch`, `positional_map_all_columns`, `positional_map_column_list` |
| C-007 | Positives (widening, narrowing, NULL-fill, atomic→string, date↔timestamp, identity) still write. | PROVEN — six positive controls in `test_insert_store_assign.py`; 342/342 `repark-iceberg` unit tests |
| C-008 | Fence: `store_assign.rs` (new) + `merge/insert.rs` (move/re-import only) + `append.rs` + `overwrite.rs` + `write/mod.rs` (one `mod` decl) + map.md rows + the new python test + this ledger. Nothing else. | PROVEN — diff names |
| C-009 | Plain `INSERT INTO` has no seam inside `crates/repark-iceberg/src/write/` and cannot be gated by this unit. | PROVEN — §4 |
| C-010 | `map.md` lockstep in the same commit as the code. | PROVEN — §3 |

---

## 2. Implementation

**`crates/repark-iceberg/src/write/store_assign.rs` (new).** The matrix, moved verbatim from
`merge/insert.rs`:

* `normalize_for_assignment` — strips dictionary encoding. Unchanged.
* `ansi_store_assignable` — Spark `Cast.canANSIStoreAssign` over Arrow types. Unchanged, line for
  line.
* `refuse_unless_ansi_store_assignable(op, spark_class, column, src, dst)` — the shared refusal.
  `op` is now the whole label the message opens with and `spark_class` the citation, so the two
  caller families differ only in their arguments, never in the matrix or the format string.
* `refuse_unless_write_store_assignable(op, column, src, dst)` — the non-MERGE entry point:
  `WRITE_SPARK_CLASS` (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, the sub-class O-7
  measured on these doors) plus the C-005 nested-pair narrowing.

**`crates/repark-iceberg/src/write/merge/insert.rs`.** Matrix deleted; a five-line wrapper takes
its place that prepends `MERGE ` to the path label and passes `MERGE_SPARK_CLASS`. The message
string is reconstructed identically. `insert_gate_tests` now imports `ansi_store_assignable` from
its new home; the assertions are untouched.

**`crates/repark-iceberg/src/write/append.rs`** — `conform_batch`, the per-column resolution arm.
Gate label `append`. Covers the public `append(catalog, ident, batches)` entry point (the
downstream-consumer first-write surface, ask A1), `write_partitioned_data_files{,_with_concurrency}`
and both `_from_stream` variants — every consumer batch that meets the Iceberg write schema in this
crate.

**`crates/repark-iceberg/src/write/overwrite.rs`** — both arms of `positional_map_overwrite_batch`
(`positional_map_all_columns`, `positional_map_column_list`). Gate label `INSERT OVERWRITE`. This
is the seam the non-empty `INSERT OVERWRITE` stage-then-swap streams every source batch through
before a data file exists.

**`crates/repark-iceberg/src/write/mod.rs`** — one `pub(crate) mod store_assign;` declaration.

---

## 3. Files touched

- `crates/repark-iceberg/src/write/store_assign.rs` (new)
- `crates/repark-iceberg/src/write/merge/insert.rs`
- `crates/repark-iceberg/src/write/append.rs`
- `crates/repark-iceberg/src/write/overwrite.rs`
- `crates/repark-iceberg/src/write/mod.rs`
- `crates/repark-iceberg/src/write/map.md`
- `crates/repark-iceberg/src/write/merge/map.md`
- `python/repark/tests/test_insert_store_assign.py` (new)
- `python/repark/tests/map.md`
- `task/map.md`, `task/wi1-insert-store-gate-ledger.md` (this file)

`crates/repark-iceberg/src/write/merge/mod.rs` is **not** in the list — see C-002.

---

## 4. The half this unit could NOT close, named

Doors 1, 2, 4 and 5 of §0 all lower to the same statement: plain `INSERT INTO <table> SELECT …`
(`writeTo().append()` builds a by-name projection into one; `write.insertInto()` builds a
positional `SELECT *` into one; `INSERT INTO … VALUES` is the same statement with a `VALUES` body).
The Spark router does not intercept it —
`crates/repark-spark/src/router.rs:257` refuses a read-only catalog and then calls
`passthrough_after_p11`, handing the statement to DataFusion.

DataFusion's `insert_to_plan`
(`datafusion-sql-54.1.0/src/statement.rs:2470-2480`) then does this, at **SQL-planning** time:

```rust
Expr::Column(Column::from(source.schema().qualified_field(v)))
    .cast_to(target_field.data_type(), source.schema())?
```

— it injects the `CAST(v AS Int32)` itself and wraps the source in a `Projection` **before**
building the `LogicalPlan::Dml` node. The fork's `IcebergTableProvider::insert_into`
(`iceberg-datafusion` @ `0c5fd58`, `table/mod.rs:223`) therefore receives an input plan whose
schema is **already** the table schema; the `Date32` is gone and `18262` is baked in. Nothing in
`crates/repark-iceberg/src/write/` is on that path at all — `write/map.md` has said so since v1
("`DELETE`/`UPDATE`/`INSERT` need no adapter").

Two consequences a follow-on unit must take as given:

1. **A `TableProvider` decorator cannot fix this.** By the time `insert_into` is called the source
   type no longer exists in the plan. The existing `ProjectingMetadataTableProvider` wrap point
   (`crates/repark-iceberg/src/catalog/metadata_projection.rs`) is therefore the wrong seam, even
   though it is the natural-looking one.
2. **The seam is at logical planning, and it is outside this crate.** Two candidates, in
   preference order:
   * a DataFusion `AnalyzerRule` matching `LogicalPlan::Dml(DmlStatement { op: WriteOp::Insert(..) })`
     and inspecting the `Expr::Cast` nodes DataFusion just synthesized in the input `Projection`
     against `store_assign::ansi_store_assignable`. Registered beside `SparkExprSemantics`
     (`crates/repark-functions/src/lib.rs:139`). **Door-agnostic** — covers the Spark facade, the
     ANSI SQL door and the bare core session at once, and is the same stage Spark itself refuses at.
   * an intercept in the `Statement::Insert` arm of `crates/repark-spark/src/router.rs:257` that
     plans the source separately and zips its schema against the target's, exactly as
     `assert_empty_overwrite_types_assignment_compatible`
     (`crates/repark-spark/src/insert_overwrite.rs`) already does for the empty-overwrite wipe.
     Simpler, but covers **only** the Spark door.

Either edit lands outside `crates/repark-iceberg/src/write/`, which this unit's fence closed. The
predicate they need is now shared and tested; only the call site is missing. Recommend filing as
**WI-2** — and note that landing it also closes G6-5's write-path twin (`INT → DATE`, the reverse
direction), because `ansi_store_assignable` already answers that pair `false`.

---

## 5. Gate roster

| Gate | Result |
|---|---|
| `cargo test -p repark-iceberg` | 342 passed, 0 failed |
| `make verify` (`ci` + workspace `cargo test`) | green |
| `make preflight` (`verify` + `py-test-facade` + `audit` + `workflows-lint`) | green |
| `python/repark/tests/test_insert_store_assign.py` | 15 passed (new) |
| `python/repark/tests/test_merge_store_assign.py` | 10 passed, file untouched |
