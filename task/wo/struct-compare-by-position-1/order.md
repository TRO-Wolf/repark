# STRUCT-COMPARE-BY-POSITION-1: struct comparisons pair fields by position, as Spark does

**Grade:** B (guided). **Builder:** Claude Opus 5.5 (`claude-opus-5-5`).
**Review:** Opus 5.5 and Muse MAX in parallel, on the same head and the same tables (R7, design-heavy). Cap: one build,
one critique, one delta, one delta re-check. The scoped-review rule of 2026-11-09 binds.
**Closes:** ledger C-116 (S1, wrong write, shipped in 1.5.6). Card:
[struct-compare-by-position-1-card-2026-11-09.md](../../roadmap/mid-term/struct-compare-by-position-1-card-2026-11-09.md).
**Release:** v1.5.7.

The design source is Fable's brief, copied here as [evidence/fable-brief-2026-11-09.md](evidence/fable-brief-2026-11-09.md).
Its §4 to §7 apply **as amended below**. Where this order and the brief disagree, this order governs.

## Rulings this order carries (owner, verbatim)

- **"defaults" (2026-11-09):**
  - D1: carve the comparison slice of `StructOperandsByPosition` from #1267 at `8e929978` onto this branch, by file,
    not cherry-pick.
  - D2: this unit runs before the v2 label as a fix, replacing the never-built Step 3.2 refusal.
  - D3: reordered `EXCEPT` / `INTERSECT` stay refused permanently. That is unit C's ground, not this unit's.
  - D4: `CASE` / `IF` / `coalesce` / `array` acceptance is carded, not built.
  - D5: explicit `CAST` pairing is carded, not built.
  - D6: `INSERT ... BY NAME` nested reorder is a step-0 row; it is EQUAL on 1.5.6.
  - D7: S1 with DML reach.
  - Q5': ships in v1.5.7.
- **"default" (2026-11-09):** a struct-literal `DELETE` that Spark 4.1.2 with Iceberg 1.11.0 cannot run is held to the
  **derived oracle**: the rows Spark's `SELECT` with the same predicate selects.
- **"defaults" (2026-11-09, the Unit C park memo):**
  - Q4: use the shared walker. Its fallthrough refuses.
  - The type-variant inventory rule.
- **"Merge and go with default" (2026-11-10), #1290's findings:**
  - F-1: the walker refuses identical Arrow `Union` types. Fix it here, in `struct_walk.rs`: no mismatch when the two
    types are equal, and the message no longer says names differ when they do not.
  - F-4: add a pin that drives unit C's `RecursiveQuery` arm with a non-`List` kind.

## Clause: the three measured tables

1. `cmp-step0-table`: 4393 cells on Spark 4.1.2 and 1.5.6. Seats, doors, shapes, type pairs, the case flag, and DML.
2. `variant-table`, seat C: 218 cells, kind pairs forward.
3. `cmp-variant-ext`: 473 cells. The reverse orientation, encodings, depth, the same-order control, the typed
   DataFrame and native doors, and DML.

The build copies all three into `evidence/`, scrubbed.

Amendments to brief §4.2, from the tables:

- **Widening only in the same order.** Spark refuses a reordered pair whose field types differ, even when they would
  widen (`int` / `bigint`, `int` / `double`, `int` / `decimal`, `int` / `string`), with `BINARY_OP_DIFF_TYPES`.
  - Widening applies only to same-order pairs.
  - The parked `widens` arm is gated on same-order names (`Pairing::Widen if named`). Verify it against the table;
    do not rewrite it blind.
- **Struct-literal DELETE:** expected outcomes come from the derived oracle, not from Spark's error.
- **Kind invariance (comparison seats `=`, `IN` list, join key).**
  - Every variant cell gets the outcome of its plain-`List` twin, for every kind DataFusion's `comparison_coercion`
    unifies: `List`, `LargeList`, `FixedSizeList` of any size, same-kind `ListView`, same-kind `LargeListView`,
    `RunEndEncoded` and `Dictionary`, at any depth.
  - A view against a non-view list is refused by DataFusion's coercion on 1.5.6. That class is out of clause and
    carded (`STRUCT-SETOP-LISTVIEW-COERCION-1`); it must keep its 1.5.6 outcome.
- **Fable's evaluation cells 16, 22, 38 and 39 are superseded by the tables.**

## Design

Brief §2 and §4.4 to §4.5 apply, with these bindings:

1. **Owning layer:** a `repark-spark` analyzer rule, `struct_operands_by_position`, seated **before** `type_coercion`.
   - It is installed by one insert function called from two sites:
     - `SparkExtension::configure_analyzer_rules` (`crates/repark-spark/src/extension.rs`), after
       `insert_map_subscript_before_coercion`;
     - `build_expr_context` (`crates/repark-python/src/column/expr_build.rs`).
   - Each site has its own order pin.
   - `DuplicateFieldOperands` is registered as an ExprPlanner on both contexts.
   - *Amended 2026-11-10 (orchestrator, r4):* a third seat, the repark-python DataFrame binders
     (`bound_column`, `bound_projection`), pairs a duplicate-named struct comparison by position before DataFusion
     types it. It is inventory row 15, and `evidence/after/debug-profile.md` has the measurement.
2. **Type classification goes through `repark_core::struct_walk`:** `peel_encoding`, `list_element`, `holds_struct`.
   - `renamed_type` and `renamed_array` carry the rename through every list kind and through `RunEndEncoded` and
     `Dictionary` values.
   - A struct-bearing pair the rename cannot handle **refuses** (memo Q4); it never passes through to DataFusion's
     by-name coercion. Any refusal where Spark answers is a measured S3 over-refusal: card it.
3. **Cast-transparent operand read:** beneath one top-level `Alias` and one `Cast` / `TryCast`, exactly as brief §4.5.2
   says. A cell inside the clause that needs more than one level is a HALT.
4. **`IN (subquery)`:** brief §4.5.5, S-1 default.
   - When the subquery's output differs from the left operand only in names, rename it through a `Projection`.
   - When the types clash, refuse with `IN_SUBQUERY_DATA_TYPE_MISMATCH`.
   - The fallback, if the table shows a wrong move: refuse and card.
5. **F10:** duplicate-named operands under `IN` lists and `IN (subquery)`, per R4 (brief §4.5.3).
6. **Join keys:** brief §4.5.4, proved by the table's `join_on` and `df_join` seats.
7. **The native door gets no rule.** Pin that it still answers by name, recorded as a divergence.

## Inventories

- **Insertion inventory:** brief §6, every row answered "seen at the seat: yes / no, because" in
  `evidence/inventory.md`, before item 2 of the build.
- **Type-variant inventory (the 2026-11-09 rule):**
  - (1) the representations that reach the seat, per source, from the variant tables;
  - (2) the pairs `comparison_coercion` unifies (`datafusion-expr-common-54.1.0/src/type_coercion/binary.rs`, as the
    Unit C park memo §1.1 lists them);
  - (3) the refusing fallthrough.

  Write it in `evidence/inventory.md` too.

## Pins

Brief §7, plus:

- **Kind-invariance pins** over `variant-table` seat C and `cmp-variant-ext`, on the Spark SQL, typed DataFrame and
  string doors.
- **The derived-oracle DELETE pins.**
- **F-1:** a walker unit test on identical `Union` types.
- **F-4:** a `RecursiveQuery` pin with `LargeListView` (unit C's arm).
- **Both profiles,** with a no-panic pin.

Tests go in the **same commit** as the code they pin.

## Halts

- Brief §7.
- A cell needing a read beneath more than one cast.
- A base-EQUAL cell newly refused, outside a measured over-refusal the builder cards in the hand-back.
- Any change to unit C's or #1290's pins.
- A `struct_pairing_doors.rs` change.
- The cost bar exceeded: ≤ 1.05x median on each of `evidence/cost.md`'s cases (a) and (b). *Amended 2026-11-10
  (orchestrator):* this line named "the C-012 set", carried from Fable's brief. The build brief defined the set as
  cases (a) and (b), and case (b) is the planning-only walk over every expression that Fable's brief asked to be
  measured.

## Not in this unit

Brief §4.6, plus:
- facade ingest normalisation (`FACADE-INGEST-ARROW-NORMALIZE-1`);
- the view-against-non-view coercion (`STRUCT-SETOP-LISTVIEW-COERCION-1`);
- recursive-CTE column pruning (`RCTE-PROJECTION-PRUNE-1`).

## Review outcome (2026-11-10)

- Verdicts on PR #1292 at `647fd64a`: Opus 5.5 FAIL on F-1 only (pre-existing, outside the three tables); Muse MAX PASS.
- The owner's Q1 ruling, verbatim: "on 1 lets go with Fables recommendation".
- F-1 and its family are carded as `STRUCT-COMPARE-SEAT-FAMILY-1` (C-118): S1 with write reach, blocks v1.5.7.
- C-116 is narrowed to the four forms of C-118 and stays open until that unit merges.
