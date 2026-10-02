# map — repark-spark/src/void_type/sibling_types

## Purpose

The sibling judge for the VALUES store-assignment gate. `sibling_types.rs`
became this directory in re-verify fold 6: the judge plus its classifier pins
no longer fit one file under the ceiling, so the product code is `mod.rs` and
every pin is file-backed in `tests.rs`.

## Contents

- `mod.rs` — `SiblingJudge` and the arm-plan classifier (`Typed`, `Ambiguous`,
  `Unresolved`, `Failed`). **Fold 2026-10-01 (re-verify VT6-1, VT6-2):** the
  arm plan runs the re-parsed arm through the statement's pre-gate rewrite
  chain (`spark_ast::apply_pregate_judge_rewrites`) before the column-repair
  planner, so a bare `localtimestamp` demotes to the STRING column the
  statement plans instead of re-parsing as the TIMESTAMP function and
  skipping the position; `Unresolved` narrows to the typed
  `SchemaError::FieldNotFound`, the RePark tags
  (`[UNRESOLVED_COLUMN]` / `[UNRESOLVED_ROUTINE]` /
  `[TABLE_OR_VIEW_NOT_FOUND]`) anchored at the payload start, and
  DataFusion's anchored `Invalid function` and `table … not found` plans.
  The `Table not found` and `failed to resolve schema/catalog` prefixes are
  dropped: no producer emits them on this path (the capital-T shape exists
  only in DataFusion's own test provider), so keeping them could only
  unjudge. DataFusion 54 carries `Plan` and `Diagnostic` errors as strings,
  so the classifier matches RePark's tags and that fixed prefix list on
  text; RePark emits no error prefix of its own, hence the start anchor.
  pins: store-ts-doors-2/C-001, C-003
- `tests.rs` — the moved judge and classifier pins plus the fold-6 pins:
  one real-planner pin per DataFusion prefix (each plans failing SQL through
  `plan_statement_with_column_repair` and asserts the classification, so a
  rewording turns the pin red), the real-planner ambiguity-fallback pin, the
  `table function … not found` Failed / `table … not found` Unresolved
  boundary, mid-string tag negatives pinning the anchors, and the demoted
  nullary arm typing as its STRING column.
  pins: store-ts-doors-2/C-001, C-003

## Pointers

- Up: [../map.md](../map.md)
