# map — repark-spark/src/void_type

## Purpose

WO LTZ-STORE-INT-1 support. The parent `void_type.rs` holds the Spark-door `VOID`
handling (`CAST(NULL AS VOID)` rewrite, the non-NULL VALUES refusal into
`unknown` columns); this directory holds the VALUES store-assignment gate for
`TIMESTAMP` (LTZ) targets it calls first.

## Contents

- `ltz_values_store.rs` — `refuse_unassignable_ltz_values` judges each VALUES
  cell mapped to a microsecond `Timestamp` column through the shared
  `incompatible_update_message` gate — bare literals by their Spark kind, anything
  else by a `SELECT <cell>` probe — so `VALUES (0, 1)` refuses with Spark's
  recorded `CANNOT_SAFELY_CAST` text while NULL, DATE, TIMESTAMP, TIMESTAMP_NTZ
  and explicit `CAST`s still store. Skips TIMESTAMP_NTZ targets, partitioned
  inserts, and anything it cannot map. Reached from
  `refuse_insert_void_values`. **Fold 2026-09-28 (critic V-001):** null-valued
  cells pass; numerics refuse through a DDL-name fallback even when the shared
  gate cannot name them; `1L` reads `BIGINT`; DECIMAL casts and typed literals
  read by declared type. **Fold 2026-09-28 (verifier VL-1..VL-6):** the V-001 null
  rule narrows to bare NULL — `CAST(NULL AS T)` is judged by `T`; fractionals
  read `DECIMAL(p,s)` and out-of-`INT`-range integers read `BIGINT`.
  **Fold 2026-09-28 (re-verify RL-1..RL-3):** the VL-1 function deferral narrows
  to two-argument `nvl`/`ifnull`, probed as `coalesce(a, b)`; every other
  function is judged by its probed type; leading-zero fractions count precision
  from significant digits (six classifier tests).
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1):** the classifier peels any number of
  unary Plus/Minus signs and parentheses, and the probe parenthesizes a leading
  stacked-minus chain so its rendering cannot form a `--` comment; `- -1BD`
  (an `Identifier` to this parser) refuses through the repaired probe exactly
  as `-1BD` does (nine classifier tests). NTZ stacked cells still write: that
  probe lives in `void_type.rs`, outside this fold.
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1 verifier fold, VG-1/VG-2/VG-8):** the
  local parenthesizer is deleted in favor of the shared recursive one in
  `void_type.rs`, used at all three probe sites; `probe_source_type` propagates
  the analyzer error instead of passing the row when the probe cannot parse;
  `number_text_type` is `pub(crate)` for `update_cast.rs` (ten classifier
  tests, one asserting no `--` survives rendering).
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1 re-verify fold, RN2-2/RN2-3):** a
  `DEFAULT` cell is skipped like a bare NULL (`is_null_or_default_cell` in the
  parent), and both `nvl`/`ifnull` operands render through the shared
  parenthesizer. `ntz_probe_that_cannot_parse_refuses_instead_of_passing` is the
  NTZ door's twin of the fail-closed pin: it drives the parent's `check_ntz_row`
  with an unparsable cell and expects the `ParserError` back (eleven tests).
  **Fold 2026-09-29 (LTZ-STACKED-SIGN-1 second re-verify fold, RN3-1):** the
  probe renders through the parent's shared `probe_text`, which dollar-quotes
  a string literal holding a quote with the same value, so a backslash before
  a quote no longer refuses the row on this door either (eighteen tests).
  **Fold 2026-09-29 (re-verify 3, RN4-1):** the dollar-quote tag also grows
  past a closer the value runs into (`'it''s$p'`), so a quoted value ending in
  `$` plus the tag no longer closes the probe early and refuses (nineteen
  tests).
  pins: ltz-store-int-1/C-001
  **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):** the gate also judges numeric, DATE
  and BOOLEAN targets (`is_judged_target`, one classifier test), through
  `incompatible_store_message` so a DECIMAL target has Spark's name: TIMESTAMP
  into BIGINT, STRING into INT, BOOLEAN or DATE, BOOLEAN into INT and INT into
  BOOLEAN refuse with Spark's text. The DDL-name numeric fallback stays for LTZ
  targets only, so a numeric into a numeric column still stores.
  pins: store-ts-to-numeric-1/C-001
  **Fold 2026-09-29 (verifier VT-1):** a STRING-planned cell stores again when
  Spark widens its branches to a storable type — `spark_branch_type` reads the
  CASE/nvl/nullif/coalesce/if/nvl2/greatest/least branches (literals, DATE typed
  strings and plain CASTs without a probe, `__repark_float_to_string__` is not
  present at this altitude) and `widened_stores` skips the refusal only when the
  widened type stores (a widened numeric into an LTZ target still refuses, and
  the refusal keeps naming the planned STRING so still-refusing cells keep their
  text). DATE typed strings and plain CASTs now read by declared type, so DATE
  and `CAST(i AS DOUBLE)` VALUES rows skip the per-cell probe (verifier VT-4).
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the VT-1 widening
  escape is gone. For a numeric, DATE or BOOLEAN target the gate stays silent
  when the planned source is STRING (base behaviour: STRING into those columns
  stores again) or when `source_leaves::source_type_is_reliable` rejects the
  cell; LTZ targets keep LTZ-STORE-INT-1's judgment unchanged, so a STRING cell
  into TIMESTAMP refuses as on base. TIMESTAMP, TIMESTAMP_NTZ and DATE cells into
  numeric and BOOLEAN columns, BOOLEAN into numeric and numeric into BOOLEAN still
  refuse. pins: store-ts-to-numeric-1/C-001, C-006, C-007
  **WO STORE-TS-DOORS-2 (2026-09-29):** a SELECT over a VALUES node is judged
  too — `select_values_arms` maps each projected output position to the VALUES
  cells that flow into it (derived tables, CTEs, UNION arms, joins; table and
  view columns, literals, CASTs, functions and ambiguous refs stay unmapped)
  and each arm's projected rows run through the unchanged `check_row`, so the
  STRING silence and the leaf rule apply exactly as on the VALUES door. The
  table load moved verbatim into `load_presented`.
  pins: store-ts-doors-2/C-001
  **Fold 2026-09-29 (verifier VT-1):** a mapped STRING cell is skipped when a
  sibling set-operation arm at the same position has static type TIMESTAMP,
  TIMESTAMP_NTZ or DATE — `SiblingJudge` types mapped sibling columns by
  `leaf_type`, table provenance through the shared table loader and
  SELECT-no-FROM expressions by `leaf_type` then the existing probe, and a
  skipped cell is projected as the silent NULL cell. Any other sibling, any
  non-STRING cell and any unresolvable shape keep the pre-fold judgment.
  pins: store-ts-doors-2/C-001, C-003
  **Fold 2026-09-30 (re-verify VT2-1, VT2-2, VT2-4, VT2-5):** the judge moves
  verbatim to `sibling_types.rs` and this file keeps the refusal path
  (`refuse_value` still types by `literal_source_type` then the probe, and
  still fail-closes when the probe cannot parse). The skip call site now
  passes only the position and the cell.
  pins: store-ts-doors-2/C-001, C-003
- `insert_source_types.rs` — **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):**
  `refuse_insert_source_types` is the Spark door's INSERT gate for two source
  types the analyzer gate cannot see: a negated NULL (Spark's DOUBLE, judged by
  `repark_iceberg`'s `refuse_negated_null_writes`) and a STRING into FLOAT or
  DOUBLE (the `spark_float_stringify` rule rewrites that conform cast before
  `InsertStoreAssignment` runs). It loads the target schema, skips a table with
  no DATE, BOOLEAN, timestamp, BINARY or floating-point column, plans the source
  once, maps it positionally, by column list or by name, and refuses with
  Spark's text naming the quoted table. `void_type.rs` re-exports it behind a
  `Box::pin` wrapper. Callers: `router/insert_positional.rs`
  `prepare_positional_insert` (append, owned append, OVERWRITE, VALUES) and
  `insert_by_name.rs`. Unmapped columns and unplannable sources pass.
  pins: store-ts-to-numeric-1/C-002, C-003
  **Fold 2026-09-29 (verifier VT-1):** the STRING → FLOAT/DOUBLE refusal skips a
  source Spark widens to a storable type — `spark_source_type` walks the planned
  source like the negated-NULL lineage (views resolve through the session
  resolver, DataFrame temp views arrive inlined) and `widened_stores` judges the
  widened branch type; `__repark_float_to_string__` is transparent (it only
  wraps FLOAT/DOUBLE, so the walk reads its argument). Called now also from
  `router/insert_positional/replace_where.rs` (verifier VT-2).
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the STRING → FLOAT/DOUBLE
  refusal, its plan walk (`spark_source_type`) and the `__repark_float_to_string__`
  transparency are removed; the gate now only runs `refuse_negated_null_writes`,
  and only for a table with a DATE, BOOLEAN, timestamp or BINARY column and a
  source with a `Null`-typed column. STRING sources store on every INSERT door as
  on base. pins: store-ts-to-numeric-1/C-002, C-006
  **WO STORE-TS-DOORS-2 (2026-09-29):** `refuse_partition_overwrite_sources`
  maps the filled partition-overwrite source onto the table columns minus the
  static partition columns (or by column list, bailing when a static column is
  listed) and runs the shared `refuse_negated_null_writes`, so `-NULL` into
  TIMESTAMP, DATE and BOOLEAN refuses on the static-partition door exactly as on
  the other INSERT doors. Called from `execute_partition_overwrite`.
  pins: store-ts-doors-2/C-002
- `select_values_arms.rs` — **WO STORE-TS-DOORS-2 (2026-09-29):**
  `resolve_insert_arms` resolves an INSERT source into VALUES-cell arms: one arm
  per UNION side, each output position mapped to the VALUES cells that flow into
  it through transparent projections over derived tables, CTEs and joins.
  Unmapped positions (table and view columns, literals, CASTs, functions,
  ambiguous refs, unexpandable stars, mismatched aliases, recursive CTEs) judge
  nothing, so the gate only ever refuses a VALUES cell Spark refuses.
  `arm_row_groups` groups the arm's positions by VALUES node and `null_cell`
  builds the silent placeholder. Eight classifier tests.
  pins: store-ts-doors-2/C-001
  **Fold 2026-09-29 (verifier VT-1):** each arm position also carries sibling
  provenance — table and column for a table-arm column ref, the expression for
  a SELECT-no-FROM projection — resolved through derived tables, CTEs and
  set-operation merges next to the cell map, so the store gate can type the
  sibling arm Spark widens against. Two provenance tests.
  pins: store-ts-doors-2/C-001, C-003
  **Fold 2026-09-30 (re-verify VT2-1, VT2-2, VT2-4, VT2-5):** each arm also
  carries its rendered SELECT (`arm_sql`; VALUES leaves carry none). A plain
  or qualified star over an unresolvable factor now yields an arm with no
  positions that still carries the SELECT, instead of dropping the arm, so
  the judge can plan that arm alone as a fallback; every other drop stays a
  drop. The mixed-star and recursive classifier pins now expect one empty
  arm; their positions stay unmapped.
  pins: store-ts-doors-2/C-001, C-003
  **Fold 2026-09-30 (re-verify VT3-1):** the carried SELECT is scoped —
  `sibling_types::scoped_arm_sql` prefixes every in-scope CTE definition
  (innermost wins on shadowing) and declines to scope case twins, so an arm
  is never planned outside its statement's CTE scope. Call sites only; the
  resolver and its classifier pins are unchanged.
  pins: store-ts-doors-2/C-001, C-003
- `sibling_types.rs` — **Fold 2026-09-30 (re-verify VT2-1, VT2-2, VT2-4,
  VT2-5):** `SiblingJudge`, moved here from `ltz_values_store.rs`, decides
  the VT-1 skip from one per-position map built once per statement and
  reused for every cell. A cell and every sibling cell are typed the way
  the refusal types them — `literal_source_type`, then `leaf_type` for the
  TIMESTAMP/NTZ shapes the probe would only confirm, then the existing
  probe — so the skip and the refusal never type a cell differently
  (VT2-1: function-valued STRING cells and VALUES siblings of
  `current_timestamp()`/`make_timestamp()`). A position no cell or
  provenance entry resolves falls back to planning that arm's SELECT
  alone through the session and reading the column type (R2: stars over
  tables, expressions over table columns, constant expressions in a
  SELECT with FROM, temp-view siblings); a plan failure keeps the old
  judgment, while an ambiguity failure passes the STRING cell through
  so the analyzer raises Spark's AMBIGUOUS_REFERENCE (VT2-2). Probes
  are cached by probe text, table schemas by table, arm plans by arm,
  so a probe-heavy deep UNION plans each arm at most once (VT2-4).
  Six tests: unmapped-star shapes, static typing, the ambiguity
  matcher, probe sharing, skip beside TIMESTAMP but not BIGINT, and
  the plan-failure fall-through.
  pins: store-ts-doors-2/C-001, C-003
  **Fold 2026-09-30 (re-verify VT3-1..VT3-3):** the arm plan keeps four
  outcomes. `Typed` judges by column type as before. `Ambiguous` matches
  only the typed `SchemaError::AmbiguousReference` and the
  `[AMBIGUOUS_REFERENCE]` tag, never the bare word, and a position-less
  ambiguous arm judges instead of unjudging every position (VT3-2).
  `Unresolved` (unknown column, function or table) leaves the cell unjudged
  so the analyzer raises Spark's own class (VT3-3). `Failed` (no arm SQL, or
  the constructed SQL cannot parse) keeps the old judgment, as does a
  resolution failure retried once lowercased in a case-insensitive session
  when the retry resolves. Eight tests cover the fold: the inverted matcher,
  the unjudged failure, the kept judgment without SQL, the prefix, the
  shadowing and recursion prefix, the case-twin decline, and the retry pair.
  pins: store-ts-doors-2/C-001, C-003
  **Fold 2026-09-30 (re-verify VT4-1, VT4-4):** the arm plan runs through
  `plan_statement_with_column_repair` with the executing dialect, the same
  planner the statement itself uses, so the gate judges the arm the statement
  actually plans; a raw plan skews on any name the repair would fix, and the
  skew unjudges the arm and stores (VT4-1). The lowercase retry is gone:
  repair subsumes it. A consulted arm that plans `Ambiguous` now unjudges
  every position, superseding the fold-3 per-position rule, so the analyzer's
  AMBIGUOUS_REFERENCE surfaces instead of a sibling-gate CANNOT_SAFELY_CAST
  (VT4-4); an ambiguous arm means the statement fails, so no store is
  possible. Four tests: lowercase refs over uppercase columns, exact
  case-sensitive refs, missing case-sensitive refs, and an ambiguous arm
  unjudging a VALUES position elsewhere.
  pins: store-ts-doors-2/C-001, C-003
- `source_leaves.rs` — **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):**
  `source_type_is_reliable` decides whether a new refusal may trust RePark's
  planned source type. A plain cell (literal, typed string, CAST, a non-widening
  function, a scalar subquery projecting one column) is trusted. A branching cell
  — CASE, `coalesce`/`greatest`/`least`/`nvl`/`ifnull`/`nullif`/`if`/`nvl2`, any
  other scalar subquery, EXISTS, IN (subquery) — is trusted only when every leaf
  it can return has a statically known type that is not STRING and that itself
  refuses into the target (TIMESTAMP, TIMESTAMP_NTZ including
  `__repark_timestamp_ntz__(…)`, and DATE leaves into numeric and BOOLEAN
  columns; bare NULL leaves are neutral); anything else stays silent and the
  store behaves as on base. `is_string_type` names the STRING family. Five
  classifier tests. Replaces the VT-1 widening lattice `spark_widen.rs`, deleted
  with its call sites. pins: store-ts-to-numeric-1/C-007

## Pointers

- Up: [../map.md](../map.md)
