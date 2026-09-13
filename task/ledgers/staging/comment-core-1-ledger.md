# Unit ledger — COMMENT-CORE-1 · remove in-code comments from `dataframe/core.py`

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move).

**Unit:** COMMENT-CORE-1 round 1 · **Date:** 2026-09-13 · **Model:** grok-4.6 ·
**Branch:** `chore/comment-core-1` · **Base:** `origin/main` `4f121ab9`

**Slate:** card COMMENT-CORE-1 — delete the 341 full-line comments and 6 trailing comments
from `python/repark/src/repark/spark/dataframe/core.py`; keep the 74 `# noqa` / `# type:`
pragmas and every docstring; move each reason-carrying comment into
`python/repark/src/repark/spark/dataframe/map.md` under `## core.py rationale (COMMENT-CORE-1)`.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `python/repark/src/repark/spark/dataframe/core.py`,
`python/repark/src/repark/spark/dataframe/map.md`, `scripts/check_lib_py.py`,
`python/repark-parity/tests/test_cap_1_source_file_line_cap.py`, this ledger,
`task/ledgers/staging/map.md`, and a lockstep `map.md` line only where the pre-commit
hook demands it. Closed: `STATUS.md`, `.github/`, `crates/`, every other source file.

## Scope

Behaviour stays byte-identical: the AST of `core.py` on this branch equals `origin/main`.
The only code-file edit is deleting full-line comment lines and trailing comments (plus the
whitespace before a trailing comment). Extra blank lines that `ruff format` rejects are
removed and nothing else is reflowed, renamed, or reordered. Size ceilings ratchet DOWN to
the exact new `wc -l`.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `core.py`'s AST on the branch equals base. | Paste of the `AST-IDENTICAL` run against `origin/main`. | **OPEN** |
| C-002 | No non-pragma comment remains in `core.py`; the 74 pragmas are preserved. | Paste of the `tokenize` counts on the new file. | **OPEN** |
| C-003 | Every removed comment is classified in the table (row count = 347) and every `moved-to-map` row has its sentence in `dataframe/map.md`. | This table (347 rows) plus the `## core.py rationale (COMMENT-CORE-1)` section. | **OPEN** |
| C-004 | Both ceilings equal the exact new line count (`wc -l`) and moved down. | `scripts/check_lib_py.py` and `test_cap_1_source_file_line_cap.py` rows plus `wc -l`. | **OPEN** |
| C-005 | The facade suite count before and after are identical. BEFORE, measured by the orchestrator on this clone at base: `5985 passed, 369 skipped, 48 warnings in 805.63s`. | AFTER run of `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests -q` summary line. | **OPEN** |
| C-006 | Gates green. | Tail of each gate command in the Gates section. | **OPEN** |

`LOGIC_SCORE` = 0/6 (commit 1: table skeleton; proofs land with the strip and the gates).

## Removed-comment table

Generated from `tokenize` over `git show origin/main:python/repark/src/repark/spark/dataframe/core.py`.
Disposition is `deleted-as-narration` (the comment only named the next line, a section, or a
PySpark spelling already visible on the `def`) or `moved-to-map` (hazard, invariant, why,
Spark-parity quirk, fallback reason). The map.md anchor is the backticked function name under
`## core.py rationale (COMMENT-CORE-1)`.

Removed-comment table: **347 rows** (moved-to-map **310**, deleted-as-narration **37**).

| base line | enclosing function | comment text (verbatim, truncate at 100 chars) | disposition | map.md anchor |
|---|---|---|---|---|
| 29 | `(module)` | # SQL identifier helpers. | deleted-as-narration |  |
| 53 | `(module)` | # ``show(vertical=True)`` is supported under the Spark display style. Styled displays remain | moved-to-map | `(module)` |
| 54 | `(module)` | # horizontal and warn once when vertical output is requested. | moved-to-map | `(module)` |
| 57 | `(module)` | # WriterV2.option/options are accepted for signature parity but ignored beyond tableProperty. | moved-to-map | `(module)` |
| 58 | `(module)` | # Warn ONCE per process so migrated scripts learn the options are not applied, without spamming. | moved-to-map | `(module)` |
| 59 | `(module)` | # Reset by `_reset_writer_v2_option_warnings_for_tests` / `_reset_dropin_warnings_for_tests`. | moved-to-map | `(module)` |
| 62 | `(module)` | # Shared with ReparkSession.stop — must match session._STOPPED_MESSAGE wording. | moved-to-map | `(module)` |
| 65 | `(module)` | # SQL keywords the filter-predicate rewriter never treats as a column reference, even when a | moved-to-map | `(module)` |
| 66 | `(module)` | # column casefolds to one of them: Spark's grammar reads the keyword, so ``filter("true")`` is | moved-to-map | `(module)` |
| 67 | `(module)` | # the boolean literal and ``b IS NOT NULL`` is the null test — never a bind to a column named | moved-to-map | `(module)` |
| 68 | `(module)` | # ``true`` / ``null``. Every member has a nameable input: ``createDataFrame([(1, 2)], [kw, "b"])`` | moved-to-map | `(module)` |
| 69 | `(module)` | # builds a frame whose column is literally named ``true`` / ``false`` / ``null``, and each is | moved-to-map | `(module)` |
| 70 | `(module)` | # pinned with its discriminator in test_filter_predicate_rewrite.py (live PySpark 4.1.2 agrees: | moved-to-map | `(module)` |
| 71 | `(module)` | # on a ["false", "b"] frame, filter("false") is zero rows and filter("true") is every row). | moved-to-map | `(module)` |
| 74 | `(module)` | # Semi/anti joins filter the left side and emit no right-side columns. | deleted-as-narration |  |
| 75 | `(module)` | # Engine `how` tokens whose output schema is the LEFT side alone. Semi/anti joins are filters | moved-to-map | `(module)` |
| 76 | `(module)` | # spelled as joins: the right side decides which left rows survive and contributes no columns. | moved-to-map | `(module)` |
| 133 | `_emit_join_side_columns` | # Walk by position so frames that already carry duplicate display names | moved-to-map | `_emit_join_side_columns` |
| 134 | `_emit_join_side_columns` | # (chained joins) do not hit AMBIGUOUS_REFERENCE on name lookup. | moved-to-map | `_emit_join_side_columns` |
| 141 | `_emit_join_side_columns` | # Ordinal = len(engine_names) so chained joins that already carry | moved-to-map | `_emit_join_side_columns` |
| 142 | `_emit_join_side_columns` | # Duplicate display names on one side use distinct engine fields. | moved-to-map | `_emit_join_side_columns` |
| 153 | `_emit_join_side_columns` | # Direct binds from this side's plan_id (last-write if display dups — | moved-to-map | `_emit_join_side_columns` |
| 154 | `_emit_join_side_columns` | # bare joined["b"] stays AMBIGUOUS; parent origins use nested map). | moved-to-map | `_emit_join_side_columns` |
| 156 | `_emit_join_side_columns` | # Propagate nested origin map (chained joins / prior selects). | moved-to-map | `_emit_join_side_columns` |
| 184 | `_by_name_casefold_map` | # Exact duplicate names must not silently overwrite the prior entry. | moved-to-map | `_by_name_casefold_map` |
| 221 | `(module)` | # Object-identity MemTable names created by cache/persist (not checkpoints, not CDF/MIA). | moved-to-map | `(module)` |
| 309 | `_normalize_subset` | # keep enumerate for future position-aware diagnostics | moved-to-map | `_normalize_subset` |
| 311 | `_normalize_subset` | # PySpark's class is per-surface, NOT derivable from accept_str (oracle 4.1.2: | moved-to-map | `_normalize_subset` |
| 312 | `_normalize_subset` | # dropDuplicates + fillna → NOT_LIST_OR_TUPLE, dropna → NOT_LIST_OR_STR_OR_TUPLE). | moved-to-map | `_normalize_subset` |
| 334 | `DataFrame` | # Sticky metadata for adjacent same-spec window merging. | moved-to-map | `DataFrame` |
| 338 | `DataFrame` | # Display and origin metadata for join identity. | moved-to-map | `DataFrame` |
| 340 | `DataFrame` | # Smart CSV diagnostics. | deleted-as-narration |  |
| 341 | `DataFrame` | # Diagnostics from smartCsv (describe_ingest); None for ordinary frames. | moved-to-map | `DataFrame` |
| 354 | `DataFrame` | # right-side plan ids a semi/anti join did not emit | moved-to-map | `DataFrame` |
| 358 | `DataFrame` | # Source view eligible for declared-sort registration. | moved-to-map | `DataFrame` |
| 361 | `DataFrame` | # True after tightenNulls=True on this source or an ancestor. | moved-to-map | `DataFrame` |
| 380 | `__init__` | # Cache is object-identity based and lazy until first action. | moved-to-map | `__init__` |
| 388 | `__init__` | # Deferred facade bridge, or None for ordinary frames. | moved-to-map | `__init__` |
| 390 | `__init__` | # MemTable names for deferred bridge results; dropped during finalization. | moved-to-map | `__init__` |
| 392 | `__init__` | # Action views are replaced on the next action; plan views remain valid for children. | moved-to-map | `__init__` |
| 394 | `__init__` | # One plan-stable bridge snapshot serves all plan children. | moved-to-map | `__init__` |
| 397 | `__init__` | # Schema-bound Columns use this facade plan token to resolve join sides. | moved-to-map | `__init__` |
| 399 | `__init__` | # Join outputs may map duplicate display names to unique engine fields. | moved-to-map | `__init__` |
| 408 | `__init__` | # Set only on source frames. Transformed frames cannot declare the source view sorted. | moved-to-map | `__init__` |
| 410 | `__init__` | # Propagate the tighten-null property when a derived frame combines parents. | moved-to-map | `__init__` |
| 454 | `_identity_child` | # Identity-preserving operations keep display, engine, and origin maps. | moved-to-map | `_identity_child` |
| 468 | `_materialize_cache_if_needed` | # Cache materialization. | deleted-as-narration |  |
| 477 | `_materialize_cache_if_needed` | # Run bridge into ``_inner``; keep ``_map_bridge`` for cache so ``unpersist`` | moved-to-map | `_materialize_cache_if_needed` |
| 478 | `_materialize_cache_if_needed` | # restores re-run. Checkpoint truncates lineage. | moved-to-map | `_materialize_cache_if_needed` |
| 486 | `_materialize_cache_if_needed` | # Cache path only — never route VALUES/createDataFrame through this entry point. | moved-to-map | `_materialize_cache_if_needed` |
| 489 | `_materialize_cache_if_needed` | # Commit handle state only after successful materialize. | moved-to-map | `_materialize_cache_if_needed` |
| 495 | `_materialize_cache_if_needed` | # Checkpoint: lineage truncate; keep VALUES seam (not a session cache registry entry). | moved-to-map | `_materialize_cache_if_needed` |
| 496 | `_materialize_cache_if_needed` | # If converting an already-cached pin, drop the old __repark_cache_* view after the | moved-to-map | `_materialize_cache_if_needed` |
| 497 | `_materialize_cache_if_needed` | # ckpt view is registered so clearCache no longer owns this handle's MemTable. | moved-to-map | `_materialize_cache_if_needed` |
| 503 | `_materialize_cache_if_needed` | # Truncate lineage; do not advertise as cached (oracle: is_cached False). | moved-to-map | `_materialize_cache_if_needed` |
| 519 | `_prepare_for_plan` | # Already pinned — child plans use the MemTable; do not clear bridge. | moved-to-map | `_prepare_for_plan` |
| 541 | `_action_inner` | # Already pinned — do not re-run the UDF. | moved-to-map | `_action_inner` |
| 546 | `_action_inner` | # Fresh bridge execution each action; leave ``_map_bridge`` in place for re-run. | moved-to-map | `_action_inner` |
| 548 | `_action_inner` | # When no plan-stable snapshot is live, ``_inner`` may still point at a prior | moved-to-map | `_action_inner` |
| 549 | `_action_inner` | # action-ephemeral (e.g. post-unpersist lineage restore). ``replace_ephemeral`` | moved-to-map | `_action_inner` |
| 550 | `_action_inner` | # just dropped that view — rebind so direct ``_inner`` readers and a later | moved-to-map | `_action_inner` |
| 551 | `_action_inner` | # ``_prepare_for_plan`` cannot use a dangling MemTable. | moved-to-map | `_action_inner` |
| 552 | `_action_inner` | # Leave ``_inner`` alone when ``_mia_plan_ready``: it is a plan-stable view that | moved-to-map | `_action_inner` |
| 553 | `_action_inner` | # action tracking deliberately preserves. | moved-to-map | `_action_inner` |
| 735 | `_execute_map_in_arrow_bridge` | # Fallback: IPC path when native C-stream register is absent (version-skew). | moved-to-map | `_execute_map_in_arrow_bridge` |
| 763 | `_execute_map_in_arrow_bridge_ipc` | # Empty iterator: schema-only IPC stream (zero batches). | moved-to-map | `_execute_map_in_arrow_bridge_ipc` |
| 800 | `_register_ipc_bytes_as_inner` | # Own the view before sql() so finalize drops it even if SELECT fails. | moved-to-map | `_register_ipc_bytes_as_inner` |
| 879 | `DataFrame` | # Cache and persist. | deleted-as-narration |  |
| 881 | `DataFrame` | # Cache materialization. | deleted-as-narration |  |
| 938 | `unpersist` | # signature parity; single-node drop is always synchronous | moved-to-map | `unpersist` |
| 978 | `localCheckpoint` | # signature parity; single-node MemTable only | moved-to-map | `localCheckpoint` |
| 1024 | `sameSemantics` | # Best-effort: same native PyDataFrame object only (not full semantic equality). | moved-to-map | `sameSemantics` |
| 1058 | `DataFrame` | # PySpark spells this ``createOrReplaceTempView``; expose both so the import swap just works. | deleted-as-narration |  |
| 1061 | `DataFrame` | # Declared-sort registration. | deleted-as-narration |  |
| 1140 | `declare_sorted` | # Caching redirects the scan to another view. Declare the source before caching. | moved-to-map | `declare_sorted` |
| 1153 | `declare_sorted` | # Same bind machinery select/explode use: case-insensitive canonicalization | moved-to-map | `declare_sorted` |
| 1154 | `declare_sorted` | # (raises listing the available columns), then the display-to-engine overlay. | moved-to-map | `declare_sorted` |
| 1159 | `declare_sorted` | # The declaration re-registers the view's MemTable, but this frame's logical plan | moved-to-map | `declare_sorted` |
| 1160 | `declare_sorted` | # still holds the table source captured when the scan was planned — re-resolve it, | moved-to-map | `declare_sorted` |
| 1161 | `declare_sorted` | # or the frame that declared would be the one frame that never sees the elision. | moved-to-map | `declare_sorted` |
| 1181 | `DataFrame` | # repark extension (no PySpark equivalent); camelCase is the disclosed repark spelling. | moved-to-map | `DataFrame` |
| 1184 | `DataFrame` | # ---- transform surface (PySpark DataFrame ops) ------------------------------------------ | deleted-as-narration |  |
| 1201 | `with_column` | # Scalar UDF markers use the withColumns→select bridge. | moved-to-map | `with_column` |
| 1210 | `with_column` | # Aggregates only lower via select/agg — withColumn→native would fail engine-side | moved-to-map | `with_column` |
| 1211 | `with_column` | # (or withColumns→select pure_global would collapse N→1 rows). Spark rejects | moved-to-map | `with_column` |
| 1212 | `with_column` | # Aggregates are rejected in withColumn. | moved-to-map | `with_column` |
| 1214 | `with_column` | # Window, random, and stratified-sampling validation. | deleted-as-narration |  |
| 1216 | `with_column` | # Generators must go through the select unnest rewrite — native with_column would | moved-to-map | `with_column` |
| 1217 | `with_column` | # project the array placeholder without multiplying rows. | moved-to-map | `with_column` |
| 1220 | `with_column` | # Plan-collapse. | deleted-as-narration |  |
| 1221 | `with_column` | # Route the ordinary path through with_columns→select so alias-chain squash and | moved-to-map | `with_column` |
| 1222 | `with_column` | # adjacent same-spec window merge apply to both withColumn and withColumns. | moved-to-map | `with_column` |
| 1223 | `with_column` | # Multi-name identity is handled on the with_columns/select path. | moved-to-map | `with_column` |
| 1226 | `DataFrame` | # PySpark spells this ``withColumn``; expose both. | deleted-as-narration |  |
| 1250 | `with_columns` | # Validate keys + values before any `.alias` so bad maps raise TypeError early | moved-to-map | `with_columns` |
| 1251 | `with_columns` | # Validate keys and values before aliasing. | deleted-as-narration |  |
| 1269 | `with_columns` | # Aggregates only lower via select/agg — withColumns always select(*) and | moved-to-map | `with_columns` |
| 1270 | `with_columns` | # pure_global would collapse N rows → 1 for all-agg/foldable maps (Spark | moved-to-map | `with_columns` |
| 1271 | `with_columns` | # rejects aggregates in withColumns. | moved-to-map | `with_columns` |
| 1273 | `with_columns` | # Adjacent same-spec window merge. | deleted-as-narration |  |
| 1274 | `with_columns` | # Only when the immediately-prior layer (sticky meta on this frame) used the same | moved-to-map | `with_columns` |
| 1275 | `with_columns` | # structural window AND no new column may read a name defined in that prior layer. | moved-to-map | `with_columns` |
| 1276 | `with_columns` | # filter/drop/select never copy sticky meta → intervening ops block merge. | moved-to-map | `with_columns` |
| 1277 | `with_columns` | # When in doubt, fall through to a new stacked layer. | moved-to-map | `with_columns` |
| 1281 | `with_columns` | # Select accepts Column and scalar UDF markers. | deleted-as-narration |  |
| 1283 | `with_columns` | # Multi-name frames iterate engine/display bindings. | moved-to-map | `with_columns` |
| 1292 | `with_columns` | # Preserve origin on replacement when multi-name so select keeps identity. | moved-to-map | `with_columns` |
| 1324 | `with_columns` | # Sticky layer meta for a subsequent adjacent same-spec merge. | moved-to-map | `with_columns` |
| 1331 | `DataFrame` | # PySpark spells this ``withColumns``. | deleted-as-narration |  |
| 1347 | `_try_merge_adjacent_window_layer` | # Do not merge past a cache mark; that would orphan the intermediate MemTable pin. | moved-to-map | `_try_merge_adjacent_window_layer` |
| 1358 | `_try_merge_adjacent_window_layer` | # Replay both maps on the pre-layer frame → one WindowAggr (DataFusion fuses). | moved-to-map | `_try_merge_adjacent_window_layer` |
| 1390 | `filter` | # Generators only lower via select unnest — filter on a generator would | moved-to-map | `filter` |
| 1391 | `filter` | # A generator predicate would target the array placeholder. | moved-to-map | `filter` |
| 1393 | `filter` | # Compounds clear origin but keep join_sql QCOL | moved-to-map | `filter` |
| 1394 | `filter` | # tokens — rewrite to local engine fields and use filter_sql (native Column | moved-to-map | `filter` |
| 1395 | `filter` | # path cannot re-apply ops without stored children). | moved-to-map | `filter` |
| 1401 | `filter` | # Pure origin Columns rebind to engine fields before native filter. | moved-to-map | `filter` |
| 1415 | `DataFrame` | # PySpark aliases ``where`` to ``filter``. | deleted-as-narration |  |
| 1457 | `select` | # Multi-name frames cannot re-resolve bare display strings (duplicate | moved-to-map | `select` |
| 1458 | `select` | # "b" → AMBIGUOUS_REFERENCE). Expand via engine fields + display identity. | moved-to-map | `select` |
| 1499 | `select` | # DataFusion requires unique *engine* projection names; live PySpark allows duplicate | moved-to-map | `select` |
| 1500 | `select` | # *display* names (join both sides / select(x, x.cast(...))). Origin-qualified | moved-to-map | `select` |
| 1501 | `select` | # duplicates keep bare display names via the facade identity map. Non-origin duplicates | moved-to-map | `select` |
| 1502 | `select` | # (cast / year / compound same display) use the same multi-name map with synthetic | moved-to-map | `select` |
| 1503 | `select` | # engine aliases — DataFusion never sees colliding field names. | moved-to-map | `select` |
| 1523 | `select` | # Join identity and multi-name select share display/engine maps. | moved-to-map | `select` |
| 1540 | `select` | # Non-origin columns use a synthetic engine id. | moved-to-map | `select` |
| 1555 | `select` | # Keep composed join_sql (fillna coalesce / cast) so the | moved-to-map | `select` |
| 1556 | `select` | # QCOL SQL select path does not fall back to a bare leaf | moved-to-map | `select` |
| 1557 | `select` | # token without stripping the operation. | moved-to-map | `select` |
| 1567 | `select` | # Identity alias squash. | moved-to-map | `select` |
| 1582 | `select` | # Identity alias squash. | moved-to-map | `select` |
| 1584 | `select` | # All-aggregate or aggregate-plus-foldable select lists use the global aggregate path. | moved-to-map | `select` |
| 1585 | `select` | # Classify projections from Column metadata, not expression text. | moved-to-map | `select` |
| 1586 | `select` | # Mixed aggregate and generator projections raise MISSING_GROUP_BY because they cannot | moved-to-map | `select` |
| 1587 | `select` | # share one grouping stage. | moved-to-map | `select` |
| 1590 | `select` | # A generator and aggregate cannot share one projection grouping stage. | moved-to-map | `select` |
| 1596 | `select` | # Pure global: every projection is aggregate and/or foldable, with no free | moved-to-map | `select` |
| 1597 | `select` | # attributes and no sticky ungroupable. ``all(not free)`` alone was incomplete — | moved-to-map | `select` |
| 1598 | `select` | # ``row_number().over(...)`` is neither free nor foldable nor aggregate and must | moved-to-map | `select` |
| 1599 | `select` | # raise. Nested ``sum+over`` / ``coalesce(sum,window)`` need | moved-to-map | `select` |
| 1600 | `select` | # ``_has_ungroupable``; ``F.rand`` is non-foldable. | moved-to-map | `select` |
| 1608 | `select` | # Pure bare aggregates use the native aggregate | moved-to-map | `select` |
| 1609 | `select` | # path for name/type fidelity with ``df.agg``. Composed post-agg ops | moved-to-map | `select` |
| 1610 | `select` | # (``sum(x)+1``, ``cast``, ``abs(sum)``) and non-agg companions need SQL — | moved-to-map | `select` |
| 1611 | `select` | # DataFusion's ``DataFrame.aggregate`` rejects non-AggregateFunction exprs | moved-to-map | `select` |
| 1612 | `select` | # and bare literals. | moved-to-map | `select` |
| 1619 | `select` | # Multi-name rewrite assigns unique engines before | moved-to-map | `select` |
| 1620 | `select` | # this early return — attach the display/engine overlay so ``sum,sum`` | moved-to-map | `select` |
| 1621 | `select` | # surfaces Spark-legal ``sum(v)`` x2 (not ``__repark_sel_h2_*`` leaks). | moved-to-map | `select` |
| 1627 | `select` | # Mixed aggregate and free companion without GROUP BY — Spark | moved-to-map | `select` |
| 1628 | `select` | # ``[MISSING_GROUP_BY]`` (live PySpark 4.1.2). | moved-to-map | `select` |
| 1634 | `select` | # Duplicate display names cannot pass through the generator SQL rewrite because | moved-to-map | `select` |
| 1635 | `select` | # engine aliases would become ambiguous. Keep this refusal explicit. | moved-to-map | `select` |
| 1643 | `select` | # Compounds that still carry QCOL tokens (cast / arithmetic of parent Columns) | moved-to-map | `select` |
| 1644 | `select` | # cannot use unrebound native exprs on multi-name frames — SQL-project via rewrite. | moved-to-map | `select` |
| 1680 | `_select_global_aggregate_sql` | # One plan-stable snapshot for uncached mapInArrow (and no-op for ordinary frames). | moved-to-map | `_select_global_aggregate_sql` |
| 1683 | `_select_global_aggregate_sql` | # Register the prepared plan — never the empty MIA placeholder (raw ``_inner`` | moved-to-map | `_select_global_aggregate_sql` |
| 1684 | `_select_global_aggregate_sql` | # before prepare) and never a second action re-run via DF createOrReplaceTempView. | moved-to-map | `_select_global_aggregate_sql` |
| 1687 | `_select_global_aggregate_sql` | # Empty group-by only for the shared rebind helper (schema bind). Already | moved-to-map | `_select_global_aggregate_sql` |
| 1688 | `_select_global_aggregate_sql` | # prepared above — do not call ``self.group_by()`` (second ``_prepare_for_plan``). | moved-to-map | `_select_global_aggregate_sql` |
| 1692 | `_select_global_aggregate_sql` | # Case-preserving rebind for bare AF builders (``F.sum("X")`` + lit), | moved-to-map | `_select_global_aggregate_sql` |
| 1693 | `_select_global_aggregate_sql` | # including post-``.alias`` pure AFs that clear ``_agg_name`` but keep | moved-to-map | `_select_global_aggregate_sql` |
| 1694 | `_select_global_aggregate_sql` | # structural ``sql_expr``. | moved-to-map | `_select_global_aggregate_sql` |
| 1734 | `_select_with_generator` | # Private array field — uuid so it cannot collide with user projection names. | moved-to-map | `_select_with_generator` |
| 1740 | `_select_with_generator` | # Array expression only (cast after unnest via _generator_cast). | moved-to-map | `_select_with_generator` |
| 1743 | `_select_with_generator` | # for_select already applied Spark projection names on the native expr. | moved-to-map | `_select_with_generator` |
| 1745 | `_select_with_generator` | # Project from ``_plan()`` (not raw ``_inner``) so uncached ``mapInArrow`` parents | moved-to-map | `_select_with_generator` |
| 1746 | `_select_with_generator` | # materialize the bridge before unnest — raw ``_inner`` is the empty schema | moved-to-map | `_select_with_generator` |
| 1747 | `_select_with_generator` | # placeholder and would silently yield zero rows. Ordinary select/filter also use | moved-to-map | `_select_with_generator` |
| 1748 | `_select_with_generator` | # ``_plan()``. | moved-to-map | `_select_with_generator` |
| 1751 | `_select_with_generator` | # The second SQL projection refers only to quoted identifiers from the intermediate schema. | moved-to-map | `_select_with_generator` |
| 1753 | `_select_with_generator` | # Top-level length only (not multi-dim cardinality product) —. | moved-to-map | `_select_with_generator` |
| 1756 | `_select_with_generator` | # Drop null/empty arrays (Spark explode). Element type is not needed — | moved-to-map | `_select_with_generator` |
| 1757 | `_select_with_generator` | # do not call outer-type resolution (struct arrays are legal;). | moved-to-map | `_select_with_generator` |
| 1761 | `_select_with_generator` | # explode_outer / explode_keep_null: CASE + NULL element. | moved-to-map | `_select_with_generator` |
| 1762 | `_select_with_generator` | # Type is taken from the intermediate field (covers coalesce/compounds — | moved-to-map | `_select_with_generator` |
| 1763 | `_select_with_generator` | # ); never fail-open to BIGINT. Void / Null elements have | moved-to-map | `_select_with_generator` |
| 1764 | `_select_with_generator` | # Keep make_array(NULL) untyped so the engine infers its element type. | moved-to-map | `_select_with_generator` |
| 1771 | `_select_with_generator` | # NULL list → one null-element row; EMPTY list stays empty and drops. | moved-to-map | `_select_with_generator` |
| 1777 | `_select_with_generator` | # explode_outer: null/empty → single-element array of NULL of element type. | moved-to-map | `_select_with_generator` |
| 1785 | `_select_with_generator` | # Element cast after unnest (explode(...).cast(...)) — sticky via _generator_cast. | moved-to-map | `_select_with_generator` |
| 1786 | `_select_with_generator` | # Re-validate each Spark token before SQL embed (defense-in-depth; Column.cast already | moved-to-map | `_select_with_generator` |
| 1787 | `_select_with_generator` | # allowlists — /). A tuple is a cast *chain* (innermost first) | moved-to-map | `_select_with_generator` |
| 1788 | `_select_with_generator` | # from chained ``.cast().cast()`` — apply nested CAST wrappers. | moved-to-map | `_select_with_generator` |
| 1832 | `_array_element_sql_type` | # bind uses array_sql only (no display substring match) | moved-to-map | `_array_element_sql_type` |
| 1852 | `_array_element_sql_type` | # Prefer exact spelling; otherwise require a unique casefold hit. | moved-to-map | `_array_element_sql_type` |
| 1864 | `_array_element_sql_type` | # Field bound but element type unsupported (map / nested-void / …). | moved-to-map | `_array_element_sql_type` |
| 1870 | `DataFrame` | # Smart CSV diagnostics. | deleted-as-narration |  |
| 1921 | `__getattr__` | # Half-built instances (copy/pickle protocols create the object before filling | moved-to-map | `__getattr__` |
| 1922 | `__getattr__` | # __dict__) must not recurse: `_ensure_alive` reads `self._inner`, which re-enters | moved-to-map | `__getattr__` |
| 1923 | `__getattr__` | # Bail to a plain AttributeError for half-built instances. | moved-to-map | `__getattr__` |
| 1927 | `__getattr__` | # A bare AttributeError handles copy, pickle, and hasattr probes before initialization. | moved-to-map | `__getattr__` |
| 1928 | `__getattr__` | # not user misuse of a DataFrame attribute — PySpark's PySparkAttributeError models | moved-to-map | `__getattr__` |
| 1929 | `__getattr__` | # User attribute misses use the classified error below. | moved-to-map | `__getattr__` |
| 1932 | `__getattr__` | # Permanent out-of-scope surfaces use named errors. | moved-to-map | `__getattr__` |
| 1948 | `__getattr__` | # Exact membership only (case-sensitive, like PySpark attr). Quoted bind so | moved-to-map | `__getattr__` |
| 1949 | `__getattr__` | # non-lowercase schema fields remain re-selectable. | moved-to-map | `__getattr__` |
| 1976 | `_resolve_getitem_column_name` | # De-dupe preserving order for case-insensitive multi-hit reporting. | moved-to-map | `_resolve_getitem_column_name` |
| 1982 | `_resolve_getitem_column_name` | # Same display repeated (join dup) already handled above; casefold multi. | moved-to-map | `_resolve_getitem_column_name` |
| 2032 | `_select_via_qcol_sql` | # Token resolution requires a post-join origin map. | moved-to-map | `_select_via_qcol_sql` |
| 2053 | `_select_via_qcol_sql` | # Prefer multi-name engine aliases when the outer select already assigned them. | moved-to-map | `_select_via_qcol_sql` |
| 2064 | `_select_via_qcol_sql` | # Unique display — use as engine name when safe; CAST display needs alias. | moved-to-map | `_select_via_qcol_sql` |
| 2209 | `_rebind_origin_column` | # Only pure leaf refs: join_sql is absent, a bare QCOL token, or a quoted ident. | moved-to-map | `_rebind_origin_column` |
| 2210 | `_rebind_origin_column` | # ``coalesce(...)`` / ``CAST(...)`` / binary ops keep native + origin for select. | moved-to-map | `_rebind_origin_column` |
| 2241 | `_rebind_origin_column` | # Keep join rewrite tokens so further composition (filter compounds built | moved-to-map | `_rebind_origin_column` |
| 2242 | `_rebind_origin_column` | # *before* rebind) is not required; pure rebound is already engine-local. | moved-to-map | `_rebind_origin_column` |
| 2244 | `_rebind_origin_column` | # Preserve sort markers through origin rebind (orderBy(parent.col.desc())). | moved-to-map | `_rebind_origin_column` |
| 2267 | `_bind_schema_column` | # Quote the *engine* schema field for free-SQL embeds. | moved-to-map | `_bind_schema_column` |
| 2268 | `_bind_schema_column` | # Join ON rewrite uses origin_plan_id and origin_field, not this fragment. | moved-to-map | `_bind_schema_column` |
| 2293 | `_quote_filter_sql_identifiers` | # Do not rewrite function names or SQL boolean and null literals. | moved-to-map | `_quote_filter_sql_identifiers` |
| 2296 | `_quote_filter_sql_identifiers` | # Protect single-quoted SQL string literals, then double-quoted idents inside the rest. | moved-to-map | `_quote_filter_sql_identifiers` |
| 2332 | `_rebind_stable_name_column` | # Origin pins a specific side/engine field — skip bare-name rebind. | moved-to-map | `_rebind_stable_name_column` |
| 2346 | `_rebind_stable_name_column` | # Sort markers force a new Column: preserve sticky bits like ``Column.asc`` / | moved-to-map | `_rebind_stable_name_column` |
| 2347 | `_rebind_stable_name_column` | # ``desc`` (sql_expr / generator / is_aggregate_function). Prefer bound's | moved-to-map | `_rebind_stable_name_column` |
| 2348 | `_rebind_stable_name_column` | # schema-quoted ``sql_expr`` so cube/rollup free-SQL SELECT keeps reserved | moved-to-map | `_rebind_stable_name_column` |
| 2349 | `_rebind_stable_name_column` | # Names such as ``order`` are quoted. | moved-to-map | `_rebind_stable_name_column` |
| 2368 | `_rebind_stable_name_column` | # Keep origin and join_sql through sort-marker rebind. | moved-to-map | `_rebind_stable_name_column` |
| 2399 | `__getitem__` | # Star projection token used by count(df["*"]) and select(df["*"]). | moved-to-map | `__getitem__` |
| 2404 | `__getitem__` | # Live PySpark 4.1.2: CI getitem is a NamedExpression with the *requested* | moved-to-map | `__getitem__` |
| 2405 | `__getitem__` | # spelling (same display identity as F.col("X")), not Alias(canonical AS item) | moved-to-map | `__getitem__` |
| 2406 | `__getitem__` | # text pollution. Quoted schema bind also keeps the field | moved-to-map | `__getitem__` |
| 2407 | `__getitem__` | # re-selectable after a non-lowercase projection. | moved-to-map | `__getitem__` |
| 2481 | `schema` | # "Null" is the Arrow Debug spelling, which reaches every flat void column — | moved-to-map | `schema` |
| 2482 | `schema` | # a plain NULL literal included, not just a void explode (engine spells every | moved-to-map | `schema` |
| 2483 | `schema` | # other standard type lowercase) — W-1. | moved-to-map | `schema` |
| 2487 | `schema` | # decimal(p,s) | deleted-as-narration |  |
| 2499 | `schema` | # Overlay Spark-legal display names while engine fields stay unique. | moved-to-map | `schema` |
| 2524 | `printSchema` | # treeString ends with a newline and print adds Spark's second one. | moved-to-map | `printSchema` |
| 2567 | `toDF` | # Multi-name frames cannot re-bind bare display strings; rename | moved-to-map | `toDF` |
| 2568 | `toDF` | # positionally via engine/display bindings. | moved-to-map | `toDF` |
| 2594 | `selectExpr` | # A bare ``*`` keeps multi-name display identity. | moved-to-map | `selectExpr` |
| 2598 | `selectExpr` | # Use a plan-stable bridge snapshot rather than action registration. | moved-to-map | `selectExpr` |
| 2623 | `alias` | # Register one plan-stable bridge snapshot. | moved-to-map | `alias` |
| 2624 | `alias` | # Mirrors selectExpr / select / filter so post-prepare alias agrees with peers. | moved-to-map | `alias` |
| 2626 | `alias` | # -1: the NAME stays one-part (the user chose it), but the read is home-pinned — | moved-to-map | `alias` |
| 2627 | `alias` | # a bare/quoted one-part reference is re-resolved against the live default catalog. | moved-to-map | `alias` |
| 2630 | `alias` | # SQL SELECT * surfaces engine field names; re-attach display identity so | moved-to-map | `alias` |
| 2631 | `alias` | # Multi-name joins keep duplicate display columns positionally. | moved-to-map | `alias` |
| 2743 | `replace` | # Multi-name frames bind by engine/display pairs. | moved-to-map | `replace` |
| 2753 | `replace` | # Preserve origin for multi-name select identity. | moved-to-map | `replace` |
| 2772 | `DataFrame` | # Repartition argument validation; execution is single-node. | moved-to-map | `DataFrame` |
| 2782 | `repartition` | # Spark: first position is int count, or a Column/str partition expr when the | moved-to-map | `repartition` |
| 2783 | `repartition` | # call is ``repartition(*cols)``. List/bool/float/… → NOT_COLUMN_OR_STR always | moved-to-map | `repartition` |
| 2784 | `repartition` | # Reject a sole-argument list instead of treating it as no columns. | moved-to-map | `repartition` |
| 2861 | `repartitionById` | # Type-check simple name refs so non-int partition columns fail loud (Spark analysis). | moved-to-map | `repartitionById` |
| 2867 | `repartitionById` | # Bare attribute only — casts / expressions stay deferred to the engine seed. | moved-to-map | `repartitionById` |
| 2915 | `offset` | # Fetch a very large tail after skip (practical unbounded offset for single-node). | moved-to-map | `offset` |
| 2927 | `drop` | # Join identity. | deleted-as-narration |  |
| 2936 | `drop` | # Live Spark 4.1.2: drop(right["k"]) after leftsemi/leftanti is a no-op. | moved-to-map | `drop` |
| 2945 | `drop` | # Name-based: drop every engine field whose display matches. | moved-to-map | `drop` |
| 2980 | `order_by` | # Sort does not change column identity; keep display and engine maps. | moved-to-map | `order_by` |
| 2985 | `DataFrame` | # PySpark spells this ``orderBy`` and also aliases ``sort`` to it. | deleted-as-narration |  |
| 3001 | `DataFrame` | # PySpark camelCase. | deleted-as-narration |  |
| 3036 | `join` | # Join identity and self-join handling. | moved-to-map | `join` |
| 3038 | `join` | # Normalize Spark aliases to engine tokens. | moved-to-map | `join` |
| 3049 | `join` | # semi family. `.replace("_", "")` already folded `left_semi`/`left_anti` in. | moved-to-map | `join` |
| 3066 | `join` | # A conditionless semi/anti join is NOT a Cartesian product: Spark keeps every left | moved-to-map | `join` |
| 3067 | `join` | # row iff the right side is non-empty (semi) / empty (anti), with no m*n fan-out. | moved-to-map | `join` |
| 3068 | `join` | # Both conditionless shapes (`on=None`, `on=[]`) fall through to crossJoin below, so | moved-to-map | `join` |
| 3069 | `join` | # they are refused loud here rather than silently answering with a cross join's rows. | moved-to-map | `join` |
| 3077 | `join` | # Cartesian product requires crossJoin or conf spark.sql.crossJoin.enabled. | moved-to-map | `join` |
| 3078 | `join` | # Read the same effective value as RuntimeConfig.get (runtime map, then builder). | moved-to-map | `join` |
| 3088 | `join` | # Name equi-join: SubqueryAlias both sides only when names collide or self-join | moved-to-map | `join` |
| 3089 | `join` | # — unconditional alias leaked permanent session views. | moved-to-map | `join` |
| 3103 | `join` | #: empty key list is a cartesian product — same gate as on=None | moved-to-map | `join` |
| 3104 | `join` | # (vacuous all-str would otherwise call join_on_names([]) and skip the conf check). | moved-to-map | `join` |
| 3145 | `_join_on_condition_h1` | # Register both plans as temp views (plan-stable), analyze SQL join, then drop views. | moved-to-map | `_join_on_condition_h1` |
| 3158 | `_join_on_condition_h1` | #: a semi/anti join emits the left side only, so a right-hand name that merely | moved-to-map | `_join_on_condition_h1` |
| 3159 | `_join_on_condition_h1` | # SHARES a left name is not a duplicate in the output — counting it would mangle the | moved-to-map | `_join_on_condition_h1` |
| 3160 | `_join_on_condition_h1` | # left engine field for no reason (and `k` is shared on essentially every semi join). | moved-to-map | `_join_on_condition_h1` |
| 3204 | `_join_on_condition_h1` | # Always attach identity when any display name collides OR origin map needed. | moved-to-map | `_join_on_condition_h1` |
| 3214 | `DataFrame` | # Aggregation. | deleted-as-narration |  |
| 3226 | `group_by` | # Generators lower through select unnest, not as grouping keys. | moved-to-map | `group_by` |
| 3230 | `DataFrame` | # PySpark spells this ``groupBy`` and also accepts the lowercase ``groupby``. | deleted-as-narration |  |
| 3244 | `grouping_sets` | # Full Spark groupingSets API is multi-list; v1: one set per col +. | moved-to-map | `grouping_sets` |
| 3445 | `DataFrame` | # Set operations. | deleted-as-narration |  |
| 3455 | `union` | # Keep left-side display identity when present (union-by-position inherits | moved-to-map | `union` |
| 3456 | `union` | # left engine field names — Spark keeps left display names). | moved-to-map | `union` |
| 3460 | `union` | # Origin map is left-only; right-parent Columns no longer resolve (disclosed). | moved-to-map | `union` |
| 3464 | `DataFrame` | # PySpark keeps ``unionAll`` as a historical alias of ``union``. | moved-to-map | `DataFrame` |
| 3487 | `DataFrame` | # PySpark spells this ``unionByName``. | deleted-as-narration |  |
| 3496 | `_sql_binary_set_op` | # Materialize + register both under try/finally so a right-side MIA failure after | moved-to-map | `_sql_binary_set_op` |
| 3497 | `_sql_binary_set_op` | # left registration cannot leak the left staging MemTable. | moved-to-map | `_sql_binary_set_op` |
| 3498 | `_sql_binary_set_op` | # Register plan-stable bridge snapshots. | deleted-as-narration |  |
| 3504 | `_sql_binary_set_op` | # Re-attach left multi-name display maps after SQL set-op. | moved-to-map | `_sql_binary_set_op` |
| 3554 | `DataFrame` | # PySpark also exposes ``exceptAll``; ``except_`` is the Python keyword escape (not shipped). | moved-to-map | `DataFrame` |
| 3564 | `crossJoin` | # Materialize + register both under try/finally (; same as set-ops). | moved-to-map | `crossJoin` |
| 3565 | `crossJoin` | # Register plan-stable bridge snapshots. | deleted-as-narration |  |
| 3600 | `drop_duplicates` | # Ambiguous display names in a subset expand to every matching engine field. | moved-to-map | `drop_duplicates` |
| 3601 | `drop_duplicates` | # (Spark keeps one row per distinct key multiset of those columns). | moved-to-map | `drop_duplicates` |
| 3614 | `drop_duplicates` | # Empty subset → full-row distinct (avoids DataFusion empty ORDER BY internal error; | moved-to-map | `drop_duplicates` |
| 3615 | `drop_duplicates` | # Same outcome as subset == all columns. | moved-to-map | `drop_duplicates` |
| 3621 | `drop_duplicates` | # Use row_number keep-first rather than groupBy+first. | moved-to-map | `drop_duplicates` |
| 3622 | `drop_duplicates` | # (preserves non-key columns without collapsing via first()). | moved-to-map | `drop_duplicates` |
| 3644 | `DataFrame` | # PySpark spells this ``dropDuplicates``. | deleted-as-narration |  |
| 3670 | `with_column_renamed` | # Multi-name frames bind by engine/display pairs (bare name rebind | moved-to-map | `with_column_renamed` |
| 3671 | `with_column_renamed` | # raises AMBIGUOUS_REFERENCE on duplicate display names. | moved-to-map | `with_column_renamed` |
| 3681 | `DataFrame` | # PySpark spells this ``withColumnRenamed``. | deleted-as-narration |  |
| 3720 | `with_columns_renamed` | # Multi-name frames already carry Spark-legal duplicate displays; allow them | moved-to-map | `with_columns_renamed` |
| 3721 | `with_columns_renamed` | # and rename via engine bindings. Ordinary frames still refuse duplicate names. | moved-to-map | `with_columns_renamed` |
| 3735 | `with_columns_renamed` | # Keep origin so multi-name select identity survives the rename. | moved-to-map | `with_columns_renamed` |
| 3751 | `DataFrame` | # PySpark spells this ``withColumnsRenamed``. | deleted-as-narration |  |
| 3833 | `DataFrame` | # Null handling. | deleted-as-narration |  |
| 3857 | `DataFrame` | # Write surfaces. | deleted-as-narration |  |
| 3916 | `_column_of` | # Stable-name rebind (F.col / requested spelling) then origin rebind so | moved-to-map | `_column_of` |
| 3917 | `_column_of` | # orderBy/groupBy/select parent Columns hit the correct post-join engine field. | moved-to-map | `_column_of` |
| 3934 | `_cross_join_enabled` | # Spark default is true (Cartesian allowed unless conf disables). | moved-to-map | `_cross_join_enabled` |
| 3975 | `_sort_specs` | # Generators lower through select unnest; ordering by the placeholder is invalid. | moved-to-map | `_sort_specs` |
| 3976 | `_sort_specs` | # ``.asc()`` and ``.desc()`` keep the sticky generator marker. | moved-to-map | `_sort_specs` |
| 3982 | `_sort_specs` | # PySpark's `DataFrame._sort_cols`: | moved-to-map | `_sort_specs` |
| 3983 | `_sort_specs` | #     if isinstance(ascending, (bool, int)): | moved-to-map | `_sort_specs` |
| 3984 | `_sort_specs` | #         if not ascending: jcols = [jc.desc() for jc in jcols] | moved-to-map | `_sort_specs` |
| 3985 | `_sort_specs` | #     elif isinstance(ascending, list): | moved-to-map | `_sort_specs` |
| 3986 | `_sort_specs` | #         jcols = [jc if asc else jc.desc() for asc, jc in zip(ascending, jcols)] | moved-to-map | `_sort_specs` |
| 3987 | `_sort_specs` | # A FALSY entry replaces that column's marker with `desc()` — descending, nulls last. A | moved-to-map | `_sort_specs` |
| 3988 | `_sort_specs` | # TRUTHY entry is a NO-OP: the column keeps whatever it arrived carrying, marker and all. | moved-to-map | `_sort_specs` |
| 3989 | `_sort_specs` | # Falsy entries apply descending markers. RePark rejects a short list instead of silently | moved-to-map | `_sort_specs` |
| 3990 | `_sort_specs` | # truncating it. Tuples are accepted as a sequence for compatibility. | moved-to-map | `_sort_specs` |
| 3998 | `_sort_specs` | # `.desc()` — descending, nulls last. | moved-to-map | `_sort_specs` |
| 4020 | `_ascending_remark_flags` | # PySpark raises NOT_BOOL_OR_LIST here, which is a `PySparkTypeError`; a wrong TYPE for | moved-to-map | `_ascending_remark_flags` |
| 4021 | `_ascending_remark_flags` | # the keyword must not arrive as a value error. | moved-to-map | `_ascending_remark_flags` |
| 4059 | `DataFrame` | # Arrow export errors use the facade exception taxonomy and display names stay positional. | moved-to-map | `DataFrame` |
| 4079 | `collect` | # Convert batches directly so collect does not hold a second full Arrow table. | moved-to-map | `collect` |
| 4094 | `take` | # Re-run map bridge but only keep ``num`` output rows. | moved-to-map | `take` |
| 4146 | `tail` | # Live PySpark routes ``tail`` through JVM ``tailToPython`` and accepts a negative as | moved-to-map | `tail` |
| 4147 | `tail` | # empty (unlike ``take``/``head``/``limit``, which raise AnalysisException). Match that. | moved-to-map | `tail` |
| 4150 | `tail` | # Must gate stopped sessions even when num<=0 short-circuits (take(0)/isEmpty fail loud | moved-to-map | `tail` |
| 4151 | `tail` | # via limit/collect; returning [] after stop would be a silent wrong lifecycle outcome). | moved-to-map | `tail` |
| 4168 | `isEmpty` | # Stop after the first output row. | moved-to-map | `isEmpty` |
| 4173 | `DataFrame` | # Snake_case alias — not a PySpark name; convenient for Python call sites. | deleted-as-narration |  |
| 4185 | `toLocalIterator` | # signature parity only | moved-to-map | `toLocalIterator` |
| 4186 | `toLocalIterator` | # Honest streaming: pull RecordBatches via the C-stream, convert one batch at a time. | moved-to-map | `toLocalIterator` |
| 4189 | `DataFrame` | # Snake_case alias — not a PySpark name; convenient for Python call sites. | deleted-as-narration |  |
| 4200 | `_iter_rows_from_record_batch` | # Collect rows directly from each batch. | deleted-as-narration |  |
| 4201 | `_iter_rows_from_record_batch` | # RecordBatch shares column/schema APIs with Table — skip Table.from_batches wrap. | moved-to-map | `_iter_rows_from_record_batch` |
| 4239 | `_require_non_negative_limit` | # Live PySpark 4.1.2 (zulu-17): AnalysisException | moved-to-map | `_require_non_negative_limit` |
| 4240 | `_require_non_negative_limit` | # [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is | moved-to-map | `_require_non_negative_limit` |
| 4241 | `_require_non_negative_limit` | # invalid. The limit expression must be equal to or greater than 0, but got -1. | moved-to-map | `_require_non_negative_limit` |
| 4242 | `_require_non_negative_limit` | # SQLSTATE: 42K0E; + a plan dump. repark drops SQLSTATE and the plan dump (no repark | moved-to-map | `_require_non_negative_limit` |
| 4243 | `_require_non_negative_limit` | # error carries SQLSTATE; plan text is engine-internal). | moved-to-map | `_require_non_negative_limit` |
| 4282 | `to_arrow_batches` | # Capture schema before drain — empty streams yield no batches from the reader, but the | moved-to-map | `to_arrow_batches` |
| 4283 | `to_arrow_batches` | # C-stream still declares a schema (same source :meth:`to_arrow` uses). | moved-to-map | `to_arrow_batches` |
| 4293 | `to_arrow_batches` | # Preserve the declared schema when the stream has no rows. | moved-to-map | `to_arrow_batches` |
| 4297 | `DataFrame` | # CamelCase alias for the repark batch iterator (disclosed extension; not PySpark). | moved-to-map | `DataFrame` |
| 4333 | `DataFrame` | # PySpark spells this ``toPandas``; expose both so the one-line import swap just works. | deleted-as-narration |  |
| 4353 | `(module)` | # Re-export bindings. Keep plan_collapse first because sibling modules import its helpers. | moved-to-map | `(module)` |
