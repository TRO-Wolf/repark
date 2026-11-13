# SIGNED-ZERO-NORMALIZE-1 part B part 1: the second delta (2026-11-13)

Builder `claude-opus-5-5`, effort xhigh, on `fix/signed-zero-normalize-1-part-b` from `3179c2fd0`. Rulings: the order's
"Rulings for the second delta". Findings answered: the delta re-check's N1–N5.

## Commits

| commit | items | product files |
|---|---|---|
| `78308ef0d` | 1 only (N1, N2) | `crates/repark-python/src/logical_names.rs`, `python/repark/src/repark/spark/subset_resolve.py`, `python/repark/src/repark/spark/dataframe/core.py` (line-neutral) |
| `132327453` | 2 (N3), 3 (N4) | `crates/repark-core/src/session/df_guards.rs` |
| the records commit | 4 (N5), 5 | none |

## Item 1: `dropDuplicates(subset)` keeps its input's fields

- **Seat:** the delta round's `_normalized_keys` re-selected every column and named the key with a bare `.alias(name)`:
  a fresh attribute id and no relation, so `a.*` dropped `d` and `a.d` stopped resolving.
- **Fix:** `_native.normalized_float_keys(frame, positions)` builds the projection in Rust from the plan's own schema.
  A FLOAT or DOUBLE field at a key position becomes `__repark_normalize_zero__(column)` aliased to the same relation,
  name and field metadata (the alias carries the field's attribute id, which the stamp keeps); every other field
  passes as its column. The UDF's return field copies its argument's, so nullability and metadata hold. The positions
  come from the `dropDuplicates` input, not from names: the chain renames a self-join's fields (`__repark_sel_N`), so a
  name match missed both keys of `l.join(r).dropDuplicates(["d"])` on the delta round's head.
- **Pins:** `python/repark/tests/dataframe/test_drop_duplicates_schema.py` — `a.*`, `select("a.d")`, `F.col("a.d")`,
  `F.col("a.d").alias("d")`, a join on `a.d`, `x.d` after `x.join(y).dropDuplicates(["d"])`, a self-join's two keys,
  and `writeTo().create()` of the `a.*` frame read back with every column; the permanent table of 24 cases over eight
  shapes (plain, aliased, requested spelling, joined, self-joined, nested struct, metadata-carrying, non-nullable; one
  key and several).
- **Red on the delta round's head (`pkg-head2`, `4442b88f1`, the product of `3179c2fd0`):** every shape pin and 22 of the
  24 schema cases (the self-join cases were green there only because no key was normalised). **Green:** all 60 tests
  of both pin files on the new head's build.
- **The chain finding (not this round's change):** the `with_column`/`row_number`/`filter`/`drop` chain that
  `dropDuplicates(subset)` has used since before part B drops the plan's relation qualifiers, renames a self-join's
  engine fields and drops user field metadata, for every subset, float or not. Measured identically on `pkg-head`
  (`66fb7f13d`, before the delta round) and on the new head; the facade's qualifier state and the attribute ids carry
  every qualified reference, so `a.d` resolves on all three builds except the delta round's. The permanent test
  therefore compares the output with its input in names, order, ids, types, nullability and facade qualifiers, with the
  same chain over a non-float key in plan qualifiers, engine names, field metadata and the exported Arrow schema, and
  checks that every writable qualified reference resolves to the same attribute. Spark keeps field metadata through
  `dropDuplicates`; that loss is a finding for the hand-back, outside part B.
- **Round-1 D2 cells:** the delta round's dedup pins stay green (relaxed by item 3).

## Item 2: refuse where the configuration is used

- **DataFusion 54.1.0, vendored source:** `SessionState::optimize` (`datafusion/src/execution/session_state.rs:733-739`)
  runs `self.analyzer.execute_and_check(plan, &self.options(), …)` and then `self.optimizer.optimize(…)`; the `EXPLAIN`
  branch (`:666-731`) runs the analyzer first as well. `Analyzer::execute_and_check`
  (`datafusion-optimizer/src/analyzer/mod.rs:118-157`) runs every rule unconditionally; no file under
  `datafusion-optimizer/src/analyzer/` reads `max_passes` or `skip_failed_rules`. The optimizer loop reads both
  (`datafusion-optimizer/src/optimizer.rs:388`, `:394-396`). RePark's own optimizer calls run the analyzer first:
  `repark_functions::analyze_eagerly` (Spark door) and `session/temp_views.rs:154-162`.
- **Rule:** `OptimizerSeatConfig` (`repark_optimizer_seat_config`, `crates/repark-core/src/session/df_guards.rs`), added
  in `finish_session_build` before seat 1. On every plan it reads both keys from the live config and, while
  `max_passes` is 0 or `skip_failed_rules` is true, refuses any plan but a statement, an empty relation or a bare
  `VALUES` (so `SET`/`RESET` still restore the session), with the SET-time message. The SET-time refusal stays.
- **Inventory (every RePark-internal session or config builder that sets either key):** no product code sets either
  key; the defaults are `max_passes = 3`, `skip_failed_rules = false` (`datafusion-common/src/config.rs:1208-1211`). The
  two other mentions: `crates/repark-core/tests/cast_plan_profile.rs` builds a standalone `Optimizer` with
  `OptimizerContext::with_max_passes(1)`, not a session config, which the rule accepts; and the C-002 bench
  `python/repark-parity/bench/cast/attribute.py` opens a user session with `max_passes = 0` on purpose. That is a
  measurement harness on the public builder, not an internal session, and the builder already refuses it since the
  delta round (D1). It is reported, not a halt.
- **Pins:** `test_every_spelling_that_stops_the_key_seat_refuses_the_next_query` (the 11 spellings the re-check found
  plus the two plain ones, both doors; a spelling the SET path refuses must name the key, an accepted one leaves three
  queries refused and a reset restores the answers 4 and 18); `key_tests.rs::a_config_that_stops_the_seat_refuses_every_query_whatever_set_it`
  (the spellings, then each value written straight into the native `SessionConfig`, then the controls `max_passes = 1`
  and `batch_size`). The pinned analyzer list (`extension/tests.rs`) gains the rule.
- **Red:** the 11 bypass spellings fail on `pkg-head2`; the Rust pin fails without the rule ("… GROUP BY d) answered").
  **Green:** both, and `a_set_statement_cannot_reach_the_door_policy` and the facade's SET pin.

## Item 3: the dedup pins

`test_drop_duplicates_keeps_the_normalised_key_and_the_row_values` and `test_drop_duplicates_writes_the_normalised_key`
now assert the multiset of key classes and their normalised bits, each surviving row's own key normalising to its
output key, and (read path) the row's own non-key bits; never which tied row survives.

## Item 4: the window form

The registry's COLLECTSET and COLLECTSET-NAN rows each gain one sentence: they cover the aggregate form only, and the
window form keeps both zeros and every NaN row on base and head alike (its SQL twin keeps one element per bit
pattern), as the re-check measured.

## Item 5: cost3 records

`../../cost/` now holds cost3's `cost-raw.jsonl`, `samples.jsonl`, `summary.md` and `l4l.md`. Verdicts per gate: every
§8 gate passes (head `4442b88f1` against `04d84ab9`): n1 1.0214, a 1.0179, f1 0.9844, f2 1.0302, f3 1.1297 (bar
1.20), C-012 like-for-like 1.0101. The `n2` case, the step-0 reads, is not a §8 gate; it reads 1.0583 (base against
base 0.9812).

## Not yet run (budget)

The full facade suites, clippy, panic-ban and the static gates on the new head; `pkg-head3`; the n2 attribution
(`n2/`); and `cost4` (n1, n2, a, f3 over `04d84ab9`, `04d84ab9-aa`, `pre` = `pkg-head` and `head` = `pkg-head3`). The
hand-back carries the one command that runs them in order.
