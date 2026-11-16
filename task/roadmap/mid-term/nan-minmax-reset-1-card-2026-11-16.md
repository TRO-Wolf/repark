# Card NAN-MINMAX-RESET-1: `RESET` or `spark.conf.unset` of the guarded aggregate dynamic-filter key does not return to RePark's default, so `min` answers 3.0 where Spark answers 1.0

**Date:** 2026-11-16. **Filed by:** the orchestrator's records clerk, on the owner's ruling of 2026-11-16. **Source:** the
[NAN-MINMAX-1 card](nan-minmax-1-card-2026-11-13.md), its paragraph "**Residual, measured, not fixed:**", and the
[umbrella measurement](../../wo/nan-minmax-1/evidence/delta2/umbrella-d7ae795a.txt) beside its
[probe](../../wo/nan-minmax-1/evidence/delta2/probe_umbrella.py).

**Status:** PROPOSED, queued after DECIMAL-DOUBLE-UNION-COERCION-1 part 2, not label-blocking. The owner, 2026-11-16,
about 11:10 UTC, verbatim: "Great work. Go with your recommendation on that". The recommendation it adopts: the
NAN-MINMAX-1 RESET/unset gap becomes its own small follow-up unit, NAN-MINMAX-RESET-1, and it does not block the label.

**Severity: S1 by class.** It is a wrong answer: `min` reads 3.0 where Spark 4.1.2 reads 1.0. The readback is wrong too:
after `RESET`, `spark.conf.get` answers `'false'` while the engine runs DataFusion's default `true`. It is reachable only
by resetting the internal DataFusion key `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown` by its full
name, with `RESET` or `spark.conf.unset`. The umbrella key `datafusion.optimizer.enable_dynamic_filter_pushdown` is
refused at `true` on both doors, so it is not a route. By reading, the bare Spark-door `RESET` and `RESET ALL` reach the
same unset path (not run). The owner ruled that this does not block the label.

## Finding

On both doors, `RESET` of `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown` sets the engine to DataFusion's
default `true`, not RePark's session default `false` that the guard applies at session build. The aggregate dynamic filter
is back on, and with it the upstream typed-NULL defect that the NAN-MINMAX-1 residual analysis names (N1, and the
upstream section below). The filter then drops rows that hold the true minimum. On the Spark door, `spark.conf.get` still
answers `'false'`, because RePark's store is not updated.

On the Spark door, `spark.conf.unset` removes the key from RePark's store and does not reach the engine (seats below).
The engine keeps its last value. The gap therefore shows after `unset` only when the key was set to `true` first.

## Measured

Each line of [umbrella-d7ae795a.txt](../../wo/nan-minmax-1/evidence/delta2/umbrella-d7ae795a.txt) gives the door and step,
the counter of `min`/`max` answers over 10 runs, whether `DynamicFilter` appears in the EXPLAIN ANALYZE plan (`True` means
the filter is on), and the readback of the key. The lines quoted:

Spark door:

- default (line 2): `spark default {"{'max(datafusion.public.t.d)': 6.0, 'min(datafusion.public.t.d)': 1.0}": 10} False`.
  `min` 1.0 in 10 of 10 runs.
- after `spark.conf.set` of the key to `true` (line 11): `spark then A=true {"{'max(datafusion.public.t.d)': 6.0, 'min(datafusion.public.t.d)': 3.0}": 10} True`.
  `min` 3.0 in 10 of 10 runs.
- after `spark.conf.unset` of the key (line 13): `spark then unset A {"{'max(datafusion.public.t.d)': 6.0, 'min(datafusion.public.t.d)': 3.0}": 10} True {'datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown': None, ...}`.
  `min` 3.0 in 10 of 10 runs; the readback is `None`.
- after SQL `RESET` of the key (line 16): `spark then RESET A {"{'max(datafusion.public.t.d)': 6.0, 'min(datafusion.public.t.d)': 3.0}": 10} True {'datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown': 'false', ...}`.
  `min` 3.0 in 10 of 10 runs; the readback still says `'false'`.

Native door:

- default (line 3): `native default {"{'max(t.d)': 6.0, 'min(t.d)': 1.0}": 10} False`. `min` 1.0.
- after `RESET` of the umbrella key with the specific key at `false` (line 23): `native then RESET U {"{'max(t.d)': 6.0, 'min(t.d)': 1.0}": 10} False`.
  The specific key stays `false`, so `min` stays 1.0.
- after `RESET` of the specific key (line 25): `native then RESET A {"{'max(t.d)': 6.0, 'min(t.d)': 3.0}": 10} True`.
  `min` 3.0 in 10 of 10 runs.

The right answer is `min` 1.0. Spark 4.1.2 answers `[(6.0, 1.0)]` on the three-file set
([spark-4.1.2.txt](../../wo/nan-minmax-1/evidence/delta2/spark-4.1.2.txt), line 2).

What the measurement does not show:

- The measured head `d7ae795a` is a branch commit and is not an ancestor of main `9062a3ca`. Its
  `crates/repark-core/src/session/df_guards.rs` blob (`f31349db`) equals the blob at #1333 (`d843d37e`), and the Python
  and Rust seats the probe exercised are byte-identical between `d7ae795a` and main. The measurement describes main's seats.
- Since #1333, `df_guards.rs` changed in two commits: `954423e0` (#1335, three lines in `finish_session_build`) and
  `d1679e4f` (#1336, one line in `unnest_safe_optimizer_rules`). Neither touches `apply_df_54_1_config_guards`, which reads
  the same on #1333 and on main (`crates/repark-core/src/session/df_guards.rs:155-159`).
- The `unset` step ran after the `true` step (lines 10 to 12), so the engine already held `true`. The probe does not show
  what `unset` returns the engine to from RePark's default. By reading, `unset` leaves the engine at `false` there, so `min`
  should stay 1.0 (not run).
- Not run: `RESET` of the key from the default with no earlier set (by reading, the engine takes `true`); bare `RESET` and
  `RESET ALL` on the Spark door; the DataFusion 55.2.0 path.

## Where a fix would sit

Seats on main `9062a3ca`, from reading only. No code is proposed here.

- **Spark door, SQL `RESET <key>`.** `python/repark/src/repark/spark/session/sql_set_statements.py:155-156` returns `None`
  for a `datafusion.*` key, so the statement goes to the engine. `crates/repark-spark/src/spark_ast.rs:244-246` refuses only
  a collation key. The statement is then planned, and DataFusion's `reset_variable`
  (`datafusion-54.1.0/src/execution/context/mod.rs:1151-1158`) calls `ConfigOptions::reset`, which restores the default.
  RePark's store is not touched, which is why the readback stays `'false'`.
- **Spark door, `spark.conf.unset`.** `python/repark/src/repark/spark/session/builder_conf.py:409` is `RuntimeConfig.unset`.
  A `datafusion.*` key falls through to `builder_conf.py:478-485`: the store entry is popped and a tombstone is added. The
  engine is called only for the Iceberg write keys and the default catalog (`:478-479`).
- **Spark door, bare `RESET` and `RESET ALL`** (by reading, not run). `sql_set_statements.py:157-158` and `:161-162` reach
  `_reset_all` (`:276-281`), which calls `_restore_or_unset` (`:284-305`) for each stored key. That ends in
  `RuntimeConfig.unset` (`:305`) or `conf.set` (`:303`). The engine is not reset for a `datafusion.*` key.
- **Native door.** RePark's native path has no `RESET` handling. `PyReparkSession.native`
  (`crates/repark-python/src/session.rs:151-167`) installs `session_dialect`
  (`crates/repark-core/src/local_fs_policy.rs:103-109`), which wraps `DataFusionDialect`
  (`crates/repark-core/src/dialect.rs:145-161`). The only `RESET` arm in `repark-sql`
  (`crates/repark-sql/src/router.rs:105-108`) belongs to `AnsiDialect`, which no door installs outside tests;
  `crates/repark-sql/src/v3/create.rs:361,404` call only its `on_session_built`. The column-resolution pass treats `RESET`
  as a no-op (`crates/repark-core/src/column_resolution/dereference.rs:19`). So the native `RESET` goes straight to
  DataFusion's `reset_variable`.
- **The default.** `apply_df_54_1_config_guards` (`crates/repark-core/src/session/df_guards.rs:155-159`) runs once, at session
  build (`crates/repark-core/src/session.rs:280`). No `RESET` or `unset` reaches it, so a change to that function alone does
  not reach either statement.
- **NAN-MINMAX-1's note.** The source card says: "Neither reaches the guard's seat; a third product file would. An owner
  word is owed on whether a later unit closes it." The phrase "inside its cap" does not appear in the source card.

## Upstream

The DataFusion defect the source card names: `datafusion-physical-plan` 54.1.0, `src/aggregates/no_grouping.rs:254-262`,
where `scalar_cmp_null_short_circuit` matches only the untyped `ScalarValue::Null`. The same function, with the same match
arms, is at `src/aggregates/aggregate_stream.rs:254-262` in 55.2.0 (read in the local registry copy; not run). The shared
dynamic-filter bound that calls `scalar_min` is `no_grouping.rs:187` in 54.1.0. An upstream issue draft exists outside the
repository. No issue is filed, and filing waits on the owner.

Up: [map.md](map.md).
