# SUM-DEC-I128WRAP-1 — the card's "Built" append (2026-11-17)

The card `task/roadmap/mid-term/consult-2026-11-17/sum-dec-i128wrap-1-card-2026-11-17.md` is not on this branch yet
(records PR #1358); this is the append to paste under it when main is merged in.

## Built (2026-11-17, Claude Opus 5.5 `claude-opus-5-5`, xhigh) — **HALT** on the bar: 46 layout cells

**Seat A** (sketch §4), branch `fix/sum-dec-i128wrap-1`, product commit `a8016e75`. Five product files (cap 6; the two
`mod` lines in `aggregate.rs` and the test `mod` in `repark-spark/src/extension.rs` do not count):

1. `crates/repark-functions/src/aggregate/spark_sum.rs` (new) — A1/A2: `SparkSum`, `sum` (`Ansi`) and `try_sum`
   (`Try`) over DataFusion's `Sum` / datafusion-spark's `SparkTrySum`; the decimal arm, delegation of every other type,
   `placed` / `legacy` for A3.
2. `crates/repark-functions/src/aggregate/sum_decimal.rs` (new) — the accumulators (`DecimalSum`, `DecimalSumGroups`,
   `DecimalSumDistinct`), Spark's check and conditions, the marker, the magnitude bound.
3. `crates/repark-functions/src/aggregate.rs` — registration in `functions()`, `sum_udaf()`, `try_sum_udaf()`, and A3:
   `SparkAvgAnsiMode` places each `sum` with ANSI on and swaps it to `Legacy` with ANSI off.
4. `crates/repark-functions/src/avg_decimal.rs` — `safe_rows`, `add_batch`, `sub_batch` made `pub(crate)` (shared
   arithmetic only; no behaviour change).
5. `crates/repark-python/src/column/function_dispatch.rs` — the `:938` arm returns `sum_udaf()`.

**The rulings carried (owner, 2026-11-17, verbatim):** "dispatch Opus xhigh effort levels for these fixes, then use
Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out"; "defaults on both"
(Q1 S1 label-blocking; Q2 follow Spark's per-row grouped check, raise at once with ANSI on, a marker with ANSI off and
in `try_sum` chosen by K1 at 1.05x or less, `avg` keeps ruling (a); Q3 the native door carded NATIVE-DECIMAL-SUM-1,
S2; Q4 SUM-BIGINT-ANSI-OVERFLOW-1 a second unit; Q5 ICE-DECIMAL-WRITE-PRECISION-1 its own card, S1, orphans S3; Q6
`ARITHMETIC_OVERFLOW` for an overflowed partial or a single-phase aggregate, `NUMERIC_VALUE_OUT_OF_RANGE` when the
partials fit and the merged total does not); "Defaults, merge 1355" (views read under other settings:
TEMP-VIEW-SESSION-SETTINGS-1, not fixed here).

**The marker:** the **sentinel** `i128::MIN` in the one declared state column (NULL stays "empty"), with ANSI off and
in `try_sum`; with ANSI on the grouped accumulator raises at the first overflowing row and the partial at `state()`,
so its state is the same one column with no marker. K1 (200,000 groups, head / main, five runs): `(10,2)` 0.909 on and 0.954 off, `(38,0)` 0.983 on and 0.950 off ([cost.md](cost.md)); the second state column was not needed. Survival pins
(`aggregate::sum_tests`): an Arrow IPC round trip (spill and the distributed data path), a spilling grouped aggregate
(`spill_count > 0`), `RepartitionExec` with and without skipped partial aggregation (`convert_to_state`); no
`validate_decimal_precision` sits between the partial and final steps. The distributed plan codec carries the UDAF by
name (Ballista's codec), so a decoded executor gets the registered `sum` in `Ansi` mode: the mode is lost there, as
`avg`'s already is (behind `cluster`, which no door uses).

**The bar** ([bar.md](bar.md)): 1,329 cells move; decimal `sum` 430 / 449 silent cells to Spark's answer, the shapes
47 / 51 of 52, `try_sum` 16 / 17, every Iceberg statement EQUAL or BOTH_RAISE, no wrapped or 39-digit value in any data
file, Spark reads every table; `avg`'s 330 cells per mode, the controls and the native door byte-for-byte unchanged.
**Halt:** 46 cells (23 per mode) go from EQUAL to another verdict, all pass-back sums whose answer depends on the
split or order: (a) 20 where Spark's default pass read several files as one split (their multi-partition twins moved
to Spark's answer), (b) 14 grouped and distinct merges whose arrival order is not deterministic, (c) 12 DataFrame
`repartition(4)` cells (RoundRobinBatch keeps the batch in one partition). Named classes left: the native door, FNP-AGG-1,
`BIGINT` (Q4), `avg` (ruling (a)), views (TEMP-VIEW-SESSION-SETTINGS-1), the layout and order classes, the observations,
message-text classes. The writer card has no `sum` cell left.

**Cost** ([cost.md](cost.md)): every held row at or below 1.05x except K3 `sum(d38)` at 1.067 after ten runs, inside its control band (0.978 to 1.089); K2's 1000-row frame 0.929x / 1.016x of the 3-row frame; C-012 1.001 and 0.995; K5 (reported) 0.110.
