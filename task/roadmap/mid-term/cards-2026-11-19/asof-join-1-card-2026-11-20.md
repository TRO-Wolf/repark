**ASOF-JOIN-1 — `DataFrame._joinAsOf` (PySpark's private as-of join) has no door on RePark.**

**Rating: S3, scheduled after the label (the owner, 2026-11-21).** Fable's guidance supports S3 and would schedule it after the label.

Proposed rating
**S3** (a missing capability with a working, slower workaround; not on the label path). Step 0 measured
2026-11-20 on main `9a48932f` (product of `a4d8857f`) against Spark 4.1.2:
[task/wo/asof-join-1/](../../../wo/asof-join-1/map.md).

- **Spark:** `left._joinAsOf(right, leftAsOfColumn, rightAsOfColumn, on, how, *, tolerance, allowExactMatches,
  direction)` matches each left row to the right row with the smallest distance that satisfies the as-of condition
  (`backward`, `forward`, `nearest`), optionally within a tolerance (inclusive only when `allowExactMatches=True`)
  and equal keys (`===`, not null-safe); `how` is `inner` or `left`; the output is `left.*` then `right.*`, both
  key columns kept. Ties (a duplicate right as-of value, or a `nearest` tie) go to whichever row `min_by` meets
  first. `pyspark.pandas.merge_asof` lowers to the same call with `how="left"` and gives the same matches in every
  measured cell. Spark SQL has no `ASOF JOIN`.
- **RePark today:** `df._joinAsOf` raises `ATTRIBUTE_NOT_SUPPORTED`; there is no `pyspark.pandas` door, so
  `merge_asof` is missing too. Two workarounds run today and give Spark's rows in every grid cell up to the tie
  pick: a range join plus `row_number` (quadratic without keys) and a union plus a window fill (sort-bound).
  `min_by`/`max_by` and `monotonically_increasing_id` are not available.
- **Seats:** (a) a Rust plan builder that lowers the call to the union-and-window-fill plan; (b) a dedicated
  sort-merge `AsOfJoinExec` through `StackExtensionPlanner`. One unit under the one-round cap either way;
  [sketch.md](../../../wo/asof-join-1/sketch.md) §5 to §6.
- **Side findings, carded separately if the owner wants them:** a correlated scalar subquery with a non-equality
  predicate fails in DataFusion's `scalar_subquery_to_join` with a schema error (Spark answers); `select` (including
  string lists), `groupBy` and `orderBy` refuse a list with
  `PySparkTypeError` (PySpark accepts a list); the subquery refuses in every non-equality shape measured; proposed
  severities S3 for each, and S2 for the list finding if the owner chooses; `l ASOF JOIN r ON …` is a plain inner join
  with alias `ASOF` on
  Spark and a parse error on RePark.

Fable's guidance: [guidance.md](../../../wo/asof-join-1/guidance.md).

- Dated 2026-11-21 (the owner's ruling, decision 6: S3 after the label, the five items): the rating is S3, scheduled after the label, not left as backlog. The three kept divergences are accepted: at the integer bounds with ANSI off, RePark returns the nearest row; a STRING as-of column is refused in both modes; for equal infinities, RePark picks the equal row. In each case, Spark's own row is not the nearest by its own rule. The weaker tie promise: a `nearest` tie goes to the backward row, and the row picked among duplicate right values is unspecified, beyond being one of them. COLUMNS-LIST-ARG-1: S2, one card over every method that takes a list of columns. Unit 1 is held while both lanes carry S1 work.
