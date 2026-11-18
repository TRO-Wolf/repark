# task/wo/sum-dec-i128wrap-1/ — SUM-DEC-I128WRAP-1 step 0 (measured sketch)

The decimal `sum` that answers a wrapped or 39-digit `i128` value where Spark raises (registry row
`SUM-DEC-I128WRAP-1`, backlog since 2026-09-10; found again in Fable's reply of 2026-11-17, §7). Dispatched under the
owner's ruling of 2026-11-17 (evening), as the orchestrator reads it. Measured on main `6f80793b` against Spark
4.1.2; documents and probes only, no product code. Severity proposed, not ruled.

- [sketch.md](sketch.md) — the faults, Spark's `Sum` read from the 4.1.2 jar (`Sum`, `TrySumExpressionBuilder$`,
  `CheckOverflowInSum`, `DecimalAddNoOverflowCheck`, `UnsafeRow.setDecimal`) and where its check lands, the counts,
  seat A (a RePark `sum`/`try_sum` on the avg unit's exact sum, recommended) and seat B (an analyzer rewrite), the
  cost cases, the two-unit split and six owner questions.
- [domain-table.md](domain-table.md) — 1,656 decimal `sum`, 399 `try_sum`, 150 control and 330 `avg` cells per ANSI
  mode on the Spark door (SQL, DataFrame, Iceberg), the named wrong writes, the native-door sample, the controls.
- [inventory.md](inventory.md) — every seat that types, accumulates, checks or evaluates a decimal `sum`, "seen at
  the seat: yes / no, because", with file:line and how it was searched.
- [evidence/](evidence/map.md) — the probes, the raw cells and verdicts, the `javap` excerpts, the provenance.

**Revised 2026-11-17** after the critic's verdict (GAPS): the Iceberg writer checks precision only from bounds, so
39-digit values commit under `none` / `counts` and past column 100 (proposed card ICE-DECIMAL-WRITE-PRECISION-1),
the missing seats (`SlidingFrameReScan`, the DataFrame grouping-set, pivot and `call_function` routes), the grouped
marker (Q2), the missed shapes, the view cells, the cost list and the observations; sketch §9 records where it
agrees and disagrees with the critic.

Up: [../map.md](../map.md).
