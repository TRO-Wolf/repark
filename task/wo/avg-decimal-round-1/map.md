# task/wo/avg-decimal-round-1/ — AVG-DECIMAL-ROUND-1 step 0 (with DECIMAL-DIVIDE-LITERAL-1)

Ruling B7 (owner, 2026-11-16): S1, label-blocking; owner "Defaults" and "Merge them. Dispatch the avg unit"
(2026-11-17): this step 0 runs next and measures DECIMAL-DIVIDE-LITERAL-1 beside it. Measured on main `a792ce47`
against Spark 4.1.2; documents and probes only, no product code. Card:
[../../roadmap/mid-term/decimal-common-type-1-side/avg-decimal-round-1-card-2026-11-16.md](../../roadmap/mid-term/decimal-common-type-1-side/avg-decimal-round-1-card-2026-11-16.md).

- [sketch.md](sketch.md) — the five faults, Spark's `avg` and division rules read from the 4.1.2 jars
  (`Average`, `DecimalDivideWithOverflowCheck`, `Decimal.$div`, `Divide.resultDecimalType`, `fromLiteral`), the
  counts, seat A (the averager; the literal step and `PyColumn::div`; the `try_*` coercion, recommended) and seat
  B, the cost cases, the two-unit split and ten owner questions (restated after the critic).
- [domain-table.md](domain-table.md) — 616 `avg` and 456 division cells per ANSI mode on the Spark door, the
  DataFrame door, `describe` and `summary`, every Iceberg wrong write, the native door, the two confs and the other
  causes measured beside.
- [inventory.md](inventory.md) — every seat that sets a decimal `avg`'s type, scale or rounding, or a decimal
  division's operand or result type, "seen: yes / no, because", with file:line.
- After the inventory critic (`GAPS`, no cell contradicted; revision 2026-11-17): `mean` is DataFusion's `Avg`
  (seat A1 takes the alias), the native door is outside A1, `mod`/`pmod` need a call arm in A2, `try_*` covers all
  five functions, Spark's sum is not sticky; the cells are in the domain table's revision section and
  [evidence/raw/rev/](evidence/raw/rev/map.md); the owner questions restated (ten).
- [evidence/](evidence/map.md) — the probes, the raw cells, the verdicts and grids, the `javap` excerpts and the
  provenance.

Up: [../map.md](../map.md).
