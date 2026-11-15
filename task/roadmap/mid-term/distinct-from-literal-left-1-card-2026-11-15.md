# Card DISTINCT-FROM-LITERAL-LEFT-1: on the Spark door, `<literal> IS DISTINCT FROM <column>` refuses at planning, where Spark answers

**Date:** 2026-11-15. **Filed by:** the DECIMAL-DOUBLE-UNION-COERCION-1 part 1 delta (PR #1339), from its probe set; the
orchestrator reproduced it on main `dc6b4c94`'s package.

**Status:** PROPOSED, not dispatched. Pre-existing: main refuses it too.

**Severity: S3, a refusal.** RePark refuses the statement where Spark answers, and no answer is wrong. It blocks no label.

## Finding

On the Spark door, `<literal> IS DISTINCT FROM <column>`, with the literal on the left, refuses at planning with
``[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `spark_catalog`.`default`.`v` cannot be resolved.``
It happens on a table and on a temp view. `<column> IS DISTINCT FROM <literal>` and `<literal> <=> <column>` both answer
correctly. Spark 4.1.2 answers all three forms.

## Measured

- The orchestrator reproduced it on main `dc6b4c94`'s package, on a temp view `t(v double)`:
  - `SELECT 0.5 IS DISTINCT FROM v FROM t` refuses as above;
  - `SELECT v IS DISTINCT FROM 0.5 FROM t` gives `[True, True]` for the rows `1.5` and NULL;
  - `SELECT 0.5 <=> v FROM t` gives `[False, False]`.
- The #1339 delta builder measured 48 such cells per ANSI mode in its probe set
  (`task/wo/decimal-double-union-coercion-1/evidence/fix/probes/preimage_cells.py`), on `DECIMAL(10,2)` and
  `DECIMAL(38,18)` columns. They fail the same way on main, on the part-1 head and on the delta. The cells are left out
  of the unit's pins.

## Not yet known

The seat. Step 0 must find which resolver path loses the column's qualifier when the literal is on the left.

Up: [map.md](map.md).
