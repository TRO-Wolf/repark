# CSV-STRING-FIELD-TEXT-1, the wiring unit — lines for the clerk to file (fix round 1, 2026-11-20)

The wiring unit does not edit the CSV-READ-MODES-AND-SHAPES-1 card. These lines are for the orchestrator's clerk to
add to [that card](../../../roadmap/mid-term/cards-2026-11-19/csv-read-modes-and-shapes-1-card-2026-11-20.md), each
as one dated line.

1. **The corrupt-record column (F2).** "Dated 2026-11-20 (the wiring unit's fix round 1): a CSV read schema holding
   the field named by `columnNameOfCorruptRecord` (the option, else `spark.sql.columnNameOfCorruptRecord`, else
   `_corrupt_record`; matched exactly, as Spark's `StructType.getFieldIndex` does) is refused by name before the
   read, where Spark keeps that field out of positional pairing and fills it with a malformed row's raw line;
   capturing it belongs to this card (the wiring verifier's 10 cells, plus `corrupt/clean_rows`, where no row is
   malformed and Spark answers NULL)."
2. **Long rows (verdict C1).** "Dated 2026-11-20: long rows (more fields than the header) raise
   `incorrect number of fields` on main and on the wiring head, beside short rows, where Spark drops the extra
   tokens."
3. **Two empty or two `nullValue` header names (verdict C2).** "Dated 2026-11-20: two empty or two `nullValue`
   header names (`,,k`, `NA,NA,k`, `x,,X,`) still raise, under the exact-duplicates residual: the header-only
   schema merges equal names before Spark's `_c<i>` rule can apply, where Spark answers `_c0, _c1, …`."

Up: [map.md](map.md).
