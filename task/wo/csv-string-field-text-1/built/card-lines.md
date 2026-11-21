# CSV-STRING-FIELD-TEXT-1, the wiring unit — lines for the clerk to file (fix round 1, 2026-11-20)

The wiring unit does not edit the CSV-READ-MODES-AND-SHAPES-1 card. These lines are for the orchestrator's clerk to
add to [that card](../../../roadmap/mid-term/cards-2026-11-19/csv-read-modes-and-shapes-1-card-2026-11-20.md), each
as one dated line.

1. **The corrupt-record column (F2).** "Dated 2026-11-20 (the wiring unit's fix round 1): a CSV read schema holding
   the field named by `columnNameOfCorruptRecord` (the option, else `spark.sql.columnNameOfCorruptRecord`, else
   `_corrupt_record`; matched exactly, as Spark's `StructType.getFieldIndex` does) is refused by name before the
   read, where Spark keeps that field out of positional pairing and fills it with a malformed row's raw line;
   capturing it belongs to this card (the wiring verifier's 10 cells, plus `corrupt/clean_rows`, where no row is
   malformed and Spark answers NULL). Widened in delta 2 to the family: any read schema holding the corrupt-record column over a file with no malformed row (6 cells: `corrupt/clean_rows` 2, `corrupt/default_clean_last` under both `caseSensitive` values 4)."
2. **Long rows (verdict C1).** "Dated 2026-11-20: long rows (more fields than the header) raise
   `incorrect number of fields` on main and on the wiring head, beside short rows, where Spark drops the extra
   tokens."
3. **Two empty or two `nullValue` header names (verdict C2).** "Dated 2026-11-20: two empty or two `nullValue`
   header names (`,,k`, `NA,NA,k`, `x,,X,`) still raise, under the exact-duplicates residual: the header-only
   schema merges equal names before Spark's `_c<i>` rule can apply, where Spark answers `_c0, _c1, …`."

4. **A new card, not a line on CSV-READ-MODES-AND-SHAPES-1:** WITHCOLUMNS-KEY-COLLISION-1. "Dated 2026-11-20 (the wiring
   unit's delta 2): when two `withColumns` keys match one existing column under `caseSensitive=false`, Spark takes
   the first key and keeps its name; RePark takes the last. This is silent, and it is on main too:
   `withColumns({"I": 5, "ı": 6})` over a column `I` answers `I` = 5 on Spark and `ı` = 6 on main. Delta 2's Java
   final sigma lets `withColumns({"ΑΣ Β": 5, "ασ β": 6})` reach the same path (RAISE to WRONG, 2 cells). The fix
   lives in the facade's `withColumns`." Proposed S2 (silent), off the label path; the owner rates it.
5. **A new card:** JAVA-CASE-WORD-CLASS-TABLE-1. "Dated 2026-11-21 (the wiring unit's round 3): `java_case.rs`'s `WORD_CLASS_RUNS` is generated from `Character.getType`, where Zulu 17's word `BreakIterator` uses its own compiled break data. They disagree on U+E0001, U+E007F, U+110BD, U+110CD, U+13438, U+1BCA3, U+1D17A and the unassigned ranges U+18CD6–18CFF, U+2A6DE–2A6FF, U+2B735–2B73F, U+2B81E–2B81F, U+2CEA2–2CEAF and U+2FA1E–2FFFF. With a cased mark (U+0345) between such a code point and Σ, the final sigma differs from Java's, and CSV header dedup under `caseSensitive=false` differs from Spark's (4 cells lost against main, such as `ΑE0001ͅΣ,αE0001ͅς,k`). The fix: derive the table from `BreakIterator`'s behaviour." Proposed S4, off the label path; the owner rates it.

Up: [map.md](map.md).
