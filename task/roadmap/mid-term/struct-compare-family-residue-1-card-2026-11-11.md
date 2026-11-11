# Card STRUCT-COMPARE-FAMILY-RESIDUE-1: class and acceptance residue from the STRUCT-COMPARE-SEAT-FAMILY-1 verifier

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 verifier's round 1 (`verify-fam-opus-out/verdict.md`, items C1 to C8).

**Status:** filed 2026-11-11, not dispatched. Severity **S3** or lower, as each item says. Pre-existing: base equals head on every item, so none is a regression.

**Items** (one line each):

- **C1:** a DataFrame alias join with a non-key condition, where both sides have a column of the same name, cannot be filtered by `l.s` or `l.k` (`No field named l.s`). The same holds for `INT` (P09) and for crossJoin.
- **C2:** `ON L.s IS NOT DISTINCT FROM R.s` with an uppercase view qualifier fails to resolve, `INT` too (M01). The lowercase aliases work (M02).
- **C3:** an unqualified `USING` key in `WHERE` is ambiguous (O15, O15i), and `F.col("s.a")` on a `USING`-join frame does not resolve (O10).
- **C4:** a correlated scalar subquery is unsupported (P06).
- **C5:** `coalesce` of differently named structs is accepted with a by-name conversion, where Spark refuses with `DATA_DIFF_TYPES` (N03).
- **C6:** duplicate `selectExpr` items fail with "Projections require unique expression names", where Spark allows them (X05, X06, X25).
- **C7:** struct-versus-scalar operands on `nullif`, `CASE` and `BETWEEN` refuse without a class. `BETWEEN` surfaces DataFusion's "Internal error ... likely caused by a bug" as `PySparkException` (E09 to E11, E20, E22, E23).
- **C8:** the SQL door renders unaliased names in DataFusion's qualified form (`nullif(datafusion.public.f.s, ...)`).

**Close:** each item is fixed, or declined by a recorded ruling.
