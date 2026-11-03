# Card ADD-FILES-IDLESS-DECIMAL-BOUNDS-1: `add_files` refuses an id-less decimal file whose value is out of the table's precision when column-bounds metrics are on (RePark card)

**Date:** 2026-11-03. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-65 records round, from the RP-65 builder's hand-back (lead L1).

**Status:** filed; grade C, P3; open; not dispatched. Spark's behaviour is unmeasured: the lane that found the shape ran no JVM. The fork ledger's S-27 record is the only evidence that Spark admits it.

**Why:** An id-less Parquet file (RePark's own frame writer, no field ids) holding a `DECIMAL(12,2)` column with the unscaled value 123456789012 is refused by `add_files` into a table whose column is `DECIMAL(10,2)`, when the table keeps column bounds (the default metrics mode). RePark answers `DataInvalid => Decimal value 123456789012 is too large for precision 10`. The message names no column and no table type, and it comes from the fork's bound conversion in `add_files_datafile.rs`, not from RePark. The fork ledger's S-27 record says Spark's `add_files` admitted this shape.

After the file is admitted with the metrics mode set to `counts`, the read is loud: `DataInvalid => cannot read a data file column of type Decimal128(12, 2) as Decimal128(10, 2)`. That refusal is correct and is what the RP-65 pin asserts; the open question is only the admission.

**Reachability:** RePark's frame writer, then `CALL <catalog>.system.add_files(...)` into the narrower table. Measured at the RP-65 pin `66901f02`, both on the Spark door.

**Site:** the fork's `add_files` bound conversion (`add_files_datafile.rs`). The fork is not edited from RePark; the fix, if any, is a fork unit.

**Measure first:** on a live Spark 4.1.2 oracle, admit the same file under `DECIMAL(10,2)` with the table's metrics at the default and at `counts`; record the admission text or the admitted state, the read, and the `d > 0` and `IS NOT NULL` answers; then confirm or retract the S-27 record.

**Fix (to decide after measurement):** if Spark admits the shape, `add_files` admits it too, and the read answers as Spark does. If Spark refuses, the refusal names the column and both precisions instead of the bound text.

**Retires:** the card leaves when `add_files` answers as the measured Spark answer, and any refusal names the column and both types; the fork S-27 record is then confirmed or corrected by citation.

**Relation:** [RP-65-IDLESS-REPIN-1](rp-65-idless-repin-1-card-2026-11-03.md) (the pin that sets the metrics mode to `counts`, and the lead's source); [ADD-FILES-IDLESS-PROMOTION-1](add-files-idless-promotion-1-card-2026-11-01.md) (the same admission route, a different statistics message); [F-IDLESS-TOPLEVEL-POSITIONAL-1](f-idless-toplevel-positional-1-card-2026-11-01.md) (the fork card the RP-65 repin carries).

Up: [map.md](map.md).
