# Card STRUCT-TYPE-UNION-ACCEPTS-REORDER-1: CASE, IF, coalesce and array accept a reordered struct pair that Spark refuses

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Fable evaluation of 2026-11-09 (cells 17 to 20) and the Fable brief of 2026-11-09 (section 3.2).

**Status:** filed 2026-11-09, not dispatched; S3, accepts what Spark refuses. No wrong value was measured. It is a class outside unit `STRUCT-COMPARE-BY-POSITION-1` and waits on its own ruling.

**Why:** `CASE`, `IF`, `coalesce` and `array` over a struct pair whose fields are declared in a different order answer on RePark, where Spark raises `DATATYPE_MISMATCH.DATA_DIFF_TYPES`. Pre-existing on 1.5.6 and head, measured there; earlier wheels not measured.

**Close:** a pin per expression form asserting Spark's refusal on every door, or a recorded ruling that the acceptance stays with the reason.
