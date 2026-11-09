# Card STRUCT-USER-CAST-POSITIONAL-1: an explicit struct cast pairs fields by name where Spark pairs them by position

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Fable evaluation of 2026-11-09 (cell 24) and the Fable brief of 2026-11-09 (section 3.2).

**Status:** filed 2026-11-09, not dispatched; S2, a wrong value on an explicit cast, Spark door. It is a class outside unit `STRUCT-COMPARE-BY-POSITION-1` and waits on its own ruling.

**Why:** `CAST(named_struct('b',3,'a',4) AS STRUCT<a: INT, b: INT>)` pairs the fields by name, where Spark pairs them by position. Spark answers `{3,4}`, RePark answers `{4,3}`. Pre-existing on 1.5.6 and head, measured there; earlier wheels not measured.

**Close:** a pin of the explicit cast on the Spark door that asserts Spark's positional value, with the ruling on the cast's door recorded in the ledger.
