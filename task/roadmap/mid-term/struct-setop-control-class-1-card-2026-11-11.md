# Card STRUCT-SETOP-CONTROL-CLASS-1: set operations over struct-arity and map operands refuse without Spark's class

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 round 1 and Muse MAX round 1 controls on PR #1295.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**, class only: both sides refuse. Pre-existing: base equals head.

**Measured:**

- Q0742 to Q0744 (struct-arity set operations): DataFusion's text where Spark says `INCOMPATIBLE_COLUMN_TYPE`.
- Q1238 and Q1239 (map set operations): `SET_OPERATION_ON_MAP_TYPE` where Spark says `INCOMPATIBLE_COLUMN_TYPE`.

**Close:** the cells refuse with Spark's class, with a pin.
