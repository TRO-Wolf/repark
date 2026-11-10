# Card STRUCT-SETOP-LISTVIEW-COERCION-1: a ListView set operation against a plain List with the same names is refused by coercion

**Date:** 2026-11-10. **Filed by:** the records clerk (`claude-haiku-5-5`), from the scoped review of STRUCT-SETOP-TYPE-FAMILY-1 (PR #1290, finding F-2).

**Status:** filed 2026-11-10, not dispatched; S3. It is an over-refusal that fails closed, with no wrong data.

**Why:** a `ListView` of structs (a pandas Arrow-dtype input) set against a plain `List` of structs with the same field names is refused by DataFusion's type coercion ("Incompatible inputs for Union"). Spark and the `List` twin answer. The cells are `P4-00963` and `P4-00969`: a map value, `list_view<struct<a,b>>` against `list<struct<a,b>>`, `union_all`. The same refusal holds on 1.5.6, on `b8ad7a9c` and on #1290's head. DataFusion 54.1.0's list coercion has no view arm, so the walker plays no part.

**Fix direction:** the review places the fix in FACADE-INGEST-ARROW-NORMALIZE-1 (normalise views to `List` at ingest). Not decided here.

**Close:** both cells answer as Spark does, or an owner ruling declines the over-refusal.
