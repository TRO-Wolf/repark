# Card MAP-STRUCT-KEY-SUBSCRIPT-1: a map subscript with a struct key panics in DataFusion's get_field, where Spark answers the value

**Date:** 2026-11-01. **Filed by:** the third-audit records round (`claude-haiku-5-5`), from the targeted re-audit of the format-v2 label, finding AUD-016 ([the report](../../../docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), section 5).

**Status:** dispatched 2026-11-01 on an Opus 5.5 lane (branch `fix/map-struct-key-subscript-1`), not merged. Grade A, P1, High. A blocker of the format-v2 label (ledger clause C-080).

**Why:** `m[named_struct(...)]` on a map whose key type is a struct panics. DataFusion 54.1.0's `get_field` slices a `BooleanBuffer` past its length, and the facade reports the panic as an internal error on an ordinary SELECT. Spark answers the value for each row. The same map under `element_at` is correct, and so is a string-keyed subscript. The panic is in DataFusion, not Iceberg-specific: a literal map shows it too. The production definition lists panics in library code as a disqualifier, and the order treated the second audit's AUD-008 the same way. The fault predates the third audit: it is identical at `321c6095`.

**Site:** the subscript path from RePark's SQL layer into DataFusion's `get_field` (`datafusion-functions` 54.1.0, `core/getfield.rs`, reached from the expression simplifier). The panic is upstream of the fork. The charter chooses the route: a RePark-side rewrite of a struct-keyed subscript, or a pinned upstream fix.

**Fix:** a struct-keyed subscript answers Spark's value on every row, with no panic, through the Spark door and the native door. The pins use the audit's two-row, two-field-key shape, a one-field key with one entry (which already answers), a literal map, and the `element_at` and string-keyed controls.

**Measured (the third audit; copied, and reproduced by the orchestrator):** a table with a `MAP<STRUCT<k: INT, j: INT>, INT>` column, holding `map(named_struct('k',1,'j',2), 7, named_struct('k',3,'j',2), 8)` in one row and `map(named_struct('k',1,'j',5), 9)` in another.

```text
SELECT id, m[named_struct('k', 1, 'j', 2)] FROM c;                   RePark: internal error (BooleanBuffer slice)   Spark: (1, 7), (2, NULL)
SELECT id, m[named_struct('k', 3, 'j', 2)] FROM c WHERE id = 1;      RePark: Join Error                              Spark: (1, 8)
SELECT m[named_struct('k', 1, 'j', 2)] FROM (SELECT map(named_struct('k', 1, 'j', 2), 7) AS m);   RePark: the same panic   Spark: 7
SELECT id, element_at(m, named_struct('k', 1, 'j', 2)) FROM c;      both: (1, 7), (2, NULL)   (control)
```

The backtrace runs through `BooleanArray::slice` in `arrow-buffer` to `datafusion-functions` `getfield.rs:134`, then the expression simplifier.

**Retires:** the card when the struct-keyed subscript answers Spark's values on the probe shapes through both doors with no panic, each shape pinned. The label's AUD-016 row (ledger clause C-080) then closes by citation.

**Relation:** [F-STRUCT-EMPTY-CHILD-PANIC-1](f-struct-empty-child-panic-1-card-2026-10-29.md), the second audit's emptied-struct panic, fixed in the fork on another route; [F-MAP-KEY-DROP-ALTER-1](f-map-key-drop-alter-1-card-2026-10-31.md), the map-key schema route.
