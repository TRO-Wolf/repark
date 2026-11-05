# Card NATIVE-DOOR-SILENT-VALUES-1: the native door answers a different value silently where Spark answers, for a 1-based subscript and element_at on a map

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-026).

**Status:** filed, awaiting the owner's decision; severity S2; does not block the format-v2 label, which names the Spark 4.1.2 door and the facade and sets the native door's DataFusion semantics outside the parity claim (owner, 2026-11-03). Ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-102.

**Why:** On a native path view, `l[1]` answers `10` (a 1-based subscript) where Spark answers `20` for the same row, and `element_at(m, 'k')` answers a one-element list `[1]` where Spark answers `1`. These are silent values, not refusals. Wiring the native dot access now would write the 1-based answers, which is why [SUBSCRIPT-DOT-ACCESS-NATIVE-1](subscript-dot-access-native-1-card-2026-11-04.md) is not dispatched.

**Proposed fix:** the owner decides between refusing these forms on the native door, as the facade does, and recording the native door's value semantics as a stated limit with a pin per form.

**Close:** the owner's decision, then the refusal or the recorded limit with its pins.
