# Card DECIMAL-LITERAL-OVERFLOW-1: an overflowing DECIMAL literal refuses at planning, where Spark answers NULL with ANSI off

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 4 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md) and its [bar table](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md), where ruling 6 carded the cells instead of fixing them; the build's hand-back, kept outside the repository; Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q3.

**Status:** filed 2026-11-17, not dispatched.

**Severity: S2, not label-blocking** (ruled 2026-11-17). One card, not folded into the string-cast build.

## Finding

`SELECT CAST('1.5' AS DECIMAL(38,38))` refuses even alone, on main and on the head: the literal's own cast fails at planning. With ANSI off, Spark answers NULL and RePark raises. With ANSI on, both engines raise, except in the `coalesce` shape: Spark never takes that literal branch and answers, but RePark's planning fold raises. So that shape over-raises with ANSI on.

The build gives its reason for carding rather than fixing: the class "fits no existing card cleanly. ANSI-OFF-NARROWING-OVERFLOW-1 covers numeric narrowing casts, while this is a string-to-decimal literal folded at planning" (build hand-back).

Fable, on the same class (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q3, a recommendation): "With ANSI on, the literal's cast raises on both engines, which is right. The string-cast build has nothing to fix there." The ANSI-off half "is outside the label's scope and sits beside ANSI-OFF-NARROWING-OVERFLOW-1."

## Cells

Eight cells: `u_overflow_lit`, `where_overflow_lit`, `u_overflow39` and `coalesce_overflow_lit`, each on `t` and `tx` (step 0's two table variants). Modes from [bar-table.md](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md) and [bar.md](../../../wo/decimal-common-type-1/evidence/built/bar.md):

| cell | ANSI off | ANSI on |
|---|---|---|
| `literal/u_overflow_lit` (`t`, `tx`) | over-raise | both raise |
| `literal/where_overflow_lit` (`t`, `tx`) | over-raise | both raise |
| `literal/u_overflow39` (`t`, `tx`) | over-raise | both raise |
| `literal/coalesce_overflow_lit` (`t`, `tx`) | over-raise | over-raise |

## Related cards

- [ANSI-OFF-NARROWING-OVERFLOW-1](../ansi-off-narrowing-overflow-1-card-2026-11-16.md) (S2) covers numeric narrowing casts, not a string literal. This card's ANSI-off half sits beside it.
- [AND-GUARD-ANSI-1](../sweep-2026-11-16/and-guard-ansi-1-card-2026-11-16.md) and [CASE-BOOL-GUARD-ANSI-1](../sweep-2026-11-16/case-bool-guard-ansi-1-card-2026-11-16.md) (S2, not label-blocking) list the `coalesce_overflow_lit` shape, a planning fold that raises in a branch Spark never evaluates.
- [STRING-CAST-SILENT-1](../string-cast-silent-1-card-2026-11-16.md) is the string-cast build. This card is not folded into it.

Up: [map.md](map.md).
