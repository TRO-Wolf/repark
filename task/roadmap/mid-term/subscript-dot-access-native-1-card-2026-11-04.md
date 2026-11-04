# Card SUBSCRIPT-DOT-ACCESS-NATIVE-1: the native (ANSI) door does not answer a field access after a subscript, a call or a parenthesis

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L1 (clause C-007) of [SUBSCRIPT-DOT-ACCESS-1](../../ledgers/completed/subscript-dot-access-1-ledger.md): the builder's question 1, held by the orchestrator's ruling Q1 (b).

**Status:** filed, not dispatched. Depends on D5: the native array subscript is one-based, and the wiring is unsafe until that is fixed.

**Why:** SUBSCRIPT-DOT-ACCESS-1 answers these forms on the Spark door only. The native door (`PreExecute::plan`, then `create_logical_plan`) is not wired, so `SELECT map('a', named_struct('x', 1))['a'].x` on the native door still fails with DataFusion's dot text. Wiring the respelling there, measured on the nested-cells table: 52 native D2 rows become EQUAL, but 105 native rows that refuse today become wrong answers, because `map_keys(mk)[0].x` and `ls[0].x` read the 1-based element (D5). The native duplicate-field row would answer where Spark refuses (S2).

**Fix shape:** fix the native one-based array subscript (D5) first, which moves the D5 control rows; then wire the respelling into the native planning path and pin the 52 rows against Spark's values.

**Retires:** when the native door answers Spark's value for these D2 rows, pinned; or when the owner declines the native door with a dated ruling on this card.

**Relation:** [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md); the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md) (D2, D5); the [v2 label draft](../../../docs/corpus/v2-label-supported-behaviour-draft-2026-11-03.md), family 3 (native outside the parity claim).

Up: [map.md](map.md).
