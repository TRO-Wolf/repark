# Unit ledger — WO ATTR-ID-1 · attribute identity as a function of the plan

**Date:** 2026-09-30 · **Branch:** `feat/attr-id-1` · **Base:** `db3a1f37` (`origin/main`)
**Model:** claude-opus-5-5 (S1 executor, high) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Work order:** `/tmp/oc-worker/direct/wo/attr-id-1-design.md` (owner-adopted 2026-09-30, §7a),
slice S1 (the Rust core; no facade change).

**Retires:** this ledger moves to `../completed/` in the unit's last commit (S4).

**Why.** Six verifier passes on #881 found eight S1 findings that are one bug: the DataFrame
door rebuilds attribute identity after the fact from four encodings that must agree, and every
op that does not propagate them leaves identity "unknown". Spark decides both halves (twins of
one attribute bind; two attributes with one name refuse) with one fact, the attribute's
`exprId`. This unit gives every output field of every DataFrame plan one attribute id, carried
in field metadata under `repark.attr`, and resolves names by that id.

## Round S1 (2026-09-30)

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On DataFusion 54.1.0 alone, the `repark.attr` field-metadata key propagates as measured: a bare column and an alias of a column inherit; a cast copies the source id; arithmetic, literal, function, one-argument `coalesce` and `CASE` carry none; Filter, Limit, Sort, Distinct and SubqueryAlias keep every id; a self-join repeats the ids on both sides; Union and `union_by_name` keep an id only where every input carries the same one; Aggregate keys keep theirs and values carry none; Window passes its input ids and its value carries none; a temp view read back by SQL keeps them, and so does the facade's join shape over two views. | One pin per row of the work order's §3.5, ids asserted by position after the node. | PROVEN | `crates/repark-core/src/session/tests/attr_id.rs`, 10 passed at the first commit. |
| C-002 | `stamp`, `attribute_ids`, `resolve` and the join re-mint land in `df_guards/attr_id.rs` as §3.1–§3.4 specify, re-exported through `frame_names`, with the three pyfunctions in `repark-python`. | Unit pins per helper; mutations M1–M4 turn their pins red. | OPEN (the core lands in the next S1 commit) | — |
