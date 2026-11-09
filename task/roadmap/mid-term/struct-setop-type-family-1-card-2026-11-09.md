# Card STRUCT-SETOP-TYPE-FAMILY-1: a set operation pairs a reordered struct by name when the list kinds differ

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Unit C park memo (2026-11-09) and the final verdict of the Unit C re-check.

**Status:** filed 2026-11-09, ruled 2026-11-09 (owner: defaults); S1 with write reach; blocks v1.5.7; runs before STRUCT-COMPARE-BY-POSITION-1's build. Not dispatched. Ledger row C-117 in [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) stays OPEN until this unit merges and its table-driven pins pass.

**Why:** The set-operation name check (`mismatch_at` in `struct_setop.rs`) descends into a list only when both sides are the same list kind. Every other pair falls through, so DataFusion's coercion unifies the two operands and casts the element struct by name. A reordered struct inside such a pair then pairs its fields by name where Spark pairs them by position. Measured on the parked head `b8ad7a9c`, facade door, two one-row frames with element struct `{a:1,b:2}` on the left and `{b:3,a:4}` on the right; "hidden" means the statement answered and paired by name:

| Left kind | Right kind | `union` | `subtract` |
|---|---|---|---|
| `list` | `list` (control) | refused | refused |
| `large_list` | `list` (F-I) | **hidden** | **hidden** |
| `list` | `large_list` | refused | **hidden** |
| `fixed_size_list[1]` | `list` | refused | **hidden** |
| `list` | `fixed_size_list[1]` | **hidden** | **hidden** |
| `fixed_size_list[1]` | `fixed_size_list[2]` | **hidden** | refused |
| `large_list` | `fixed_size_list[1]` | **hidden** | **hidden** |
| `list_view` | `list_view` | refused | refused |
| `large_list_view` | `large_list_view` | **hidden** | **hidden** |
| `list_view` | `list` | DataFusion coercion error | DataFusion coercion error |
| `large_list_view` | `large_list` | DataFusion coercion error | DataFusion coercion error |
| `run_end_encoded` | `run_end_encoded` | **hidden** | **hidden** |
| `run_end_encoded` | `struct` | refused | **hidden** |
| `struct<x: large_list>` | `struct<x: list>` | **hidden** | **hidden** |
| `map` | `map`, value struct reordered (control) | refused | refused as Spark does (`SET_OPERATION_ON_MAP_TYPE`) |
| `map` | `map`, entry fields renamed, same value struct | answers (correct, no over-refusal) | refused as Spark does |
| `map` | `map`, entry fields renamed, value struct reordered | refused | refused as Spark does |
| `dictionary` of struct | anything | `createDataFrame` refuses the frame | same |

The orchestrator re-ran the 20-shape probe on the head build at `b8ad7a9c`, and it matched the table. Impact: a silent wrong read and a wrong persisted Iceberg write, reachable from a supported facade input (polars list columns are Arrow `large_list`). Compatibility: pre-existing, not a regression; the native-door shape was measured on 1.5.6 by the verifier. The fix refuses more shapes, all of which are wrong today.

**Proposed fix:** the Q2 design of the Unit C park memo, in four parts:
1. peel `Dictionary` and `RunEndEncoded` on both sides;
2. one list family for `List`, `LargeList`, `FixedSizeList`, `ListView` and `LargeListView`, descending into the element;
3. `Map` with `Map` and `Struct` with `Struct` unchanged, the NULL-source skip first;
4. a fallthrough that refuses when both sides hold a struct anywhere inside them, naming the two types.

The walker lives in one shared function in `repark-core` that the comparison rule (STRUCT-COMPARE-BY-POSITION-1) also calls. A variant table is measured first. The acceptance property is kind invariance: every variant cell carries the same label as its `List` twin, and on the native door, where `arrow_cast` rows have no Spark leg, the `List` twin on the same door is the reference. The review is a scoped review, capped at one round.

**Close:** the table-driven pins over the variant table on Spark, 1.5.6 and head, with the axis of the memo's Q2 (list kind, encoding, payload, depth, map, operation, orientation, source, door, effect). The Status line of this card is updated on merge.
