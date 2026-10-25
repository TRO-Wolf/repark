# Unit ledger — GATE-RETIRE-1 · retire R-011's container-cast gate arm the zone seam made unreachable, and its stale hint sentence

**Date:** 2026-10-25 · **Branch:** `feat/gate-retire-1` · **Base:** `e91dd8fa` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [../../wo/gate-retire-1/order.md](../../wo/gate-retire-1/order.md) (grade B, release 1.5.6);
parity row R-011 with its 2026-10-25 status line in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md); the seam's
ledger [zone-seam-1-ledger.md](zone-seam-1-ledger.md).

**Orchestrator ruling (2026-10-25, on the builder's H3 halt, as the builder's notes record it):** the
three lineage-walker tests in `nested_ns/tests.rs` keep their assertions and expected values and take a
predicate local to that file that restates the retired arm.

## PROPOSITION LEDGER — GATE-RETIRE-1 — 2026-10-25

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | R-011's container-cast gate arm (sketch arm A2) and its helper are removed; the census recount moves no cell, and a whole-container `CAST` of an instant outside UTC stores the session wall. | The WRITE-SPINE-1 step-0 census replayed on the head against the main fixture; the New York whole-struct `CAST` INSERT pin. | **PROVEN** | Census replay `--main` on head `b39576de`: 0 of 12192 cells differ from `write_spine_1_main_7a789fc5.json`. Pin `ntz_nested_refuse::a_whole_container_cast_of_an_instant_stores_the_session_wall_and_the_hint_no_longer_names_it`: stores 1767305045123456 in one snapshot, Spark's value for census cell `ctas.append.struct_ntz.inst.new_york`. |
| C-002 | The micros refusal hint no longer says a `CAST` of the whole struct, array or map is refused; its parenthesis reads "(in an UPDATE assign the field itself)"; no hint or refusal text names a data-deleting action (ruling 76(a)). | The whole-text pin, and the 76(a) word loop over the expected text. | **PROVEN** | `grep -rn "whole struct, array or map" crates/` prints nothing. The 76(a) loop (`drop`, `truncate`, `overwrite`, `recreate`, `delete`, `replace`) passes on the new text. Pin `ntz_nested_refuse::the_refusal_names_the_table_the_column_and_the_leaf`. |
| C-003 | Step-3 finding: the removed arm had no refusal pin. With its body forced to `false` on the unmodified head, the only red tests are the two lineage-walker unit tests that used it as their probe; no refusal test goes red. | Mutant run on `bfc87fe4`: the nextest selection of `nested_ns`, `ntz_nested_refuse` and `nested_ns_routes`; the census replay; `test_zone_seam_1.py`. | **PROVEN** | 22 run, 20 passed, 2 failed (the two walker tests). Every `ntz_nested_refuse` and `nested_ns_routes` test and all 47 `test_zone_seam_1.py` tests stayed green; the census replay differed in 0 of 12192 cells. Walker pins: `router::nested_ns::tests::a_column_that_resolves_in_no_input_reads_every_input_whole`, `router::nested_ns::tests::an_outer_reference_no_enclosing_join_resolves_fails_closed`, `router::nested_ns::tests::an_unmodelled_node_that_refers_out_reads_the_other_side_whole`. |
| C-004 | The coverage attestation is filed from the verifier's verdict. | The Muse MAX critic runs on this head. | **OPEN** | The Muse MAX critic runs on this head, and a clerk files the COVERAGE_ATTESTATION block from its verdict before merge (the ZONE-SEAM-1 ledger's AT-1 to AT-10 shape). |
