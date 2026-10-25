# Unit ledger — GATE-RETIRE-1 · R-011's container-cast arm retained (GR-9): the micros hint corrected and the arm's refusals pinned

**Date:** 2026-10-25 · **Branch:** `feat/gate-retire-1` · **Base:** `e91dd8fa` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [../../wo/gate-retire-1/order.md](../../wo/gate-retire-1/order.md) (grade B, release 1.5.6);
parity row R-011 with its 2026-10-25 status line in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md); the seam's
ledger [zone-seam-1-ledger.md](zone-seam-1-ledger.md).

**Orchestrator ruling (2026-10-25, on the builder's H3 halt, as the builder's notes record it):** the
three lineage-walker tests in `nested_ns/tests.rs` keep their assertions and expected values and take a
predicate local to that file that restates the retired arm. *Superseded by GR-9 (2026-10-25, round 2):
the local predicate is gone, and `nested_ns/tests.rs` is byte-identical to `origin/main`.*

**Round 1 verdict (2026-10-25):** the Muse MAX verifier FAILED f7b78fed. Its findings S1-1 and S1-2 were
reproduced by the orchestrator on the head build and on Spark 4.1.2: the arm is load-bearing for two
container casts the zone seam does not rewrite, an arity-mismatch whole-struct `CAST` and a
cross-position whole-struct `TRY_CAST`, which the arm refuses in New York. Ruling GR-9: the arm is
retained, and the unit is the hint correction plus pins. The verifier's S2-2 found that the
outer-reference walker test passes with any predicate, so C-003 names the two discriminating walker
tests only.

**Round 2 verdict (2026-10-25):** Muse MAX PASS on `5b46c6b0`, no S1 and no S2. Seven claims hold; claim 3 holds with a note that the supply-walk use is killed by its own UPDATE and MERGE pins, not by the INSERT pins. The census recount compares 12192 cells, 0 differing from the main fixture (EQUAL 7135, DIFFERENT 1731, REFUSED 2380, ERROR 946). P3, the pin shape, stores the session wall in each zone and the other probes refuse at planning; no head STORED disagrees with Spark 4.1.2 across the eleven off-diff shapes, eight of them mirrored live. Mutants M0, M1' and M2' are each killed by named pins.

## PROPOSITION LEDGER — GATE-RETIRE-1 — 2026-10-25

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | R-011's container-cast gate arm (sketch arm A2) and its helper are removed; the census recount moves no cell, and a whole-container `CAST` of an instant outside UTC stores the session wall. | The WRITE-SPINE-1 step-0 census replayed on the head against the main fixture; the New York whole-struct `CAST` INSERT pin. | **REJECTED** (round 1, 2026-10-25, GR-9) | Census replay `--main` on head `b39576de`: 0 of 12192 cells differ from `write_spine_1_main_7a789fc5.json`. Pin `ntz_nested_refuse::a_whole_container_cast_of_an_instant_stores_the_session_wall_and_the_hint_no_longer_names_it` (renamed on `8c483ced`): stores 1767305045123456 in one snapshot, Spark's value for census cell `ctas.append.struct_ntz.inst.new_york`. **Dated reason (2026-10-25, round 1, Muse MAX FAIL on f7b78fed, reproduced by the orchestrator on the head build and on Spark 4.1.2):** the claim is refuted by measurement. The seam rewrites only same-position container casts, so an arity-mismatch `CAST(named_struct('a', x, 'b', 1) AS STRUCT<a: TIMESTAMP_NTZ>)` and a cross-position `TRY_CAST(named_struct('a', x, 'b', 1) AS STRUCT<a: INT, b: TIMESTAMP_NTZ>)` still reach the gate as native casts. Main refuses both in New York (CANNOT_SAFELY_CAST, 0 snapshots); with the arm removed, the head stored {a: 1767323045123456} (the UTC wall) and {a: NULL, b: 1}, and Spark 4.1.2 raises [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] on both. The step-0 census could not see them, because it carries whole-container casts only in table-creating spellings (ctas, rtas, create_or_replace_as, native_ctas). The arm is retained. |
| C-002 | The micros refusal hint's parenthesis says a `CAST` of the whole struct, array or map is refused unless its fields line up with the target's, so cast the leaf instead, and that in an UPDATE the field itself is assigned; no hint or refusal text names a data-deleting action (ruling 76(a)). | The whole-text pin, the clause pin, and the 76(a) word loop over the expected text. | **PROVEN** | `grep -rn "whole struct, array or map" crates/` prints only the hint in `nested_ns_gate.rs` and its expected text in `ntz_nested_refuse.rs`. The 76(a) loop (`drop`, `truncate`, `overwrite`, `recreate`, `delete`, `replace`) passes on the new text. Pins: `ntz_nested_refuse::the_refusal_names_the_table_the_column_and_the_leaf`, `ntz_nested_refuse::a_whole_struct_cast_whose_fields_line_up_stores_the_session_wall_and_the_hint_says_so`. |
| C-003 | Restated 2026-10-25 (round 2): with the arm's body forced to `false`, the discriminating walker tests are `router::nested_ns::tests::a_column_that_resolves_in_no_input_reads_every_input_whole` and `router::nested_ns::tests::an_unmodelled_node_that_refers_out_reads_the_other_side_whole`; `router::nested_ns::tests::an_outer_reference_no_enclosing_join_resolves_fails_closed` passes with any predicate and does not pin the arm. | Mutant run on `bfc87fe4` (round 1, retained as recorded) and the walker tests on `e170ba41`: the nextest selection of `nested_ns`, `ntz_nested_refuse` and `nested_ns_routes`; the census replay; `test_zone_seam_1.py`. | **PROVEN** | Round 1 (`bfc87fe4`, with a test-local probe predicate): 22 run, 20 passed, 2 failed (the two walker tests); every `ntz_nested_refuse` and `nested_ns_routes` test and all 47 `test_zone_seam_1.py` tests stayed green; the census replay differed in 0 of 12192 cells. The outer-reference walker test passes with any predicate (the verifier's S2-2). Pins: `router::nested_ns::tests::a_column_that_resolves_in_no_input_reads_every_input_whole`, `router::nested_ns::tests::an_unmodelled_node_that_refers_out_reads_the_other_side_whole`. |
| C-004 | The coverage attestation is filed from the verifier's verdict. | The Muse MAX critic runs on this head. | **PROVEN** | The Muse MAX critic runs on this head, and a clerk files the COVERAGE_ATTESTATION block from its verdict before merge (the ZONE-SEAM-1 ledger's AT-1 to AT-10 shape). Resolved 2026-10-25: attestation filed 2026-10-25 from the Muse MAX round-2 verdict (PASS), in the Coverage block below; cited from the tests map. |
| C-005 | The arm's refusal of the container casts the zone seam does not rewrite is pinned: a whole-struct `CAST` whose fields do not line up with the target (an arity mismatch, or fields at different positions), written by INSERT, UPDATE or MERGE in New York, is refused with CANNOT_SAFELY_CAST and the hint, and writes nothing. | Each use of the arm, dropped alone, fails a pin: dropping the `naive_columns` use fails the two INSERT pins, and dropping the supply-walk use fails the UPDATE and MERGE pins; the arm's body forced to `false` fails the two INSERT pins. | **PROVEN** | Pins: `ntz_nested_refuse::an_arity_mismatch_whole_struct_cast_is_refused_outside_utc_and_writes_nothing` and `ntz_nested_refuse::a_cross_position_try_cast_of_a_whole_struct_is_refused_outside_utc_and_writes_nothing` (helper `refused_and_unwritten`, no snapshot), and `ntz_nested_refuse::an_update_assigning_a_cross_position_whole_struct_cast_is_refused_outside_utc` and `ntz_nested_refuse::a_merge_updating_a_cross_position_whole_struct_cast_is_refused_outside_utc` (helper `refused_and_seed_only`, the seed snapshot only). Measured on `8c483ced` (the naive_columns drop and the body forced to `false`, both on the two INSERT pins) and on `e170ba41` (the supply-walk drop: 27 run, 25 passed, 2 failed, the UPDATE and MERGE pins). |

## Coverage (2026-10-25)

Filed from the Muse MAX round-2 verdict on `5b46c6b0` (PASS). The verdict is the source of every entry; it is not committed.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: gate-retire-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Seven claims walked on head 5b46c6b0 (the round-2 verdict, claims 1 to 7). nested_ns.rs and nested_ns/tests.rs are byte-identical to origin/main, and nested_ns_gate.rs differs from main by one hunk, the hint. Probes P1, P2, P4 to P10 and P12 refuse in New York and Kolkata with CANNOT_SAFELY_CAST at planning; the table records zero new snapshots for P1, P2, P4, P5, P6 and P9; P3, the pin shape, stores the session wall as the control. The census recount compares 12192 cells, 0 differing from the main fixture.
      artifacts: [crates/repark-spark/tests/ntz_nested_refuse.rs, crates/repark-iceberg/src/write/nested_ns_gate.rs, task/ledgers/staging/gate-retire-1-ledger.md, task/roadmap/mid-term/container-cast-shape-1-card-2026-10-25.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: Eleven off-diff shapes hunted, Q1 to Q10 and Q12, with Q5, Q9 and Q12 checked on the head only. Eight mirrored on live Spark 4.1.2 in New York: Q1 and Q3 store equal values, and six (Q2, Q4, Q6, Q7, Q8 and Q10) are refusal-versus-error dispositions, the head refusing where Spark raises DATATYPE_MISMATCH and disposed under R-011. Mutant M0 (arm body to false) reds the two INSERT pins, the UPDATE and MERGE pins and the two discriminating walker tests; M1' (arm removed at the supply-walk use) reds the UPDATE and MERGE pins; M2' (arm removed at the naive_columns use) reds the two INSERT pins.
      artifacts: [crates/repark-spark/tests/ntz_nested_refuse.rs, crates/repark-spark/src/router/nested_ns.rs, crates/repark-spark/src/router/nested_ns/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Regression. The census replay on the head compares 12192 cells against the main fixture with 0 differing (EQUAL 7135, DIFFERENT 1731, REFUSED 2380, ERROR 946, UNRECORDED 0). The arm and its walker tests are byte-identical to main, and the only product change is one string literal in the error-format text, so no value path changed.
      artifacts: [python/repark/tests/timestamps/write_spine_1_main_7a789fc5.json, python/repark/tests/timestamps/_record_write_spine_1.py, crates/repark-spark/src/router/nested_ns.rs]
    - id: AT-4
      status: N/A
      justification: The gate predicate is stateless and the only product change is a string constant; no shared or mutable state is touched.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the hint names no data-deleting action, checked by the 76(a) word loop in ntz_nested_refuse.rs over the expected text.
    - id: AT-6
      status: ATTACKED
      evidence: The four new refusal pins assert the error, the hint and zero new snapshots (refused_and_unwritten, the two INSERT shapes) or the seed snapshot only (refused_and_seed_only, the UPDATE and MERGE shapes). Round-2 claim 3 checked these helpers.
      artifacts: [crates/repark-spark/tests/ntz_nested_refuse.rs]
    - id: AT-7
      status: N/A
      justification: The only product change is hint text; no code path changed, so no performance measurement applies.
    - id: AT-8
      status: N/A
      justification: No manifest or crate-edge change on the branch (no Cargo.toml or Cargo.lock in the diff); the one product change is a string literal in nested_ns_gate.rs.
    - id: AT-9
      status: ATTACKED
      evidence: The refusal class and text are checked live. Every New York and Kolkata refusal in the round-2 probes is CANNOT_SAFELY_CAST at planning, the live refusal contains the clause unless its fields line up with the target's, and the Spark 4.1.2 mirror disposes each refusal-versus-error shape against DATATYPE_MISMATCH.
      artifacts: [crates/repark-spark/tests/ntz_nested_refuse.rs, crates/repark-iceberg/src/write/nested_ns_gate.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Mutants M0, M1' and M2' are each killed by named pins; no mutant survives. Each arm site has at least one pin that fails when that site is removed or forced to false.
      artifacts: [crates/repark-spark/tests/ntz_nested_refuse.rs, crates/repark-spark/src/router/nested_ns.rs]
  complete: true
```
