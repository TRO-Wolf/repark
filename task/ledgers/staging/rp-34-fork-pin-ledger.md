# Unit ledger — RP-34-FORK-PIN · the fork pin moves to `43fcd243` and every cell it moves is tied to a Spark cell

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-34-fork-pin` · **Base:** `b0fb6feb`
**Model:** claude-opus-5 (orchestrator, run 24c) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — a pin bump, three boxed futures, pin flips and
two registry rows. No behaviour is written in RePark.
**Fork:** `TRO-Wolf/iceberg-rust` #306 (F-PARQUET-SIZE-1: Java's parquet footer, smaller
files) and #305 (F-DANGLING-DV-COMMIT-1: every merging commit drops the deletion vectors of
removed data files).
**Registry:** `ICE-RDF-GRANULARITY-1` (FIXED), `ICE-RDF-RPD-TARGET-SMALL-1` (new, OPEN fork
ask), `ICE-RDF-OPTIONS-1` (counts).

**Measured.** CI on the bump plus the clippy fix (`c2e79fb3`, run 35433272503, smoke job
against the built wheel): 10,805 passed, 7 failed. Three failures are `XPASS(strict)` on the
`FORK-GROUP-GRANULARITY` reasons (`test_option_cell_values[max_group_size]`,
`[partial_progress_groups]`, `test_option_cell_snapshots[partial_progress_groups]`): the
cells now equal the recorded Spark values. Four are `rpd_target_small` and
`rpd_target_small_forced`, value and snapshot: RePark rewrites 8 delete files into 8
(bytes 11,834 → 12,030) and commits 10 snapshots where the recorded Spark cell rewrites 0
and commits 9. Rust lint, the workspace Rust tests and the Python job were green.

**Not in this unit:** the fork's delete-file selection rule (run 24d, fork ask
F-RPD-TARGET-SMALL-1), `STATUS.md`, any product behaviour.

## PROPOSITION LEDGER — RP-34-FORK-PIN — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The three `ICE-RDF-GRANULARITY-1` cells answer Spark at fork `43fcd243` and run plainly. | `test_option_cell_values[max_group_size]`, `[partial_progress_groups]`, `test_option_cell_snapshots[partial_progress_groups]` lose their xfail marks; each was `XPASS(strict)` in the CI smoke job at `c2e79fb3`. Registry row FIXED. | PROVEN | Smoke job of run 35433272503. |
| C-002 | The four `rpd_target_small` cells are dated strict xfails naming the fork ask, and their rows equal Spark's. | `_VALUE_XFAIL` / `_SNAPSHOT_XFAIL` carry `F-RPD-TARGET-SMALL-1 2026-09-19`; the keep-set twins assert 200 live rows. Registry row `ICE-RDF-RPD-TARGET-SMALL-1` OPEN. | PROVEN | The CI failures above are the red evidence. |
| C-003 | The three `clippy::large_futures` errors at the new pin are fixed without an allow. | `apply_partitioning.rs` and `run_maintenance_apply.rs` box the futures (`Box::pin`), the repo idiom; CI Rust lint green at `c2e79fb3`. | PROVEN | The bump alone at `171deddf` failed clippy (run 35429860122). |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-34-fork-pin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every flipped or xfailed expectation reads the committed
        Spark 4.1.2 recording; no value is hand-computed.
      artifacts: [python/repark/tests/ice_rdf_options_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the new pin is the CI smoke list (3 strict XPASS,
        4 failures) measured before any pin moved; the strict marks turn
        red again if either side moves.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The regressed cells keep a plain live-row twin, so a row
        loss behind the xfail still reds.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added.
    - id: AT-5
      status: N/A
      justification: No I/O, credential or network surface changes.
    - id: AT-6
      status: ATTACKED
      evidence: The registry rows quote the measured counts from the CI log
        and the fixture.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the pin, the lockfile, three boxed calls,
        the test marks, two registry rows, map entries and this ledger.
      artifacts: [Cargo.toml, Cargo.lock, crates/repark-spark/src/call/apply_partitioning.rs, crates/repark-spark/src/call/run_maintenance_apply.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The residual is registered with its owner (run 24d) and its
        dated reason string, not left as an unexplained failure.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The whole facade suite and the workspace Rust tests ran in
        CI at the new pin; the only moved cells are the seven named here.
      artifacts: [task/ledgers/staging/rp-34-fork-pin-ledger.md]
  complete: true
```

## Hand-back

`Model: claude-opus-5`. `risk_tier: standard`. Round 1 CONCLUDED with three clauses PROVEN.
