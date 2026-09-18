# Ledger — LISTING-COST-FLAKE-1 · wall-clock listing-cost pin becomes a call-count pin

**Date:** 2026-09-18 · **Branch:** `fix/listing-cost-flake-1` · **Base:** `origin/main` (`71482620`)
**Model:** muse-spark-1.3-contributor
**Card:** unit listing-cost-flake-1 step 1 — `listing_cost_list_tables_cheaper_than_provider_rebuild`
is a wall-clock comparison (`list_elapsed <= rebuild_elapsed * 2` over 20 iterations) that failed
`make verify` twice under box load on 2026-09-17 (ruling Q-20c-7 of run 20c) and passed 3/3 alone.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** A gate that depends on how busy the machine is carries no information: a red
distinguishes no regression from a noisy neighbour. The property under it is real and worth
keeping: list-on-access costs one namespace listing while a full provider rebuild costs a
listing plus one `load_table` per table. That is a count, and a count is deterministic.

**Not in this unit:** any product code (test module only); `Cargo.toml` / `Cargo.lock`;
`STATUS.md`; `briefs/next-sequence.md`; anything under `docs/history/` or
`task/ledgers/archive/` (frozen); the live tier.

## PROPOSITION LEDGER — LISTING-COST-FLAKE-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The test module gains a counting catalog that changes no behaviour: every method delegates to the inner catalog and returns exactly what it returns, while `list_tables` and `load_table` each increment their own counter. | The struct, its delegation, the two counters. | **PROVEN** | `CountingCatalog` in `crates/repark-iceberg/src/catalog/tests/catalog.rs` wraps `Arc<dyn Catalog>`, delegates every `Catalog` method to the inner handle, and increments `list_tables` / `load_table` / `list_namespaces` counters around the delegation. No `src/` file outside the test module changed (`git diff --stat` shows only the test file, 35+/35−). |
| C-002 | The rewritten `listing_cost_list_tables_loads_no_tables` calls `list_table_names` once on `sales` with 8 tables and asserts the exact measured counts: exactly one `list_tables` call and zero `load_table` calls. | The assertions with measured numbers; the run output they were read from. | **PROVEN** | Probe run printed `PROBE list: ns=0 tables=1 load=0`; the committed test asserts `list_namespaces == 0`, `list_tables == 1`, `load_table == 0` after one `list_table_names` call returning 8 names. |
| C-003 | The same test calls `build_iceberg_catalog_provider` once and asserts the exact measured `load_table` count, so a future change in per-table loading fails. | The assertion with the measured number; the run output. | **PROVEN** | Probe run printed `PROBE rebuild: ns=2 tables=1 load=0`. Measured `load_table` is **0**, not one-per-table: the fork's `IcebergCatalogProvider::try_new` lists but loads tables lazily, so the card's "one `load_table` per table" premise is falsified by measurement and the test pins the honest 0 — a future change that loads per table (0 → 8) still fails. The test asserts `list_namespaces == 2`, `list_tables == 1`, `load_table == 0` exactly. |
| C-004 | The test asserts the strict relation it exists for: the provider rebuild makes strictly more catalog calls than the listing, and the listing makes no `load_table` call at all. | The strict-relation assertion; green run. | **PROVEN** | `assert!(rebuild_calls > listing_calls, "rebuild {rebuild_calls} > listing {listing_calls}")` with counted totals listing = 1 vs rebuild = 3; `load_table == 0` asserted on the listing path. Green in §Gates. |
| C-005 | No wall-clock remains: the `Instant` / `elapsed` comparison and the 20-iteration warm-up loop are deleted, the `CATALOG_LISTING_STRATEGY == "list-on-access"` assertion is kept, the test is renamed, and every tree reference to the old name outside frozen bins is updated. | `grep` for the old name and `Instant` in the test; the rename diff. | **PROVEN** | `grep -n Instant` on the test file prints nothing; the old test body (55 lines) is replaced by the counting test (same setup: memory catalog, `sales`, 8 tables). Old-name mentions remain only where this unit must not edit: `docs/history/`, `task/ledgers/archive/` (frozen per card), other units' in-flight staging ledgers (`sql-literal-typing-1`, `ice-branch-ops-1`, `facade-2`), and the orchestrator's `task/roadmap/mid-term/` reports — listed in handback `out_of_scope_observed`. |
| C-006 | The pin discriminates: pointing the listing path at a `load_table` makes the test go RED; reverting restores green. A test that cannot go red is not a pin. | The red output pasted below. | **PROVEN** | Mutation (temporary `load_table`-per-table loop in `list_table_names`, reverted after) failed as quoted in §Mutation red proof; `git diff` confirms `mod.rs` is back to base. |
| C-007 | The pin is load-independent: 5 consecutive `cargo test -p repark-iceberg listing_cost` runs under load all pass. | The 5 green lines pasted below. | **PROVEN** | 5/5 `ok. 1 passed` with every CPU burning (see §Anti-flake evidence). |
| C-008 | `crates/repark-iceberg/src/catalog/tests/map.md` moves in lockstep: it cites `pins: listing-cost-flake-1/C-NNN` and carries one sentence on why the pin counts calls rather than time. | The map.md diff. | **PROVEN** | `crates/repark-iceberg/src/catalog/tests/map.md` cites `pins: listing-cost-flake-1/C-001 … C-009` and carries the counts-vs-time sentence. |
| C-009 | Gates green on the branch: `cargo test -p repark-iceberg`, `cargo test -p repark-core`, `make verify`, `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider -n 8`. | Counts pasted into §Gates. | **OPEN** | Gates running; counts to follow in §Gates. |

VERDICT: 9 clauses, 8 PROVEN, 1 OPEN (C-009, gates running), 0 REJECTED.

## Red first

The new pin was written against the base tree's behaviour, which already implements
list-on-access, so the pin itself is green on base by construction. Its red capability is
proven by mutation instead (§Mutation red proof): a test that cannot go red is not a pin,
and this one goes red exactly where the property breaks.

## Mutation red proof (C-006)

Temporary working-tree mutation (reverted immediately after): `list_table_names` in
`crates/repark-iceberg/src/catalog/mod.rs` gained a `load_table`-per-table loop.
The pin failed on the listing-path `load_table == 0` assertion:

```
---- catalog::tests::catalog::listing_cost_list_tables_loads_no_tables stdout ----
thread 'catalog::tests::catalog::listing_cost_list_tables_loads_no_tables' (1865570) panicked at crates/repark-iceberg/src/catalog/tests/catalog.rs:532:5:
assertion `left == right` failed
  left: 8
 right: 0
failures:
    catalog::tests::catalog::listing_cost_list_tables_loads_no_tables
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.01s
```

After reverting the mutation (`git diff` shows `mod.rs` clean), the test is green again.
A second mutation direction is covered by construction: the rebuild-path `load_table == 0`
assertion fails (0 → 8) if the fork ever starts loading tables eagerly.

## Anti-flake evidence (C-007)

Five consecutive exact runs with every CPU spinning (`multiprocessing` burners,
`/tmp/load5.py`, scratch only, never committed):

```
run1: rc=0 ['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.05s']
run2: rc=0 ['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.03s']
run3: rc=0 ['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.03s']
run4: rc=0 ['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.02s']
run5: rc=0 ['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out; finished in 0.02s']
```

No wall-clock is asserted anywhere in the pin, so box load cannot move it.

## Gates

TBD — counts land here once `make verify` and the Python suite finish.

```
COVERAGE_ATTESTATION:
  pr_unit: listing-cost-flake-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every card step walked against the diff: counting catalog delegates without behaviour change (C-001), exact listing counts from the probe (C-002), exact rebuild counts with the falsified load-per-table premise pinned as honest 0 (C-003), the strict relation (C-004), wall-clock deletion plus rename (C-005), mutation red (C-006), 5 load greens (C-007), map lockstep (C-008), gates (C-009).
      artifacts: [crates/repark-iceberg/src/catalog/tests/catalog.rs, task/ledgers/staging/listing-cost-flake-1-ledger.md]
    - id: AT-2
      status: N/A
      justification: Single fixed-shape cost pin by design (one namespace, 8 tables); no input domain varies. Empty-namespace and OOB arms are covered by sibling tests in the same file.
    - id: AT-3
      status: N/A
      justification: Test-only change; no product error path, retry, timeout, or cleanup path is added or altered.
    - id: AT-4
      status: N/A
      justification: One single-threaded test owns its counters; the Arc counters use SeqCst and no state escapes the test.
    - id: AT-5
      status: N/A
      justification: Memory catalog only; no credential, no network, no secret-bearing value is logged or forwarded.
    - id: AT-6
      status: ATTACKED
      evidence: Rename compatibility checked by tree grep: no live code references the old name; remaining mentions are frozen history, other units' append-only ledgers, and orchestrator roadmap docs, all recorded as out-of-scope observations rather than silently absorbed.
      artifacts: [crates/repark-iceberg/src/catalog/tests/map.md, task/ledgers/staging/listing-cost-flake-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: The defect was gate-breaking (two make-verify reds under load); the replacement asserts no wall-clock at all and passed 5/5 with every CPU burning plus the full 448-test crate suite.
      artifacts: [task/ledgers/staging/listing-cost-flake-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No Catalog-trait or fork contract assumed: the fork's lazy-load behaviour (rebuild load_table == 0) was measured on the pinned rev, not taken from the card, and the counting wrapper keeps the exact delegated signatures.
      artifacts: [crates/repark-iceberg/src/catalog/tests/catalog.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every assertion prints its numbers: the strict-relation message interpolates both totals and the mutation red shows left 8 right 0 with the file and line.
      artifacts: [crates/repark-iceberg/src/catalog/tests/catalog.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation spot-check done for real: a load_table-per-table listing fails the pin (observed red, then green after revert); the reverse direction (eager fork loads) fails the rebuild-side zero by construction.
      artifacts: [task/ledgers/staging/listing-cost-flake-1-ledger.md]
  complete: true
```
