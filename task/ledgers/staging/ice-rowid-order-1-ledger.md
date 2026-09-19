# Unit ledger — ICE-ROWID-ORDER-1 · one statement's v3 row ids are deterministic

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `chore/rp-31-fork-pin` · **Base:** `f53f917e` (RP-31 pin bump)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — test-only unit: two fixtures, one recorder, one
test module, two registry rows, maps. No product code changes.
**Fork:** `TRO-Wolf/iceberg-rust` #300 (F-ROWID-ORDER-1), in the workspace pin since RP-31
`f53f917e`.
**Registry:** rating claim C-3, V3-02; V3-COV-3 and V3-FILEORDER-1.

**Why now.** At fork `50350e33` the DataFusion commit exec appends a statement's data files in
ascending partition value, then write-task index, so a statement's `first_row_id` assignment no
longer follows task completion order. The twelve-run a/b/c pins hold one mapping equal to
Spark's recording; the eight-category pin holds RePark's ascending order with Spark's
hash-partitioner order as a DECLARED divergence.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock` (the bump is done), any
product-code change, the non-default order configurations (recorded, not pinned), the a/b/c
storm cells (ICE-APPEND-RETRY-1 owns them).

## The measured oracle

Two Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` recordings from run 23a, both checked
in byte-identical (see `ice_rowid_order_1/map.md`): `spark_rowid_abc_oracle.json` (twelve runs
each of INSERT INTO SELECT, literal VALUES and CTAS on a v3 table partitioned by `cat`, Hadoop
and InMemory catalogs, `local[8]`, shuffle 4 — every run a:0, b:100, c:200, VALUES a:0, b:2,
c:4) and `spark_rowid_order_oracle.json` (eight categories `d, a, z, m, b, q, c, x`, six runs
per adaptive x row-count x distribution-mode configuration — default configuration file order
`z, x, m, a, q, b, c, d` in 6 of 6).

## Fix

Test-only. `test_ice_rowid_order_1.py` repeats each a/b/c shape twelve times on fresh RePark
memory-catalog tables and asserts all twelve give one mapping equal to Spark's recorded one;
repeats the eight-category shape twelve times and asserts one ascending mapping
(`a, b, c, d, m, q, x, z` at 50-row steps); and asserts Spark's recorded default-configuration
order differs from ascending, so a future convergence reds the pin. Ruling Q-23b-2 binds the
shape: RePark deterministic (ascending partition value, then task index).

## Rulings

- **Q-23b-2 (orchestrator, binds this unit), quoted:** RePark is now deterministic (ascending
  partition value, then task index). The pins assert the a/b/c cell over 12 runs gives one
  mapping equal to Spark's recorded a:0, b:100, c:200, VALUES and CTAS the same; the
  eight-category shape over 12 runs gives one mapping, RePark's ascending order, with a
  DECLARED-divergence assertion that Spark's recorded default-config order differs.
- **Q-1 — no live Spark leg in this unit's pins.** The recorded Spark orders are the oracle;
  the pins run JVM-free and the file runs green under `REPARK_PARITY_LIVE=1` unchanged. The
  recorder (not the pins) is the live-reproducible artifact.
- **Q-2 — the v3 coverage row-id pins stand, one docstring repaired.** Both pass at the new
  pin; only the CTAS control's truncated docstring changed — no assertion touched.

## PROPOSITION LEDGER — ICE-ROWID-ORDER-1 — 2026-09-18

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | Both Spark truths are in the repository byte-identical with map and parent link. | `spark_rowid_abc_oracle.json` (SHA-256 `ff500c59429d64124465b1e8c86415286b6acd7f8235a10d05e25ada985e869d`) + `spark_rowid_order_oracle.json` (SHA-256 `06d6a8b266e553835c37f8698b883c2c53bf85e430e56189d2a30aa6739c1c82`), both `cmp` clean, + `map.md` + parent `data/map.md` row. | PROVEN | Commit `95e49eeda`. |
| C-002 | One recorder reproduces both recordings. | `_record_ice_rowid_order_1.py` (`record_abc` the eight cells, `record_order` every configuration; storm cells declared out of scope in the fixture map). | PROVEN | Commit `95e49eeda`. |
| C-003 | Twelve runs of each a/b/c shape give one mapping equal to Spark's recorded one. | `test_insert_select_mapping_is_one_and_matches_spark`, `test_values_mapping_is_one_and_matches_spark`, `test_ctas_mapping_is_one_and_matches_spark` — 5 passed offline and live. | PROVEN | See §Facade evidence. |
| C-004 | The twelve-run pins are red on the old pin. | Orchestrator-measured at main before RP-31 (probe `p_rowid_order.py`): RePark `INSERT INTO t SELECT …` gave 6 distinct partition-to-first-`_row_id` mappings in 12 runs (VALUES and CTAS stable 12/12); the pins assert one mapping, so they fail there. | PROVEN | Quoted, not re-run (no old native here). |
| C-005 | Twelve eight-category runs give one ascending mapping with Spark's default order as DECLARED divergence. | `test_eight_category_mapping_is_one_and_ascending` + `test_recorded_spark_default_order_differs_from_ascending` — ascending `a..z` at 50-row steps vs recorded `z, x, m, a, q, b, c, d`. | PROVEN | See §Facade evidence. |
| C-006 | The two v3 coverage row-id pins re-run green with their meaning intact. | `test_v3_statement_coverage.py -k row_id` → 2 passed; CTAS docstring completed (`never rode the fork`). | PROVEN | Commit `95e49eeda`; Q-2. |
| C-007 | The fixture map carries provenance and SHAs; the data and tests maps carry their rows. | `ice_rowid_order_1/map.md`, `data/map.md`, `tests/map.md` rows with `pins:` citations. | PROVEN | Commits `95e49eeda`, docs commit. |
| C-008 | V3-COV-3 is FIXED for determinism and the a/b/c shape with the residual stated; V3-FILEORDER-1 holds on every writer. | `docs/spark-sql-iceberg-parity.md`: V3-COV-3 rewritten FIXED with RESIDUAL + divergence pointer; V3-FILEORDER-1 repark bullet covers the delegated INSERT and cites the eight-category cell. | PROVEN | This unit's last commit. |

## Red evidence

- Old-pin red (orchestrator, quoted): at main before RP-31, probe `p_rowid_order.py` measured
  6 distinct partition-to-first-`_row_id` mappings in 12 runs of the a/b/c INSERT INTO SELECT
  (VALUES and CTAS stable 12 of 12) — the one-mapping assertion fails there. Not re-run in
  this round (no old-pin native here); the 2026-09-16 rating's 5-distinct probe at `a92a68db`
  is the same shape.
- Convergence red (by construction): `test_recorded_spark_default_order_differs_from_ascending`
  fails the day Spark's default-configuration file order becomes ascending partition value.

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

- Offline: `pytest python/repark/tests/test_ice_rowid_order_1.py -q -p no:cacheprovider -n 4` →
  5 passed.
- Live: the same file under `REPARK_PARITY_LIVE=1` (with the list-null file) → 234 passed,
  32 xfailed across both files.
- `pytest python/repark/tests/test_v3_statement_coverage.py -k row_id` → 2 passed, 163
  deselected (offline); 2 passed, 168 deselected (live).
- Lint: `ruff check` + `ruff format --check` clean on both new Python files; no non-ASCII
  bytes in either.

## Gates

- `ruff check`, `ruff format --check` on `_record_ice_rowid_order_1.py` and
  `test_ice_rowid_order_1.py`: clean.
- Pre-commit hooks (map-sync, crate-dag, lib-rs, rust-file-size, lib-py,
  docstring-presence, docs-compaction, manifest) green on every commit of this unit.
- Comment-ban driver over the branch against `origin/main`: 0 hits (§Coverage attestation).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-rowid-order-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation is read from the two committed Spark 4.1.2
        recordings (byte-identical copies, SHAs in the fixture map.md); the
        twelve-run loops assert the recorded mappings, so no hand-computed row
        id exists.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_rowid_order_1/spark_rowid_abc_oracle.json, python/repark-parity/fixtures/torture/data/ice_rowid_order_1/spark_rowid_order_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the old pin is the orchestrator's quoted six-mapping
        probe (not re-runnable here); red on convergence is constructed into
        the divergence pin. Both directions bite without touching the pins.
      artifacts: [python/repark/tests/test_ice_rowid_order_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The determinism claim is twelve fresh tables per shape (not one
        table read twelve times); the spec-correctness (contiguous, unique,
        running-sum first ids) is asserted as the exact 50-step mapping.
      artifacts: [python/repark/tests/test_ice_rowid_order_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; the unit adds no product code.
    - id: AT-5
      status: ATTACKED
      evidence: The offline pins run on tmp_path memory catalogs removed with
        the session; the recorder uses a fresh temp warehouse it removes; no
        network, no credentials.
      artifacts: [task/ledgers/staging/ice-rowid-order-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (the committed Spark
        recordings, the release-native twelve-run mappings), not prose: the
        SHAs, the one-mapping assertions, the recorded default order.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_rowid_order_1/spark_rowid_abc_oracle.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries only the fixtures, the recorder, the pins, the
        two registry rows, three map.md entries and this ledger; no Cargo.toml,
        lockfile, workflow, STATUS.md or product change.
      artifacts: [task/ledgers/staging/ice-rowid-order-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The determinism claim lives in its registered homes (V3-COV-3
        FIXED, V3-FILEORDER-1 true) with the pin pointers beside them; the
        fixture map, the data map, the tests map and the staging map carry
        their entries in the same round.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: N/A
      justification: Single-round test-only unit; no prior round to regress.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
eight clauses PROVEN and the gates green.
