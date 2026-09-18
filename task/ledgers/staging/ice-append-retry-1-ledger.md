# Unit ledger — ICE-APPEND-RETRY-1 · the insert-storm row corrected to what Spark measures

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `docs/ice-append-retry-1` · **Base:** `origin/main` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `ICE-OCC-SCOPED-1-INSERT-STORM` (rating row V2-20a remaining distance).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Ruling Q-23b-1 (orchestrator, binds this unit), quoted:**

> The unit's premise ("if Spark commits 16 of 16, wire retry-on-rebase") is false; NO product
> change. The row stays BACKLOG with the corrected numbers.

**Why docs plus fixture plus recorder.** There is no product change: both engines lose appends
to the commit-retry budget under the 16-writer barrier storm, Spark somewhat less often, so the
row stays BACKLOG with the measured distributions on both sides. The unit records Spark's storm
over repetitions on two catalogs, transcribes RePark's measured distribution, cites the fork's
retry budget, and corrects the registry row and the test docstring.

## PROPOSITION LEDGER — ICE-APPEND-RETRY-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Spark's storm is recorded over repetitions on two catalogs: six barrier-released 16-insert repetitions per catalog × format version, every cell a repetition list. | `spark_occ_oracle4.json` plus `_record_ice_append_storm_1.py`. | **PROVEN** | The orchestrator's 2026-09-18 recording (Spark 4.1.2 + Iceberg 1.11.0, one session, Hadoop and InMemory catalogs; raw-recording SHA-256 `bcaafb93b6b2685f1e9143880d6d1afed21fa0c15ad5ad42115fdaa65a9dd887`), normalized mechanically (13 warehouse-prefix replacements, nothing else) into the committed fixture (SHA-256 `fd2286d2cb692d7c3ca271cb087dc7913ad38f883b201ab57e60aefab3932763`): Hadoop v2 15,13,11,12,14,12, Hadoop v3 14,12,13,9,10,13, InMemory v2 9,9,8,9,9,8, InMemory v3 9,8,8,7,9,9; every loser a `CommitFailedException`. The recorder replays the draft storm and writes the normalized form directly (repetitions as `REPETITIONS = 6`). |
| C-002 | RePark's distribution is measured: ten repetitions per format version on the release native, memory catalog. | Registry repark bullet. | **PROVEN** | Orchestrator-measured 2026-09-18: v2 7,5,5,6,5,6,6,6,5,5, v3 6,5,7,5,5,6,5,5,6,7; every loser `CatalogCommitConflicts`; rows = snapshots = commits in every repetition. Transcribed into the registry row; not re-run in this round (no native here). |
| C-003 | The fork honours the four retry properties: its commit loop reads them into an exponential backoff. | Registry repark bullet (code citation). | **PROVEN** | The fork's `Transaction::build_backoff` (`crates/iceberg/src/transaction/mod.rs` at fork `9e67e000`) reads `commit.retry.num-retries` (default 4), `commit.retry.min-wait-ms`, `commit.retry.max-wait-ms` and `commit.retry.total-timeout-ms` from the table properties into an exponential backoff (factor 2). What differs from Java is only jitter — a hypothesis for RePark sitting ~2 below Spark's in-memory count, not a finding. |
| C-004 | The registry row and the test docstring are corrected: neither says Spark commits 16 of 16. | Registry row `ICE-OCC-SCOPED-1-INSERT-STORM`; `test_insert_storm_loses_only_to_the_retry_budget` docstring. | **PROVEN** | Row title `RePark commits 5–7 of 16, Spark 7–15`, BACKLOG 2026-09-17 corrected 2026-09-18; both catalogs' distributions with the oracle pointer; the rationale retires Q-21a-5's one-repetition read and names the jitter fork card; the table line is true. The test docstring states the measured ranges and cites the fixture; the test body is untouched. |
| C-005 | Ruling Q-23b-1 — no product change — with its reason. | Registry rationale; the tree. | **PROVEN** | The premise is false (0 of 20 repetitions reached 16), so there is no retry-on-rebase work to wire; the row stays BACKLOG and the remaining distance is the jitter question, a fork card (F-COMMIT-JITTER-1, not filed). The tree carries no product change: fixture, recorder, registry, docstring, maps, ledger only. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 1. What was measured (orchestrator, 2026-09-18 evening — not re-run in this round)

Spark 4.1.2 + Iceberg 1.11.0, `local[8]`, one SparkSession, 16 threads released by one
`threading.Barrier`, `INSERT INTO ins VALUES (i, i)` for i in 0..16 on a fresh unpartitioned
table per repetition, 6 repetitions per catalog × format version. Committed: Hadoop v2
15,13,11,12,14,12; Hadoop v3 14,12,13,9,10,13; InMemory v2 9,9,8,9,9,8; InMemory v3
9,8,8,7,9,9. Every loser is a `CommitFailedException`. An earlier 4-repetition pass gave
Hadoop v2 11,13,13,11 and v3 14,14,15,12, InMemory v2 9,7,8,7 and v3 8,9,8,9; only that
earlier pass's shape sits behind the old 16-of-16 v2 cell, which is not reproducible (0 of 20
repetitions reached 16).

RePark main (release native, memory catalog, same storm, 10 repetitions): v2
7,5,5,6,5,6,6,6,5,5; v3 6,5,7,5,5,6,5,5,6,7. Every loser `CatalogCommitConflicts`; rows =
snapshots = commits in every repetition.

## 2. What this round changed

- Fixture `spark_occ_oracle4.json` (four renamed cells, normalized) plus the fixture `map.md`
  entry, SHAs and corrected reading bullet.
- Recorder `_record_ice_append_storm_1.py` (GAV from `_oracle_pins`, Ivy from
  `REPARK_ORACLE_IVY`, warehouse from `tempfile`, `REPETITIONS = 6`) plus the tests `map.md`
  entry.
- Registry row `ICE-OCC-SCOPED-1-INSERT-STORM` (title, both distributions, rationale, table
  line) and the test docstring (ranges plus fixture citation, body untouched).
- This ledger plus the staging `map.md` row.

## 3. Out of scope (observed, not worked)

- The jitter fork card (F-COMMIT-JITTER-1) is named in the registry rationale, not filed.
- The earlier 4-repetition pass is quoted in §1 for context; only the 6-repetition recording
  is committed.
- The recorder was not executed here (no JVM work in this round); it is import-clean by AST
  parse and ruff-clean.
- Per the brief's machine rule, `make verify`, `make preflight`, `make ci`, the facade suite
  and the parity harness were deliberately not run (docs-plus-fixture unit; no native here).

## 4. Gates

| Command | Result |
|---|---|
| AST parse plus `ruff check` plus `ruff format --check` (pinned 0.15.22) on the recorder | 0 — all checks passed, formatted |
| Comment-ban driver over the branch against `origin/main` | 0 hits (§5) |
| `sync_map_md.py --check` | 0 — maps clean |
| `typos` on the changed files | 0 |

## 5. Comment-ban gate (verbatim)

```text
comment-ban hits=0
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-append-retry-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The Spark cells are the orchestrator's 2026-09-18 recording (§1),
        normalized mechanically (raw SHA-256, 13 prefix replacements, committed
        SHA-256 all in the fixture map.md); RePark's numbers are the
        orchestrator's release-native measurement transcribed into the registry
        row. No hand-computed expectation anywhere.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_occ_scoped_1/spark_occ_oracle4.json, python/repark/tests/_record_ice_append_storm_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The recorder replays the draft's barrier storm on both catalogs
        and writes the normalized fixture directly (GAV from _oracle_pins, Ivy
        from REPARK_ORACLE_IVY, tempfile warehouse, REPETITIONS = 6); it cannot
        run in this round (no JVM), so import-cleanliness is proven by AST parse
        and ruff check plus format.
      artifacts: [python/repark/tests/_record_ice_append_storm_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The storm's whole surface is committed + losers = 16 with typed
        losers and durable commits; the pin asserts exactly that shape
        (committed + losers == of, CatalogCommitConflicts losers, rows and
        snapshots equal the commits) and the corrected docstring cites the
        measured ranges.
      artifacts: [python/repark/tests/test_ice_occ_scoped_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; the unit adds no product code.
    - id: AT-5
      status: ATTACKED
      evidence: No JVM, cargo or maturin work in this round; the recorder uses a
        fresh temp warehouse it removes; no network, no credentials.
      artifacts: [task/ledgers/staging/ice-append-retry-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (§1 verbatim
        distributions), not prose: the four Spark repetition lists, the two
        RePark lists, the loser classes, the SHAs.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_occ_scoped_1/spark_occ_oracle4.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries only the fixture, the recorder, the registry
        row, the test docstring, three map.md entries and this ledger; no
        Cargo.toml, lockfile, workflow, STATUS.md or product change.
      artifacts: [task/ledgers/staging/ice-append-retry-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The correction lives in its registered home (row
        ICE-OCC-SCOPED-1-INSERT-STORM plus the table line) with the test
        docstring beside the pin; the fixture map, the tests map and the staging
        map carry their entries in the same commits.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: N/A
      justification: Single-round docs-plus-fixture unit; no prior round to regress.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
five clauses PROVEN and the gates green.
