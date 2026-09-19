# Unit ledger — ICE-LIST-NULL-1 · DELETE and UPDATE with IS NULL on nested columns answer Spark

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `chore/rp-31-fork-pin` · **Base:** `f53f917e` (RP-31 pin bump)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — test-only unit: one fixture, one recorder, one
test module, one registry row, maps. No product code changes.
**Fork:** `TRO-Wolf/iceberg-rust` #299 (F-LIST-NULL-ACCESSOR-1), in the workspace pin since RP-31
`f53f917e`.

**Why now.** At fork `50350e33` the DataFusion filter conversion binds list, map and struct
columns in `IS [NOT] NULL` tests, so `DELETE` / `UPDATE … WHERE` over a nested column answers
instead of refusing `Accessor for Field xs not found`. This unit pins the whole shape matrix
against Spark.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock` (the bump is done), any
product-code change, the CAST-MAP-SPELL-1 parser gap (BACKLOG, substitution only), the
fork #299 conjunction/disjunction residue (strict xfail only, new fork ask not filed).

## The measured oracle

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, recorded 2026-09-18 (run 23a), checked in as
`python/repark-parity/fixtures/torture/data/ice_list_null_1/spark_list_null_oracle.json`
(see its `map.md`): 128 cells, shapes `list_int` / `list_struct` / `map_int` / `struct` by
predicates `xs IS NULL` / `xs IS NOT NULL` / `id > 1 AND xs IS NULL` /
`xs IS NULL OR id = 1` by `delete` / `update` by `copy-on-write` / `merge-on-read` by
versions 2 / 3, each with `sql`, `ok`, `ids`, `operation`, `added_delete_files` and
`added_dvs`. Every cell answers (`ok` true).

## Fix

Test-only. `test_ice_list_null_1.py` creates each cell's DDL on a fresh RePark memory
catalog (v3 CREATE allowed), seeds the recorder's four rows, runs the cell's statement, then
asserts the ids left equal Spark's and the newest snapshot's operation equals Spark's; a
second parametrization asserts the delete-file / DV counts. The `map_int` empty-map seed row
runs through `map_from_arrays(CAST(array() AS ARRAY<STRING>), CAST(array() AS ARRAY<INT>))`,
which reads back equal to Spark's seed. The sixteen copy-on-write DELETE compound-predicate
cells run verbatim under strict xfail (fork #299 residue); the sixteen merge-on-read
`xs IS NOT NULL` cells pin RePark's measured 1 file against Spark's 2. The live tier
re-derives one cell per shape on Spark through the recorder's own `record_cell`.

## Rulings

- **Q-1 — the sixteen copy-on-write compound cells stay strict-xfail, verbatim.** The brief's
  premise (every cell answers) does not hold for them: the fix covers the single-predicate
  path only. The pins keep one test id per cell with the loud error named, so a fork-side fix
  XPASSes them red. No substitute predicate preserves the cell under test.
- **Q-2 — the empty-map substitute is `map_from_arrays` over two empty typed arrays.** The
  first candidate (the same expression) carried a doubled `>` from the measurement draft and
  was re-measured clean; `map_filter` over a one-entry map answers identically but reads
  further from the recorded seed. Measured read-back on the release native, v2 and v3.
- **Q-3 — count divergences pin RePark's values, per the brief's file-count rule.** Sixteen
  cells, all `xs IS NOT NULL` under merge-on-read; rows, ids and operations agree, only the
  delete-file packing differs (task-count noise).

## PROPOSITION LEDGER — ICE-LIST-NULL-1 — 2026-09-18

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The 128-cell Spark oracle is in the repository byte-identical with its map and parent link. | `ice_list_null_1/spark_list_null_oracle.json` (SHA-256 `08b8b5f7219dd1e0cfd50bfcec19009c7624caa631972335544e21901c77291f`, `cmp` clean) + `map.md` + parent `data/map.md` row. | PROVEN | Commit `52385e03`. |
| C-002 | The record driver reproduces every recorded statement and DDL and holds the live-leg entry. | `_record_ice_list_null_1.py`; `SHAPES`/`record_cell`/`record_all` match the run-23a recorder cell for cell; the live tier imports `record_cell`. | PROVEN | Commit `52385e03`. |
| C-003 | One pin per cell asserts Spark's ok, ids and operation; 112 pass, 16 run verbatim under strict xfail. | `test_ice_list_null_1.py::test_cell_answers_spark` — offline 112 passed, 16 xfailed; live 112 passed, 4 re-derived, 16 xfailed. | PROVEN | See §Facade evidence. |
| C-004 | The substitute empty-map seed row reads back equal to Spark's seed. | `test_map_seed_substitution_reads_back_empty` plus the §Red-evidence probe: `{k:1}` / NULL / `{}` / `{k:NULL}` on v2 and v3. | PROVEN | Q-2. |
| C-005 | Delete-file / DV counts are pinned where equal and pin RePark's measured values where they differ. | `test_cell_file_counts_match_spark` — 96 cells assert Spark's counts, 16 assert `FILE_COUNT_DIVERGENCES` (v2 `1/0`, v3 `1/1`). | PROVEN | Q-3; see §Facade evidence. |
| C-006 | Live Spark re-derives one cell per shape and answers the recorded ok, ids and operation. | `test_live_spark_rederives_shape_cell` — 4 passed under `REPARK_PARITY_LIVE=1` through the recorder's `record_cell`. | PROVEN | See §Facade evidence. |
| C-007 | The fixture map carries provenance, SHAs and the substitution; the data and tests maps carry their rows. | `ice_list_null_1/map.md`, `data/map.md`, `tests/map.md` rows with `pins:` citations. | PROVEN | Commits `52385e03`, `95e49eeda`. |
| C-008 | Registry row ICE-LIST-NULL-1 is FIXED with before/after, pins and both residues. | `docs/spark-sql-iceberg-parity.md`: new row after ICE-ARRAY-INSERT-1. | PROVEN | This unit's last commit. |

## Red evidence

- `DELETE FROM t WHERE id > 1 AND xs IS NULL` (and `xs IS NULL OR id = 1`, either order) on a
  copy-on-write table with a nested `xs` refuses `PySparkException DataInvalid => Accessor for
  Field xs not found` at pin `50350e33` — the 16 strict xfails pin this text. Bare nested
  `IS NULL`, compound predicates on plain columns, and every merge-on-read shape answer.
- `SELECT CAST(map() AS MAP<STRING, INT>)` on RePark refuses `ParserError("Expected: ), found:
  <")` — the CAST-MAP-SPELL-1 refusal the substitution avoids (no xfail: the pins never run
  the recorded spelling).
- `map_from_arrays(CAST(array() AS ARRAY<STRING>), CAST(array() AS ARRAY<INT>))` on RePark
  types `map<string, int32>` and answers `[]`; the seeded table reads row 3 as `{}` on v2
  and v3.

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

- Offline: `pytest python/repark/tests/test_ice_list_null_1.py -q -p no:cacheprovider -n 4` →
  225 passed, 4 skipped, 32 xfailed.
- Live: `REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64
  SPARK_LOCAL_IP=127.0.0.1 ... jvm-lock.sh .venv/bin/python -m pytest
  python/repark/tests/test_ice_list_null_1.py python/repark/tests/test_ice_rowid_order_1.py
  -q -p no:cacheprovider` → 234 passed, 32 xfailed (4 of them the live re-derivations).
- Lint: `ruff check` + `ruff format --check` clean on both new Python files; no non-ASCII
  bytes in either (E501 counts multibyte).

## Gates

- `ruff check`, `ruff format --check` on `_record_ice_list_null_1.py` and
  `test_ice_list_null_1.py`: clean.
- Pre-commit hooks (map-sync, crate-dag, lib-rs, rust-file-size, lib-py,
  docstring-presence, docs-compaction, manifest) green on every commit of this unit.
- Comment-ban driver over the branch against `origin/main`: 0 hits (§Coverage attestation).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-list-null-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation is read from the committed Spark 4.1.2 oracle
        (byte-identical copy of the run-23a recording, SHA-256 in the fixture
        map.md); the seed read-back asserts the recorded seed semantics, so no
        hand-computed id list exists.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_list_null_1/spark_list_null_oracle.json, python/repark/tests/_record_ice_list_null_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first on the 16 compound cells is the fork #299 residue
        itself (loud Accessor refusal at the bumped pin, quoted in Red
        evidence); the count divergences pin RePark's measured values, so a
        convergence on either side reds the suite.
      artifacts: [python/repark/tests/test_ice_list_null_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The matrix is exhaustive over the recorded shape x predicate x
        statement x mode x version product (128 cells, one pin id each, both
        parametrizations); the seed test covers the one substituted row.
      artifacts: [python/repark/tests/test_ice_list_null_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; the unit adds no product code.
    - id: AT-5
      status: ATTACKED
      evidence: The offline pins run on tmp_path memory catalogs removed with
        the session; the recorder uses a fresh temp warehouse it removes; the
        live tier reuses the shared session with its shuffle override restored.
      artifacts: [task/ledgers/staging/ice-list-null-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (the committed Spark
        cells, the release-native RePark measurement of all 128 cells), not
        prose: the SHAs, the 112/16/96/16 split, the divergence values.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_list_null_1/spark_list_null_oracle.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries only the fixture, the recorder, the pins, the
        registry row, three map.md entries and this ledger; no Cargo.toml,
        lockfile, workflow, STATUS.md or product change.
      artifacts: [task/ledgers/staging/ice-list-null-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The fix lives in its registered home (row ICE-LIST-NULL-1 with
        the pin pointer) with the pins beside it; the fixture map, the data
        map, the tests map and the staging map carry their entries in the same
        round.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: N/A
      justification: Single-round test-only unit; no prior round to regress.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
eight clauses PROVEN and the gates green.
