# Unit ledger — ICE-ARRAY-INSERT-1 · inserts into array columns answer Spark on every door

**Date:** 2026-09-18 · **Branch:** `chore/rp-29-fork-pin` · **Base:** `387a4dca` (RP-29 pin bump)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — test-only unit: one fixture, one recorder, one
test module, registry rows, maps. No product code changes.
**Fork:** `TRO-Wolf/iceberg-rust` #295 (F-LIST-INSERT-1), in the workspace pin since RP-29
`387a4dca`.

**Why now.** At fork `9e67e000` every Iceberg writer relabels nested fields to the table's
Iceberg types, so `ARRAY`, list-of-struct and map-of-list inserts carry element, key and
value field ids in the parquet footer, and `INSERT … VALUES` into nested columns plans.
The `INSERT … VALUES` into `list<int>` / `list<struct>` failure that made
ICE-NESTED-INSERT-LIST-1 OPEN is gone; this unit pins the whole door matrix against Spark.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock` (the bump is done), any
product-code change, the CAST-MAP-SPELL-1 parser gap (BACKLOG, pinned xfail only).

## The measured oracle

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, recorded 2026-09-18, checked in as
`python/repark-parity/fixtures/torture/data/ice_array_insert_1/spark_array_insert_oracle.json`
(see its `map.md`): 30 cells, shapes `list_int` / `list_struct` / `map_list` by doors
`sql_values` / `sql_select` / `writeto_append` / `insert_into` / `save_as_table_append` by
format versions 2 / 3, each with `table_ddl`, `statement`, `rows`, `schema`, `data_files`
and `parquet_field_ids`.

## Fix

Test-only. `test_ice_array_insert_1.py` creates each cell's DDL on a fresh RePark memory
catalog (v3 CREATE allowed), writes through the cell's door — SQL doors run the recorded
statement verbatim, DataFrame doors build the source with `session.sql(<statement>)` and
write with `writeTo(t).append()` / `write.insertInto(t)` /
`write.format("iceberg").mode("append").saveAsTable(t)` — then asserts the read-back rows
(`ORDER BY id`, `asDict(recursive=True)`) equal the cell's rows and the first data file's
footer ids, walked with the recorder's own `field_ids`, equal the cell's ids. `data_files`
is never pinned. The 8 non-VALUES `map_list` cells run verbatim under strict xfail
(CAST-MAP-SPELL-1) with substitute-source twins beside them; the live tier has Spark adopt
each RePark-written `sql_values` table and read the recorded rows.

## Rulings

- **Q-1 — the forkwrite xfails flip in this unit's first commit.** At the bumped pin the
  two `forkwrite-list_element_child_add_read` cells XPASS(strict) and the ignored Rust
  read-back passes unignored, so the bump-fallout commit turns them into plain passing
  pins with their map rows. The registry sentences follow in this unit's last commit.
- **Q-2 — the substitute NULL-map row is `CASE WHEN false THEN map(...) END`.** It is the
  only spelling tried that RePark types as `map<string, array<int>>` and Spark answers
  with NULL; measured identical on live Spark before pinning.
- **Q-3 — no adopted Spark tables.** RePark CREATE succeeds for all three shapes, so the
  pins create with RePark's own DDL; the live tier adopts in the RePark-to-Spark direction.

## PROPOSITION LEDGER — ICE-ARRAY-INSERT-1 — 2026-09-18

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The 30-cell Spark oracle is in the repository byte-identical with its map and parent link. | `ice_array_insert_1/spark_array_insert_oracle.json` (SHA-256 `d41216e5156cb7eef11b24664e525399b1d20424d5552be0a6699dd50a8d7040`, `cmp` clean) + `map.md` + parent `data/map.md` row. | PROVEN | Commit `34e86f60`. |
| C-002 | The record driver reproduces every recorded statement and DDL and holds the one footer walk. | `_record_ice_array_insert_1.py`; statement/DDL rebuild check 0 mismatches over 30 cells; pins import `field_ids` from it. | PROVEN | Commit `34e86f60`. |
| C-003 | One pin per cell asserts Spark's rows and footer ids; the 22 cells outside CAST-MAP-SPELL-1 pass plain. | `test_ice_array_insert_1.py::test_cell_matches_spark` — 30 passed incl. 8 twins, 8 xfailed, 6 live-skipped offline. | PROVEN | See §Facade evidence. |
| C-004 | The 8 non-VALUES `map_list` cells run the recorded statement verbatim and strict-xfail on CAST-MAP-SPELL-1. | `test_cell_matches_spark[map_list_*]` xfail with the BACKLOG reason; the verbatim statement refuses `ParserError("Expected: ), found: <")` without the mark. | PROVEN | Flips when CAST-MAP-SPELL-1 is fixed. |
| C-005 | The substitute NULL-map source answers Spark identically, measured once on live Spark. | `CASE WHEN false THEN map('k1', array(1, 2)) END`: all 8 cells rows_match and ids_match on Spark 4.1.2 UTC (3 files vs recorded 4 — task-count noise, unpinned). | PROVEN | Result in the fixture `map.md`. |
| C-006 | The 8 substitute twins write the same rows through the same door and pass. | `test_cell_substitute_source_matches_spark` — 8 passed offline and live. | PROVEN | See §Facade evidence. |
| C-007 | Live Spark adopts each RePark-written `sql_values` table and reads the recorded rows. | `test_live_spark_reads_repark_table` — 6 passed under `REPARK_PARITY_LIVE=1` via `register_table`. | PROVEN | See §Facade evidence. |
| C-008 | Registry row ICE-ARRAY-INSERT-1 is FIXED with before/after and pins; the nested-evo sentences calling list INSERT a fork-writer defect are true. | `docs/spark-sql-iceberg-parity.md`: new row, ICE-NESTED-DDL-1 pointer sentence, ICE-NESTED-INSERT-LIST-1 rewritten FIXED. | PROVEN | This unit's last commit. |
| C-009 | Bump fallout: the forkwrite list-INSERT pins are plain passing pins at this pin. | `test_ice_nested_evo_1.py` 54 passed 5 xfailed; `repark-spark --lib nested_column_ddl` 12 passed 1 ignored; both maps true. | PROVEN | Commit `17ddc262`; XPASS(strict) red-first before the flip. |
| C-010 | No adopted Spark tables were needed. | All three shapes CREATE and write on RePark's own DDL; registry says so. | PROVEN | Q-3. |

## Red evidence

- `test_ice_nested_evo_1.py -k forkwrite` at the bumped pin before the flip: 2 XPASS(strict)
  (the `#295` write relabel fixed the fork-writer defect the marks pinned).
- `SELECT 2, CAST(NULL AS MAP<STRING, ARRAY<INT>>)` on RePark: `ParseException`
  `ParserError("Expected: ), found: <")` — the CAST-MAP-SPELL-1 refusal the 8 verbatim
  xfails pin.
- `CASE WHEN false THEN map('k1', array(1, 2)) END` on RePark types
  `map<string, list<element: int32>>` and answers NULL — the substitute the twins pin.

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

- Offline: `pytest python/repark/tests/test_ice_array_insert_1.py -q -p no:cacheprovider -n 4` →
  30 passed, 6 skipped, 8 xfailed.
- Live: `REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64
  SPARK_LOCAL_IP=127.0.0.1 ... jvm-lock.sh .venv/bin/python -m pytest
  python/repark/tests/test_ice_array_insert_1.py -q -p no:cacheprovider` →
  36 passed, 8 xfailed.
- Bump fallout: `test_ice_nested_evo_1.py` → 54 passed, 5 xfailed;
  `cargo test -p repark-spark --lib nested_column_ddl` → 12 passed, 1 ignored.
- Lint: `ruff check` + `ruff format --check` clean on both new Python files;
  `cargo fmt --all --check` clean.

## Gates

- `ruff check`, `ruff format --check` on `_record_ice_array_insert_1.py` and
  `test_ice_array_insert_1.py`: clean.
- `cargo fmt --all --check`: clean.
- Pre-commit hooks (map-sync, crate-dag, lib-rs, rust-file-size, lib-py,
  docstring-presence, docs-compaction, manifest) green on every commit of this unit.

## Coverage attestation

Written by the orchestrator (claude-opus-5) after re-running the local gate on head `1fd7af61`:
comment-ban hits=0; release native rc=0; `repark-spark --lib nested_column_ddl` 12 passed, 1 ignored;
the two unit files 84 passed, 6 skipped, 13 xfailed offline and 90 passed, 13 xfailed live.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-array-insert-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation is read from the committed Spark 4.1.2 oracle
        (byte-identical copy of the 2026-09-18 recording, SHA-256 in the fixture
        map.md); the footer walk is the recorder's own field_ids, imported, so no
        hand-computed id list exists.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_array_insert_1/spark_array_insert_oracle.json, python/repark/tests/_record_ice_array_insert_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red at the old pin, measured by the orchestrator at 3296ffc7
        (main f62fb11f) — INSERT VALUES into list<int> and list<struct> failed
        loud with the Arrow concat error; the map_list non-VALUES statements
        refuse on CAST-MAP-SPELL-1 and stay strict xfails that flip when that
        row is fixed.
      artifacts: [python/repark/tests/test_ice_array_insert_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-3
      status: ATTACKED
      evidence: All 30 cells (3 shapes x 5 doors x v2/v3) are one test id each;
        the 8 substitute twins cover the CAST-MAP cells; the live tier has Spark
        adopt RePark-written tables and read Spark's rows (90 passed live).
      artifacts: [python/repark/tests/test_ice_array_insert_1.py]
    - id: AT-4
      status: N/A
      justification: No product code and no shared mutable state change on the RePark side; the behaviour change is the fork pin.
    - id: AT-5
      status: ATTACKED
      evidence: Each cell uses a fresh memory catalog under tmp_path; the live tier
        runs under the JVM lock; no network, no credentials.
      artifacts: [python/repark/tests/test_ice_array_insert_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Rows and footer ids are asserted as values; data-file counts are
        deliberately not pinned (Spark's count follows its task count), recorded
        in the fixture map.md.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_array_insert_1/map.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The bump touches only the five rev lines and Cargo.lock (plus the
        fork-sync and root map rows); the rest is tests, fixture, recorder,
        registry, maps and this ledger. No STATUS.md edit.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: The behaviour is registered where it lives — row ICE-ARRAY-INSERT-1
        FIXED with ICE-NESTED-INSERT-LIST-1 brought true, the fixture map, the
        tests map and the staging ledger map; the one broken anchor CI found was
        repaired and check_docs_links is clean.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The two pins that were blocked on the fork writer (the Rust
        nested_column_ddl ignore and the forkwrite strict xfail) run plainly at
        this pin; the nested-evo file stays green (54 passed, 5 xfailed).
      artifacts: [crates/repark-spark/src/tests/nested_column_ddl.rs, python/repark/tests/test_ice_nested_evo_1.py]
  complete: true
```
