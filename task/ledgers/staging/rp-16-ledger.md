# Charter ledger — RP-16 · fork pin 090bc821 consumer (F-WRITE-COMPRESS-1; close PERF-CATALOG-CACHE-WEIGHT-1)

**Date:** 2026-09-11 · **Branch:** `chore/repin-rp-16` · **Base:** `origin/main`
**Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-10-repin-f25-ledger.md](../completed/rp-10-repin-f25-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The orchestrator bumped the iceberg-rust pin to
`090bc82142c09e0afd8c0ddaf594ab5843e1e77d` (`make bump-fork-pin`). The reason for the
bump is `#276` F-WRITE-COMPRESS-1: `INSERT INTO` data files now carry the table's
`write.parquet.compression-codec` (Iceberg default zstd). Rider `#274` F-CATIO-WEIGHT
is the fork fix `PERF-CATALOG-CACHE-WEIGHT-1` was waiting for: the shared manifest
`ObjectCache` charges each retained parsed object graph instead of `entries × 768 B`.
The registry's red-when-fixed pin redded as designed. This unit turns that forecast
into measured truth. Do not re-measure AP-1.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` (already on the branch); STATUS.md;
`docs/perf/adapt-part-ap1-2026-09-11.md`; any AP-1 re-measure.

## Take / skip — riders on `090bc821`

| Fork PR | Ask | Take or skip | What it means for RePark |
|---|---|---|---|
| `#276` | F-WRITE-COMPRESS-1 | **take** (reason for the bump) | `IcebergWriteExec` now parses `write.parquet.compression-codec` / `write.parquet.compression-level` and the INSERT path sets the codec (default zstd). Four other writer sites stay uncompressed as fork residue F-WRITE-COMPRESS-1-R-001…R-004. AP-1 re-measure is the orchestrator's next step, not this unit. |
| `#273` | F-INSERT-DIST-1 | **take** (no RePark edit) | INSERT declares hash distribution on evaluated partition values. write-distribution-2 filed the ask; this bump consumes it. No cell re-measured here. |
| `#274` | F-CATIO-WEIGHT | **take** (this unit's consumer fix) | Shared manifest `ObjectCache` charges retained object graphs. `PERF-CATALOG-CACHE-WEIGHT-1` FIXED. |
| `#275` | F-HMS | **skip** (fixture-only) | Hive metastore JARs checksum-pinned. No RePark product change. |
| `#277` | F-MINIO-QUAY | **skip** (fixture-only) | MinIO images move to quay.io. No RePark product change. |

## PROPOSITION LEDGER — RP-16 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The pin moved and the five `rev` lines are byte-identical: `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u` prints exactly one line, `090bc82142c09e0afd8c0ddaf594ab5843e1e77d`. | The grep; five Cargo.toml hits; six Cargo.lock sources; `docs/fork-sync.md` RP-16 row. | **PROVEN** | `sort -u` prints one line `rev = "090bc82142c09e0afd8c0ddaf594ab5843e1e77d"`. Cargo.toml 5 hits, Cargo.lock 6. Pin-history row 2026-09-11 names F-WRITE-COMPRESS-1 `#276` and riders `#273`–`#277`. Orchestrator commit `ad053a52`. This unit does not edit the pin. |
| C-002 | The measured budgets and the two re-shaped pins: smallest round retaining budget and largest round evicting budget recorded; `test_a_budget_sized_to_the_charged_weight_retains_every_table` keeps its name and takes a budget sized to charged weight; a sibling pin holds that 280000 now evicts the coldest table and the miss names the manifest. The thrash test is untouched. | Bisection log; the two pins green; red-first of the 280000 retain pin. | **PROVEN** | Bisection `/tmp/rp16_weight_bisect.py` (same `_session` / `_range_view` / 256 × `_RSS_ROWS_EACH` shape): 280000 → 64 retain / 192 evict, r0 evict; 1,000,000 → r0 evict; 1,071,000 smallest retain-all; 1,070,000 largest r0-evict (~4,184 B/table). Confirm 1,071,000 retain 3/3 isolated; pytest then flaked r0 once at 1,071,000 (TinyLFU single-victim). Retain pin budget **1,250,000**. File `33 passed, 3 skipped in 147.71s`. Two-pin run `2 passed in 42.38s`. Red-first pasted below. |
| C-003 | Registry `PERF-CATALOG-CACHE-WEIGHT-1` is FIXED 2026-09-11 at pin `090bc821` (fork `#274`), naming the two measured budgets and the two pins; every BACKLOG claim matching `PERF-CATALOG-CACHE-WEIGHT-1` / `F-CATIO-WEIGHT` is trued up; the IO-3 ledger gets a dated RP-16 consumer note without rewritten clause verdicts. | The registry row; grep; the IO-3 note; maps. | **PROVEN** | Registry FIXED with 1,071,000 / 1,070,000 measured and pins at 1,250,000 / 280000. BOUND-1 past-tenses the estimated-weight sentence and stays BACKLOG for the metadata LRU. Baseline §6.3 appends the close. Catalog map, tests map, perf map, session-and-conf, staging map, IO-3 ledger note. |
| C-004 | The whole facade suite and `make preflight` are green on this branch. | The listed gates with counts. | **PROVEN** | Listed gates all exit 0 (counts in §Gates). preflight members: verify 0, facade 5929 passed / 369 skipped, parity-cap 23 passed, audit 0, workflows-lint 0. `make py-test-dbt` hit `PermissionError: [Errno 13]` on `multiprocessing.SemLock` (sandbox `/dev/shm`, not this diff; 17 failed / 42 passed / 1 skipped). |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

### C-002 red-first (280000 retain pin on this tree, before reshape)

```
FAILED python/repark/tests/test_perf_ice_catalog_io_1.py::test_a_budget_sized_to_the_charged_weight_retains_every_table
E  repark.errors.PySparkException: Unexpected => Failed to load manifest list in cache, source:
   DataInvalid => Failed to read file …/manifest_weight/repark_ctas/ice/ns/r0/metadata/snap-….avro:
   No such file or directory
1 failed, 5927 passed, 369 skipped in 766.64s
```

### C-002 bisection (command and numbers)

```
cd /tmp/grok-noom3 && PYTHONUNBUFFERED=1 .venv/bin/python /tmp/rp16_weight_bisect.py
```

| budget | retained | evicted | r0 | seconds |
|---:|---:|---:|---|---:|
| 280000 | 64 | 192 | evict | 24.8 |
| 1000000 | 237 | 19 | evict | 24.4 |
| 2000000 | 256 | 0 | retain | 24.0 |
| 1500000 | 256 | 0 | retain | 24.1 |
| 1250000 | 256 | 0 | retain | 25.0 |
| 1125000 | 256 | 0 | retain | 24.6 |
| 1062000 | 251 | 5 | evict | 23.9 |
| 1093000 | 256 | 0 | retain | 24.0 |
| 1077000 | 256 | 0 | retain | 24.0 |
| 1069000 | 255 | 1 | evict | 23.8 |
| 1073000 | 256 | 0 | retain | 24.2 |
| 1071000 | 256 | 0 | retain | 24.9 |
| 1070000 | 255 | 1 | evict | 24.4 |

Confirm: 1,071,000 retain 3/3 isolated (~24 s each); 280000 r0 evict. Pytest of the 1,071,000 retain pin then failed on r0 once (TinyLFU edge). Pin constant is 1,250,000.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-16
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Retain pin at 1250000 answers r0 and r255 after every manifest is deleted. Evict pin at 280000 raises naming manifest on r0. Thrash pin untouched.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Bisection recorded the 1,071,000 / 1,070,000 pair; 280000 evicts 192 of 256; 1,250,000 retains in the bisection table.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: 280000 miss names the manifest (Failed to load manifest list / No such file). Retain pin fails loud on a miss rather than answering wrong rows.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-4
      status: N/A
      justification: No new shared state. The weigher change is fork-side inside ObjectCache; RePark only re-sizes the pin budget.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. This consumer commit does not edit Cargo.toml or Cargo.lock.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public API break. The session key repark.iceberg.manifestCacheBytes is unchanged; only the charged weight behind it moved.
      artifacts: [crates/repark-iceberg/src/catalog/map.md]
    - id: AT-7
      status: ATTACKED
      evidence: Structural pins, not a wall-clock CI pin. RSS at-bound cell not re-measured (orchestrator / AP-1).
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs are one line 090bc82142c09e0afd8c0ddaf594ab5843e1e77d. Family freeze is the orchestrator's bump; this unit does not touch it.
      artifacts: [docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: PERF-CATALOG-CACHE-WEIGHT-1 FIXED 2026-09-11 at pin 090bc821 with the two measured budgets and two pins. BOUND-1 stays BACKLOG for the metadata LRU.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses. Red-first of the 280000 retain pin is the designed cache miss on r0. 1,071,000 flake recorded; pin uses 1,250,000.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
  complete: true
```

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py -q` | 0 — 33 passed, 3 skipped in 147.71s |
| `make py-test-facade` | 0 — 5929 passed, 369 skipped, 48 warnings in 756.07s |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 740 passed, 1 skipped, 11 xfailed in 117.53s |
| `python3 scripts/check_docs_links.py` | 0 — 762 files, 4859 links |
| `make check-ledgers` | 0 — 316 ledgers, 854 links |
| `make verify` | 0 |
