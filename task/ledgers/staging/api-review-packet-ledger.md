# Charter ledger — API-REVIEW · the v1.0 API review packet

**Date:** 2026-09-02 · **Branch:** `docs/v1-0-api-review` · **Base:** `origin/main` `3eb6b71` ·
**Model:** claude-opus-5 (medium) · **Ruling under review:**
[../../../docs/design/python-facade.md](../../../docs/design/python-facade.md) §4 ·
**Gate:** [../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3 · **Path:** STANDARD (docs-only; one Actor cycle).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The API-forever clock started at the first tagged release (2026-08-15) and the
north-star gate says an API review rides it. The packet exists so the owner answers row by row
instead of ruling on 913 names at once.

**Not in this unit:** answering the rows (owner), writing the versioning policy
`docs/release.md` "Open items" still lacks, any code or test change, any registry edit.

## PROPOSITION LEDGER — API-REVIEW — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The packet's public-name inventory is the tree's, not a hand list: rows A1–J9 partition exactly the 913 names `scripts/check_example_coverage.py` enumerates, no name in two rows, and every per-row example count is that script's covered/total. | Run the gate; sum the row member counts; sum the row example counts. | **PROVEN** | `example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 67 covered; 844 backlog; 2 exceptions; 15 examples`. Rows A1–J9 sum to 913 members and 67 covered. The `F.*` split is AST-measured module ownership (`functions_expr` 234, `functions` 44, `functions_declared` 62, …, plus the 5 class/decorator names §2.2 of `spark-function-parity.md` names); the DataFrame split is by declared return type (70 return `DataFrame`, 62 do not), aliases resolved to their target. |
| C-002 | Every non-Python surface row is read off the tree too: the 50 dialect-neutral surfaces of `repark_common::surfaces::ALL` with each door's Tested / DeliberatelyAbsent verdict, the seven `CALL` procedures of `SUPPORTED_PROCEDURES`, the three `repark.sql.*` keys plus their snake aliases, the two `repark.merge.*` keys, both catalog-config prefixes, the eleven `repark.errors.__all__` classes, and the packaging facts from the two `pyproject.toml` files. | Read the constants; count the matrix rows per door. | **PROVEN** | 50 surface ids; Spark door 47 Tested + 3 DeliberatelyAbsent (`TABLE_OPTION_SORT_ORDER`, `TABLE_OPTION_UNKNOWN_KEY_REFUSE`, `WRONG_DOOR_SNIFF`, the set `spark_door_absences_are_the_declared_ones` pins); ANSI door 47 Tested + 3 absent (`ALTER_TABLE_PARTITION_FIELDS`, `INSERT_OVERWRITE`, `MAINTENANCE_CALL`). Seven procedures. Keys: `maxArrayElements` / `allowLocalFilesystemDDL` / `allowCreateFormatVersion3` (+ `_ALT` snake spellings), `repark.merge.scan-pruning`, `repark.merge.file-scoped-rewrite`. Packaging: `repark`, `requires-python >= 3.12`, `module-name = repark._native`, four extras. |
| C-003 | Every divergence-registry row still open on this base lands in exactly one packet row, and the packet's residual tags match the registry's own sections. | Parse the registry headings, drop rows whose own section says CLOSED / FIXED / retired, intersect with the packet rows. | **PROVEN** | 81 open rows (51 in §7 BACKLOG, 30 DECLARED in §2–§5); 81/81 mapped, none unassigned and none invented. Rows the parse proves CLOSED and therefore absent: `REF-1`, `REF-4`, `DML-1`, `DML-2`, `BL-3`, `BL-4`, `BL-5`, `TZ-8`, `G6-3`, `G6-5`, `LOG-1`, `MOR-1`, `RDF-1`, `V3-LINEAGE-1`, `V3-DANGLE-1`, `V3-ADOPT-1`, `S3T-V3-1`, `V3-COW-1`, `V3-MOR-1`, `V3-DV-1`, `V3-UPGRADE-1`, `TZ-6`, `TZ-7`. |
| C-004 | The recommendation is a stated rule applied uniformly, not a per-row opinion: R1–R5 in §3 decide all 35 rows, exactly one YES row carries an open BACKLOG residual and that row is named in the counts table. | Re-derive each row's verdict from its residual tags. | **PROVEN** | 15 YES, 15 YES-except, 5 NO. The one YES row with an open BACKLOG residual is J7 (`WIN-SLIDE`), listed in §4 and answered on D1. R5 is the declared deviation from the brief: tree-wide example coverage is 67/913 (7.3 %), so a ≥ 90 % examples threshold would refuse all 35 rows; examples are reported per row and pins plus residuals decide. |
| C-005 | §5 quotes what a `yes` would bind rather than inventing it, and says plainly where the tree is silent. | Grep the two sources for each quoted sentence. | **PROVEN** | Quoted verbatim from `docs/release.md` (the API-forever clock, and "Open items": "**Cadence** — release cadence and versioning policy remain **unwritten**"), `docs/design/python-facade.md` §4 Q1 (pre-1.0 breaking changes; the one-minor-series deprecation shim) and the north-star gate paragraph. The additive-only and major-version rules a `yes` would bind are recorded as **unwritten**, which is the packet's one dependency. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: api-review-packet
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every number in the packet is re-derived from the tree by script — the coverage gate's own output, the surface-id list, the two door matrices, the procedure constant, the settings constants and the registry parse — and the row sums are checked against the gate's totals rather than asserted.
      artifacts: [docs/design/v1-0-api-review-2026-09-02.md, docs/design/v1-0-api-review-2026-09-02.json]
    - id: AT-2
      status: ATTACKED
      evidence: The registry parse was run over every heading in sections 2 through 7 and the CLOSED / FIXED rows were excluded by their own text, so a row retired on 2026-09-02 cannot appear as a live residual; the 81 open rows are then covered 81/81.
      artifacts: [docs/design/v1-0-api-review-2026-09-02.md]
    - id: AT-3
      status: N/A
      justification: Docs-only unit; no error path, refusal or failure mode changes.
    - id: AT-4
      status: N/A
      justification: No concurrency surface is touched.
    - id: AT-5
      status: ATTACKED
      evidence: No credentials, no account values, no private paths and no .github change; the packet cites only tracked repo paths and public names.
      artifacts: [docs/design/v1-0-api-review-2026-09-02.md]
    - id: AT-6
      status: N/A
      justification: No API or contract changes in this unit — the packet recommends, the owner rules.
    - id: AT-7
      status: N/A
      justification: No runtime code; the packet is read once by a human.
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or toolchain change.
    - id: AT-9
      status: ATTACKED
      evidence: The packet states its own deviation from the brief (R5, the example threshold) instead of quietly applying a different rule, and records the unwritten versioning policy as a dependency rather than quoting a rule the tree does not contain.
      artifacts: [docs/design/v1-0-api-review-2026-09-02.md]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses, each checkable from the tree; docs/design/map.md in lockstep; the ledger moves to completed in the last commit.
      artifacts: [docs/design/map.md, task/ledgers/staging/api-review-packet-ledger.md]
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Deliverable: [../../../docs/design/v1-0-api-review-2026-09-02.md](../../../docs/design/v1-0-api-review-2026-09-02.md)
