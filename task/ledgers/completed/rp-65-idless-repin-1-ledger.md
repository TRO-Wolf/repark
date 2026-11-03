# Unit ledger — RP-65-IDLESS-REPIN-1 · the fork repins to 66901f02 (F-IDLESS-TOPLEVEL-POSITIONAL-1, fork #381), and the id-less top-level no-mapping pins run through both doors and the facade

**Date:** 2026-11-03 · **Branch:** `chore/rp-65-idless-repin` (PR #1246) · **Base:** `24d76986` (the merge base with origin/main) · **Head:** `bbd87737` · **Squash:** `63bbb1b6` on main
**Model:** builder `claude-opus-5-5` · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (round 1) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-03 at `bbd87737` (squashed to main as `63bbb1b6`) after both verifiers passed round 1. Clauses C-009 to C-011 are OPEN (leads L1 and L2, and the residue of the fork card); C-012 is REJECTED. The records pass that writes this file closes the RP-65 card and the fork card.

**Why.** At `56ff5545` (fork PR #380), an id-less Parquet file admitted by `add_files` with no name mapping, the state that `ALTER TABLE … UNSET TBLPROPERTIES ('schema.name-mapping.default')` reaches, read a top-level string/binary pair as silent NULL on both doors, and every filter on the pair answered `[]`. The repin moves the five fork `rev` lines to `66901f02` (fork PR #381, the squash of F-IDLESS-TOPLEVEL-POSITIONAL-1) and pins the measured no-mapping answers through both doors and the facade.

**Clause.** The RP-65 card's measured table, carried into RePark: the string read as binary, the binary read as string, the overflowing narrowed decimal, the mapping-route pair, the reordered-file controls and the facade pins. C-001 binds the nine Rust pins in `crates/repark-sql/tests/idless_toplevel_door.rs` (through the Spark door and an adopted native session); C-002 binds the two facade pins in `python/repark/tests/iceberg/test_ice_idless_toplevel_1.py`. The pins are bound in [rp-65-idless-repin-1-pins.md](rp-65-idless-repin-1-pins.md).

## PROPOSITION LEDGER — RP-65-IDLESS-REPIN-1 — 2026-11-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The nine Rust pins in `crates/repark-sql/tests/idless_toplevel_door.rs` answer the measured cells through the Spark door and an adopted native session at `66901f02`, and eight of them fail at `56ff5545`, where the declared mapping control stays green. | `cargo test --locked -p repark-sql --test idless_toplevel_door` green at head and red at the old pin, with the control green at both. | PROVEN | Opus claim 1 and Muse claim 1: 9 passed at head; at `56ff5545` 8 failed and the control passed (silent NULL cells, and Ok where a refusal is due). The mapping-route pair pin is red at the old pin, NULL on every row. |
| C-002 | The two facade pins in `python/repark/tests/iceberg/test_ice_idless_toplevel_1.py` answer the same pair through the facade with no JVM at `66901f02`, and fail at `56ff5545`. | The facade idless suites green at head and red at the old pin. | PROVEN | Opus claim 1 and Muse claim 1: the three idless files pass 21 at head; at the old pin 2 fail and 19 (the RP-64 files) pass. The module was rebuilt with maturin develop from python/repark. |
| C-003 | The repin moves only the fork lines: the five `rev` lines in `Cargo.toml` move to `66901f02a2df26098cfe073605152aeb518808f5`, and `Cargo.lock` changes exactly six fork `source` lines (iceberg and the five fork packages) and no other line. | The lock diff against origin/main shows six fork pairs only; `cargo metadata --locked` exits 0. | PROVEN | Opus claim 2 and Muse claim 2: six `source` pairs for iceberg, catalog-glue, catalog-s3tables, datafusion, sketches and storage-opendal; `cargo metadata --locked` exit 0; no duplicate-edge churn. |
| C-004 | The suites and gates are green at head: `cargo test --locked -p repark-sql -p repark-iceberg -p repark-spark` passes, the facade idless suites pass, and the make gates the verifiers named exit 0. | The verifiers' command logs (untracked). | PROVEN | Opus claim 4 and Muse claim 4: 4856 passed, 0 failed, 11 ignored; fmt, clippy, panic-ban, file size, lib-rs, audit, deny, map size and map sync exit 0; `make check-ledger-grammar` exit 0 before this ledger existed. |
| C-005 | Hygiene: no code comment is added in `.rs`, `.py` or `.toml` files (attributes only); each commit carries one `Authored-By` trailer and no co-author or session trailer line; no scratch or home path, session link, account id, ARN, bucket or region is added; pin-stating lines say `66901f02`, and `56ff5545` appears only in removals and history rows. | Diff inspection by the verifiers. | PROVEN | Opus claim 4 hygiene and Muse hygiene: comments added 0; the five commits carry one trailer each under the TRO-Wolf identity; the one 12-digit match is a decimal's unscaled value. |
| C-006 | Each pinned answer and refusal traces to a measured Spark cell or to a stated rule: the fork ledger cells at `66901f02` (S-23, S-27, S-33 to S-36, S-43 and S-45), the Java name-mapping rule for the mapping control, and the fork card's first Why bullet for the positional refusal. | Each pinned cell is matched to its cell; no pin asserts an answer that conflicts with a measured cell. | PROVEN | Opus claim 3 table and Muse claim 3. The fork ledger was read as a copy at `66901f02`, since no live JVM runs on the box in this lane. The one neighbour that conflicts, S-40, sits outside the pins (C-011). |
| C-007 | Scope facts of the pinned surface: `IN` on the string/binary pair refuses naming `=`, because the pushdown is an OR of equalities (Spark also refuses `IN`, so the refusal is Spark-equal and only the operator name differs); `STARTS WITH` on a string column read as binary never reaches the fork predicate from either door, so RePark answers it above the scan with Spark's `[1]`; `!=` excludes the NULL row, because DataFusion re-applies the filter above the scan. | Plan and answer probes on both doors (builder). The verifier attested the `IN` naming and did not re-measure the `STARTS WITH` plan, which it recorded from the plan text. | PROVEN | Builder handback notes items 5, 6a and 6b; Opus claim 3 and AT-8 for `IN`. The probe output is untracked. |
| C-008 | The records of the unit are in place: the RP-65 card closed with its ledger link, the fork card F-IDLESS-TOPLEVEL-POSITIONAL-1 closed by fork #381 with its residue carded to C-010, the mid-term and completed map rows, the fork-sync row for `66901f02`, and the parity row (merged, no longer pending). | The records gates named in the records pass exit 0 on the records commit. | PROVEN | Records pass 2 gate run (exit codes recorded in its hand-back, not tracked); the fork-sync row and the parity row were written by pass 1 and the parity row is corrected in pass 2. |
| C-009 | Lead L1: RePark's `add_files` refuses an id-less DECIMAL(12,2) file holding the unscaled value 123456789012 under a DECIMAL(10,2) table when column-bounds metrics are on. The pins set `write.metadata.metrics.default` to `counts` to admit the file. The fork ledger's S-27 record says Spark's add_files admitted the shape. | Spark's add_files on the same shape under column-bounds metrics, measured on Spark 4.1.2. | OPEN | Builder finding c; not measured on Spark in this unit (no JVM run in the lane). Carded as ADD-FILES-IDLESS-DECIMAL-BOUNDS-1 (task/roadmap/mid-term/add-files-idless-decimal-bounds-1-card-2026-11-03.md). |
| C-010 | The re-added top-level name and the top-level partial-mapping shape (the fork card's Retires clause) read as Spark does at `66901f02`. The Muse differential found both Spark-equal over its old-against-new cells, with copied fork answers, but no RePark pin holds either shape. | A RePark pin for each shape, through both doors, red at the old pin, or a decline. | OPEN | Muse claim (AT-6): the re-added and partial cells are Spark-equal at head. No RePark pin in idless_toplevel_door.rs or test_ice_idless_toplevel_1.py covers either shape. Carded here until pinned or declined. |
| C-011 | Lead L2 (fork-side, outside the pin span): on the mapping route a binary column read as string answers `=` and `>` where Spark raises (the `= 'abc'` filter answers k = 1 and the `> 'b'` filter answers k = 2), and a string column read as binary answers `= X'616263'` with k = 1 where Spark raises. The repin moved these neighbour cells from no rows to those answers. | The fork's S-40 fixed upstream, after which the RePark neighbour cells must refuse as Spark does; no RePark card until then. | OPEN | Muse R1F1 (S2, neighbour): the fork ledger's S-40 row is OPEN, copied; the repro log is untracked. Recorded on the RP-65 card as lead L2. |
| C-012 | The nested by-name reads on the no-mapping route are in this unit's fix. | The fork card's Fix paragraph and the RP-65 card's out-of-scope note. | REJECTED | Out of scope: every nested by-name read on the no-mapping route is the kept divergence in RePark's favour, because Spark reads no nested child of an id-less file there (the fork card's round-4 scope). |

## Evidence

**Verifier reports.** Opus 5.5, round 1: PASS, with findings F1 to F4 (records) and observation O1. Muse, round 1: CONCLUDED, PASS, with finding R1F1 (S2 neighbour). Both reports are untracked.

**Gates, as the verifiers ran them at `bbd87737`.** `cargo metadata --locked` exit 0; `cargo test --locked -p repark-sql --test idless_toplevel_door` 9 passed at head, and at `56ff5545` 8 failed with the control passing (the red run exits 101); `cargo test --locked -p repark-sql -p repark-iceberg -p repark-spark` 4856 passed, 0 failed, 11 ignored; the facade idless suites 21 passed at head and 2 failed at `56ff5545`; `make rust-fmt-check rust-clippy rust-panic-ban check-rust-file-size check-lib-rs rust-audit rust-deny check-map-size check-map-sync` exit 0; `make check-ledger-grammar` exit 0 before this ledger existed.

## Review findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S3
  category: AT-1
  clause: C-006
  claim: The card row for the same file under a name mapping gave NULLs at 56ff5545; measured, it reads 1 abc and 2 NULL there, and the mapping-route pair pin had no row on the card.
  evidence: Opus verifier F1 at bbd87737, reproduced from the builder's old-pin probe, the reordered-file-under-mapping line
  disposition: REMEDIATED (records pass 2: the card row corrected and the mapping-route pair row added)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-1
  clause: C-008
  claim: The fork card F-IDLESS-TOPLEVEL-POSITIONAL-1 still read open with dispatch awaiting a lane slot, and its mid-term row said the same, after fork 381 merged.
  evidence: Opus verifier F2, reproduced by grep of the fork card and the mid-term map for the dispatch phrase
  disposition: REMEDIATED (records pass 2: the fork card closed as carried into RePark, its residue carded to C-010, and the mid-term row reworded)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-1
  clause: C-006
  claim: The card did not mark the binary LIKE prefix pin, NOT LIKE, and the mapping-route IS NOT NULL and LIKE cells as following the measured rule; their LIKE spelling is unmeasured on Spark.
  evidence: Opus verifier F3; the fork ledger's S-45 measures startswith(v, X'61') at 66901f02
  disposition: REMEDIATED (records pass 2: the card's Spark parity paragraph marks those spellings; the pins still hold the measured answers)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-1
  clause: C-006
  claim: The card did not say that the string/binary, decimal and raw-bytes Spark answers come from the fork ledger at 66901f02 (S-23, S-27, S-33 to S-36, S-45), not from the RePark fork card.
  evidence: Opus verifier F4; grep of the RePark fork card for the word binary returns no match
  disposition: REMEDIATED (records pass 2: the card's Spark parity paragraph names the fork ledger and its cells)
```

```yaml
FINDING:
  id: MUSE-R1F1
  severity: S2
  category: AT-6
  clause: C-011
  claim: On the mapping route a binary column read as string answers = and > where Spark raises, and a string column read as binary answers = to X'616263' where Spark raises; the repin moved these neighbour cells from no rows to answers. The cells sit outside the pin span.
  evidence: Muse verifier round 1, neighbour differential of 95 statements; the repro is in the verifier's untracked log
  disposition: OPEN (fork-side S-40, recorded upstream; lead L2 on the RP-65 card; no RePark card until the fork fixes S-40)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-65-idless-repin-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers walked the four claims and all eleven pins against the card, the builder notes and the fork cells; the card rows were corrected by the records pass (OPUS-F1 to OPUS-F4).
      artifacts: [crates/repark-sql/tests/idless_toplevel_door.rs, python/repark/tests/iceberg/test_ice_idless_toplevel_1.py, Cargo.toml, Cargo.lock]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-3
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-6
      status: ATTACKED
      evidence: The lock and manifest scope was walked; the 95-statement old-against-new differential found the silent NULL state turned into loud or correct answers, and one S2 neighbour (MUSE-R1F1) was filed.
      artifacts: [Cargo.toml, Cargo.lock, crates/repark-sql/tests/idless_toplevel_door.rs]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-8
      status: ATTACKED
      evidence: The fork's DataInvalid texts were checked needle by needle in both doors, the IN refusal names = as the pushdown does, and cargo metadata with --locked, deny and audit are clean.
      artifacts: [Cargo.lock, crates/repark-sql/tests/idless_toplevel_door.rs, python/repark/tests/iceberg/test_ice_idless_toplevel_1.py]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..bbd87737
    - id: AT-10
      status: ATTACKED
      evidence: Eight of nine Rust pins and both facade pins are red at the old pin and green at head; the one green pin at the old pin is the declared mapping control; the suites pass 4856.
      artifacts: [crates/repark-sql/tests/idless_toplevel_door.rs, python/repark/tests/iceberg/test_ice_idless_toplevel_1.py]
```

## Scope notes

- Observed by the Opus verifier, not carded: `uvx maturin@1.14.1 develop` must run from `python/repark`, because the root `pyproject.toml` is a virtual workspace with no build-system; `make develop` does that.
- Observed by the Muse verifier, not carded, pre-existing and outside the RP-65 pins: a native-door `CREATE VIEW` over the memory catalog fails (a register_table data limit); `ORDER BY` null placement differs by door (Spark NULLS FIRST, the native door NULLS LAST, the existing convention); the facade `lit()` rejects bytes literals on the df-filter eq and ne cells at both pins.
- The `pins:` citations of the RP-65 clauses in the parity doc sit outside the gate's citation roots, so they are reader pointers, not gate bindings.
