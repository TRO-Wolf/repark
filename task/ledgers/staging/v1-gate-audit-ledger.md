# Charter ledger — V1-GATE · the v1.0 north-star gate statement

**Date:** 2026-09-03 · **Branch:** `docs/v1-gate-audit` · **Base:** `origin/main` `84c1801` ·
**Model:** claude-opus-5 (medium) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) ·
**Registry:** [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`S3T-V3-1` · **Path:** STANDARD (`risk_tier: standard`; docs only plus two meta-pins).
**Gate:** [../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3.1.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

## 1. Scope

| C | Clause | Verdict | Evidence |
|---|---|---|---|
| C-001 | Every §3 row is audited into one row of §3.1: glyph today, the claim, the residual → its registry row, that row's class and date, and the pin. A BACKLOG residual is listed as a blocker. | **PROVEN** | §3.1 carries twenty numbered rows and the fork table; `test_v1_gate_docs.py` holds the glyphs, the seven residual rows and every cited pin path |
| C-002 | The gate paragraph gains ONE dated line stating the audit result, and never claims the tag. | **PROVEN** | `**Audit result (V1-GATE, 2026-09-03).**` — one occurrence, pinned by `test_the_gate_carries_one_dated_audit_line_and_claims_no_tag` |
| C-003 | STATUS carries the units merged since DOCS-1 where they changed a claim, the SCALE-v3 line, and stays ≤ 25,000 B by compacting restatements. | **PROVEN** | 24,995 B → 24,473 B with SCALE-v3, V3-10, RDF-1, LOG1P-1 and the gate line added; `check-docs-compaction` clean |
| C-004 | Every 🟡 `GAP_MATRIX.md` row the north star leans on has a dated cell at the consumed pin rev; any that does not is a blocker. | **PROVEN** | R88, R91, R114, R126, R167 read at `ff4764d3`; all five dated; none is a blocker (§2 below) |
| C-005 | `test_v1_gate_docs.py` pins the audit table's claims — glyph per row, the gate line, the registry classes it cites — with one-line docstrings, and every mutation reds. | **PROVEN** | 10 tests; 10 of 10 mutations red (§3) |
| C-006 | The published gate board is filed verbatim at `docs/artifacts/v1-0-gate-closing-2026-09-02.html` with a `map.md` row in the shape of the existing rows. | **PROVEN** | `cmp` against the source is byte-equal; map row added above the v0.7 row |
| C-007 | `S3T-V3-1`'s "not re-dispatched since the tightening" note is replaced by the run that re-dispatched both legs under V3-11's exact inserted-row-id assertion, and the meta-pin follows. | **PROVEN** | run `33699342417` on `main` `a0fe83a`, 2026-09-03 00:48 UTC, `6 passed in 230.67s`; `test_live_v3_docs.py::test_the_tightened_assertion_is_confirmed_by_a_live_run` |

The brief numbers C-007 as C-006a; the clause grammar takes `C-NNN` only, so it is filed as C-007.

## 2. The audit, in one place

The full table is the north star's §3.1 — this ledger does not restate it. Its readings:

| Reading | Result |
|---|---|
| §3 rows audited | 20 |
| Rows ✅ with no residual | 13 |
| Rows ✅ whose residual is a dated DECLARED registry row | 7 (`V3-ROWID-2`, `V3-GEO-1` + `V3-VARIANT-SHRED-1`, `ENC-1`, `V3-UPGRADE-V4-1`, `V3-FILEORDER-1` + `V3-UPGRADE-DV-PLAIN-1` + `V3-UPGRADE-DV-PART-1`, `B-MOR-3`, `S3T-1`) |
| Rows blocked by a BACKLOG residual | 0 |
| Glyphs corrected to the gate's own wording | 3 — types ⚠→✅, encryption ❌→✅, DV maintenance ⚠→✅ |
| Row whose v1.0 requirement was still open on the matrix | 15 `rewrite_manifests` "exercised on v3" — discharged by SCALE-v3 (MoR 59 → 1, COW 10 → 1 at 1e7 x 50); the Spark-compared semantics stay MANIFEST-1/2/3, measured on v2 |
| Fork 🟡 rows leaned on, dated at `ff4764d3` | R88 (2026-08-24/25), R91 (2026-09-01), R114 (2026-09-02 F-18 + PR-7 re-audit), R126 (c) (2026-08-27 F-9), R167 (2026-08-28) |
| Fork ❌ rows | R89 and R130 — the mirrors of `V3-GEO-1` and `ENC-1`, both owner-dated |

One classification is worth naming rather than smoothing: **`B-MOR-3` is housed under the
registry's §7 heading, which is titled BACKLOG, while its own Rationale reads "DELIBERATE,
stricter than Spark on purpose (owner decision OD-2)"** — a permanent difference under a dated
owner ruling, so DECLARED in class. §3.1 row 13 says exactly that rather than picking one word.
Its section placement is a registry-housekeeping question, not a gate question.

## 3. Mutation

`test_v1_gate_docs.py` (10 tests) and the C-007 half of `test_live_v3_docs.py` (12 tests). Each
mutation applied alone and restored; **10 red out of 10**.

| Mutation | Red |
|---|---|
| M1 soften §3.1 row 13's glyph to ⚠ | 1 of 10 |
| M2 drop the dated gate line's marker | 1 of 10 |
| M3 drop the `SCALE-v3` bold from STATUS | 1 of 10 |
| M4 delete fork row R114 from the fork table | 1 of 10 |
| M5 restore ❌ on the encryption matrix row | 1 of 10 |
| M6 replace row 5's class word with "ruled" | 1 of 10 |
| M7 say `rewrite_manifests` is exercised on v2 only | 1 of 10 |
| M8 unfile the status board | 1 of 10 |
| M9 restore the "not re-dispatched" note on `S3T-V3-1` | 1 of 12 |
| M10 drop the confirmation run id | 1 of 12 |

## 4. Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark-parity/tests -q -k "plan_1 or reg_1 or v3r_1 or live_v3 or v1_gate or scale"` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `uv run --no-sync ruff check python` · `ruff format --check python` | 0 |

```
COVERAGE_ATTESTATION:
  pr_unit: v1-gate-audit
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every §3 row read against its registry row and its pin; the three softened glyphs and the open rewrite_manifests requirement were found by reading, not assumed.
      artifacts: [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-2
      status: ATTACKED
      evidence: All twenty rows enumerated; residual rows checked for class and date; the fork side read at the consumed pin rev rather than from the handoff.
      artifacts: [python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-3
      status: ATTACKED
      evidence: The gate line states the audited result and refuses to claim the tag; the pin reds if it ever does.
      artifacts: [python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-4
      status: N/A
      justification: Documents and two read-only meta-pins; no concurrency surface.
    - id: AT-5
      status: ATTACKED
      evidence: No .github, IAM, secret or dependency change. The filed status board was scanned for host paths, session links and vendor names before it was copied.
      artifacts: [docs/artifacts/v1-0-gate-closing-2026-09-02.html]
    - id: AT-6
      status: ATTACKED
      evidence: A BACKLOG residual would be listed as a blocker rather than softened; the pin asserts no audit row carries the word.
      artifacts: [python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-7
      status: N/A
      justification: No runtime code changed.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml or lockfile change; the fork pin was read, not moved.
      artifacts: [Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: S3T-V3-1 re-dated in place with the run that confirms it; no new registry row; STATUS keeps state and pointers, the registry keeps semantics.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md]
    - id: AT-10
      status: ATTACKED
      evidence: STATUS compacted 24,995 B to 24,473 B by replacing restatements with pointers; every touched map.md moved in the same commit; the ledger files last.
      artifacts: [STATUS.md, docs/artifacts/map.md, python/repark-parity/tests/map.md]
```

## 5. Not in scope

EX-3 (#307) changed no STATUS claim — the example inventory's home is
[../../../briefs/example-backfill.md](../../../briefs/example-backfill.md) and the EX ledgers —
so no STATUS line was written for it. `B-MOR-3`'s section placement in the registry (§2 above)
is left for a registry-housekeeping unit; it does not move the gate.
