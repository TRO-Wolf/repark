# Promotion ledger — H-1 phase mid-campaign archival (G-9)

**Date:** 2026-08-11 · **Unit:** G-9 · **Charter:**
`planning/grok/BRIEF-g9-h1-ledger-promotion.md` (owner YES 2026-08-11; Q&A addendum A1–A11)
· **Close gate:** repark #35–#46 (dbt-repark #1–#2 closed the parallel product lane; not archived
here).

## What this archive is

**`docs/history/hardening-h1/`** holds **everything delivered through the H-1 close gate
(repark #35–#46), including the parallel G/N corpus units whose gap-map homes are H-2.** This is a
**mid-campaign** phase promotion — the V2 Engine Hardening campaign continues into H-2. It is
**not** a campaign close-out and must not be read as one.

Basenames kept. Destination pattern matches Front-Door FD-4 / port-v2. Every moved ledger carries
a dated **ARCHIVED 2026-08-11 (G-9)** banner; relative links were repaired; nothing else in those
files was rewritten.

## Reconciliation identity

> Every still-live rule R carried by an archived source either (a) already lives in an
> authoritative current document, (b) was promoted into one by this unit, (c) has been superseded,
> or (d) is historical and binds nothing going forward.
>
> **No active rule is reachable only through an archived file.**

## Dispositions

| Disposition | Meaning |
|---|---|
| **HOMED** | Already stated in a current authoritative document; the archived copy is provenance. |
| **QUEUED** | Still-live follow-up, parked on the orchestrator unit-queue (outside this git root). |
| **RESIDUAL** | Documented residual intentionally left open (named at the live code/map site). |
| **HISTORICAL** | Gate transcripts, counts, discharged findings — binds nothing going forward. |
| **PROMOTED** | Was homeless; this unit copied it into a current document before the move. |

**PROMOTED count this unit:** **0** — every still-live carry already had a home (checked below).
No `task/lessons.md` append was required.

---

## Per-ledger rows (toothed)

### [h1d-ledger.md](h1d-ledger.md) — H-1d divergence registry

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Divergence registry document + citation-resolves / pin rules | HOMED | [docs/spark-sql-iceberg-parity.md](../../spark-sql-iceberg-parity.md) (live; **not** re-pointed by this unit — A6 orchestrator-side on audit) |
| D-6: live-tier `DISCLOSURES` is a machine-checked mirror of live-mirrored registry rows | HOMED | Registry § + facade mirror test (`test_disclosures_mirror_the_registry`) |
| Sweep queue seeded here | HOMED / closed | Closed by G-5; seed text historical; see [g5-sweep-ledger.md](g5-sweep-ledger.md) |
| Gate evidence, provocation transcripts, fix-pass tables | HISTORICAL | — |

**Checked basis:** read ledger header + D-1…D-8 + "Deviations / open items"; grepped for deferred /
follow-up / TODO; no homeless standing rule found.

### [h1a-ledger.md](h1a-ledger.md) — H-1a both splits + § Split B

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Session-timezone conf surface + extraction-in-session-zone behavior | HOMED | Code + [docs/design/session-extension-conf-seam.md](../../design/session-extension-conf-seam.md) + STATUS "Known correctness issues" |
| TZ-4 (grown) — TIMESTAMP representation unit design inputs (§ Split B D-B4/D-B5) | QUEUED | Orchestrator unit-queue **TZ-4 (grown)**; citable design record remains this ledger § Split B at archived path |
| TZ-5 — `CAST(TIMESTAMP AS BIGINT)` ns-vs-s | QUEUED + HOMED | unit-queue **TZ-5** + STATUS + registry TZ-5 |
| TZ-7 / TZ-8 remainders after partial TZ-1 fix | HOMED | Registry + STATUS partial-fix narrative |
| B-TZ-1…B-TZ-5 backlog class notes | HOMED / QUEUED | Registry / STATUS as rowed; not re-scoped here |
| Write-path partition-key check risk note | QUEUED | unit-queue **Write-path partition-key check** |
| Live-scenario conversion (G1 class) note | QUEUED | unit-queue **Live-scenario conversion** |
| Gate evidence / matrix / flip inventory | HISTORICAL | — |

**Checked basis:** read § Split B decisions D-B1…D-B6, residual table, ready-to-paste rows; grepped
TZ-4/TZ-5/follow-up/deferred.

### [h1c-ledger.md](h1c-ledger.md) — H-1c `$`-metadata rider

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Filter-at-catalog-layer decision | HOMED | [docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| F-2 fork sharp edge (base names containing `$`) — document-and-pin, not engineer-around | RESIDUAL | ADR-0006 "Residue" + `crates/repark-iceberg` / catalog maps + pin `the_filter_keeps_names_the_fork_did_not_synthesize`; fork-side row is fork-intake (orchestrator) |
| Gate / fix-pass transcripts | HISTORICAL | — |

**Checked basis:** read decision block + F-1…F-4; confirmed ADR is authoritative home.

### [h1b-ledger.md](h1b-ledger.md) — H-1b time-travel ephemeral pins

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Statement-boundary pin release on every `?`/return path (both doors, one counter) | HOMED | Code + `crates/repark-spark` / `repark-core` / `repark-sql` maps; `task/lessons.md` DO release-ephemeral rule |
| Reader-options `read_table_at` registration kept for the user DataFrame lifetime | RESIDUAL | Documented residual at `crates/repark-core` / spark maps (not a defect; not queued to "fix") |
| Gate / two-mutation pins / fix-pass F-1…F-20 table | HISTORICAL | — |

**Checked basis:** read §5 residual + §11 fix pass; STATUS closed-out pointer re-pointed this unit.

### [g4-tests-split-ledger.md](g4-tests-split-ledger.md) + [g4-artifacts/](g4-artifacts/map.md)

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Identity-gate procedure (leaf multiset + name map) | HOMED (evidence archived) | Procedure described in [g4-artifacts/map.md](g4-artifacts/map.md) + live [crates/repark-spark/src/tests/map.md](../../../crates/repark-spark/src/tests/map.md) (paths re-pointed this unit) |
| Production-module alignment cut | HOMED | Live `crates/repark-spark/src/tests/` layout + maps |
| Open follow-ups | none — all content historical | Grep of ledger for deferred/TODO/follow-up: only ACC/drift-application history |

**Pre-existing gap (noted, fixed this unit):** `task/g4-artifacts/` had **no** `map.md` while
AGENTS requires one in every directory. G-9 added [g4-artifacts/map.md](g4-artifacts/map.md) at
archival rather than re-homing the silent violation.

### [g5-sweep-ledger.md](g5-sweep-ledger.md) — G-5 registry sweep

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Rows NS-1/NS-2/ST-1/ID-3/TY-4/TY-5/FA-2/FA-3 and dispositions | HOMED | [docs/spark-sql-iceberg-parity.md](../../spark-sql-iceberg-parity.md) |
| TY-4 / TY-5 → H-2 gap G10 conversion candidates (no scenarios built) | QUEUED (phase note) | Design/slate H-2 gap G10; ledger § "G9 / H-2 conversion candidates" remains provenance |
| Open-item rulings (a)(b) in-unit | HOMED / HISTORICAL | As rowed in registry or discharged in-unit |
| Gate evidence | HISTORICAL | — |

**Checked basis:** read triage table + open-item rulings + G9/H-2 candidates section.

### [g6-chores-ledger.md](g6-chores-ledger.md) — G-6 chores

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| Parity markdown default → `target/census-reports/` | HOMED | Makefile / parity runner (shipped) |
| Glue location-mismatch fail-loud guard | HOMED | Acceptance harness code |
| Dual-wire `parity-live` checker | HOMED | `scripts/check_parity_live_dual_wire.py` + make/ci |
| getDatabase facade API (activates location guard live leg) | QUEUED | unit-queue **getDatabase facade API** |
| Engine namespace-create location guard (altitude-correct) | QUEUED | unit-queue **Engine namespace-create location guard** |
| Residual ACCEPTED_FLAGGED dual-wire shell-lexer limits | RESIDUAL | Named in ledger; not a homeless rule |
| Gate / overload transcripts | HISTORICAL | — |

**Checked basis:** read Item 3 follow-ons + Item 4 residuals + convergence.

### [g7-decimal-ledger.md](g7-decimal-ledger.md) — G-7 decimal corpus

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| DEC-1…DEC-9 semantics + pins (paste-true §6) | HOMED | Registry landed by #45; corpus tests live under `python/repark/tests/` |
| G-7b — Rust bit-exact pins + 2 cross-door rows | QUEUED | unit-queue **G-7b** (declared split; §9 reserved) |
| CONVERGED-flip-don't-delete disclosure policy | HOMED | Corpus + STATUS decimal note |
| Gate / fix-round §11 evidence | HISTORICAL | — |

**Checked basis:** read D-G7-4, §5 residual, §6, §9 G-7b reserved.

### [n2-merge-ledger.md](n2-merge-ledger.md) — N-2 / H-2 gap G3 MERGE corpus

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| MERGE differential corpus (10 recorded rows) + lifecycle helper | HOMED | `python/repark/tests/test_merge_differential_parity.py` + tests map |
| Ready-to-paste REG-G3 rows | HOMED / pending land | Corpus + registry as orchestrator lands; ledger remains paste provenance |
| N-2b — 4 Rust pins + 2 live-tier scenarios + `_live_parity` lifecycle abstraction | QUEUED | unit-queue **N-2b** (§9) |
| Gate / fix-round evidence | HISTORICAL | — |

**Checked basis:** read D-N2-5, §3 deviations, §9 N-2b table. **Note:** this ledger does **not**
document the facade "N2 plan-collapse" (`withColumn` chain merge) — see ghost row below.

### [g8-file-size-ledger.md](g8-file-size-ledger.md) — G-8 file-size gate

| Still-live carry | Disposition | Authoritative home today |
|---|---|---|
| `scripts/check_rust_file_size.py` default + EXCEPTIONS + fail-closed stale key | HOMED | Script + `make ci` / ci.yml guards |
| check_lib_rs stale-exception-row backport | QUEUED | unit-queue **check_lib_rs stale-exception backport** |
| Pre-commit path HOLD (mold parity with check_lib_rs) | RESIDUAL | Ledger ACC C2-4 HOLD — not filed as a unit; convention residual |
| Gate / provocation transcripts | HISTORICAL | — |

**Checked basis:** read §4.5 stale keys, deliverables, ACC HOLD table.

---

## Move inventory

| From | To |
|---|---|
| `task/h1d-ledger.md` | [h1d-ledger.md](h1d-ledger.md) |
| `task/h1a-ledger.md` | [h1a-ledger.md](h1a-ledger.md) |
| `task/h1c-ledger.md` | [h1c-ledger.md](h1c-ledger.md) |
| `task/h1b-ledger.md` | [h1b-ledger.md](h1b-ledger.md) |
| `task/g4-tests-split-ledger.md` | [g4-tests-split-ledger.md](g4-tests-split-ledger.md) |
| `task/g4-artifacts/` | [g4-artifacts/](g4-artifacts/map.md) |
| `task/g5-sweep-ledger.md` | [g5-sweep-ledger.md](g5-sweep-ledger.md) |
| `task/g6-chores-ledger.md` | [g6-chores-ledger.md](g6-chores-ledger.md) |
| `task/g7-decimal-ledger.md` | [g7-decimal-ledger.md](g7-decimal-ledger.md) |
| `task/n2-merge-ledger.md` | [n2-merge-ledger.md](n2-merge-ledger.md) |
| `task/g8-file-size-ledger.md` | [g8-file-size-ledger.md](g8-file-size-ledger.md) |

**Not moved (still live under `task/`):** `lessons.md`, `metrics.md`, `todo.md`, `port/`, `census/`.

**Also landed this unit (new, not moved):** [README.md](README.md), [map.md](map.md),
[g4-artifacts/map.md](g4-artifacts/map.md), this file.

---

## Ghost citation fixed this unit

| Site | Was | Disposition |
|---|---|---|
| `python/repark/src/repark/map.md` (r23b N2 plan-collapse bullet) | `task/n2-plan-collapse-ledger.md` | **Never existed** on any ref. The test surface `test_n2_plan_collapse.py` is real. [n2-merge-ledger.md](n2-merge-ledger.md) documents MERGE INTO differential corpus, **not** plan-collapse. **Fix:** reworded the map bullet to describe the pin without a ledger cite. |

---

## Citation policy applied (A6)

| Class | Action |
|---|---|
| `STATUS.md` | Re-pointed task/ ledger links + dated H-1 archive note |
| Non-history `map.md` files citing moved paths | Re-pointed to `docs/history/hardening-h1/…` |
| `task/map.md` "I want to…" + content rows | Collapsed into redirect section; template → archived h1d |
| Rustdoc / ADR bodies / design bodies | Left on **redirect-note** precedent (task/ basename → this dir) |
| `docs/spark-sql-iceberg-parity.md` | **LANE DID NOT TOUCH** (standing ban). Carries six+ `task/` ledger links; orchestrator lands repairs on this branch during audit review before SQM. |
| History files citing `task/h1b` etc. | Left as-is (history citing pre-move paths; redirect note covers) |

---

## Verified live-elsewhere (summary)

No rule was found live **and** homeless. Follow-ups already on the orchestrator unit-queue:
G-7b, N-2b, TZ-4 (grown), TZ-5, write-path partition-key check, live-scenario conversion,
getDatabase, engine namespace-create location guard, check_lib_rs stale-exception backport.
Documented residuals (reader-options keep-registration; F-2 fork `$`-base; dual-wire shell-lexer
limits; G-8 pre-commit HOLD) are named at their live code/map sites.
