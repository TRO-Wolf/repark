# G-4 ledger — split `crates/repark-spark/src/tests.rs` (declared-rename unit)

> **ARCHIVED 2026-08-11** (G-9 — H-1 phase ledger promotion) — a historical record of everything
> delivered through the H-1 close gate (repark #35–#46), including the parallel G/N corpus units
> whose gap-map homes are H-2, kept for provenance and **not a source of live rules**: every rule
> still in force was verified live-elsewhere or promoted first
> ([promotion-ledger.md](promotion-ledger.md)). Relative links were repaired for this location on
> the same date; nothing else changed. Current state: [STATUS.md](../../../STATUS.md).

**Date:** 2026-08-10 · **Branch:** `grok/g4-tests-split` · **Base:** `main` @ G-5 squash
`669f9a3` · **Path:** STANDARD · **critic_engine:** ACC then `/critic-overload` (owner request) ·
**Charter:** `planning/grok/BRIEF-g4-tests-rs-split.md` + approved cut map
`planning/grok/G4-CUT-MAP.md`.

## Scope

Declared-rename only: split the ~14.5-KLOC lib-root battery into `src/tests/` by production-module
alignment. Zero behavior change. Citation chase for present-tense pins (registry filesystem form +
`matrix.rs` `--list` strings + maps). Pre-split ledgers cite pre-split paths **by design** and are
not rewritten.

## Mapping rule (frozen)

1. Production-module alignment by name / primary assertion.
2. Margin: two-module tests follow the primary; cross-cutting residue → small shared home
   (`dml` for DELETE/UPDATE + BUG-001 valve; `common` for multi-leaf helpers).
3. Nested mods lift to sibling files of the same name (path-preserving).
4. Helpers move byte-identically; only new lines are `mod` declarations and `use` adjustments.
5. Flat membership is by test identity (non-contiguous OK); nested mods are contiguous banner+body.

## Identity gate

| Metric | Value |
|---|---|
| BEFORE `--list` count | **352** (re-derived at rebase over merged #40+#43 — 349 at draft + 1 H-1c Spark-door pin + 2 split-B carrier pins) |
| AFTER `--list` count | **352** |
| Leaf multiset | **identical** (`task/g4-artifacts/leaf-diff.txt` empty) |
| Full-path renames | **202** (`task/g4-artifacts/name-map.md` — 201 at draft + the transplanted H-1c pin) |
| Path-preserving (nested lifts) | **55** (+ other non-`tests::` modules unchanged → 148 total unchanged paths) |

BEFORE/AFTER raw lists: `task/g4-artifacts/before-list.txt`, `after-list.txt`.

## Layout delivered

```
crates/repark-spark/src/tests/
  mod.rs, map.md, common.rs
  ctas, create_table, namespace_ddl, catalog_ops, describe_show, alter, dml,
  insert_overwrite, merge, call, ref_ddl, time_travel, metadata_tables,
  normalize, local_fs_ddl, router
  partitioned_ctas, partitioned_merge, transform_overwrite, service_managed_ctas
```

`src/tests.rs` removed (directory module replaces it). `lib.rs` still
`#[cfg(test)] mod tests;`.

## Citation chase

| Surface | Action |
|---|---|
| `docs/spark-sql-iceberg-parity.md` | filesystem pins → `tests/<file>.rs::leaf` |
| `crates/repark-spark/src/matrix.rs` | 30 pin strings updated path-only |
| `crates/repark-spark/src/map.md` + `tests/map.md` | split reflected; Debug populated |
| Merged ledgers (`h1d`, archive) | **not** rewritten — pre-split paths by design |

## Out of scope (held)

Assertion changes; test add/delete; file-size gate (H-0c §3); re-pointing matrix surfaces.

## Gate evidence

| Gate | Result |
|---|---|
| Identity: count | BEFORE 352 / AFTER 352 |
| Identity: leaf multiset | identical (`task/g4-artifacts/leaf-diff.txt` empty) |
| Name map | 202 rows (`task/g4-artifacts/name-map.md`) |
| `make ci` | green (exit 0) — log `task/g4-artifacts/make-ci.log` |
| `make test` | green (exit 0) — log `task/g4-artifacts/make-test.log` |
| `bash scripts/check_map_md.sh` | green |
| Stale-path grep (present-tense) | merged ledgers (`h1d`, archive) keep pre-split paths by design; live maps chased (incl. crate `map.md`, `router/map.md`, `lib.rs` comment — W1-Q-001/002 fixed) |

## ACC / Critic Overload

### ACC (SEPMO critic stage)

Context break executed; attacking artifacts, not memory.

| Critic | Focus | Verdict |
|---|---|---|
| Critic-1 quality | Diff is moves + scaffolding; no test-body edits; identity gate holds; maps/ledger present | CLEAN for ≥S1 |
| Critic-2 security | No engine behavior change; no AWS/credentials; pin path strings only | CLEAN for ≥S1 |

### Critic Overload

`OVERLOAD-CONVERGED`. Wave 1: Critic-1 filed W1-Q-001..003 (S2 map citations); Critic-2/3 CLEAN.
Wave 2: remediations to crate `map.md`, `router/map.md`, `lib.rs` comment, ledger gate row.
Waves 3–5: no residual ≥S1. Report: `/tmp/critic-overload-repark-g4-2026-08-10/OVERLOAD-REPORT.md`.

---

## Drift application at rebase (orchestrator, 2026-08-11)

Main moved under the draft (#40, #43, #38 merged). Per the cut map's drift rule, applied at the
conflict fix rather than a fresh regeneration by the executor:

- **#40's `tests.rs` delta** (one insertion: `metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door`)
  transplanted into `tests/metadata_tables.rs` at its original anchor (byte-identical body);
  membership for that file is now **3** tests; the name map gained its row (202 total).
- **`src/map.md`**: G-4's split bullet merged with main's three pin descriptions, re-pathed to
  the split layout. **`task/map.md`**: all ledger rows kept.
- **Identity gate re-run on the rebased tree**: BEFORE (settled main `c61aa19`) = 352, AFTER =
  352, leaf multiset identical (`diff` empty), 202 full-path renames. Artifacts refreshed in
  `task/g4-artifacts/` (before/after lists + leaves regenerated; draft versions superseded).
- Applied by the orchestrator as a mechanical conflict fix; the identity gate is the arbiter
  either way.
