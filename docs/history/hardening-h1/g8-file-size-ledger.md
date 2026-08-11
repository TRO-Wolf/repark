# G-8 — general Rust file-size gate (check_lib_rs companion)

> **ARCHIVED 2026-08-11** (G-9 — H-1 phase ledger promotion) — a historical record of everything
> delivered through the H-1 close gate (repark #35–#46), including the parallel G/N corpus units
> whose gap-map homes are H-2, kept for provenance and **not a source of live rules**: every rule
> still in force was verified live-elsewhere or promoted first
> ([promotion-ledger.md](promotion-ledger.md)). Relative links were repaired for this location on
> the same date; nothing else changed. Current state: [STATUS.md](../../../STATUS.md).

**Date:** 2026-08-11 · **Branch:** `grok/g8-file-size-gate` · **Worktree:** `/tmp/grok-g8` ·
**Base:** `origin/main` @ G-4 merge `69d1493` · **Charter:**
`planning/grok/BRIEF-g8-file-size-gate.md` (owner-approved; discharges H-0c open question #4) ·
**Path:** STANDARD · **critic_engine:** `acc` (Actor → Critic-1 quality + Critic-2 security).

Standing rules applied: measure-first; monolith never grandfathered (it no longer exists);
prose points at the script and never restates ceilings; dual-wire Makefile + ci.yml guards;
tests/ledgers/maps in the same change.

---

## Charge

`check_lib_rs` protects crate roots; nothing protected any other file — which is how
`tests.rs` reached ~14.5 KLOC. Build the companion: a per-file line-ceiling gate over
`crates/**/*.rs`, in the `check_lib_rs` mold, seeded from post-G-4 measured reality.

---

## Measurement (seed)

Command: `find crates -name '*.rs' -type f -print0 | xargs -0 wc -l` at unit start
(post-G-4 tree, branch tip `69d1493` + this unit's base).

| Metric | Value |
|---|---|
| Files scanned | **181** |
| Total lines | 91_564 |
| p50 / p75 / p90 / p95 / max | 269 / 630 / 1282 / 1769 / **3290** |
| Files > 800 / 1000 / 1500 / 2000 | 33 / 28 / **13** / 7 |

Largest files (measured lines):

| Lines | Path |
|---:|---|
| 3290 | `crates/repark-iceberg/src/write/merge/streaming_scan_tests.rs` |
| 2616 | `crates/repark-iceberg/src/write/merge/mod.rs` |
| 2508 | `crates/repark-ta/src/momentum.rs` |
| 2207 | `crates/repark-iceberg/src/write/append.rs` |
| 2136 | `crates/repark-python/src/column.rs` |
| 2098 | `crates/repark-ta/src/udf.rs` |
| 2020 | `crates/repark-functions/src/datetime.rs` |
| 1926 | `crates/repark-iceberg/src/catalog/tests.rs` |
| 1915 | `crates/repark-spark/src/alter.rs` |
| 1837 | `crates/repark-ta/src/overlap.rs` |
| 1769 | `crates/repark-iceberg/src/write/alter.rs` |
| 1579 | `crates/repark-core/src/session.rs` |
| 1556 | `crates/repark-sql/src/tests.rs` |

The former `crates/repark-spark/src/tests.rs` monolith is **gone** (G-4 split into
`src/tests/` leaves; largest leaf `alter.rs` = 1448, under the default). Not grandfathered.

---

## Scope decisions (recorded)

### 1. Default ceiling = **1500**

Chosen so the **overwhelming majority pass unlisted**: 168/181 files (≈92.8%) are ≤1500;
13 files need an `EXCEPTIONS` row. Alternatives considered:

| Candidate | Exception count | Notes |
|---:|---:|---|
| 1000 | 28 (15.5%) | Tighter; exception table noisy; many post-G-4 leaves would need rows |
| **1500** | **13 (7.2%)** | **Chosen** — vast majority green; outliers are the real review surface |
| 2000 | 7 (3.9%) | Too loose: `datetime.rs` / ALTER modules would slip under default |

The former 14.5-KLOC monolith would fail any of these. Ceilings live only in
`scripts/check_rust_file_size.py` — never restated in docs beyond this ledger's seed note.

### 2. `#[cfg(test)]`-only files — **same default** (no higher test ceiling)

The monolith that motivated this gate **was** a test battery. A higher test-only default
would re-open the exact failure mode G-4 closed. Integration tests under `crates/*/tests/`,
file-backed `src/**/tests.rs`, and `src/tests/*.rs` leaves all share the production default.
Large batteries get honest `EXCEPTIONS` rows with a RATCHET note (split later), not a
privileged class.

Detection of "test-only" is also heuristic and brittle (cfg-gated modules vs. `tests/`
integration crates vs. production modules that happen to end in `_tests.rs`); one ceiling
keeps the SSOT simple.

### 3. Generated / vendored exclude audit — **none found**

Audit at unit start:

- No `generated/`, `vendor/`, `third_party/`, or `gen/` directories under `crates/`.
- No `@generated` / `DO NOT EDIT` / bulk `include!` of generated sources in `crates/**/*.rs`.

Therefore: **no exclude list**. Every `crates/**/*.rs` file is in scope. If a generated
tree lands later, the exclude decision is a follow-up unit with a real driver — not seeded
empty here.

### 4. `mod.rs` lower ceiling — **no**

Measured `mod.rs` sizes:

| Lines | Path | Nature |
|---:|---|---|
| 26 | `repark-spark/src/tests/mod.rs` | True manifest (mod decls only) |
| 88 | `repark-iceberg/src/write/mod.rs` | Thin module map |
| 195 | `repark-iceberg/src/catalog/mod.rs` | Catalog wiring + re-exports |
| **2616** | `repark-iceberg/src/write/merge/mod.rs` | **Full MERGE executor body** living in `mod.rs` |

A special lower `mod.rs` ceiling would only usefully fire on the MERGE module — which already
exceeds the general default and already needs an `EXCEPTIONS` row. True manifests are well
under 1500. Adding a second ceiling class for a social convention ("mod.rs should be tiny")
buys nothing mechanical today; the MERGE file's row carries
`RATCHET: after COW/MOR/plan split out of mod.rs`.

### 5. Stale EXCEPTIONS keys — **fail-closed**

An `EXCEPTIONS` key whose path no longer exists on disk is an **ERROR** (not a silent skip).
Without this, a rename/delete would leave a dead grandfather that never re-validates. Checked
in `main()` before the scan (~sorted key walk). The `check_lib_rs` crate-name form of the same
check is a separate queued backport — **not** part of G-8.

---

## Seeded EXCEPTIONS (path → ceiling; measured; one-line reason)

Ceilings include small slack (~3–5%) so a one-line edit does not force table churn. Keys
sorted alphabetically in the script. **Ratchet DOWN only.**

| Path | Measured | Ceiling | Reason (summary) |
|---|---:|---:|---|
| `crates/repark-core/src/session.rs` | 1579 | 1650 | Session builder + everything-through-Session surface |
| `crates/repark-functions/src/datetime.rs` | 2020 | 2100 | Spark calendar/datetime family + TZ-aware extractors |
| `crates/repark-iceberg/src/catalog/tests.rs` | 1926 | 2000 | Catalog-adapter unit battery |
| `crates/repark-iceberg/src/write/alter.rs` | 1769 | 1850 | Iceberg ALTER TABLE adapter |
| `crates/repark-iceberg/src/write/append.rs` | 2207 | 2300 | Public append entry point |
| `crates/repark-iceberg/src/write/merge/mod.rs` | 2616 | 2700 | MERGE INTO executor (RePark-owned) |
| `crates/repark-iceberg/src/write/merge/streaming_scan_tests.rs` | 3290 | 3400 | MERGE streaming-scan unit battery |
| `crates/repark-python/src/column.rs` | 2136 | 2200 | PyO3 Column expression surface |
| `crates/repark-spark/src/alter.rs` | 1915 | 2000 | Spark-dialect ALTER TABLE planner |
| `crates/repark-sql/src/tests.rs` | 1556 | 1600 | Native ANSI-door unit battery (still monolithic) |
| `crates/repark-ta/src/momentum.rs` | 2508 | 2600 | TA-Lib momentum indicators (verbatim port) |
| `crates/repark-ta/src/overlap.rs` | 1837 | 1900 | TA-Lib overlap studies (verbatim port) |
| `crates/repark-ta/src/udf.rs` | 2098 | 2200 | DataFusion window-UDF wrappers for TA kernels |

Full reason strings (with RATCHET notes) live only in `scripts/check_rust_file_size.py`.

---

## Delivered surfaces

| Surface | Detail |
|---|---|
| SSOT | `scripts/check_rust_file_size.py` (default + EXCEPTIONS + fail-closed empty/unreadable/stale key) |
| Wrapper | `scripts/check_rust_file_size.sh` (thin; dual-wire shape) |
| `make check-rust-file-size` | target + `##` help row |
| `make ci` | includes `check-rust-file-size` after `check-lib-rs` |
| ci.yml guards job | step `rust file-size guard (check_rust_file_size)` — **job display name unchanged** (branch-protection lesson) |
| pre-commit | `.pre-commit-config.yaml` id `rust-file-size-guard` + `make install-hooks` |
| AGENTS.md | Mechanical structure gates roster line — points at script; no ceilings restated |
| maps | `scripts/map.md`, `task/map.md`, `.github/workflows/map.md`, `DEVELOPMENT.md` pointer |

Out of scope held: shrinking any file; Python (`check_lib_py`); `.github/` beyond the one
guards-job step; `Cargo.lock` / `uv.lock`.

---

## Provocation proofs (never committed red trees)

All three run in a dirty worktree that is restored before commit. Verbatim stderr captured
2026-08-11 on this unit's tree.

### must-PASS — clean tree

```text
$ ./scripts/check_rust_file_size.sh
rust-file-size: 181 files clean (default ceiling 1500; 13 exceptions)
(exit 0)
```

### must-FAIL 1 — file pushed over its ceiling

Padded `crates/repark-common/src/lib.rs` (normally well under default) to 1502 lines with
`// g8-provocation-pad-*` comments; restored after.

```text
ERROR: crates/repark-common/src/lib.rs is 1502 lines (ceiling 1500). Reason on file: default ceiling. Sanctioned outs: (1) split the module, or (2) edit EXCEPTIONS in scripts/check_rust_file_size.py with a reason (ceilings ratchet down only).
rust-file-size: FAIL — 1 violation(s) across 181 files
(exit 1)
```

### must-FAIL 2 — exception row deleted while file exceeds default

Removed the `crates/repark-functions/src/datetime.rs` EXCEPTIONS entry (measured 2020 > 1500);
restored after.

```text
ERROR: crates/repark-functions/src/datetime.rs is 2020 lines (ceiling 1500). Reason on file: default ceiling. Sanctioned outs: (1) split the module, or (2) edit EXCEPTIONS in scripts/check_rust_file_size.py with a reason (ceilings ratchet down only).
rust-file-size: FAIL — 1 violation(s) across 181 files
(exit 1)
```

### must-FAIL 3 — stale EXCEPTIONS key (path does not exist)

Temporarily added
`EXCEPTIONS["crates/repark-common/src/does_not_exist_g8_provocation.rs"] = (1500, "provocation")`;
restored after. Verbatim stderr (2026-08-11 fix-round):

```text
ERROR: EXCEPTIONS key has no file on disk: crates/repark-common/src/does_not_exist_g8_provocation.rs (remove the row or restore the path)
rust-file-size: FAIL — 1 violation(s) across 181 files
(exit 1)
```

### Raised-without-reason — convention, not mechanical

Raising a ceiling (or adding a row with a vacuous reason string) is **not** mechanically
detectable: the table cannot judge whether a reason string is real. The contract is
social/review — same as `check_lib_rs` / `check_lib_py`: the raising commit must state why,
and ceilings ratchet DOWN only by convention + review. Recorded here honestly so a future
reader does not expect a red gate for a silent raise.

---

## Fail-closed behaviour

| Condition | Result |
|---|---|
| `crates/` missing | exit 2, `ERROR: crates/ not found` |
| Stale EXCEPTIONS key (path missing) | exit 1, `ERROR: EXCEPTIONS key has no file on disk: <path> …` |
| Zero `*.rs` files under `crates/` | exit 2, `ERROR: … scan set is empty — refuse to pass closed` |
| Unreadable file | exit 1, `ERROR: <path>: unreadable (…)` named in the violation list |

---

## Gate evidence

| Gate | Result |
|---|---|
| `./scripts/check_rust_file_size.sh` | green — 181 files, 13 exceptions |
| `make check-rust-file-size` | green |
| `make ci` | **green (exit 0)** — includes the new gate after `check-lib-rs` |
| Provocations | must-PASS + two must-FAIL green; raise-without-reason = convention |

---

## ACC notes

Critic-1 (quality) and Critic-2 (security) run after Actor delivery; findings and
remediations append below this line.

### Critic-1 (quality)

| ID | Finding | Disposition |
|---|---|---|
| C1-1 | Guards **job display name** must not change (branch-protection required-context lesson) | **HOLD** — job id `guards:` and `name:` left as pre-G-8 string; only a new step added |
| C1-2 | Exception reason strings must be real review surface, not boilerplate | **HOLD** — each row names the module's job + a RATCHET condition |
| C1-3 | Default 1500 vs 1000 vs 2000 | **HOLD** — 1500 is the measurement-seeded pick (92.8% pass; 13 rows); ledger table records the alternatives |
| C1-4 | Docs must not restate ceilings | **HOLD** — AGENTS/DEVELOPMENT/scripts/map point at the `.py`; only this ledger carries the seed numbers |
| C1-5 | `mod.rs` / test-file special ceilings? | **HOLD** — decided no (recorded above); MERGE-in-mod.rs already needs a general exception |
| C1-6 | Ruff format clean on the new `.py` | **FIXED** pre-commit — `ruff format` collapsed one f-string |

### Critic-2 (security)

| ID | Finding | Disposition |
|---|---|---|
| C2-1 | Script purity (no network, no eval, no AWS) | **HOLD** — pure text walk of `crates/**/*.rs` |
| C2-2 | Fail-closed empty/unreadable | **HOLD** — exit 2 on empty scan / missing crates/; unreadable named as ERROR |
| C2-3 | Dual-wire: Makefile alone is not enough | **HOLD** — ci.yml guards step added; make ci chain updated |
| C2-4 | Pre-commit path | **HOLD** — both `.pre-commit-config.yaml` and `make install-hooks` (mold parity with check_lib_rs) |
| C2-5 | No secrets / no lockfile churn | **HOLD** — Cargo.lock/uv.lock untouched |

**Label:** ACC-CONVERGED (no open ≥MAJOR).
