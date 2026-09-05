# Charter ledger — PERF-ICE-CATALOG-IO-3 · The shared manifest cache goes ON by default

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-3` · **Base:** `origin/main` `b4af56d0`
· **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: elevated** (a default changes every session: every
default-built memory catalog gains a 32 MiB shared manifest cache, so the flip is proven
on the fixed pin by the full staleness battery plus the four upgrade-lineage tests running
on default sessions).
**Registry:** `PERF-ICE-MANIFEST-1` (FIXED with the default-session number, see C-006),
`PERF-CATALOG-CACHE-BOUND-1` (closed or narrowed with the measured RSS, see C-005),
`PERF-CATALOG-LINEAGE-CACHE-1` (FIXED at RP-13 — the precondition, not this unit's claim),
`PERF-CATALOG-COMMIT-CACHE-1` (BACKLOG behind fork ask `F-CATIO-COMMIT`, untouched).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** PERF-ICE-CATALOG-IO-2 wired the fork's shared manifest cache behind
`repark.iceberg.manifestCacheBytes` but HALTED on `PERF-CATALOG-LINEAGE-CACHE-1` (the
shared cache served wrong-context lineage on upgrade-boundary tables) and landed with the
default OFF per the round-2 ruling. RP-13 then landed the fork fix (`F-CATIO-KEY` at pin
`2ed39cb0`: the cache stores the context-free parse and applies each caller's list-entry
inheritance and `first_row_id` assignment per read) and the knob-on detector redded as
designed before it was re-pinned to the assigned lineage. The only remaining step is this
unit: flip `DEFAULT_MANIFEST_CACHE_BYTES` to 32 MiB and prove the flip is safe on the
fixed pin.

**Not in this unit:** any fork change; any `Cargo.toml` / `Cargo.lock` change (`git diff
origin/main -- Cargo.toml Cargo.lock` is empty); any AWS measurement; Glue / S3 Tables
wiring (unchanged again — their builders have no `with_shared_object_cache_bytes` at
`2ed39cb0`); the `F-CATIO-COMMIT` fix (filed, not fixed); `STATUS.md` and
`briefs/next-sequence.md`; `make ledger-archive` (it would touch `STATUS.md`, so the
still-staging IO-1 and IO-2 ledgers are left for the orchestrator's pickup — this unit
edits the IO-2 ledger's status lines only, pointing its follow-up sentences at this unit).

## PROPOSITION LEDGER — PERF-ICE-CATALOG-IO-3 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DEFAULT_MANIFEST_CACHE_BYTES` is `33554432` (32 MiB); the size is argued from the IO-1/IO-2 measurements and the fork's weight-based bound, and the memory ceiling per session is stated; both spellings still size the cache, a bad value still fails loud naming BOTH the key the user set and the canonical spelling, and an explicit `0` still disables. | Rust parse pins (default, both spellings, both refusals, `0` accepted); Python refusal legs. | **OPEN** | Red-first: the default-enables pin lands before the flip and reds while the default is `0`. Closing question: is the constant `33554432` with the size argument recorded in the catalog map and every refusal pin green? |
| C-002 | The four v3 upgrade/legacy tests that HALTED IO-2 are green with the cache ON by default: they run on the default session with no knob set. | The four tests green after the flip on the fixed pin. | **OPEN** | The four: `test_v3_legacy_delete_merge.py` × 2 (position-delete merge, plain-where MoR delete), the `alter-set-format-version-3-mor` statement row, `test_v3_upgrade.py::test_alter_upgrade_with_the_opt_in_serves_v3_lineage`. Closing question: do all four pass on a default session after the flip? |
| C-003 | The IO-2 staleness battery and the knob-on lineage pins run on the DEFAULT session: every explicit-knob leg is rewritten so the default-session leg is the primary and an explicit `0` is the off-control, and both directions stay knob-sensitive. | Default-session legs green after the flip; explicit-`0` controls green; knob flips red both ways. | **OPEN** | Red-first: the default-session delete-manifest and timing pins red on the base (the default builds no shared cache, so manifests re-open). Closing question: is every staleness cell green on the default session with an explicit-`0` twin that reds when flipped? |
| C-004 | The concurrency leg holds: two sessions over one warehouse, one v2 table upgraded to v3 in session A while session B holds a warm cache of the pre-upgrade manifest, then B reads lineage that is Spark-equal and assigned. | The two-session leg green on the default session. | **OPEN** | The fork fix's contract: the cache stores the context-free parse and applies the caller's lineage per read, so B's v2-warmed entry cannot poison its v3 read. Closing question: does B read the assigned triples after A's upgrade? |
| C-005 | The byte budget binds by measurement: peak RSS of a session that touches 500 small tables with the default cache versus explicit `0`, each in a fresh subprocess via `ru_maxrss`, and the bound holds. | The subprocess RSS comparison with the measured delta against the stated ceiling. | **OPEN** | Closing question: is the default-minus-off RSS delta within the stated ceiling with all 500 tables row-correct in both columns? |
| C-006 | The default-off control becomes the explicit-`0` control; the docs say ON by default and how to turn it off (the catalog map, the session config docs, the baseline part 3); `t_many/count_id/stmt2` and `t_many_merged` are re-measured on the default session before/after (5 iterations, medians, spread, floor, load); `PERF-ICE-MANIFEST-1` is FIXED with the default-session number; `PERF-CATALOG-CACHE-BOUND-1` is closed or narrowed with the measured RSS; the IO-2 ledger's follow-up sentences point at this unit. | The baseline note; the registry rows; the IO-2 status-line edit. | **OPEN** | Closing question: does every doc say ON-by-default with the `0` escape, do the registry rows carry the default-session numbers, and does the IO-2 ledger name this unit as the flip? |
| C-007 | Every touched `map.md` moves in lockstep with reasons and `pins:` citations; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **OPEN** | Closing question: are all seven maps current and does the diff add zero comment lines? |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED. Charter committed red-first; verdicts
flip when the pins land and the measurements are in.
