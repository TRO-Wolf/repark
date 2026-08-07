# Deferred-test manifest — phase-1 cone

## Purpose

The checked-in ledger of every v1 phase-1-cone test **not** ported yet, each with its target
phase. Together with the ported set it makes the census auditable at every phase boundary, not
just at the phase-3 census (design decision:
[../../docs/design/session-api.md](../../docs/design/session-api.md) §7, the deferred-test
manifest graft).

## Reconciliation rule (hard)

At every phase boundary: **(ported ∪ deferred) = the v1 phase-1-cone totals** at the pinned
port-source SHA. The ported side is `cargo test --workspace -- --list` in this repo under the
generated old→new rename map (the four prefix rules — never hand-written); the deferred side is
this file. The union must reconcile exactly — no test may be absent from both sides, none may
appear on both. Zero `#[ignore]`, zero skipped-in-CI: a test is either ported with its name or
listed here with a target phase.

v1 phase-1-cone totals at the pin (from the brief/design census):

| v1 crate | tests |
|---|---|
| repark-core (error seed) | 2 |
| repark-catalog | 50 |
| repark-write | 191 |
| repark-session cone (session 49 + catalog-config 26 + object-store 4, + hoisted repark-sql tests + ta_window) | audited in PR-C |

*(Corrected 2026-08-06 at PR-B: the brief's original 51/192 came from a grep over test
attributes, which counted two doc-comment `#[tokio::test]` mentions — v1
`repark-catalog/src/tests.rs:1783` and `repark-write/src/merge/mod.rs:425`; `cargo test
-- --list` at the pin is ground truth.)*

## Deferred entries

Format per row: `v1 test name (old path) | target phase | reason`. Filled by the PR that defers
the test — PR-B (repark-iceberg) and PR-C (repark-core session-test audit); empty sections mean
"no deferrals recorded yet", not "none exist".

### repark-common (from v1 repark-core)

*(none expected — both tests port in PR-A)*

### repark-iceberg — catalog/ (from v1 repark-catalog)

**Deferred: NONE** (PR-B, 2026-08-06). All 50 port under the generated rename map.

### repark-iceberg — write/ (from v1 repark-write)

**Deferred: NONE** (PR-B, 2026-08-06). All 191 port under the generated rename map
(split `merge/` shape). Zero `#[ignore]`, zero skipped-in-CI.

### repark-core — session (from v1 repark-session + hoisted repark-sql subset)

*(PR-C fills — the session-test audit's port-now vs deferred split, including the ta_window
group deferred whole)*

## Reconciliation runs

Each phase-1 PR appends a dated entry here: the pinned-SHA v1 `--list` count, this repo's
`--list` count, the deferred count, and the empty-diff confirmation.

- **2026-08-06 — PR-B (repark-iceberg):** v1 `cargo test -p repark-catalog -p repark-write
  -- --list` at pin `fc3f48102e437e2843ded460bc161edb434dac93` = 241 (catalog 50 + write 191);
  this repo's sorted per-package `--list` = 243 (241 `repark_iceberg::*` +
  2 `repark_common::*` from PR-A); diff against the generated rename map: **EMPTY**.
  (ported 241 ∪ deferred 0) = v1 PR-B cone total 241. Evidence:
  [../p1b-repark-iceberg-ledger.md](../p1b-repark-iceberg-ledger.md). PR-B additionally adds
  one NEW fork-pin proof test (not a ported name; outside the census).
