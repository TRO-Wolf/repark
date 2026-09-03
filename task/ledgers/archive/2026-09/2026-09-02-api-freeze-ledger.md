# Unit ledger — API-FREEZE · the owner's v1.0 API decisions, the versioning policy, the freeze pin

**Date:** 2026-09-02 · **Branch:** `docs/v1-0-api-freeze` · **Base:** `origin/main` `d7e2c4a` ·
**Model:** claude-opus-5 (medium) · **Answers:**
[../../../docs/design/v1-0-api-review-2026-09-02.md](../../../../docs/design/v1-0-api-review-2026-09-02.md) ·
**Gate:** [../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3 · **Path:** STANDARD (docs + one parity pin; one Actor cycle).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Owner decision (2026-09-02).** "All of your recommendations are perfect so proceed with them."
`R0 = yes` with the board's wording; every row's decision equals its recommendation — 15 YES,
15 YES-except, 5 NO.

**Not in this unit:** any change to a recommendation or to a `why` cell, any registry edit, any
engine or facade behaviour change, cutting the tag.

## PROPOSITION LEDGER — API-FREEZE — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The decisions are recorded where the packet lives: the markdown gains a `decision` column and the JSON a `decision` field, equal to the recommendation on all 35 rows, plus one dated owner line under "How to answer"; no recommendation text moved. | Diff the packet; compare `decision` to `recommend` row by row. | **PROVEN** | 35/35 `decision == recommend`; JSON also carries `decision_date` `2026-09-02`, `decision_rule` (the owner's sentence) and `freeze_inventory`. The `recommend` column and every `why` cell are byte-identical to `d7e2c4a` — the only prose edits are the dated answer paragraph, §4's decided totals, and §5's two now-false statements (below, C-002). Pinned by `test_every_decision_equals_its_recommendation`, `test_a_changed_decision_is_red`. |
| C-002 | The policy exists in the owner's wording and the tree stops saying it does not: `docs/release.md` replaces the "Cadence … **unwritten**" item with a "Versioning policy" section (one table: rule, what it binds, deprecation path, where the frozen inventory lives), dated, naming `docs/design/v1-0-api-freeze.json` as the frozen-surface register; `python-facade.md` §4 Q1 and the north-star §3 gate paragraph each gain one pointer line. | Grep the four documents; assert the old "unwritten" sentence is gone. | **PROVEN** | `docs/release.md` §"Versioning policy" (three rules, four columns); "release cadence and versioning policy remain **unwritten**" no longer occurs in the tree; the Cadence open item now points at the section. `python-facade.md` §4 Q1 carries a **Discharged 2026-09-02** line (Q1's ruling text untouched). North-star §3 gained exactly one sentence: "API review answered 2026-09-02 ([packet](…)); the freeze lands with the tag." Packet §5's "Unwritten." row and its closing sentence were truth-upped in the same change, because the section quotes `release.md` verbatim and the quote had become false. Pinned by `test_the_policy_sentence_is_the_owners_wording`, `test_the_freeze_pointers_are_in_lockstep`. |
| C-003 | The frozen surface is registered and mechanically held: `docs/design/v1-0-api-freeze.json` lists the exact frozen names of all 30 frozen rows (row members minus the named exceptions), generated from the tree by `scripts/build_api_freeze.py`; the pin REDs when a frozen name disappears or a frozen callable's required parameters change and stays green when a name or an optional parameter is added; the 5 `NO` rows carry `frozen: false` and are not checked. | Run the generator and the pin; run each mutation over a scratch copy of the enumerated sources. | **PROVEN** | 888 registered names — 781 Python members (650 with a required-parameter list an AST walk resolves), 85 door surfaces with their per-door disposition, 5 conf keys, 6 packaging facts, 11 error classes. `make py-test` green (527 passed), `test_api_freeze.py` 22 passed. Mutations: **8 red of 10** — frozen `def` renamed private (`Catalog.list_tables`); required parameter renamed (`DataFrameReader.format`); ANSI-door `CTAS` flipped `t(` → `absent(`; conf literal `"repark.sql.maxArrayElements"` renamed; `"ParseException"` dropped from `errors.__all__`; `requires-python = ">=3.12"` dropped; a packet decision changed away from its recommendation; a surface id no packet row claims. **2 green by design** (the additive rule): a new public member, a new optional parameter. |
| C-004 | The record is in lockstep and inside its ceilings: `docs/map.md`, `docs/design/map.md`, `scripts/map.md` and `python/repark-parity/tests/map.md` describe what moved and carry the clause citations; STATUS gains one line within the 25,000 B ceiling by compacting a restatement it re-pinned; this ledger retires to `completed/` last. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest`; `ledger_lifecycle.py check`. | **PROVEN** | Four maps updated. STATUS 24,911 B → 24,772 B: item 4 now states the answered review, the pinned freeze and the waiting gate, paid for by compacting the four inline wave-landing ledger links (`z5`/`w5`/`v5`/`s5`) to their archive-month map — the pointer the archive already maintains. `docs-compaction: clean`. Pinned by `test_the_freeze_pointers_are_in_lockstep` (STATUS, north star, facade, release). |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Measured notes

| Note | What was measured |
|---|---|
| Regenerating the register | `python3 scripts/build_api_freeze.py --write`, in the same commit as the intended additive change; bare `python3 scripts/build_api_freeze.py` is the check. `test_freeze_inventory_is_the_generated_file` holds the checked-in file equal to a fresh build, so a stale register cannot survive. |
| Where the register lives | `docs/design/v1-0-api-freeze.json`, beside the packet it answers — not a new `python/repark-parity/api_freeze/` directory. The parity tests already read checked-in inventories out of `docs/` (`docs/examples/inventory.txt`), and the register is a design record before it is a test fixture. |
| `inspect` vs AST | Parameter names are read by AST, not `inspect`. `make py-test` runs `uv run --no-project` with pyarrow, pytest and pydantic and no `repark`, so an `inspect`-based register could not be checked by the pin that must hold it; the AST walk is also the enumerator `scripts/check_example_coverage.py` already uses for the 913-name inventory, so register and inventory cannot disagree. 650 of 781 Python members resolve to a required-parameter list; the 131 that do not are class exports (`types.*`, `ml.*`), installer-generated `F.*` names and non-callable attributes, recorded as `required_params: null` and checked for presence only. |
| K1's member count | The packet's K1 cells say `18` members and `18/20 Tested`. The tree has 21: 11 DDL statement surfaces + 10 table-creation option surfaces, of which `TABLE_OPTION_SORT_ORDER` and `TABLE_OPTION_UNKNOWN_KEY_REFUSE` are the door's declared absences, so 19 are Tested. The register enumerates the 21 with their dispositions (the packet's own `why` says the two absences "are pinned as the declared set"). The packet's count cells were left as merged — correcting them is a packet edit this unit was not scoped for. |
| The surface partition | `ROW_SURFACES` plus six unpartitioned ergonomics/`SELECT_PASSTHROUGH` ids must equal `repark_common::surfaces::ALL` exactly once each; a 51st surface with no packet row is red (`test_every_surface_id_is_claimed_by_exactly_one_row`). That is what keeps the hand-written K-row partition from drifting. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: api-freeze
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every registered name is generated from the tree by script and re-derived by the pin on each run; the checked-in register is asserted equal to a fresh build, and ten mutations over a scratch copy of the enumerated sources prove eight red paths and two deliberately green additive paths.
      artifacts: [docs/design/v1-0-api-freeze.json, scripts/build_api_freeze.py, python/repark-parity/tests/test_api_freeze.py]
    - id: AT-2
      status: ATTACKED
      evidence: The mutation set attacks the pin's own vacuity, not only the tree - a removed name, a renamed required parameter, a flipped door disposition, a dropped conf key, error class and packaging literal, a decision that stops equalling its recommendation, and an unclaimed surface id each produce a named finding.
      artifacts: [python/repark-parity/tests/test_api_freeze.py]
    - id: AT-3
      status: ATTACKED
      evidence: The generator fails closed - an unparsable surface registry, an empty matrix parse, a missing errors __all__ and a frozen row of an unknown member kind each raise rather than register an empty set, so a broken read cannot look like a clean freeze.
      artifacts: [scripts/build_api_freeze.py]
    - id: AT-4
      status: N/A
      justification: No concurrency surface is touched; the generator and the pin are single-threaded readers.
    - id: AT-5
      status: ATTACKED
      evidence: No credential, account value or private path reaches committed content; no .github or dependency file changed; the register cites only tracked repo paths and public names.
      artifacts: [docs/design/v1-0-api-freeze.json, docs/release.md]
    - id: AT-6
      status: ATTACKED
      evidence: This unit writes the API contract itself. No shipped signature, conf key, error class or door disposition changed - the register records what d7e2c4a already ships, and the pin is the mechanism that makes a later change deliberate.
      artifacts: [docs/release.md, docs/design/v1-0-api-freeze.json]
    - id: AT-7
      status: N/A
      justification: No runtime code path; the generator runs on demand and the pin runs in the parity suite (22 tests, under five seconds).
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or toolchain change.
    - id: AT-9
      status: ATTACKED
      evidence: Two facts that would have been easier to leave quiet are recorded instead - the packet's K1 count disagrees with the tree by three names, and parameter shape is read by AST rather than by inspect because make py-test has no built repark. Both are in the measured notes above.
      artifacts: [task/ledgers/staging/api-freeze-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses, each checkable from the tree; four maps in lockstep with clause citations; STATUS inside its ceiling with the compacted restatement re-pinned; the ledger retires to completed in the last commit.
      artifacts: [docs/map.md, docs/design/map.md, scripts/map.md, python/repark-parity/tests/map.md]
  complete: true
```

## Pointers

- Up: [map.md](../../staging/map.md)
- Register: [../../../docs/design/v1-0-api-freeze.json](../../../../docs/design/v1-0-api-freeze.json)
- Policy: [../../../docs/release.md](../../../../docs/release.md) "Versioning policy"
