# Unit ledger — H-1d: the divergence registry

> **ARCHIVED 2026-08-11** (G-9 — H-1 phase ledger promotion) — a historical record of everything
> delivered through the H-1 close gate (repark #35–#46), including the parallel G/N corpus units
> whose gap-map homes are H-2, kept for provenance and **not a source of live rules**: every rule
> still in force was verified live-elsewhere or promoted first
> ([promotion-ledger.md](promotion-ledger.md)). Relative links were repaired for this location on
> the same date; nothing else changed. Current state: [STATUS.md](../../../STATUS.md).

**Unit:** V2 Engine Hardening H-1d · **Slate:**
[../briefs/v2-engine-hardening.md](../../../briefs/v2-engine-hardening.md), unit "H-1d" · **Design:**
[../docs/design/v2-engine-hardening.md](../../design/v2-engine-hardening.md) §2.2 (the
"no discoverable divergence registry" fact) + §4 decision D3 · **Status:** drafted 2026-08-10,
awaiting assembly.

Goal: this repository gains a discoverable list of its known divergences from Apache Spark, and
its dead citations resolve.

## Scope

- **`docs/spark-sql-iceberg-parity.md` (new)** — the divergence registry, authored at the exact
  path ~16 live sources already cite, so every citation resolves **without touching any citing
  code**. Eight sections; the five section numbers the citations name (§2.1, §2.2, §2.3, §7, §8)
  all exist and say what their citers claim they say.
- **`crates/repark-sql/tests/cross_door.rs`** — the case-folding row promoted to a
  declared-divergence test: it names registry §3 row ID-1, and it asserts the refusal *text*
  rather than only `is_err()`.
- **`crates/repark-spark/src/tests.rs`** — one new pin,
  `ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud`, so registry row REF-2 (which the
  `ref_ddl.rs` citation forces into §2.2) is not an unpinned row.
- **`python/repark/tests/test_parity_live.py`** — the new machine-checked mirror,
  `test_disclosures_mirror_the_registry` (decision D-6 below).
- **`STATUS.md`** — points at the registry; the case-folding entry is reduced to state + link.
- **`repo-manifest.toml`** — `[documentation] divergences` indexes the registry, so a move or a
  rename is a red gate instead of ~16 silently dead citations.
- **`map.md` lockstep** — `docs/`, root, `crates/repark-spark/`, `crates/repark-spark/src/`,
  `crates/repark-sql/tests/`, `python/repark/tests/`, `task/`, and — added at the fix pass, where
  two more maps stopped restating registry rows and one brief was amended —
  `python/repark/src/repark/` and `briefs/`.

Out of scope: fixing any divergence (H-1a/H-1b/H-1c own the three STATUS known issues); adding
rows for issues that have no disposition yet (see D-2); any change to a citing site.

## The citation inventory, re-verified

The brief's claim ("cited **16 times** from live sources") was treated as a claim to check, not as
truth. `grep -rn "spark-sql-iceberg-parity"` over the tree, excluding the two campaign documents
that describe the problem (`briefs/v2-engine-hardening.md:97`,
`docs/design/v2-engine-hardening.md:83`), returns exactly **16** lines:

| Site | Lines | Sections cited |
|---|---|---|
| `crates/repark-spark/src/router.rs` | 269, 341, 350 | §2.3, §2.3, §2.2 |
| `crates/repark-spark/src/ref_ddl.rs` | 322, 700 | §2.2, §2.2 |
| `crates/repark-spark/src/insert_overwrite.rs` | 88 | §2.3 |
| `crates/repark-spark/src/metadata_tables.rs` | 168 | §2.1 |
| `crates/repark-spark/src/normalize.rs` | 158 | (no section) |
| `python/repark/src/repark/session/session_core.py` | 142 | §8 |
| `python/repark/src/repark/session/builder_conf.py` | 57 | §8 |
| `python/repark/tests/test_dropin_disclosure.py` | 8 | §8 |
| `python/repark/tests/test_errors.py` | 150 | §7 |
| `python/repark/tests/test_sql_passthrough_parity.py` | 17 | §7 |
| `python/repark/tests/map.md` | 799, 945, 996 | §7, §8, §7 |

**The count is right, and so is the site list** (8 in `crates/repark-spark/src/`, 2 in the facade
session package, 3 in facade tests, 3 in `python/repark/tests/map.md`). Distinct sections cited:
**§2.1, §2.2, §2.3, §7, §8** — every one exists in the authored document. Each citing site was
read before its section was written, so the section says what the citer claims:

- §2.1 — `metadata_tables.rs` refuses time travel *composed with* a metadata table and names
  "§2.1 metadata tables" → §2.1 is the metadata-table surface, and MT-1 is that exact refusal.
- §2.2 — `ref_ddl.rs:322` names "IF EXISTS / IF NOT EXISTS stay out"; `ref_ddl.rs:700` names the
  write-to-branch STOP; `router.rs:350` names the residual `BRANCH|TAG` forms → §2.2 is the
  snapshot-ref DDL surface, REF-1 is write-to-branch and REF-2 is the trailing-clause /
  `IF [NOT] EXISTS` exclusion, and §2.2's preamble states the supported grammar the residual-form
  refusal points at.
- §2.3 — `insert_overwrite.rs:88` (PARTITION forms), `router.rs:269` (TRUNCATE), `router.rs:341`
  (unparsable MERGE) → §2.3 is the DML statement-form surface, with DML-1/2/3 in that order.
- §7 — `test_errors.py` and `test_sql_passthrough_parity.py` both call it the *backlog* and name
  the CAST class ("Known Spark-parity divergences") → §7 carries exactly that title and BL-1 is
  the CAST class, with both cited pins named in the row.
- §8 — three sites call it "the rationale table" for the drop-in no-op surface → §8 is a table,
  and every row in it is pinned in `test_dropin_disclosure.py`.

## Design decisions

- **D-1 — one row, four fields, no exceptions.** Every row carries repark's behavior, Apache
  Spark's behavior, the `path::test_name` that pins it, and the rationale. A row with no live pin
  is not admitted — §6 states that as a rule of the document, so the next author cannot open a
  TODO row. This is why REF-2 shipped with a new Rust pin rather than as prose: the citation forced
  the row, the row forced the pin.
- **D-2 — a STATUS known issue becomes a row when it is DISPOSED, not when it is known.** The
  brief says "a row per STATUS known-issue **as each resolves**"; the orchestrator's task summary
  said "one row per STATUS 'Known correctness issues' entry". **The brief wins** (flagged to the
  orchestrator). It is also the only reading consistent with D-1: the time-travel view leak
  (scheduled FIX, H-1b) and the `$`-metadata rider (DECIDE-IN-UNIT, H-1c) have no disposition, so
  neither has semantics to record and neither has a pin that would survive its unit. Case folding
  is DECLARED by D3, so it gets its row now — and it is the registry's first declared row, as the
  brief requires. §6 makes the rule explicit and §2.1 carries a one-line non-row note for the
  `$`-metadata question so a reader who arrives from `metadata_tables.rs` is not left wondering.
- **D-3 — the case-folding pin is promoted in place, never renamed.**
  `docs/testing.md` "Relocation discipline" makes any test-path change a **declared-rename unit
  that ships alone**. Promotion is therefore: a `# This is a DECLARED-DIVERGENCE test` doc section
  naming registry §3 row ID-1, and assertions that name the row. Renaming it to something like
  `declared_divergence_identifier_case_folding` would have been a slate-failing violation for a
  cosmetic gain.
- **D-4 — the promoted pin asserts the refusal TEXT, not `is_err()`.** Two reasons: `assert!(…)`
  on a bare error is one step from the pattern `docs/testing.md` bans outright, and — the real
  one — a bare `is_err` stays green if the statement starts failing for an unrelated reason, which
  would silently detach the row from identifier resolution. Both halves now assert the error names
  the unresolved identifier, and the *success* branch panics with an instruction to retire the
  registry row rather than relax the assertion.
- **D-5 — every row's Spark half declares its oracle basis** (*live* / *recorded* / *documented*),
  because "hand-computed expectations are not an oracle" (AGENTS.md, testing.md). *documented* is
  admitted only for rows where the divergence is that repark **refuses a form Spark accepts** —
  there the refusal is the whole claim and no value oracle is involved. This keeps §2 honest
  without inventing Spark results nobody in this repository has observed.
  **Amended at the fix pass** (finding 2): the panel found the rule stated with "only" and then
  broken by BL-1, which makes a *value* claim about Spark. §1's definition now reads "documented
  grammar **or** documented semantics, admitted **only where no value oracle exists yet**", and a
  documented *value* claim must say so and name the unit that will attach a real oracle. BL-1 names
  H-2 gap G6. The intent of D-5 is unchanged — no invented Spark results — but the rule now
  describes what the document actually does.
- **D-6 — YES: the live-tier `DISCLOSURES` list becomes a machine-checked mirror.** See below.
- **D-7 — the mirror keys on a per-row FIELD, not on a row class and not on an allowlist.** A row
  opts in with a `` `live-mirror: <name>` `` bullet. Rationale: (a) three of the four live
  disclosures are DECLARED rows and one (`filter_backtick_identifier`) is a BACKLOG row, so keying
  the check on "declared rows" would have quietly excluded a live disclosure and made the check
  lie; (b) `task/lessons.md` (2026-08-09) says a gate that reads a hand-maintained table must
  validate that table's key space — a field-keyed check has no separate table to validate, because
  the key space *is* the union of the two sides it compares.
- **D-8 — the registry is indexed in `repo-manifest.toml`.** ~16 live sources cite the path by
  name; without the index a move breaks all of them silently. With it, `make check-manifest`
  (in the `make ci` chain) reds. Confirmed live below.

## D-6 in full — the DISCLOSURES mirror decision

**Decision: YES.** `python/repark/tests/_live_parity.py::DISCLOSURES` is now the machine-checked
mirror of the registry rows that claim a live mirror, checked by
`python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry` (always-on,
JVM-free, both directions).

Why yes:

- It closes the exact failure the campaign cares about. A DECLARED divergence's whole value is
  that it cannot converge silently; the live tier is what detects convergence. A row whose
  `Disclosure` is deleted keeps looking authoritative while having lost its detector — nothing red.
- The reverse direction matters too: a `Disclosure` with no registry row is a divergence the
  registry does not describe, which is the "no discoverable list" state this unit exists to end.
- It is the repo's own SSOT-plus-checked-mirror pattern (`repo-manifest.toml` ↔
  `check_manifest.py`), applied with the same rule: the *document* is the SSOT, the *list* is the
  mirror, never the other way round.

Why it lands as a facade test and not as a `scripts/check_*.py` in `make ci`:

- The two sides are a markdown document and a Python module. The Python side is only importable
  with the compiled native module, so a `make ci` script could not import it and would have to
  regex-parse `_live_parity.py` — that is a *third* parser to keep true, and it would compare two
  parses instead of one parse against real data.
- `test_parity_live.py` already imports `_live_parity`, and the facade suite runs on every PR via
  the `wheels.yml` smoke job (against the packaged wheel) as well as `make py-test-facade`. The
  check therefore runs on real objects, on every PR, with no new CI wiring.

What was explicitly rejected: making the mirror **total** (every registry row must have a
`Disclosure`). The live tier expresses facade-reachable, PySpark-comparable recipes; §2's
statement-surface rows and §3's cross-door row are neither. A total mirror would have forced fake
disclosures or an opt-out allowlist, and the allowlist is the lookup table lessons.md warns about.

## Gate results

Run in the unit's isolated worktree, on the unit's full change set.

| Gate | Command | Result |
|---|---|---|
| Canonical | `make ci` | **exit 0** |
| Rust suite | `make test` | **exit 0** |
| Facade suite | `make py-test-facade` | **exit 0** — `2531 passed, 44 skipped, 37 warnings in 99.07s` |
| map.md lockstep | `bash scripts/check_map_md.sh` (over the real staged set) | **exit 0**, with a negative control |
| Structural manifest | `python3 scripts/check_manifest.py` | **exit 0** |

`make ci` tail (the two lines that matter for this unit):

```
lib-py: 53 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
index, the status document and the crate maps
…
uvx ruff@0.15.22 check .
All checks passed!
uvx ruff@0.15.22 format --check .
238 files already formatted
```

`make test` — every non-empty result line, exit 0. `repark-spark`'s lib battery is **349**
(348 before this unit + the one new REF-2 pin); `repark-sql`'s `cross_door` target is the 8-row
file, unchanged in count because the case-folding pin was promoted in place, not renamed:

```
test result: ok. 349 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.84s
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.48s
…
EXIT=0
```

`scripts/check_map_md.sh` reads the **staged** set and this unit hands off uncommitted, so it was
run against the real change set by staging, checking, and resetting — including a negative control
that proves the guard was actually looking at this unit's files:

```
--- guard (real staged set):
MAPMD_EXIT=0
--- negative control: unstage crates/repark-spark/src/map.md
ERROR: crates/repark-spark/src/map.md was not updated in this commit (map.md lockstep rule).
MAPMD_EXIT_NEG=1
MAPMD_EXIT_RESTORED=0
```

## Provocation proofs

The unit's new mechanical gate is `test_disclosures_mirror_the_registry` (P-1…P-4). The two new /
promoted **pins** are provoked as well (P-5, P-6), because `docs/testing.md` holds a pin valid only
if the thing it claims turning false turns it red. Every provocation was reverted immediately and
each reverted file was verified **byte-identical** to its pre-provocation copy (`diff -q`).
Nothing below is committed.

**P-1 — must-FAIL: a registry row loses its live mirror.** (The row keeps looking authoritative
while its drift detector is gone — the failure this gate exists for.) Deleted the
`` - `live-mirror: int_union_string` `` bullet from row TY-1:

```
E       AssertionError: the registry's live-mirrored rows and the DISCLOSURES list disagree —
        registry-only: []; disclosure-only: ['int_union_string']
E       assert {'fillna_scal...ion_bypasses'} == {'fillna_scal...union_string'}
E         Extra items in the right set:
E         'int_union_string'
python/repark/tests/test_parity_live.py:172: AssertionError
FAILED python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry
1 failed in 0.41s
PYTEST_EXIT=1
```

**P-2 — must-FAIL: a row claims a mirror that does not exist.** (The other direction: a divergence
the registry describes but the live tier never re-asserts.) Added
`` - `live-mirror: no_such_disclosure` ``:

```
E       AssertionError: the registry's live-mirrored rows and the DISCLOSURES list disagree —
        registry-only: ['no_such_disclosure']; disclosure-only: []
E       assert {'fillna_scal...h_disclosure'} == {'fillna_scal...union_string'}
E         Extra items in the left set:
E         'no_such_disclosure'
python/repark/tests/test_parity_live.py:172: AssertionError
FAILED python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry
1 failed in 0.41s
PYTEST_EXIT=1
```

**P-3 — must-FAIL: two rows claim the SAME mirror.** This is the case a set-equality check alone
would pass, which is why the duplicate assertion exists as a separate rule. Duplicated the
`int_union_string` bullet:

```
E       AssertionError: a live-mirror name is claimed by two registry rows:
        ['fillna_scalar_numeric_nullability', 'filter_backtick_identifier',
         'filter_case_collision_bypasses', 'int_union_string', 'int_union_string']
E       assert 5 == 4
python/repark/tests/test_parity_live.py:167: AssertionError
FAILED python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry
1 failed in 0.41s
PYTEST_EXIT=1
```

**P-4 — must-FAIL: the registry is moved or renamed.** This is the ~16-dead-citations failure the
unit exists to end, and it must be caught by *two* independent gates. Renamed the document to
`docs/spark-sql-iceberg-parity.md.moved`:

```
E       assert False
E        +  where False = is_file()
E        +    where is_file = PosixPath('…/docs/spark-sql-iceberg-parity.md').is_file
python/repark/tests/test_parity_live.py:162: AssertionError
FAILED python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry
1 failed in 0.40s
PYTEST_EXIT=1
----- and the manifest gate:
ERROR: [documentation] `divergences` points at docs/spark-sql-iceberg-parity.md, which does not
exist — the document moved or was archived without updating the index.
manifest: FAIL — 1 violation(s) (the structural SSOT is repo-manifest.toml; the rules are
scripts/check_manifest.py).
MANIFEST_EXIT=1
```

**P-5 — must-FAIL: the case-folding divergence DISAPPEARS.** The promoted declared-divergence pin
is only worth promoting if it reds when repark *converges* on Spark. Simulated convergence by
pointing the ANSI door's quoted read at the correctly-cased `"id"` (which resolves), so the pin
takes its success branch:

```
thread 'cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted' panicked at
crates/repark-sql/tests/cross_door.rs:628:18:
ANSI door: a double-quoted identifier is case-SENSITIVE, so "ID" must not resolve to a column
stored as `id`. If it now resolves, repark has CONVERGED on Apache Spark and
docs/spark-sql-iceberg-parity.md §3 row ID-1 must be retired, not this assertion relaxed.
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out
```

Reverted; `diff -q` against the pre-provocation copy → identical, and the file is green again
(`test result: ok. 8 passed`).

**P-6 — must-FAIL: a refusal message stops citing its registry row.** REF-2's pin asserts the
*citation*, not only the refusal, so a doc pointer cannot be deleted from the engine without the
registry row noticing. Removed `docs/spark-sql-iceberg-parity.md §2.2` from `ref_ddl.rs`'s
trailing-clause message:

```
thread 'tests::ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud' panicked at
crates/repark-spark/src/tests.rs:7167:9:
the refusal must cite the registry row it defends (ALTER TABLE ice.sales.refdecl CREATE BRANCH IF
NOT EXISTS b1): This feature is not implemented: CREATE BRANCH|TAG: trailing clause after the
supported form is not supported yet (got word "NOT") — supported: … ; IF EXISTS / IF NOT EXISTS
stay out (r25 T2)
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 348 filtered out
```

Reverted; `diff -q` identical, and `test result: ok. 1 passed`.

**P-7 — must-PASS: the clean tree.** After restoring the registry byte-identically, the whole
`test_parity_live.py` module and both structural gates are green (the 31 skips are the live-armed
tests, correctly skipping with the flag unset — never a silent pass):

```
REGISTRY RESTORED BYTE-IDENTICAL
30 passed, 31 skipped in 0.76s
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
index, the status document and the crate maps
MANIFEST_EXIT=0
```

## The sweep queue — disposed divergences pinned in the tree with no row yet

The registry is **seeded, not swept** (registry §1 "Scope", added at the fix pass below). These
eight are divergences that were already *disposed of* and pinned before the registry existed, and
whose authoritative description is still a test docstring. Each is a gap to close: closing it means
**moving** the description into a row, never copying it. They were deliberately not rowed here —
seeding on the day the document lands is a different job from sweeping the tree, and each of these
needs its own reading of what the disposition actually was.

Carried as a **candidate H-2 unit** ("sweep the pre-registry disclosures"), sized at roughly one
unit: eight rows, no engine change, each row reusing the pin that already exists.

| # | Pin (the current authoritative description) | Class it would carry |
|---|---|---|
| 1 | `python/repark/tests/test_dogfood_gaps.py::test_divergence_timestamp_ltz_collect_passthrough` — "DIVERGENCE-1 (disclose, do not build session-tz machinery)" | DECLARED §4 — **check against H-1a first**: session-timezone work may retire or convert it |
| 2 | `python/repark/tests/test_show_namespaces.py::test_show_namespaces_disclosed_divergences_fail_loud` — the no-`IN`/`FROM` form | DECLARED §2 (statement surface) |
| 3 | the same test's second disclosed divergence | DECLARED §2 (statement surface) |
| 4 | `python/repark/tests/test_catalog_surface.py::test_show_tables_in_not_implemented_divergence` | DECLARED §2 (statement surface) |
| 5 | `python/repark/tests/test_catalog_surface.py::test_list_databases_location_uri_none_divergence` | DECLARED §5 (facade drop-in) |
| 6 | `python/repark/tests/test_interchange_parity.py::test_to_polars_round_trip_int32_widens_to_int64_divergence` | DECLARED §4 (type semantics) |
| 7 | `python/repark/tests/test_interchange_parity.py::test_to_polars_round_trip_decimal_precision_widens_divergence` | DECLARED §4 (type semantics) |
| 8 | `python/repark/tests/test_errors.py::test_python_arg_errors_runtime_error_divergence_is_deliberate` — "KNOWN DIVERGENCE, pinned so it is visible rather than accidental" | DECLARED §5 (facade drop-in — the `RuntimeError` base-class decision) |

Two more that the same grep surfaces and that a sweep unit must **rule on rather than assume**:

- `python/repark/tests/test_metadata_tables.py::test_unpartitioned_partition_column_divergence` —
  "pin fork residue: empty partition column kept on unpartitioned tables (Java drops it)". This is
  a divergence from **Java Iceberg**, not from Apache Spark, so its home may be the fork's gap
  matrix rather than this registry.
- `python/repark/tests/map.md` (filter-rewriter section) — exact duplicate column names are
  rejected at construction by DataFusion where Spark accepts them and refuses only at the
  reference. Characterized in `test_filter_predicate_rewrite.py`; no row.

The queue was built from `grep -rn "DISCLOSED DIVERGENCE\|KNOWN DIVERGENCE\|DIVERGENCE-[0-9]"` over
`python/repark/tests` (24 hits). **It is not proven exhaustive** — it is a naming-convention grep,
and a divergence disposed of without one of those words would not appear. The sweep unit's first
job is a wider inventory, not this table.

**Closed by G-5 (2026-08-10):** full triage and dispositions in
[g5-sweep-ledger.md](g5-sweep-ledger.md). The table above stays as the historical seed; G-5 did
not rewrite its rows.

## Notes for later units — not registry rows

- **Filed for a future registry-adjacent row (do NOT open a row for it yet):** Iceberg table
  registration walks **every** database in the catalog — a `GetDatabases` + `GetTables` provider
  walk — rather than only the namespace being registered. Observed engine-side during tier-2
  bring-up. It is an efficiency/scoping property of registration, not a Spark divergence, so it
  does not belong in this registry as it stands; the natural home is the fork's backlog. Wording
  kept deliberately cloud-generic.
- **The Spark-door time-travel view leak has no pin today.** The ANSI door's fixed twin is pinned
  (`crates/repark-sql/tests/introspection.rs::time_travel_pinned_views_do_not_leak_into_the_introspection_surface`);
  the Spark door's leaking behavior is characterized nowhere. H-1b owns it, and its fix should
  land the pin. Deliberately NOT added here: a characterization pin authored in this unit would
  have to be red-then-updated by H-1b, putting two units in the same file for no gain.
- **`_live_parity.py` was not touched.** `test_registry_covers_the_mandated_golden_family` still
  pins the four disclosure names as a literal set; the new mirror is a second, independent
  statement about the same names, and the two reds mean different things (shrinkage vs drift).

## Deviations / open items

- **Brief vs orchestrator-summary discrepancy on STATUS rows** — resolved in favor of the brief
  (D-2), flagged in the actor report.
- The registry's §2 rows carry *documented*-basis Spark halves. If H-2's golden-corpus work makes
  any of those forms live-comparable, the basis marker should move to *live* in the same change.

---

## Fix pass (panel findings) — 2026-08-10

The FULL adversarial panel ran two lenses (registry-completeness + citation; mechanics) and
returned **REJECT / REJECT**: one BLOCKER, seven MAJORs, five NITs. This section records each
finding, the disposition applied, and what changed. No finding was argued away; two were resolved
differently from the panel's *suggested* fix, and both deviations are stated below with the
evidence that forced them.

### Findings → actions

| # | Sev | Finding | Action |
|---|---|---|---|
| 1 | **BLOCKER** | The registry claims, present tense, to be the *single* home of every divergence, while ~8 disposed, pinned divergences are authoritatively described in test docstrings with no row | §1 line 3 softened ("the place… the one home a divergence gets **once it has been disposed of**"), and a new **"Scope — this registry is SEEDED, not swept"** paragraph replaces the old one-line "Seed." note: it names the date, states the document is not an exhaustive inventory, calls each unrowed pin a **gap to close**, and points at the sweep queue. §6's rule is re-scoped to bind dispositions **from 2026-08-10 forward**. The eight candidates are listed **by name with their pins** in this ledger's new "The sweep queue", carried as a candidate **H-2** unit |
| 2 | MAJOR | `oracle: documented` is defined with the word "only" and then used for two non-refusal rows, one of which makes a bare *value* claim about Spark | §1's definition widened honestly: documented **grammar or semantics**, admitted **only where no value oracle exists yet**; a documented *value* claim must say so and **name the unit that will attach a real oracle**. **BL-1** now says exactly that and names **H-2 gap G6** (whose slate already budgets the differential rows + live disclosures), and commits the basis to move to *live* in that change. **MT-2** reframed — see #4 |
| 3 | MAJOR | Two facade maps restate rows ID-2 / BL-2 (and BL-1 twice) in full — the "no second authoritative list" gate fails | `python/repark/src/repark/map.md` and `python/repark/tests/map.md` reduced to links (§3 ID-2 / §7 BL-2), and both BL-1 restatements (`tests/map.md` at the errors and passthrough-parity entries) reduced to "registry §7 row BL-1". Same treatment the two `repark-spark` maps already had |
| 4 | MAJOR | Row MT-2 enumerates ten statement forms; the pin exercises four | **Pin extended, row not trimmed** — all ten forms reach one guard (`metadata_tables::is_write_target_context`), so the extension was a six-case loop in the existing pin (DELETE / MERGE / CTAS / CREATE VIEW / DROP / ALTER), asserting `metadata table … read-only`. MT-2 also **reframed as a diagnostic divergence** per disposition 2: both engines refuse, the row claims the *diagnostic* difference only, and it says explicitly that it makes no claim about Spark's message text |
| 5 | MAJOR | REF-2's leftover-token assertion is vacuous for 2 of 3 cases — the message's constant tail already contains `NOT` and `EXISTS` | Assertion bound to the **dynamic span**: `error.contains(&format!("(got word {leftover:?})"))`. Re-ran the panel's REDACTED provocation **per case** — all three now RED (proof below). Doc comment rewritten to say why the bare word is not enough |
| 6 | MAJOR | STATUS's new boundary paragraph is contradicted by the two bullets under it; the time-travel leak is called "a declared divergence" with no row | Paragraph reworded to the true rule: an **undisposed** issue keeps its description in STATUS; a **disposed divergence**'s semantics move to the registry; a **known defect with its fix scheduled is not a divergence** and keeps its description here until the fix lands. Time-travel bullet now reads "a known **defect**, not a declared divergence… fix scheduled (D1, H-1b)". `$`-metadata bullet now states **"no disposition yet (D2 rules on it in H-1c)"** — which is why it is described in STATUS — and says the semantics move to the registry if H-1c rules "keep and declare". Neither bullet restates a registry row |
| 7 | MAJOR | The mirror gate is false-green on a near-miss: five legitimate-looking bullet spellings admitted a lying row silently, and the registry never documented the exact spelling | Both halves fixed. (a) Registry **§6 gains "The exact spelling this gate parses"** — column-zero `- `, one backtick pair, `[a-z0-9_]+`, nothing else on the line — and names the near-miss list. (b) `test_disclosures_mirror_the_registry` is now **fail-closed**: a permissive probe (`^[ \t]*-.*live-mirror.*$`) collects candidates, and any candidate not matching the strict pattern is a loud failure **naming the offending line**. All five of the panel's near-misses now RED (proof below) |
| 8 | MAJOR | The brief's H-1b line presupposes a registry row this unit's own rule forbade it to write | `briefs/v2-engine-hardening.md` H-1b amended in place: "**create** the registry row — H-1d seeds only *disposed* issues…", with the brief-internal conflict, its date, and the pointer to this ledger written into the line. It also states the honest possibility the panel did not: if the re-port **closes** the defect, the right outcome is **no row at all**. `briefs/map.md` records the in-flight amendment |
| 9 | NIT | "the registry's **first declared row**" reads as document order; seven DECLARED rows precede it | Reworded to "**The first row admitted at seeding** (campaign decision D3, 2026-08-10)… first by *declaration*, not by position: §2's rows were back-filled from the citations that forced them, and the document is ordered by surface, never by date" |
| 10 | NIT | §1 promises an oracle basis on "every row"; §8's six rows state none | Both ends. §1's promise scoped to **§2–§7**, with the reason (§8 is a table whose rows share one basis). §8 gains an explicit preamble: **oracle basis for the whole table is *documented* — PySpark's documented API contract**, plus why that is admissible and the note that `4.1.2` is illustrative ("e.g."), not a claimed value |
| 11 | NIT | `router.rs`'s residual `BRANCH\|TAG` refusal cites §2.2, whose two rows do not cover that case | §2.2 gains a **non-row note** that describes the residual guard, says what pins its recognizer, and says why it has no row — see **Deviation D-A** below: the guard is **unreachable** today, proven, so a row would be unpinnable |
| 12 | NIT | `normalize.rs` cites the registry for an "increment roadmap" the registry never mentions | DML-3 gains **"Where the boundary moves next"**: the `ReParkDialect` + Iceberg-extension recognizer arrives with the MERGE / CALL / branch increments — the same increments that widen DML-3's surface — and the *schedule* stays in STATUS and the briefs, never here |
| 13 | NIT | The promoted case-folding pin's `contains("ID")` is a weak discriminator (`INVALID`, `UUID`) | Strengthened to `contains("No field named") && contains("\"ID\"")` on **both** doors — the failure class **and** the identifier. Comment says why both halves are load-bearing; `crates/repark-sql/tests/map.md` updated to match |
| 14 | NIT | REF-2's doc comment says "the **two** `ALTER TABLE` forms"; there are three | Corrected to "three" |
| 15 | NIT | `divergences` is an optional `[documentation]` key, so deleting the row disarms the existence check without a red gate | **Ledger note only, by disposition** — no code change. The mirror test's `_REGISTRY.is_file()` assertion is the second, independent guard on the document's existence, and it is not defeatable from `repo-manifest.toml`: deleting the manifest row disarms the manifest check but the facade suite still REDs (panel provocation P-4 exercised both). A future unit that widens `REQUIRED_DOCUMENTATION_KEYS` should do it as a rules-SSOT change with its own pin, not as a rider here |

### Deviations from the panel's suggested fixes

- **D-A — finding 11: no third §2.2 row, because the case is unreachable.** The panel suggested
  "add a third §2.2 row for a `BRANCH|TAG` form the dedicated parser does not reach". I first wrote
  that row **and its pin** — and the pin failed. Probing both recognizers directly over twelve
  candidate spellings (temporary test, removed) showed **no SQL string satisfies
  `starts_with_branch_or_tag_ddl(sql) && try_parse_ref_ddl(sql).is_none()`**: the router's
  recognizer (`normalize.rs`) and the ref-DDL parser (`ref_ddl.rs`) accept the same shapes, and the
  parser answers every shape it declines with `Some(Err(…))` rather than `None`. The router's
  residual guard is therefore **unreachable defense-in-depth**, and under §6 ("a row lands with its
  pin, or it does not land") an unpinnable row must not land. §2.2 instead carries a non-row note
  that says all of this, names what *is* pinned (`branch_sniff_skips_table_name_segments` for the
  recognizer's boundary; REF-2 for the parser's answer), and states that a row lands the day the
  guard becomes reachable, with the pin that reaches it.
- **D-B — finding 4: the pin was extended, not the row trimmed.** The panel offered either. The
  brief's rule is that a row may not assert more than its pin proves; extending was the honest
  direction here because all ten forms are already implemented behind one guard, so trimming the
  row would have understated real, shipped behavior to make a test's laziness look principled.

### Provocation proofs (fix pass)

Every provocation below was reverted immediately and the reverted file verified **byte-identical**
(`diff -q`, plus an `md5sum` on the registry). Nothing is committed.

**P-8 — must-FAIL ×3: REF-2's refusal stops naming the leftover token.** The panel proved the old
assertion was vacuous for the two `IF` spellings. Replacing `(got {leftover})` with `(got REDACTED)`
in `crates/repark-spark/src/ref_ddl.rs` now reds **every** case. The loop aborts at its first
failure, so each case was run alone (`tests.rs` reduced to one tuple, three runs, restored
byte-identical):

```
=========== CASE 0 (message interpolation REDACTED)
the refusal must name the leftover token in its dynamic slot, (got word "NOT") (ALTER TABLE
ice.sales.refdecl CREATE BRANCH IF NOT EXISTS b1): … (got REDACTED) — supported: …
test result: FAILED. 0 passed; 1 failed; …
=========== CASE 1 (message interpolation REDACTED)
the refusal must name the leftover token in its dynamic slot, (got word "EXISTS") (ALTER TABLE
ice.sales.refdecl DROP BRANCH IF EXISTS b1): … (got REDACTED) — supported: …
test result: FAILED. 0 passed; 1 failed; …
=========== CASE 2 (message interpolation REDACTED)
the refusal must name the leftover token in its dynamic slot, (got word "EXTRA") (ALTER TABLE
ice.sales.refdecl CREATE TAG t1 AS OF VERSION 1 RETAIN 7 DAYS EXTRA): … (got REDACTED) — …
test result: FAILED. 0 passed; 1 failed; …
```

`ref_ddl.rs` restored (`diff -q` → identical, `git diff --stat` empty for that file); the pin is
green again — `test result: ok. 1 passed`.

**P-9 — must-FAIL ×5: every near-miss the panel proved false-green.** All five spellings that
previously passed silently now red loudly and name the line. (Runs 2–5 carry a name with **no**
`Disclosure`, which is the dangerous case: under the old gate nothing red.)

```
======== bolded bullet (near-miss, Disclosure EXISTS)
E  AssertionError: a registry bullet claims a live mirror in a spelling this gate does not parse,
   so the row would advertise a drift detector that is never checked. … Offending line(s):
   ['- **`live-mirror: int_union_string`**']
1 failed, 60 deselected
======== bolded bullet, LYING name (no Disclosure)  → Offending line(s): ['- **`live-mirror: no_such_disclosure`**']   1 failed
======== indented bullet                            → Offending line(s): ['  - `live-mirror: no_such_disclosure`']     1 failed
======== hyphenated name                            → Offending line(s): ['- `live-mirror: no-such-disclosure`']       1 failed
======== trailing parenthetical                     → Offending line(s): ['- `live-mirror: no_such_disclosure` (nightly only)']  1 failed
======== trailing space                             → Offending line(s): ['- `live-mirror: no_such_disclosure` ']      1 failed
```

**P-10 — must-PASS: the original directional reds still fire.** Hardening must not have replaced
the set-equality check with the near-miss check. Re-ran the actor's P-1 (delete a canonical bullet)
and P-3 (duplicate one):

```
E  AssertionError: the registry's live-mirrored rows and the DISCLOSURES list disagree —
   registry-only: []; disclosure-only: ['int_union_string']        → 1 failed
E  AssertionError: a live-mirror name is claimed by two registry rows: [… 'int_union_string',
   'int_union_string']                                              → 1 failed
```

**P-11 — must-PASS: the clean tree.** Registry restored byte-identically
(`md5sum 39d9c0116c358dcecb34ea61e5e67725` before and after every run above):

```
BYTE_IDENTICAL
30 passed, 31 skipped in 0.75s     (test_parity_live.py — the 31 skips are the live-armed tests)
```

### Gate results (re-run after every fix)

| Gate | Command | Result |
|---|---|---|
| Canonical | `make ci` | **exit 0** — `crate-dag: 20 internal edges clean`, `lib-rs: 9 crate roots clean`, `lib-py: 53 files clean`, `manifest: 12 components (9 delivered, 3 planned) agree…`, ruff `All checks passed!` / `238 files already formatted`, taplo + typos clean |
| Rust suite | `make test` | **exit 0** — 31 `test result: ok` lines, **1269 passed**, zero failed. `repark-spark` lib is **349** (unchanged: the MT-2 loop extends an existing pin rather than adding one, and the REF-3 experiment was removed — see D-A); `cross_door` is **8** |
| Facade suite | `make py-test-facade` | **exit 0** — `2531 passed, 44 skipped, 37 warnings in 91.69s` |
| map.md lockstep | `bash scripts/check_map_md.sh` (real staged set) | **exit 0**, with the same negative control: unstaging `crates/repark-spark/src/map.md` → `ERROR: … was not updated in this commit`, `MAPMD_EXIT_NEG=1`, restored → 0 |
| Structural manifest | `python3 scripts/check_manifest.py` | **exit 0** |

One incidental fix: `cargo fmt --check` caught a trailing blank line left in `tests.rs` by the
removal of the D-A experiment. Fixed in place; `make ci` green afterwards.

### Files touched by the fix pass

`docs/spark-sql-iceberg-parity.md` (§1 scope + oracle definition + mirror-spelling pointer, §2.1
MT-2, §2.2 residual note, §2.3 DML-3, §3 ID-1 wording, §6 boundary + exact-spelling grammar, §7
BL-1, §8 preamble) · `STATUS.md` (boundary paragraph + both bullets) ·
`crates/repark-spark/src/tests.rs` (REF-2 dynamic-span assertion + doc comment; MT-2 six-form
loop) · `crates/repark-sql/tests/cross_door.rs` (discriminating refusal needle) ·
`python/repark/tests/test_parity_live.py` (fail-closed near-miss probe) ·
`briefs/v2-engine-hardening.md` (H-1b amendment) · map.md lockstep:
`crates/repark-spark/src/`, `crates/repark-sql/tests/`, `python/repark/src/repark/`,
`python/repark/tests/`, `briefs/` · this ledger (sweep queue + this section).
