# Unit ledger — EX-1 · v0.7 example inventory widening (class surfaces)

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when EX-1 merges, or when the
owner closes the slate row.

**Unit:** EX-1 · **Date:** 2026-08-31 · **Model:** opus-5 (1M context), Actor ·
**Branch:** `feat/ex-1-class-surfaces` · **Base:** `feat/ex-0-example-drift-gate`
`e2e81877c16d4bdd922372a958aa1f94e25c29f7`
**Ruling:** owner, 2026-08-31, recorded in
[release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v0.7 — Full example documentation": widen the EX-0 closed set with Column,
Window, WindowSpec, Catalog, the public `types` module surface, `ml`, and Row.
EX-0 measured that widening at "on the order of 120+ names" and left it as an
owner decision; this unit executes it.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `scripts/check_example_coverage.py`; `docs/examples/`
(inventory, backlog, `map.md`); `python/repark-parity/tests/`; this ledger;
lockstep `map.md` files. Closed: `crates/`, `python/repark/src/` product code,
`.github/`, the divergence registry, `Cargo.toml [patch]`,
`briefs/next-sequence.md`, archival bins (`completed/`, `archive/`).

**Extension (orchestrator, 2026-09-01, Critic FIX 2):** one line of
`DEVELOPMENT.md` — the `make check-example-coverage` row, whose seed counts
(763 names / 742 backlog / five doors) went stale the moment this unit landed.
The extension is that row and nothing else.

## Scope

EX-0 shipped the gate over five families (763 names). It documented, but did not
enumerate, ten further surfaces. The owner ruled seven of them INTO v0.7. This
unit adds those seven to the closed set, sends every new name to the backlog,
and moves `BACKLOG_BASELINE` to the new true count. It writes no examples: the
per-name backfill stays the sibling lane's work.

## Surface audit (measured 2026-08-31, before any script edit)

Every canonical source was located by reading the tree, not by guessing a path.
`repark/spark/sql/types.py` and `repark/spark/sql/window.py` are `sed`-swap
re-export shims (`__all__` copied from the canonical module, `is` identity), so
they are **not** canonical; the canonical home is `repark/spark/types.py` /
`repark/spark/window.py`. `repark/spark/session/catalog.py` is a one-line
re-export binding and holds no `Catalog` class; the class lives in
`repark/spark/catalog.py`.

| Family | Surface | Canonical source | Enumeration rule | Spelling | Count |
|---|---|---|---|---|---|
| `column` | `Column` | `python/repark/src/repark/spark/column.py` | public members of class `Column` | `Column.<name>` | 40 |
| `window` | `Window` | `python/repark/src/repark/spark/window.py` | public members of class `Window` | `Window.<name>` | 14 |
| `window` | `WindowSpec` | `python/repark/src/repark/spark/window.py` | public members of class `WindowSpec` | `WindowSpec.<name>` | 8 |
| `catalog` | `Catalog` | `python/repark/src/repark/spark/catalog.py` | public members of class `Catalog` | `Catalog.<name>` | 28 |
| `types` | types module | `python/repark/src/repark/spark/types.py` | module `__all__` | `types.<name>` | 28 |
| `types` | `Row` | `python/repark/src/repark/spark/row.py` | public members of class `Row` | `Row.<name>` | 4 |
| `ml` | `ml` package | `python/repark/src/repark/spark/ml/__init__.py` | package `__all__` | `ml.<name>` | 28 |
| | | | | **total** | **150** |

The seven counts reproduce EX-0's independent measurement of the same surfaces
(Column 40, Window 14, WindowSpec 8, Catalog 28, Row 4, `types.__all__` 28,
`ml.__all__` 28), so the widening is exactly the set the ruling names.

**Family grouping.** `Row` joins the `types` family rather than taking a family
of its own: PySpark homes `Row` in `pyspark.sql.types`, and the `session` family
already mixes a module name (`repark.sql`) with class names, so the precedent
exists. The other four families are one surface area each.

**Skip rule** is EX-0's, unchanged: names that start with `_`, and every dunder.
This keeps `Column.__add__` / `__eq__` / `__and__` out of the inventory, and it
keeps `Column.sql_expr_part`, `Column.for_select`, `Row.from_mapping` and the
other engine-facing but underscore-free members **in** — they are public by the
gate's one rule, and a rule with per-name judgement is not a gate.

**Dynamic registration (checked explicitly, requirement 5).** `F.*` needed the
installer union because `install_into` mutates `functions.py` `__all__` at
import; that omission is what reddened PR #292's wheels job. None of the seven
new surfaces does anything of the kind: `column.py`, `window.py`, `catalog.py`,
`row.py`, `types.py` and `ml/__init__.py` contain no `setattr`, no
`globals()[…] =` and no installer call, and `types.__all__` / `ml.__all__` are
static list literals. The one dynamic thing nearby is `repark/spark/__init__.py`
`__getattr__`, a lazy import over a static `_EXPORTS` table — it resolves module
objects, it never adds a name to a class or to an `__all__`. Belt and braces,
the live cross-check that EX-0 runs for `F.__all__` / `ta.__all__` now also runs
for `types.__all__` and `ml.__all__` (C-003), so a future dynamic export table
on a **module door** reds the execute leg instead of hiding. The class surfaces
have no equivalent live check — their guard is this measurement plus a
source-text assertion in the pin file, which C-003 states honestly rather than
claiming the door-level strength for them.

## Orchestrator rulings (build-to)

Extend the existing per-family pattern in `scripts/check_example_coverage.py`:
the `FAMILIES` tuple, the `CLASS_SURFACES` table, the receiver-classification
`COVERS` binding. Do not build a parallel mechanism. Every new name goes to the
backlog; `BACKLOG_BASELINE` moves with the real inventory growth, the way EX-0's
installer union moved it 658 → 742.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-1-charter
  agent: Actor
  action: file the EX-1 charter ledger and lockstep staging map
  charter_trace: C-001
  preconditions:
    - branch feat/ex-1-class-surfaces at e2e8187: SATISFIED (git)
    - owner ruling widens the EX-0 closed set with seven surfaces: SATISFIED (roadmap)
    - each surface has one canonical source: SATISFIED (surface audit above)
  success_condition: staging ledger and staging/map.md are committed on the unit branch
  step_risks:
    - re-export shim mistaken for the canonical source: HANDLED(audit reads each file's docstring)
    - dynamic export table missed the way F.* was: HANDLED(C-003 live cross-check)
  contingencies: [grammar red: EXECUTABLE(fix the ledger in the next amend)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The enumerator emits the seven ruled surfaces under four new families (`column`, `window`, `catalog`, `types`, joined by `ml`), each from the canonical source and enumeration rule in the audit table, with the counts recorded there. | Inventory snapshot + family counts + pin on the new totals. | **PROVEN** |
| C-002 | The new surfaces reuse the existing machinery: class surfaces are rows in `CLASS_SURFACES`, module surfaces are rows in `MODULE_SURFACES` read by the same `__all__` reader `ta.*` uses. No parallel enumerator. | Diff shape + pin that each new family's names come from `enumerate_public_surface`. | **PROVEN** |
| C-003 | Enumeration is from static export tables only. No new surface registers names dynamically. For the **module doors** (`F.*`, `ta.*`, `types.*`, `ml.*`) the live `__all__` cross-check makes that mechanical: a future dynamic table reds the execute leg. For the **class surfaces** the guard is weaker and stated as such — the audit measurement plus a source-text assertion that the six files carry no `setattr` / `globals()[…] =` / `install_into`; a class that grows a member at import would not red today. | Audit finding above + `live_all_findings` pin + source-text pin + green `--require-execute`. | **PROVEN** |
| C-004 | A class-surface name binds only on a correctly classified receiver. `Window.*` binds on the `Window` class root; `WindowSpec.*` / `Column.*` / `Catalog.*` / `Row.*` bind on a repark-rooted local. `object().alias` binds nothing. | Red-first: each name is unbound before the change and bound after; stuffing pins. | **PROVEN** |
| C-005 | A module-surface name (`types.*`, `ml.*`) binds only on that module's door alias or on a name imported from that door — the rule `F.*` and `ta.*` already use. A `types.` cover does not bind on an `ml` alias, and neither binds on an unrelated receiver. | Red-first + door-crossing pin. | **PROVEN** |
| C-006 | Every newly enumerated name enters `docs/examples/backlog.txt`; `BACKLOG_BASELINE` moves to the new true count 742 → 892 and stays exact. No name is covered or excepted by this unit. | Backlog length + baseline pin + no new example scripts in the diff. | **PROVEN** |
| C-007 | The gate stays green on both legs: the static half (`make ci`, the `.sh` wrapper) and the wheels execute leg (`--require-execute` against the built native module) exit 0 on the widened inventory. | Quoted exit codes and the final counts line. | **PROVEN** |
| C-008 | No product behaviour changes. The diff touches the gate script, `docs/examples/` data, the pin file, `map.md` lockstep, and this ledger only. | Diff scope. | **PROVEN** |

`LOGIC_SCORE` = **8/8 `PROVEN`**.

## Family counts

Measured 2026-08-31 by `enumerate_public_surface` on this branch.

| Family | Before (EX-0) | After (EX-1) |
|---|---|---|
| functions | 444 | 444 |
| dataframe | 150 | 150 |
| ta | 86 | 86 |
| io | 42 | 42 |
| session | 41 | 41 |
| column | — | 40 |
| window | — | 22 |
| catalog | — | 28 |
| types | — | 32 |
| ml | — | 28 |
| **total** | **763** | **913** |
| covered by examples | 19 | 19 |
| exceptions | 2 | 2 |
| backlog | 742 | 892 |

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch head `e2e8187`, before the script edit. The gate was
green there: `./scripts/check_example_coverage.sh` exit **0**,
`example-coverage: 763 public names (dataframe=150, functions=444, io=42, session=41, ta=86); 19 covered; 742 backlog; 2 exceptions; 5 examples`.

1. **The names are absent from the inventory.** `grep` for `column`, `window`,
   `catalog`, `types`, `ml` family rows in `docs/examples/inventory.txt`:
   **0** matches. Feeding the seven probe names to `coverage_findings` as
   `COVERS` entries returned, one per name,
   `COVERS names Column.alias, which is not in the inventory` /
   `… Window.partitionBy … / WindowSpec.rowsBetween … / Catalog.listTables … / Row.asDict … / types.StringType … / ml.Pipeline …`.
2. **The bindings do not exist.** `cover_is_used` on honest example bodies,
   before the change: `Window.partitionBy` **False**,
   `WindowSpec.rowsBetween` **False** (the spec local inherits nothing from an
   unclassified `Window` class root), `types.StringType` **False**,
   `ml.Pipeline` **False**, `Row.asDict` **False** (the receiver
   `frame.collect()[0]` is a `Subscript`, which the root walker did not
   traverse). `Column.alias` on `column = F.col("a"); column.alias("b")` was
   already **True** — the repark-rooted-local rule EX-0 shipped covers it
   unchanged, which is C-002's point: the class table was the only thing
   missing.

After the change every one of those is **True** on the same body, and the
door-crossing negatives stay **False** (pins in
`python/repark-parity/tests/test_ex_0_example_coverage.py`).

## Binding extensions (the whole behavioural delta)

Three, each the smallest form that carries the ruled surfaces:

- **`CLASS_ROOT_KINDS`** — a class name whose members are called on the class
  itself gets a kind, the way `SparkSession` already did. `Window` is the only
  entry; `SparkSession`'s builder hint keeps its own rule. `Window.*` covers
  bind on that root, and a local assigned from it becomes repark-rooted, which
  is what makes `WindowSpec.*` bind.
- **`MODULE_DOORS`** — the `functions` / `ta` door tables generalised into one
  tuple so `types` and `ml` are rows rather than a fourth and fifth copy of the
  same three branches. `repark.sql` keeps its module-alias-only rule.
- **`Subscript` in `expression_root_id`** — `frame.collect()[0].asDict()` is the
  only honest way to reach `Row`, and the root walker stopped at the subscript.
  It now walks through it, which also retires one of EX-0's recorded round-3
  residuals (indexing did not carry repark-rootedness). It is strictly a root
  walk: it still cannot make an unrooted receiver bind.

Residuals EX-0 recorded that this unit does **not** close, kept honest here:
tuple / starred / augmented assignment and loop-target rebinding are still not
tracked, and a repark-rooted receiver can still list a method it calls only
trivially. Review holds that honesty, as before.

## Gates (2026-08-31, on `edc74c2`)

| Command | Exit |
|---|---|
| `make ci` | **0** |
| `make py-test` | **0** (496 passed) |
| `make check-map-sync check-ledger-grammar` | **0** (169 maps; 9 live ledgers, 84 clauses) |
| `python3 scripts/ledger_lifecycle.py check --base e2e8187` | **0** |
| `./scripts/check_example_coverage.sh` (static half) | **0** |
| `.venv/bin/python -I scripts/check_example_coverage.py --require-execute` | **0** |

Execute-leg counts line, after `make develop` (native module imported, no skip
line, every example executed, every module door's live `__all__` matched):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 19 covered; 892 backlog; 2 exceptions; 5 examples`

**Line-cap collateral.** Adding the tables pushed
`scripts/check_example_coverage.py` over the 1,000-line `check_lib_py` ceiling
in the working tree, and `make py-test` caught it before the commit. The fix is
a shrink, not an exception row: the nine `CLASS_SURFACES` paths became named
`*_SOURCE` constants (the convention `FUNCTIONS_SOURCE` / `TA_SOURCE` already
used), which collapses each row from seven lines to one. Committed line counts,
which is what the record can prove: **929** at `41c6d4c`, **946** from
`edc74c2` on — fourteen surfaces where nine cost 929 lines, still under the
ceiling, and no `EXCEPTIONS` row.

## Critic disposition (2026-08-31/09-01)

Verdict **FAIL**, on record integrity only. The substance held: the enumeration
matched the live wheel exactly, 19 of 23 sabotage probes behaved, the ratchet is
exact, and the red-first states reproduced against the base gate. Two S1
findings, both about what this ledger and the docs *claim*, and six sub-S1
residuals.

| ID | Finding | Disposition |
|---|---|---|
| S1-1 | The line-cap paragraph said 942 lines; the committed file is **946** (`wc -l` and `check_lib_py`'s splitlines metric agree; 929 at `41c6d4c`, 946 from `edc74c2`). The 1,016 intermediate was never committed, so the record cannot prove it. | **REMEDIATED**: count corrected to 946, the uncommitted figure dropped. The conclusion is unchanged — 946 < 1000, no `EXCEPTIONS` row. |
| S1-2 | `DEVELOPMENT.md` still carried the EX-0 seed (763 names, 742 backlog, five doors). | **REMEDIATED** under the writable-path extension above: the row now reads 913 / 892 / 2 across ten doors. |
| R1 | Cross-class leaf conflation. `cover_is_used` matches on the leaf name plus a receiver *kind*, not the owning class, so two owners that share a kind are interchangeable. Measured: 58 multi-owner leaves exist, and **four groups** conflate a new name with an old one, all on `KIND_LOCAL` — `{Column, DataFrame}.alias`, `{Column, DataFrame}.transform`, `{DataFrame, WindowSpec}.orderBy` / `.order_by`, `{DataFrameWriter, WindowSpec}.partitionBy` / `.partition_by`. | **ACCEPTED_FLAGGED**: zero impact today (nothing on these surfaces is covered — every one is a backlog row), but a live hazard for the backfill lane, which will write the first examples. Named in [docs/examples/map.md](../../../docs/examples/map.md) beside the binding rule so the lane meets it where it works. Note what does **not** conflate: the `Column` / `F` pairs (`asc`, `like`, `round`, `when`, …) are split by door kind, and `Window.*` / `WindowSpec.*` by the class root versus a local — both splits are load-bearing and pinned. |
| R2 | `types` as a door alias could collide with the standard library's `types` module. | **ACCEPTED_FLAGGED**: the overlap between `repark.spark.types.__all__` and `dir(types)` is measured **empty**, so a false positive is impossible today. A deliberate one (importing stdlib `types` and calling a name repark also exports) dies on the execute leg, where the example must actually run. |
| R3 | `Window` gets its class-root kind only through `ImportFrom`, not `import repark.spark.window`. | **ACCEPTED_FLAGGED**: the miss direction is safe — it under-binds, so a cover fails as unused rather than binding falsely. |
| R4 | `ML_MODULES` holds only `repark.spark.ml`, so `from repark.spark.ml.pipeline import …` does not bind. | **ACCEPTED_FLAGGED**: identical in shape to `TA_MODULES`, which EX-0 shipped and review accepted; under-binding, same safe direction. The canonical door is the package. |
| R5 | The C-003 test re-hardcoded `catalog.py` / `row.py` paths instead of using the gate's constants. | **REMEDIATED**: the test now reads `gate.CATALOG_SOURCE` / `gate.ROW_SOURCE`, so a moved source cannot leave the assertion pointing at a file that no longer exists. |
| R6 | C-003 claimed "reds the execute leg" for surfaces the live cross-check does not reach. | **REMEDIATED**: C-003 and the audit paragraph now separate the two strengths — mechanical for the four module doors, a measurement plus a source-text assertion for the class surfaces, with the residual stated. |

## Disk

Pickup: 526 GB free of 1.8 TB (70% used). No worktree; the unit works in a
clone. `.venv` + `target/` built once for the execute leg.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml's
python job (`./scripts/check_example_coverage.sh`). Execute half: wheels.yml
smoke `python -I scripts/check_example_coverage.py --require-execute` after the
packaged wheel is installed. EX-1 widens what both legs enumerate; it moves no
wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the seven new surfaces come from their canonical sources and the inventory snapshot must match the walk.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-2
      status: ATTACKED
      evidence: A class-surface or module-surface COVERS name on a wrong receiver is unused and red; the widened backlog is an exact baseline.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-3
      status: ATTACKED
      evidence: A missing class, a missing nested class, or a module with no __all__ raises a hard RuntimeError; there is no silent skip on shape drift.
      artifacts: [scripts/check_example_coverage.py, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface. Example execution, its env scrub and the exceptions ratchet are EX-0's and are unchanged.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the widening is a walk of public names that already exist.
    - id: AT-7
      status: N/A
      justification: The static gate is AST-only; execution stays optional on import of repark._native.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the widened inventory; the walk adds six source files and no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pin file cites C-001 through C-008 of this unit alongside EX-0's clauses.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py]
  reattested: []
  complete: true
```
