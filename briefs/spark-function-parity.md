# BRIEF — the Spark function parity campaign (execution slate)

**Charter:** [docs/design/spark-function-parity.md](../docs/design/spark-function-parity.md).
**Ledger gate:** `task/fnp-0-charter-ledger.md` — no unit opens before that ledger is fully
`PROVEN` and the owner confirms.
**Delivery shape (owner decision, 2026-08-20):** ONE pull request. Every unit lands as its own
commit on one branch, with its own `task/` ledger, so review runs commit by commit.

## Orchestration (standing rules)

These narrow [AGENTS.md](../AGENTS.md) "Delegated-agent standing rules"; they never relax it.

- **SEPMO governs the flow** ([.agents/skills/sepmo/SKILL.md](../.agents/skills/sepmo/SKILL.md)). Severity floor
  **S1**. Every unit runs at least one frontier Actor–Critic cycle; the Critic calls convergence,
  never the Actor.
- **Actor–Critic on Opus** — the owner's standing grant. Review and critic sub-agents stay
  `model: opus`; mechanical fan-out (searches, per-name edits) may go to Sonnet.
- **The orchestrator never merges.** Units go ready-for-review; the owner merges.
- **Green is an exit condition.** Unit gate `make verify`; pre-merge `make preflight`. Real exit
  codes, never a pipe's.
- **One ledger per unit**, `task/fnp-<n>-<slug>-ledger.md`, linked from `task/map.md` in the same
  commit. Ledger presence is a gate item.
- **Disk** — [AGENTS.md "Resource discipline"](../AGENTS.md#resource-discipline--disk-and-artifacts)
  binds every unit: check before the build, re-check at phase boundaries, scoped cleanup, and the
  handoff report.
- **Closed paths for every unit unless the unit names them writable:** `Cargo.toml [patch]`,
  lockfiles, `.github/`, STATUS.md, the divergence registry. The close-out unit owns those.

## The testing contract is binding — every unit, no exceptions

[docs/testing.md](../docs/testing.md) is a hard block, not advice.

- A function is delivered when it is pinned on the **Arrow path** (`collect` / `to_arrow`),
  **value AND type**. `show` output is never evidence — it is a masking surface under the binding
  manifest's `s0_fresh_execution` rule.
- The **entry-point matrix** is the structure: native DataFrame, ANSI SQL, Spark facade. A
  function reachable from more than one door pins on each door it is reachable from.
- A quantified claim pins **per enumerated element**, not by one representative case. "The eleven
  higher-order functions work" means eleven functions × every lambda arity each accepts.
- Divergences from Spark get an honest `_divergence` pin and a registry section. Never silent
  absorption.
- Oracle files are named deliverables per unit. Live-oracle output is recorded verbatim;
  hand-computed expectations are not an oracle.

## Sequencing

Twenty units, one branch, one PR. Each unit is one commit with its own
`task/fnp-<n>-<slug>-ledger.md`. The full roster, scope and rationale is
[docs/design/spark-function-parity.md §7](../docs/design/spark-function-parity.md); this slate
adds the per-unit execution contract.

Correctness first, then free names, then the family the users notice, then the batches:

```
FNP-1  two-door asymmetry        →  FNP-2  free names   →  FNP-3  de-stub what ships
FNP-4a seam + exists  →  FNP-4b Spark-door dialect  →  FNP-4c the eight kernels
FNP-5  wire-only aggregates      →  FNP-6  reuse our kernels  →  FNP-7a try_* (raising paths)  →  FNP-7b BLOCKED on F-Y10-1
FNP-8  repatriation (55)
FNP-9  collections/generators    →  FNP-10 JSON  →  FNP-11 timestamp/TIME
FNP-12 aggregates + formatting   →  FNP-13 collation (G15 retirement)  →  FNP-14 crypto + mask
FNP-15 unreachable: register     →  FNP-16 declared families: register
FNP-Z  close-out (owns the closed paths)
```

Two units need a design pass before they open. **FNP-11** is entangled with the open TZ-4 residues
and TZ-6/TZ-7/TZ-8. **FNP-13** retires registry row G15, which armed refusal sites well beyond the
two function names — enumerate every one of them before writing code. FNP-8 may run in parallel
with FNP-9/10 if the branch stays clean; nothing else may.

## Per-unit contract

Every unit, without exception:

1. **Charter** — the clause IDs from `task/fnp-0-charter-ledger.md` this unit discharges. A unit
   that touches a name outside its clause set is scope creep and trips Invariant V.
2. **Writable paths** — named explicitly. Everything else is closed. `Cargo.toml [patch]`,
   lockfiles, `.github/`, STATUS.md and the divergence registry are closed to every unit except
   FNP-Z.
3. **Rubric result** — LIGHT or STANDARD, recorded. FNP-2 is the only plausible LIGHT unit;
   anything adding a kernel fails criterion 4 (new architectural pattern) or 1 (public interface).
4. **Pins per enumerated element** — see the testing contract above. A unit that adds names to the
   surface inherits the pinning obligation for those names in the same unit.
5. **Oracle** — the live PySpark differential harness (`python/repark-parity`) is the oracle for
   any value claim. Output recorded verbatim. Hand-computed expectations are not an oracle.
6. **Gates** — `make verify` to exit the Actor phase; `make preflight` before the unit is called
   ready. Real exit codes.
7. **Ledger** — `task/fnp-<n>-<slug>-ledger.md`, linked from `task/map.md` in the same commit,
   carrying the GO/deferred table, the findings ledger with dispositions, the coverage attestation,
   and the disk report.

## Unit notes that are easy to get wrong

- **FNP-1** is a correctness unit on a live divergence. Both `to_timestamp` and `avg` need a pin
  that fails before the fix on the *facade* path specifically — a SQL-door pin proves nothing,
  because the SQL door was always right. The 17 latent names each need a decision recorded: closed,
  or registered with the divergence stated.
- **FNP-3** — `datediff` is arg-order only over the existing `date_diff`; do not write a kernel.
  `regexp_extract` over DF's `regexp_match` returns first-match groups only — check that against
  Spark's index semantics before claiming it.
- **FNP-4** — do **not** alias Spark `transform`/`filter` onto `array_transform`/`array_filter`.
  Those kernels declare one lambda parameter and Spark's `(element, index)` form is a hard plan
  error against them. Write one RePark kernel per name declaring `[element, index]`; the index
  closure is lazy, so the unary form costs nothing. `exists` and `reduce` **are** pure aliases and
  `forall` is a rewrite — do not build kernels for those three.
- **FNP-4** — the dialect is named at the Spark door's parse sites. A session-wide
  `sql_parser.dialect` change was measured on 2026-08-20 and fails 8 workspace tests, 6 of them in
  `repark-sql/tests/cross_door.rs`. Registry row BL-2 is re-checked in this unit.
- **FNP-5** — `approx_count_distinct(rsd)` is HLL++ in Spark and plain HLL in DataFusion. That is a
  value divergence to register, not a build to attempt.
- **FNP-8** — `abs` and `concat` are the two named divergence pins (design §4.4). `abs` currently
  plans a `CASE` while claiming `abs(…)` in its SQL text; `concat` carries three spellings of the
  null rule, one of which has no null guard. Both need a pin that fails first.
- **FNP-12** — DataFusion's `to_char` is a false friend. Its own doc states numeric formatting is
  unsupported; Spark's `to_char`/`to_varchar`/`to_number` are numeric format strings. Do not wire
  the name through.
- **FNP-13** is the only unit that *retires* a declared divergence. G15's refusals are not confined
  to `F.collate` / `F.collation`: they cover `repark-spark/src/collation.rs`, the type-position
  guard reached from `execute_passthrough`, `DataFrame.filter_sql`, and facade `AttributeError`s.
  The unit opens with an enumeration of every armed site; a site left refusing after the unit
  closes is an unpinned clause.
- **FNP-14** introduces the campaign's only new workspace dependency. Name the cipher crate in the
  ledger with the reason, run `make audit` and `cargo-deny` before the unit is called ready, and
  ship `mask` independently of it so a dependency dispute cannot block a trivial string function.
- **FNP-16** registers 56 names as deferred-by-cost, which is **not** the same claim as FNP-15's
  unreachable. The registry language must distinguish them; writing "unsupported" for both would
  misreport what the engine can do.
- **FNP-7b must not ship before F-Y10-1.** repark's `int + int` widens to int64 instead of
  overflowing, so `try_add`/`try_subtract`/`try_multiply`/`try_avg` have no raising path to
  invert. Implementing them anyway produces functions that never return NULL and therefore look
  correct while diverging from Spark on every overflow. The dependency is on a tracked open item
  (F-Y10-1 / DEC U5 / G13), not on this campaign.
- **FNP-6d** (the three `bitmap_*_agg`) is sequenced AFTER FNP-7a deliberately: they are UDAFs
  needing Spark's exact 4096-bit bitmap layout, which cannot be verified without a live Spark, and
  they are among the least-used names in the gap. Effort high, confidence low, value low.
- **FNP-Z** — owns the `#[path]` conversion (design R-5) and the `function_dispatch.rs` split into
  `column/dispatch/`. Both are mechanical and both are gate-driven; neither may be done piecemeal
  inside a feature unit.

## What "done" means for the campaign

Every clause in `task/fnp-0-charter-ledger.md` `PROVEN` at campaign scope, `make preflight` green,
the census re-run and its numbers in STATUS.md, and
`set(pyspark.sql.functions.__all__) - set(repark.spark.functions.__all__) == set()` asserted in the
facade suite.
