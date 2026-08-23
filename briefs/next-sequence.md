# Slate — the next sequence of work (opened 2026-08-21)

**What this is.** One ordered queue across three open tracks, written because the tracks now
interleave and the order between them is a decision rather than an accident. [../STATUS.md](../STATUS.md)
stays the SSOT for state; this file states **sequence and reasoning**, and each unit still earns its
own `task/<unit>-ledger.md` when it starts.

Rolling slate: a unit leaves this file when it merges, and the file closes when the queue empties.

## Standing rules for every unit below

Restated because a mixed queue makes it easy to assume the previous campaign's contract carried:

1. **Reproduce first.** The behaviour is demonstrated on this tree before anything is edited.
2. **Write the pin and watch it go red.** A pin that was never red proves nothing.
3. **Measure against the oracle, including the incidental controls.** A green pin asserting a
   divergence as parity is the most expensive wrong test in the repo.
4. **Gate alone.** `make preflight` runs by itself and its own exit code is read immediately.
5. **`map.md` in lockstep, in the same commit.** Not a follow-up.
6. **One group at a time**, manual PR, owner merges.
7. **Pickup ritual first, departure edit last.** First act of a unit: fetch, confirm the prior
   unit's PR merged and that the local base carries its departure edit, `make ledger-archive`
   (files the prior unit's ledger; zero tokens), run the drift checks (`make check-map-sync`,
   `make check-ledgers`), and compact the context docs **against the just-merged delta only**,
   as a docs-only first commit. Last commit of the unit: the departure edit to this file, STATUS
   trued up for what this unit changed and nothing else, the unit's ledger `move`d from
   `task/ledgers/staging/` to `completed/`, `map.md` in lockstep.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| 1 | **RP-1** | Iceberg | — | S |
| 2 | **MW-6** | Iceberg | RP-1 | S/M |
| 3 | **MW-7** | Iceberg | MW-6 | M |
| 4 | **MW-8** | Iceberg | MW-6, MW-7 | S |
| 5 | **V3-2** | Format v3 | RP-1 (MW closed) | S |

**V3-1 merged as [#203](https://github.com/TRO-Wolf/repark/pull/203)** and left this file.
**PYC-1 merged as [#204](https://github.com/TRO-Wolf/repark/pull/204)** and left this file (the
rolling rule): 35 nested defs lifted across the two DataFrame modules plus `_emit_side`, the two
EXCEPTIONS rows deleted.
**PYC-2 merged as [#207](https://github.com/TRO-Wolf/repark/pull/207)** and left this file:
12 lifts + 2 pragmas across the remaining ten shipped files, plus `session_core.probe`
under `if`. Ten nested-def EXCEPTIONS rows deleted.
**PYC-3 merged as [#208](https://github.com/TRO-Wolf/repark/pull/208)** and left this file:
`spark/merge.py` `_Clause` and the four `_csv_smart.py` records are Pydantic v2
`BaseModel`; those two DATACLASS_EXCEPTIONS rows are deleted, not zeroed;
`pydantic>=2.10,<3` is the wheel's second hard runtime dep.

**PYC-4 merged as [#209](https://github.com/TRO-Wolf/repark/pull/209)** and left this file:
20 parity dataclass files → `BaseModel`; nested-def EXCEPTIONS emptied (lifts +
pragmas); dual-wire stays a dataclass because the gate runs as bare `python3`;
the ten unannotated returns in `test_compare.py` are annotated and the ANN
ignores are split.

**PYC-5 merged as [#211](https://github.com/TRO-Wolf/repark/pull/211)** and left this file:
hook re-measured n=5 median **0.996 s** (max 1.011 s) over 164 files and dropped
from pre-commit (stays in `make ci` + CI); facade tests no longer ignore ANN201;
dual-wire dataclass row stays the sanctioned leftover.

**PYC-6 merged as [#216](https://github.com/TRO-Wolf/repark/pull/216) and left this file:** public-docstring presence
(`D101`/`D102`/`D103`/`D105`/`D107`) armed with a seeded ratchet (136 findings /
39 files, tests excluded) in `scripts/check_docstring_presence.py`; style `D`
declined permanently. The dual-wire dataclass leftover and the D-presence
EXCEPTIONS table are remaining debt, not sequenced work.

**A13 merged as [#217](https://github.com/TRO-Wolf/repark/pull/217) and left this file:**
`register_memory_catalog` uses the supplied warehouse as the location-less CTAS
fallback root, so two sessions with different warehouses no longer share
`<temp>/repark_ctas/<catalog>/<ns>/<table>`. Same warehouse + same names still
share; MW-3 refuse stays on that fallback tree. The dual-wire dataclass leftover
is remaining debt, not sequenced work.

**MW-4 merged as [#218](https://github.com/TRO-Wolf/repark/pull/218) and left this
file:** Glue live merge-on-read compact+expire in the aws-acceptance module
(`testing_mw4_mor_*`, same helper as the always-run memory analog). OD-3 scoped
object-delete is on the warehouse scratch prefix; Glue tables still accumulate.
S3 Tables MOR compact+expire is out of this unit.

**MW-4b merged as [#219](https://github.com/TRO-Wolf/repark/pull/219) and left this
file:** Glue/HMS `table_exists` `DataInvalid` on a two-level namespace no longer
aborts the Spark dotted metadata-table rewrite.

**DL-1 merged as [#221](https://github.com/TRO-Wolf/repark/pull/221) and left this file**
(chartered 2026-08-23 outside the slate, run before MW-5 because it rewrites every ledger
link and conflicts with anything in flight): the ledger bins under `task/ledgers/`,
`scripts/ledger_lifecycle.py` + `make check-ledgers` in `make ci`, the 122-ledger backfill,
`task/roadmap/{mid-term,epic-term}/`, the census eviction.

**DL-2 merged as [#222](https://github.com/TRO-Wolf/repark/pull/222) and left this file**
(stacked on DL-1; chartered 2026-08-23 from the owner's SEPMO architecture note): the ledger
grammar gate `make check-ledger-grammar` in `make ci` — clause rows, `pins:` citations, the
Critic's attestation form — with the measured floor seeded and XML declined. Every unit from
here on pins its `PROVEN` clauses from its tests and files its attestation before the
departure `move`. [#223](https://github.com/TRO-Wolf/repark/pull/223) dual-wired those
guards into `ci.yml` (owner-granted; not a slate unit).

**MW-5 merged as [#224](https://github.com/TRO-Wolf/repark/pull/224) and left this file:**
campaign close. The MW-0 1,000-row / ten-MERGE demo is re-run and pinned (delete files 1→10
then compact 10→1, data files →1, `COUNT(*)` 1,000 `int64`, live names and CTAS identity,
expire needle). STATUS scorecard; guide lockstep; design and slate archived at
[../docs/history/iceberg-maintenance-wave/](../docs/history/iceberg-maintenance-wave/README.md).
**DL-3 merged as [#225](https://github.com/TRO-Wolf/repark/pull/225) and left this file**
(chartered 2026-08-23 outside the slate, from an agent's report that the 2026-08 archive
month map cost ~13k tokens to read): archive month maps condense to one line per ledger
(`_condense_row`, the 2026-08 migration 55.5 kB → 29.3 kB, the off-the-read-path note).

**2026-08-23 — the owner set the v1.0 north star: full production-grade format-v3**
([task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)).

**Owner-chartered 2026-08-23:** the post-MW remainder is sequenced. **RP-1 first**
(the fork batch the intake treated as future has landed: F-0 `#214`, F-1 through
`#213`, F-2 `#215`; engine pin is 20 commits behind fork `main`). Then **MW-6**
`rewrite_manifests`, **MW-7** scale measurement, **MW-8** the Airflow-shaped
runbook, **V3-2** create-v3 opt-in (first format-v3 unit on that north star, after
the fork pin). MW-9 (`MOR-2` / `write.delete.granularity`) stays gated on MW-7's
numbers. S3 Tables MOR (intake "MW-4b") stays owner-gated on OD-3b. DML-A/B/C
and Track A W-0 are not in this queue.

**PYC did not lead originally, despite being freshly measured.** The gate is already armed, so
new Python cannot make the debt worse while it waits — which is precisely the property that
made it safe to schedule behind V3-1 rather than ahead of it. Burning the tables down is
valuable; it is not urgent, and it is the one track in this queue with no user-visible outcome.

**A13 sat last on purpose** because MW-3 already refused the dangerous sweep, so the
exposure was a shared scratch root rather than a deletion. That write-path addressing
is now warehouse-keyed.

---

## PYC — the conventions burn-down

**Context.** `scripts/check_python_conventions.py` is armed and holds the measured debt in two
EXCEPTIONS tables. The tree is green and cannot regress. PYC empties those tables. The rules
themselves are [../AGENTS.md](../AGENTS.md) "Python"; the reasoning and the sanctioned exceptions
are [../.agents/skills/code-quality/SKILL.md](../.agents/skills/code-quality/SKILL.md).

**The campaign invariant, and it is the LRS one:** *no call that worked before returns a different
value.* Every unit here is a refactor of working code that 3,639 passing facade tests were never
written to pin. A change that cannot meet this bar is a behaviour change wearing a cleanup's
clothes and belongs in its own commit with its own justification.

**The two hazards, named before they are hit:**

- **Lifting a closure changes what it can see.** The nested function reads its parent's locals, so
  its real signature is invisible. Lifting it means writing that signature down, and getting it
  wrong produces a function that compiles, passes a smoke test, and reads stale state.
- **`dataclass` → `BaseModel` adds validation that was not running.** It can reject input the old
  container silently accepted. That is usually the bug being found rather than introduced, but it
  is a behaviour change and it goes in the commit message.

### PYC-4 — done (merged #209)

20 `dataclass` files plus nested defs plus 10 unannotated returns under `python/repark-parity`,
and one of each under `scripts/`. Signal handlers / shrink predicate / spy / dual-wire
comparator ended as pragmas; bootstrap factories lifted together. Dual-wire kept as the
one DATACLASS_EXCEPTIONS row (bare `python3`). ANN ignores split:
`python/repark/tests/**` kept ANN201/ANN202; `python/repark-parity/tests/**` does not.

### PYC-5 — done (merged #211)

Tables: nested-def EXCEPTIONS empty; one sanctioned DATACLASS row (dual-wire). ANN:
facade tests dropped ANN201 (isolated count 0); two nested helpers annotated
(ANN202; not what earned the drop). ANN202 stays. Guard: 164 files, n=5 median
0.996 s (max 1.011 s). Hook dropped from pre-commit; dual-wired in `make ci` + CI.

### PYC-6 — done (merged #216): arm the docstring-presence subset, and the declined armings

**Measured 2026-08-22** with the pinned Ruff (`uvx ruff@0.15.22`), check-only, recorded here per
the arming method in [../.agents/skills/code-quality/SKILL.md](../.agents/skills/code-quality/SKILL.md)
"Arming a rule" — a rule measured and declined is written down so nobody re-litigates it from a
fresh `--select` run. The armed baseline config is **clean: 0 findings** repo-wide.

**Owner-ruled 2026-08-22: arm the docstring *presence* rules only.** Full `D` costs 803 findings
(556 facade / 234 parity / 13 scripts). The split the ruling turns on:

- Presence rules — `D103` undocumented function (163), `D105` magic method (57), `D102` public
  method (27), `D107` `__init__` (17), `D101` class (2) — ≈266 findings, and they enforce the
  conventions' "every function has a docstring" rule mechanically.
- Style rules — `D401` imperative mood (193), `D202`/`D205`/`D413` blank-line shape (289),
  the rest — are churn on a facade whose docstrings deliberately mirror upstream PySpark's own
  text. **Declined permanently, same ruling.**

The unit: select the five presence rules, re-measure at execution time (the 266 is today's
count, not the seed), seed the ratchet table from that measurement with per-file rows and
reasons (ceilings down only), keep the tests per-file ignore, and ship the gate with
provocation proofs per [../docs/testing.md](../docs/testing.md).

**Measured and declined, with the reasons:**

- **`PL` (1068: 738 facade / 326 parity / 4 scripts).** `PLC0415` import-outside-top-level (416
  facade) is the sanctioned lazy-import pattern — heavy imports moved into functions to keep
  parse/startup fast. `PLR0124` self-comparison (26) is the `value != value` NaN check (verified:
  `spark/_csv_smart.py:268`, `spark/dataframe/core.py:5157` and siblings) — flagging it would flag
  correct code. The `PLR09xx` complexity counters are refactor *indicators*, not gates.
- **`A` builtin shadowing (75, all facade).** The facade faithfully mirrors PySpark's API, which
  shadows `filter`/`type`/`id`/`format` by upstream design; renaming breaks the drop-in contract.
- **`print()` ban.** The four `dataframe/core.py` sites implement `df.show()`'s stdout contract
  (Spark itself prints there); the parity CLIs (`compat/redact.py`, `compat/runner.py`,
  `compat/compare_reports.py`) own their stdout as their interface.

**Standing constraint for every PYC unit:** `dataframe/core.py` sits ~14 lines under its 6880
`check_lib_py` ceiling and `ml/feature/_transformers.py` ~67 under 2800 (per the EXCEPTIONS
comments — re-measure before relying on either number). A unit that adds lines there budgets the
next split or the ratchet-raise reason first; it does not discover the ceiling at commit time.

---

## MW-5 — done (merged #224)

Registry close as a pointer: MW-1/MW-2 closed the schema gaps as columns; remaining
rows stay MOR-2, ORPHAN-1, ORPHAN-2, B-MOR-3 (`MOR-1` retired at RP-1). The MW-0 demo is pinned.
Design and slate are in
[../docs/history/iceberg-maintenance-wave/](../docs/history/iceberg-maintenance-wave/README.md).

**Post-MW remainder, owner-chartered 2026-08-23** (intake remains the evidence
home: [../task/roadmap/mid-term/roadmap-intake-2026-08-23.md](../task/roadmap/mid-term/roadmap-intake-2026-08-23.md);
fork queue: [../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)).
The intake's "MW-6 now" line is stale: F-0 and F-2 landed fork-side after it
was written, so **RP-1 leads**. MW-9 is not in the table — MW-7 decides whether
it is urgent.

### RP-1 — fork repin (this unit)

Re-pin `[patch.crates-io]` `iceberg*` to current fork `main`
(`5e7b2e4f8fcb`, 20 commits past `0c5fd58`). Family (`datafusion` /
`datafusion-spark` / `arrow*` / `parquet` / `rust-toolchain.toml`) does **not**
move — the fork did not change its DataFusion base. Standing repin duties
(Catalog trait re-enumeration, metadata-projection shim criterion, two
emptiness pins, IcebergSchemaProvider name-directory freeze) plus the landed F-item flips:

- **F-0** (`#214`, behaviour change): `Operation::Replace` in both files-exist
  conflict guards. Engine follow-up: `write.merge.isolation-level = snapshot`
  **is** exposed (drops `validate_no_conflicting_data`); pin that arm against
  the gap F-0 closed, either way.
- **F-1** (breaking default, floor 2 → 5): flip
  `call_mor1_compacts_below_sparks_min_input_files_floor` to equality, retire
  `MOR-1`, check no engine test leaned on two-file compaction.
- **F-2** (`#215`, additive; `CleanupReport` is `#[non_exhaustive]`): emit
  Spark's six `expire_snapshots` columns from the fork's typed views; retire
  `ExpireCounts::tally` / the pre-expiry walk.
- **F-8a:** retire the `a$b` "unresolvable" residue note; the ADR-0006
  enumeration filter **stays** (`table_names` still synthesizes).

Do not wait for open fork **F-3** (`#216`). Do not mix this with MW-6 (handoff
§5: one engine repin per landed batch, never a passenger).

Ledger: [../task/ledgers/staging/rp-1-fork-repin-ledger.md](../task/ledgers/staging/rp-1-fork-repin-ledger.md).

### MW-6 — `CALL system.rewrite_manifests`

Engine-only. Fork action is already there (R100 ✅). F-4 answered: counts from
the new snapshot summary, `manifests-replaced` → `rewritten_manifests_count`,
`manifests-created` → `added_manifests_count` (fork-pinned keys). Oracle-pin
Spark's result schema (`rewritten_manifests_count:int`,
`added_manifests_count:int`) from the 1.10.0 jar constant if the 4.1.2 oracle
cannot execute it (Q5 precedent). `spec_id` refuses loud (no fork filter);
`use_caching` is a documented no-op. First missing procedure operators run
after every MOR merge.

### MW-7 — scale measurement (measure-only)

A partitioned v2 table, 1e7 rows, 100 MERGEs touching ~2 % of rows each, MOR
and COW legs: delete files, manifests, manifest-list size, `COUNT(*)` and a
predicate scan p50/p99 per 10 merges, then the full maintenance sequence and
the same scans again. Peak RSS. Numbers planning-side like P-2; ratios over
absolutes on this box. **Gates MW-8's defaults and decides whether MW-9 is
urgent.** No product change.

### MW-8 — the maintenance runbook

Docs + one executable local-catalog test of the Airflow-shaped sequence:
merge → `rewrite_position_delete_files` → `rewrite_data_files` →
`rewrite_manifests` → `expire_snapshots` → `remove_orphan_files` dry-run →
armed. S3 Tables conflict-retry guidance folded in; defaults from MW-7.
No engine change.

### V3-2 — create v3 tables behind an explicit opt-in

Lift the CREATE/CTAS `format-version = 3` refusal behind an explicit opt-in;
the default stays v2 until V3-3 lands, because a v3 table this engine cannot
do row-level writes on is a trap. MW is closed, so the "wait for live-catalog
evidence" hold is gone. V3-3 (DV writes) is the large unit after that;
`V3-LINEAGE-1` / `B-MOR-3` stay fork-blocked for maintenance.

---

## A13 — done (merged #217): warehouse-keyed CTAS fallback

Roadmap item in [../task/roadmap-intake-2026-08-21.md](../task/roadmap/mid-term/roadmap-intake-2026-08-21.md),
surfaced by MW-3. `register_memory_catalog`'s location-less fallback root is now the
supplied warehouse (`{warehouse}/repark_ctas|repark_ansi_ctas/…`). Two sessions with
different warehouses no longer share a directory. Same warehouse + same names still
share; `remove_orphan_files` still refuses that fallback tree (table location, CALL
`location`, parent prefix, `file://` aliases). Ledger:
[../task/a13-shared-ctas-fallback-ledger.md](../task/ledgers/archive/2026-08/2026-08-23-a13-shared-ctas-fallback-ledger.md).
