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
| 1 | **DL-4** — the live documents carry only live state ([charter](../task/ledgers/staging/dl-4-live-doc-compaction-charter-ledger.md)) | document lifecycle | — | M |
| 2 | **V3E-4** — refs + time travel on v3; expiry/orphans with real work | v3 evidence | — (oracle named: pyspark-4.1.2+iceberg-1.11.0) | M |
| 3 | **V3E-5** — the nightly-oracle v3 leg | v3 evidence | the scoped `.github/` grant (below) | S |

**Why DL-4 goes ahead of V3E-4 (chartered 2026-08-25).** A faithful walk of the read path
for a fresh work group costs ~97k tokens before a ledger exists, ~35k of it live signal; the
deficit is closed-campaign diary on `STATUS.md` (a 36 kB "Active workstreams") and merged-unit
obituaries in this file, and it is paid by **every Actor and Critic a unit spawns**, not once
per session. DL-4 moves the diaries to `docs/history/`, makes merged units leave this file with
no residue, and arms a byte ratchet so the two files cannot regrow unnoticed. Landing it first
means V3E-4's agents onboard on the compacted files. No engine code; one script, one gate.

**Lane A — the v3 evidence intake (owner-chartered 2026-08-24).** Five measure-first units
against the north-star matrix
([../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3); none depends on fork work. The owner's three rulings, all dated 2026-08-24:

1. **Lane A is the next sequence.** V3E-1 + V3E-2 merged as
   [#235](https://github.com/TRO-Wolf/repark/pull/235): COW DML on an adopted v3
   table commits and **reassigns** lineage (registry `V3-COW-1`, BACKLOG); Spark preserves
   `_row_id` on DELETE. **Guard-or-not is a second owner ruling on those numbers.** The v3
   maintenance oracle is PySpark 4.1.2 + Iceberg 1.11.0. **V3E-4 is Lane A's next unit** —
   queue #2 behind DL-4 since 2026-08-25.
2. **Table encryption keys are a dated DECLARED exclusion from the v1.0 gate.** Registry
   `ENC-1` ([#235](https://github.com/TRO-Wolf/repark/pull/235)).
3. **A one-time scoped `.github/` grant** for V3E-5 only: add the v3 fixture leg to the nightly
   parity workflow, in its own reviewable PR. No other workflow edit rides it.

The **fork lane runs in parallel and is owner-run** via
[../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
— suggested order F-16 (small; repark's C-011 pin flipping red is the acceptance signal) →
F-13 (the DV write path, gates V3-3) → F-14. Each fork landing returns here as a repin unit
(RP-2, …). V3-3 and later engine units stay owner-sequenced and are **not** in this queue.

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

**RP-1 merged as [#228](https://github.com/TRO-Wolf/repark/pull/228) and left this file:** re-pin `iceberg*` to fork
`main` `5e7b2e4` (F-0 `#214`, F-1 floor 5, F-2 `#215`, F-8a last-`$`; T6
name-directory freeze; Spark `position_deletes` rewrite). Family frozen. Ledger:
[../task/ledgers/completed/rp-1-fork-repin-ledger.md](../task/ledgers/archive/2026-08/2026-08-23-rp-1-fork-repin-ledger.md).

**MW-6 merged as [#230](https://github.com/TRO-Wolf/repark/pull/230) and left this file:** `CALL system.rewrite_manifests`
over the fork's `RewriteManifestsAction`. The counts come from the new snapshot's
summary (`manifests-replaced` / `manifests-created`), because the action returns
none. Two facts the charter did not have, both measured on the 4.0.1 oracle:
Spark rewrites DELETE manifests in a second leg (the fork keeps them — registry
`MANIFEST-1`, and a zero answer refuses rather than reading as "already clean"),
and Spark's default filters to the CURRENT partition spec (the fork's `rewrite_if`
pins that; `spec_id` still refuses — registry `MANIFEST-2`). Ledger:
[../task/ledgers/completed/mw-6-rewrite-manifests-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-mw-6-rewrite-manifests-ledger.md).
**MW-7 merged as [#230](https://github.com/TRO-Wolf/repark/pull/230) and left this file:** the scale measurement, measure-only.
**The charter said 1e7 rows × 100 MERGEs; it ran 1e7 × 50.** A naive rows² projection off the
mandated 1e6 × 10 calibration said 15.6 h; measuring the law at 1e7 instead showed both legs
PLATEAU (merge-on-read ~28 s/merge, copy-on-write ~113 s), which put 1e7 × 100 at 3.72 h on
top of the 0.509 h the calibration and three scaling probes had already spent — over a ~4 h
budget for everything. One rung down the charter's own ladder: 1e7 × 50, projected 1.91 h,
actual 2:09:29 (+13.0 %). Full arithmetic in §1 of the ledger.

What the numbers say. Merge-on-read grows linearly and hard — **+8 delete files, +200,000
delete records, +32 data files, +2 manifests per merge** — reaching **4.18×/4.58×** the
copy-on-write control on the two predicate probes by merge 50 and crossing **2× at 19.6
merges**. The copy-on-write control is flat over the same 50 merges; the gap is delete files
**plus the data-file fan-out** merge-on-read leaves behind (16.3× the control's data files at
merge 50), which this unit does not separate. **MW-9 is urgent**, and the deciding number is
the point probe: **858 → 3,878 ms** for a predicate that returns 0.02 % of the rows, because
`partition` granularity forces open every delete file in every partition it touches — 400
files and 10,000,000 delete records for 2,000 rows returned.

**The Critic's pass (2026-08-24) refuted this unit's first mechanism and it is worth reading
before MW-8 starts.** The delete files that survive the runbook are **not** dangling, the
missing `remove-dangling-deletes` option is **not** why, and it is **not**
`write.delete.granularity` — Spark ends the same sequence at zero delete files at both
granularity settings with that option off. The fork at `5e7b2e4` **defers** Java's
`tooHighDeleteRatio` candidate clause (`DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`), so a correctly
sized data file whose rows are 100 % deleted is never a rewrite candidate and its dead rows are
retained without bound. New registry row **`RDF-1`**, new fork ask **F-16**, characterization
pin `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_survives_the_runbook` (C-011,
written to go RED when F-16 lands). The second finding stands as a disclosure: position-delete
compaction cuts the file count 50× while growing the delete bytes 31 % (F-MW7-2, S3). Ledger:
[../task/ledgers/completed/mw-7-scale-measurement-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-mw-7-scale-measurement-ledger.md).

**MW-8 merged as [#230](https://github.com/TRO-Wolf/repark/pull/230) and left this file:** the Airflow-shaped runbook, docs plus one
executable test, no engine change. The guide gains "The maintenance sequence" — seven numbered
steps, the cadence (every 10 merges, ceiling 20), the load-bearing order, the delete-file
trigger, the step nobody may skip, the day of latency on the orphan net, the S3 Tables retry,
and the five edits a migrating Spark DAG needs. Every number is MW-7 §6's and is cited there,
never re-homed; a clause holds those citations mechanically so the section cannot rot silently.
**The runbook states its own limit:** it cannot reclaim delete-laden data files (registry
`RDF-1`, fork ask F-16), so a cycle leaves a merge-on-read table reading at 2.02× (point) /
2.45× (partition) a compacted control with every answer correct — documented so nobody debugs a ghost. The pin runs one
documented cycle at gate scale and censuses after every step, including the ARMED orphan call
against the 24-hour floor, which MW-3's floor pin does not cover. Ledger:
[../task/ledgers/completed/mw-8-maintenance-runbook-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-mw-8-maintenance-runbook-ledger.md).
**V3-2 merged as [#232](https://github.com/TRO-Wolf/repark/pull/232)** and left this file.
**MW-9 merged as [#233](https://github.com/TRO-Wolf/repark/pull/233)** and leaves this file: honor `write.delete.granularity`
(`file` / `partition`) on RePark-owned MERGE; Spark default `file`; fork SQL
`DELETE`/`UPDATE` still partition-group. Ledger:
[../task/ledgers/completed/mw-9-delete-granularity-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-mw-9-delete-granularity-ledger.md).
**V3E-1 + V3E-2 merged as [#235](https://github.com/TRO-Wolf/repark/pull/235)** and leave this file: adopted v3 COW DELETE/UPDATE/MERGE
contents correct; lineage reassigned (`next_row_id` 3→5/6/7); Spark DELETE preserves
`_row_id`; ENC-1 DECLARED; maintenance oracle `pyspark-4.1.2+iceberg-1.11.0`. Ledger:
[../task/ledgers/completed/v3e-1-2-cow-oracle-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-v3e-1-2-cow-oracle-ledger.md).

**Owner-chartered 2026-08-23:** the post-MW remainder is sequenced. RP-1 led
(the fork batch the intake treated as future had landed). Then **MW-6**
`rewrite_manifests`, **MW-7** scale measurement, **MW-8** the Airflow-shaped
runbook, **V3-2** create-v3 opt-in (first format-v3 unit on that north star, after
the fork pin). MW-9 (`MOR-2` / `write.delete.granularity`) was gated on MW-7's
numbers; **they said it was urgent** (2026-08-24) — the owner sequenced it, and
it merged as #233. V3-3 (DV writes) is the next
format-v3 unit, still owner-sequenced. S3 Tables MOR (intake "MW-4b") stays
owner-gated on OD-3b. DML-A/B/C
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
rows stay ORPHAN-1, ORPHAN-2, B-MOR-3 (`MOR-1` retired at RP-1; `MOR-2` retired at
MW-9 for MERGE). The MW-0 demo is pinned.
Design and slate are in
[../docs/history/iceberg-maintenance-wave/](../docs/history/iceberg-maintenance-wave/README.md).

**Post-MW remainder, owner-chartered 2026-08-23** (intake remains the evidence
home: [../task/roadmap/mid-term/roadmap-intake-2026-08-23.md](../task/roadmap/mid-term/roadmap-intake-2026-08-23.md);
fork queue: [../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)).
The intake's "MW-6 now" line was stale: F-0 and F-2 landed fork-side after it
was written, so **RP-1 led**. MW-9 is not in the table — MW-7 was to decide whether
it is urgent, and on 2026-08-24 it did: yes. Sequencing it is the owner's.

### RP-1 — done (merged #228)

Re-pin `[patch.crates-io]` `iceberg*` to fork `main` `5e7b2e4` (20 commits past
`0c5fd58`). Family frozen. F-0 Replace in files-exist; F-1 RPDF floor 5 (`MOR-1`
retired); F-2 expire typed views; F-8a last-`$` filter. Readiness also froze the
fork's lazy name directory at snapshot (C-011) and added Spark `position_deletes`
rewrite (C-012, schema-only collect refuse). Ledger:
[../task/ledgers/completed/rp-1-fork-repin-ledger.md](../task/ledgers/archive/2026-08/2026-08-23-rp-1-fork-repin-ledger.md).

### MW-6 — done (merged #230)

Wired. The schema was read from the 1.10.0 jar constant AND executed on the live
4.0.1 oracle, which agreed (`5, 1` on five manifests, two non-nullable `int`
columns). The no-op answers two zeros and commits NO snapshot, which is Spark's
rule, not the fork's. The delete-manifest leg is the one thing this engine cannot
do, and it is a registry row rather than a silent partial answer.

### MW-8 — done (merged #230)

The runbook is [../docs/guide/iceberg-guide.md](../docs/guide/iceberg-guide.md) "The maintenance
runbook"; the pin is `python/repark/tests/test_mw8_runbook.py`. Three things the charter did not
have, all from writing it and from the Critic's pass. The **armed** orphan call had no floor pin
— MW-3 pinned the floor on the dry-run form only, and `dry_run => false` is the one call in the
cycle that destroys data. The `RDF-1` residue needs no pathological fixture: the ordinary
documented cycle at 6,000 rows leaves both CTAS files inside Java's bin-pack band carrying 3,600
dead rows through all seven steps. And **`expire_snapshots` needs an explicit `older_than`**
(F-MW8-1): without it the fork falls back to a 5-day time-travel default, three documented cycles
reclaimed nothing, and the warehouse grew 6.00× — the runbook producing the pathology it warns
about. A docs unit needs a clause that reads the SQL it PRINTS, not only the prose around it;
that is C-010. Ledger:
[../task/ledgers/completed/mw-8-maintenance-runbook-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-mw-8-maintenance-runbook-ledger.md).

### V3-2 — done (merged #232): create v3 tables behind an explicit opt-in

CREATE/CTAS `format-version = 3` (Spark) / `format_version = 3` (ANSI) behind
`repark.sql.allowCreateFormatVersion3` (default false). SQL must still request
v3; unspecified create stays v2; ALTER stays refused; `V3-LINEAGE-1` is not
lifted. Copy-on-write MERGE/DELETE/UPDATE on v3 is the existing default and is
not format-gated (V3-3 / V3-4). Ledger:
[../task/ledgers/completed/v3-2-create-v3-opt-in-ledger.md](../task/ledgers/archive/2026-08/2026-08-24-v3-2-create-v3-opt-in-ledger.md).

### MW-9 — done (merged #233): close MOR-2 (`write.delete.granularity`) for MERGE

Honor `write.delete.granularity` (`file` / `partition`) in the merge-on-read
writer. Spark's default is `file`. Fork SQL `DELETE`/`UPDATE` still
partition-group. Independent of V3-3.

V3-3 (DV writes) remains owner-sequenced.

---

## A13 — done (merged #217): warehouse-keyed CTAS fallback

Roadmap item in [../task/roadmap-intake-2026-08-21.md](../task/roadmap/mid-term/roadmap-intake-2026-08-21.md),
surfaced by MW-3. `register_memory_catalog`'s location-less fallback root is now the
supplied warehouse (`{warehouse}/repark_ctas|repark_ansi_ctas/…`). Two sessions with
different warehouses no longer share a directory. Same warehouse + same names still
share; `remove_orphan_files` still refuses that fallback tree (table location, CALL
`location`, parent prefix, `file://` aliases). Ledger:
[../task/a13-shared-ctas-fallback-ledger.md](../task/ledgers/archive/2026-08/2026-08-23-a13-shared-ctas-fallback-ledger.md).
