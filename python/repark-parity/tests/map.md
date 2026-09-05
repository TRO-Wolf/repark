# map — python/repark-parity/tests

CC-4 (2026-08-30): CAP-1 mirror tuples ratchet down only
(pins: cc-3-comment-condensation/C-009). analyzer.rs 1194→1161; datetime.rs 1783→1709;
dynamic_flatten/tests.rs 1469→1443→1442; declared_sorted.rs 1381→1348.

CC-3 (2026-08-30): comments condensed to one line; banners removed. CAP-1 mirror tuples ratcheted with each slice, including the Python binding files. D-001 catalog.rs 1845→1843; TA kernels 2284→2098 / 1676→1578 / 1873→1821. Spark CAP-1 rows ratcheted; session_timezone.rs retired at 891. Rust exception count 39→38. Router comment-pin expected string restored byte-exact (pins: cc-3-comment-condensation/C-003, C-004).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

## Purpose

Unit tests for the parity comparison core **and the dataset generators** (no Spark, no
JVM, no repark required). See [../map.md](../map.md).


Docstrings here are one line each: `check_docstring_presence` (D101/D102/D103/D105/D107)
requires one, and nothing may say more. Reasons live in this map, not in the source.


**PERF-DYNFLATTEN-1 round 4:** `test_dynflatten_bed.py` pins the isolation shapes
(`*_nonull`, `cartesian_legs_only`, `cartesian_tags_only`) as flagged and separate from the
headline set, and pins the ranking contract: candidates are ranked by isolated cost and queued
only above 3x the measured noise floor, so a wide floor queues nothing.
pins: perf-dynflatten-1-measure/C-001, C-003

## Contents

- `test_ex_0_example_coverage.py` — **EX-0 (2026-08-31):** the v0.7 example-drift
  gate: five-family enumerator, uncovered / stale-backlog / covered-in-backlog
  reds, backlog and exceptions baselines, COVERS-must-be-used, seed `COVERS`,
  cloud exceptions, nonzero example exit, Makefile `make ci` + ci.yml dual-wire
  + wheels.yml `python -I … --require-execute`; F.* includes installer
  `__all__` mutations (`try_*`, `zip_with`, xpath); backlog pins are
  campaign-true since 2026-09-01 (baseline is a `<=` direction ratchet in
  lockstep with the file — the ex-2 ledger's blocker section records the
  ruling). pins: ex-0-example-drift-gate/C-001,
  C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
  **EX-1 (2026-08-31)** widens the same file: the ten-family enumerator at 913
  names, the `CLASS_SURFACES` / `MODULE_SURFACES` tables, hard-error shape
  drift, the live-`__all__` door roster, `Window.*` on its class root versus
  `WindowSpec.*` / `Column.*` / `Row.*` on a repark-rooted local, a `types.*`
  cover refusing the `ml` door and the reverse, and every new name in the
  backlog at baseline 892. The no-dynamic-registration assertion reads the
  gate's own `*_SOURCE` constants, so a moved facade file reds the test instead
  of leaving it pointed at a path that no longer exists.
  **EX-IDIOM (2026-09-01)** adds the session-root parity pin (`ex-idiom/C-001`,
  cited in prose because no `ex-idiom` ledger exists and grammar rule B reds an
  unresolvable `pins:` line): `ReparkSession` and `SparkSession` bind the
  session covers identically. The gate's `SESSION_BUILDER_HINTS` has carried
  both spellings since EX-0, and the examples construct sessions as
  `repark = ReparkSession.builder…` (owner ruling, 2026-09-01).
  pins: ex-1-class-surfaces/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008
  **EX-17 (2026-09-04):** `test_ex_1_every_new_name_is_in_the_backlog` accepts a widened name that an
  example now covers (backlog OR covered); the first `Column.*` batch was the first to cover one.
- `test_plan_1_northstar_fnp_sequence.py` — **PLAN-1 (2026-08-28; tree pins):** the guarded
  North Star sequence, F-17's measured shared-Puffin closure request, the live slate, the
  per-unit FNP remaining order (FNP-7a/7b delivered 2026-08-31; remaining FNP-9/10 → FNP-8
  → FNP-11/12 → FNP-Z) and delivery boundary, FNP-Z retirement, fork independence, and map
  lockstep, including the archived V3-3 and F-rp3-c7 record. **V3-8:** STATUS Next is
  row-lineage carry complete on every served DML shape.
  (pins: plan-1-northstar-fnp-sequence/C-001, C-002, C-003, C-004, C-005, C-006;
  v3-3-dml/C-003; v3-4-serve-lineage-columns/C-010; fnp-7-try-inversions/C-016;
  v3-5-dv-compaction/C-006; rp-6-fork-repin/C-006; v3-8-subquery-where-lineage/C-003).
  **DOCS-1 (2026-09-02):** this pin and `test_v3r_1_rulings.py` are the gate the post-merge
  truth-up re-reads — STATUS's `Next:` sentence, the north-star §3 rows and the ledger bins
  they cite all moved in that pass and both stayed green
  (pins: docs-1-truth-up/C-001, C-002, C-003, C-004, C-005, C-006).
  **V3-9 (2026-09-02):** the STATUS `Next:` sentence it reads now names lineage carry **and**
  merge-on-read as complete. pins: v3-9-mor-predicate-dml-dv/C-005
  **API-REVIEW (2026-09-02):** the north-star §3 gate paragraph this pin reads is what calls for
  an API review, and [../../../docs/design/v1-0-api-review-2026-09-02.md](../../../docs/design/v1-0-api-review-2026-09-02.md)
  is that packet: 35 surface rows whose inventory and coverage are `scripts/check_example_coverage.py`'s
  own output (913 names, 67 covered), whose door rows are read from `repark_common::surfaces::ALL`
  and both door matrices, whose residual column maps all 81 open divergence-registry rows, whose
  recommendation follows one stated rule set, and whose §5 quotes what a `yes` would bind — the
  gate paragraph itself is untouched by that unit
  (pins: api-review-packet/C-001, C-002, C-003, C-004, C-005).
  **API-FREEZE (2026-09-02):** that same gate paragraph now carries one appended line — "API
  review answered 2026-09-02 (packet); the freeze lands with the tag" — and this pin stayed green
  across it. pins: api-freeze/C-002
- `test_api_freeze.py` — **API-FREEZE (2026-09-02; release 2026-09-03: the STATUS pointer now names the cut tag, not the waiting gate):** the v1.0 freeze pin. Holds three things
  at once: every packet row's `decision` equals its `recommend` (15 YES / 15 YES-except / 5 NO,
  dated 2026-09-02, the owner's rule sentence byte-equal in packet, inventory and
  `docs/release.md`); the checked-in register
  [../../../docs/design/v1-0-api-freeze.json](../../../docs/design/v1-0-api-freeze.json) equals a
  fresh `scripts/build_api_freeze.py` build of the tree, so an unrecorded surface move is red; and
  the additive rule holds in both directions. Ten mutations over a scratch copy of the enumerated
  sources — **8 red** (a frozen member's `def` renamed private; a frozen callable's required
  parameter renamed; a door surface flipped Tested → `absent`; a frozen conf-key literal, error
  class or packaging literal dropped; a packet decision that stops equalling its recommendation; a
  surface id that no packet row claims) and **2 green by design** (a new public member, a new
  optional parameter). The five `NO` rows carry `frozen: false` and no member, so nothing about
  them is checked. Regenerating the register is the sanctioned move for an intended additive
  change: `python3 scripts/build_api_freeze.py --write`, in the same commit.
  The unit's docs half rides the same file: the release policy section, the discharged facade
  ruling, the north-star line and the STATUS line are each asserted here, so a docs rollback
  is red. (pins: api-freeze/C-001, C-002, C-003, C-004)
- `test_pr_247_owner_ruling.py` — **PR #247 revalidation (2026-08-27):** the owner-ruling blocks
  in `AGENTS.md` and `CLAUDE.md` stay byte-exact, unique, at the document start, and in regular
  files; one-byte drift, malformed or missing files, relocation, duplication, and symlink
  redirection fail closed. The review-held enforcement boundary stays exact, unique, and adjacent
  to the ruling. The attribution-blind density gate stays absent. CAP-1's exact-baseline Rust and
  Python source gates remain wired. No source-comment sweep belongs to this unit.
  `pins: pr-247-revalidation/C-001, C-002, C-003, C-004, C-005, C-006, C-007`
- `test_proc_1_tiered_review.py` — **PR-244 revalidation:** current source and map guards, tiered
  SEPMO `review_profile` and `critic_engine` bindings in `binding-manifest.md`, MW-6 evidence,
  disk guidance, clause pins, and ledger lifecycle. B-MOR-3 (2026-09-03): the handoff F-7
  acceptance pin reads the retired refusal pin's zeros replacement.
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_pr_245_revalidation_record.py` — PR #245 source-size ratchets, frozen SQP-1 artifacts,
  bounded parser guards, exact literal-helper inventory, and lifecycle-aware navigation.
  H3-SPILL-1 (2026-09-05): the literal-helper inventory gains
  `bench/spill/cell_worker.py` `sql_string_literal` 1 — the spill harness escapes its own
  warehouse path into `CREATE NAMESPACE … LOCATION`, so it uses the helper rather than a
  second escape rule. pins: h3-spill-1/C-001
- `test_cap_1_source_file_line_cap.py` — **FN-FIX-2 (2026-09-04):** `analyzer.rs` 1161→1142. PERF-FACADE-1 (2026-09-05): `core.py` row 6368 → 6303 with the script baseline. CUTOVER-SCHEMA-1 (2026-09-05): `session.rs` 1040 → 1039 and `repark-python/src/dataframe.rs` 1171 → 1127 with the script baselines; the REG-1 DEC-9 pin follows the row's narrowed rationale. PERF-ICE-CATALOG-IO-1 (2026-09-05): `session.rs` 1039 → 1002 in both tables.
  DF-PRINTSCHEMA-1 (2026-09-04): the `dataframe/core.py` row ratchets 6371 → 6368 with the gate table.
  FN-REGEXP-EXTRACT-1 (2026-09-04): the `functions_expr.py` row ratchets 2261 → 2259 with the gate table.
  pins: fn-fix-2-string-rows/C-002
  **FN-FIX-1 (2026-09-03):** `datetime.rs` 1709→1704, `column/mod.rs` 1105→1102, `functions_expr.py` 2265→2261. pins: fn-fix-1-registry-rows/C-002
  RP-7 (2026-09-02) mirrors the two downward ratchets `write/merge/mod.rs` 1889→1795 and `write/predicate_dml.rs` 1164→1142 (pins: rp-7-f18-repin/C-005). V3-10 ratchets `repark-spark/src/alter.rs` 1831→1830 and
  `repark-iceberg/src/write/alter.rs` 1641→1630
  (pins: v3-10-upgrade-v2-to-v3/C-003). **CAP-1 (2026-08-26):** exact Rust and Python source-size RP-6 ratchets merge/mod.rs 1894→1892 and predicate_dml.rs 1227→1226; V3-8 ratchets predicate_dml.rs 1226→1164 after the lineage helpers move to `predicate_dml/lineage.rs` (pins: v3-8-subquery-where-lineage/C-002).
  (**DML-B 2026-08-30:** `insert_overwrite.rs` tests 1249→1233, `writer_readwriter.py` 1117→1113)
  B-MOR-3 (2026-09-03): ratchets `repark-spark/src/tests/call.rs` 1307→1303 after the
  live-DV refusal and its counter helper are deleted
  (pins: b-mor-3-rewrite-position-deletes-v3/C-002).
  exception sets and baselines mirrored from the live guard tables (DML-A:
  `merge/mod.rs` 2131 → 2086; `call.rs` 1404 → 1111 after
  RP-2's `call_args.rs` split; RP-3 1407 → 1361; **REF 2026-09-01:** the
  `repark-spark/src/ref_ddl.rs` row is retired, 38 → 37 Rust rows, after that file's
  in-module tests moved to a file-backed `ref_ddl/tests.rs`); blank-line boundaries;
  growth, shrink, retirement,
  missing-path, unreadable-path, and empty-scan provocations; fixture exclusions; unchanged
  facade no-stub scope; existing Makefile/CI wiring and contract/navigation carriers. The
  owner correction restores `position_delete.rs` to 1,068 lines with model provenance. The
  production file-size refactor removes `session/_funcs.py` when its exception retires. The
  catalog-registration test split ratchets `session/tests/session.rs` from 1,485 to 1,461 lines.
  DML-C ratchets `session.rs` 1178 → 1177 and `repark-sql/src/tests.rs` 1523 → 1520.
- `test_live_v3_docs.py` — **LIVE-v3-M (2026-09-02; tree pins):** the live v3 legs are documented
  as **measured green** — registry `S3T-V3-1` is FIXED by measurement and carries run
  33635288918, its link, base `8c4bc55`, the `6 passed in 122.13s` line, the accepted branch and
  `exact_commit_counts=False`; the north-star "Live: Glue + S3 Tables v3 legs" row is ✅, names
  the run, both legs and the registry row, and keeps the MW-10 sentence with its links; no
  pending wording ("unmeasured", "nothing has run against AWS", "the first measurement is
  pending", "not yet run") survives in the registry row, the north-star row or the STATUS clause;
  `docs/tier2-aws.md` §6 lists one row per leg, its two v3 rows state the answer, it says the v3
  legs need no new IAM action or workflow variable, and it carries no run id because measured
  state belongs to STATUS; both legs and the local pin exist as real `def`s; STATUS's v3
  **V3-11 (2026-09-02):** the `V3-ROWID-3` meta-pin flips from BACKLOG to FIXED, and three
  more join it — `F-v3-10-partition-file-order` names fork ask **F-20** as RePark's rule rather
  than Spark's, and **RP-8 (2026-09-03)** flips that meta-pin to the closed reading: the ask
  landed as fork `#261`, the residual is FIXED, and the registry must still say the drain buys
  determinism and one rule, **not** parity; `V3-FILEORDER-1` must carry the decoded
  `JavaHashes$StructLikeHash` order, the collision caveat and every measured arm; and the
  retired `DataSourceV2Relation` maintenance-oracle note must appear ONCE (under MOR-1) with
  five pointers, so no row can quietly regrow its own copy of a claim that was false on all
  six.
  pins: rp-8-repin-f21-f22/C-004
  **RP-8 (2026-09-03):** `test_v1_gate_docs.py`'s fork-side meta-pin reads the consumed pin, so
  it moves with the repin — the north star's "Fork side, at the consumed pin" heading and
  `Cargo.toml` both name `c1d6c9de`, and R114's dated cell names F-21 and F-22.
  pins: rp-8-repin-f21-f22/C-006
  **RP-9 (2026-09-03):** the same meta-pin moves to `594bdbe5`; R114's dated cell names F-23.
  pins: rp-9-repin-f23/C-004
  **RP-10 (2026-09-04):** the same meta-pin moves to `85a4aaf0`; R114's dated cell names F-25.
  pins: rp-10-repin-f25/C-004
  **RP-11 (2026-09-04):** the same meta-pin moves to `189a73ed`; `B-MOR-3-FLOOR-1` FIXED.
  pins: rp-11-repin-f24/C-001, C-003
  Two neighbouring meta-pins were repointed when V3-11 compacted STATUS:
  `test_plan_1_northstar_fnp_sequence.py` reads the shortened V3-6 sentence, and
  `test_v3r_1_rulings.py` reads `F-rp3-c7 consumed` from the north-star COW row — the
  artefact's own home — instead of from a STATUS restatement that no longer exists.
  `test_cap_1_source_file_line_cap.py` mirrors the `append.rs` ceiling at its ratcheted 1884.
  pins: v3-11-row-id-determinism/C-003, C-005, C-008
  workstream names the run, carries the `V3-ROWID-3` line and stays under its dual-pinned
  25,000-byte ceiling; registry `V3-ROWID-3` still carries both engines' measured answers and
  names follow-up unit V3-11; and `docs/design/format-v3-track.md` §7's two "not measured" claims
  each carry a dated correction. Whitespace-normalized reads, so a re-wrap does not red it.
  pins: live-v3-aws-legs/C-004, C-005; live-v3-first-measurement/C-001, C-002, C-003
  **V1-GATE (2026-09-03):** the `S3T-V3-1` half gains the confirmation run — the row must say
  `confirmed live 2026-09-03` and name run 33699342417, base `a0fe83a` and `6 passed in
  230.67s`, and the two phrasings of the old "not re-dispatched" note join the stale set.
  pins: v1-gate-audit/C-007
- `test_v3_cov_docs.py` — **V3-COV (2026-09-03; tree pins):** the coverage document, the registry The Step-6 count pin also reads the program count in `task/roadmap/epic-term/map.md` and `docs/design/map.md`.
  and the discharge lines hold one matrix — §1's totals are counted from §3 rather than asserted
  beside it, every DIVERGES row cites a registry row that exists, the six rows this unit filed
  carry a class, the date and their pin, the two fork-routed rows name a TRIGGER, and the north
  star and the v3 track carry the dated discharge; `_read` and `_matrix_rows` are cached, so the
  nine tests parse the document once. `_TOTALS` is the one place the counts live, so
  a re-measurement that moves a total moves this file too. Mutation battery: 9 red of 9
  (ledger §6).
  **Critic remediation round 2 (2026-09-03):** the DIVERGES-cites-a-row pin refused nothing when
  the cited cell was EMPTY (`"" != "—"`, and `" —"` occurs everywhere in the registry), so it now
  requires a non-empty cell and anchors on that row's own `^#+ <row> —` heading; and the Step 6
  pin asserts `_TOTALS`' program count appears in the track's dated line, which is how the stale
  80 survived the first cut.
  **RP-8 (2026-09-03):** `V3-COV-3` was FIXED by the repin, so the TRIGGER pin covers `V3-COV-6`
  alone and a new pin, `test_v3_cov_3_records_the_repin_that_retired_it`, holds the closed
  reading — the date, the fork PR that closed it, the twelve-of-twelve measurement and the
  renamed cell — so a row cannot be quietly re-opened or its evidence dropped. `_TOTALS` is
  unchanged: the nine partitioned rows were re-measured on both engines with the `_row_id` probe
  restored and every verdict held.
  **B-MOR-3 (2026-09-03):** `_TOTALS` moves to 72 EQUAL / 8 DIVERGES with the CALL row's flip,
  and `_CITED` drops `B-MOR-3` — the row is FIXED, not a covered divergence.
  pins: rp-8-repin-f21-f22/C-007
  pins: v3-cov-statement-coverage/C-001, C-004, C-005
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_v1_gate_docs.py` — **V1-GATE (2026-09-03; tree pins):** the v1.0 gate audit is written
  and true. The north star's §3.1 must carry twenty numbered audit rows, every one glyphed ✅
  and none naming a BACKLOG residual; each of the seven rows that has a residual must name its
  registry row, that row's class word and its date, and every `crates/` or `python/` path in a
  pin cell must exist. The three rows the audit re-glyphed (types, encryption keys, DV / delete
  file maintenance) must read `✅ by dated DECLARED …` with their ruling dates, and the
  `rewrite_manifests` row must carry the SCALE-v3 v3 exercise rather than the v2 wiring alone.
  The gate paragraph must carry exactly ONE dated audit line, must say the tag is the owner's
  step and must never claim it. The fork half reads the five 🟡 `GAP_MATRIX.md` rows the gate
  leans on and the pin rev they were read at, which must still be the one in `Cargo.toml`.
  STATUS must carry the SCALE-v3 numbers, the V3-10 / RDF-1 / LOG1P-1 lines, the audit line and
  its 25,000-byte ceiling, and the published gate board must be filed under `docs/artifacts/`
  with a map row naming its sources.
  **Critic remediation (2026-09-03):** the audit is scoped to each row's §3 v1.0-requires cell,
  so the pin no longer forbids the word BACKLOG outright — a row may name one only beside
  `outside the requires cell` — and it now holds the surface-residual table (`RDF-1`,
  `ORPHAN-1/2`, `MANIFEST-1/3` with their classes), row 13's DELIBERATE-by-analogy-to-OD-2 cell
  with its pending owner line, row 3's queue-entry clause, row 17's undated `S3T-1` clause, the
  note that §2 pillar 4's statement coverage is owed rather than discharged, the narrowed gate
  line, and the Step 6 + slate entries that queue it as V3-COV.
  **Round 2 (2026-09-03):** `_RESIDUAL_JUSTIFICATIONS` holds the verbatim §3 requires cell each of
  the two V3-COV residual rows quotes — row 6 "stays opt-in until V3-3; default remains v2" and
  row 9 "full DML including UPDATE/MERGE, round-tripped" — because a paraphrase there had widened
  row 6's ask into one the cell never made. The residual row-id match is a word-boundary regex,
  so `V3-COV-8` no longer satisfies the pin by matching inside a longer id.
  **B-MOR-3 (2026-09-03):** row 13's cell reads the FIXED ruling with the `B-MOR-3-FLOOR-1`
  residue beside it, the owner paragraph reads BUILD, `_SURFACE_RESIDUALS` and
  `_RESIDUAL_ROWS` follow row 13 out of the residual table, and the EQUAL count is 72.
  **RP-11 (2026-09-04):** that residue is FIXED; the surface-residual class is
  `FIXED 2026-09-04 (RP-11)`.
  pins: v1-gate-audit/C-001, C-002, C-003, C-004, C-005, C-006
  pins: b-mor-3-rewrite-position-deletes-v3/C-004
- `test_reg_1_registry_truth_up.py` — **REG-1 (2026-08-26; tree pins):** the divergence registry
  says what the pins prove — DEC-2 / DEC-6 / DEC-7 / DEC-8 carry dated FIXED notes naming #94 / #99
  and their equality pins (C-001); TZ-8 splits into the FIXED `CAST(ts AS DATE)` / `to_date` /
  `datediff` half (#100) and the `last_day` / `date_add` / `date_sub` residual (C-002); G3-E8
  states the delivered spellings (incl. correlated DELETE IN and uncorrelated UPDATE IN) and the
  true remainder (C-003); the three STATUS bullets match the registry under the ceiling (C-004);
  every cited test resolves and DEC-9 stays BACKLOG (C-005); no row deleted and the maps are in
  lockstep (C-006).
  Cycle 2 (Critic): the DEC notes date by the fix's landing day (2026-08-14), and the TZ-8
  residual names only the pinned spellings (`date_sub` refuses too but is unpinned — not claimed).
  Departure: the `date_sub` window ends at the next heading; DEC rows are asserted by their
  heading or FIXED-note opener, not by a bare id.
  C-006's lockstep half asserts the departed state (the ledger listed by `completed/` or the
  archive map), the way DL-5's slate pin turned over — CI caught the in-flight spelling.
- `test_dl_5_contract_compaction.py` — **DL-5 (2026-08-25):** STATUS Current milestone keeps
  the forward path and drops the H-2 wave paste (C-001, C-002); STATUS ceiling ratchets down
  (C-003); engineering-method points at AGENTS.md for invariants and keeps the method
  (C-004, C-005); AGENTS.md keeps the enumerated KEEP set (C-006); no `.agents/roles/`
  (C-007); CEILINGS (d) covers AGENTS.md and the method skill (C-008); DL-4 C-008 still
  holds (C-009); PYC-5 tokens, method how-to, slate #2 (C-010..C-012).
- `test_dl_4_live_doc_compaction.py` — **DL-4 (2026-08-25; 11 tests, incl. the Critic's three
  pinned findings and the two tree pins C-006 / C-008):** the live-document compaction on
  a scratch repository — a merged unit leaves the slate whole and the table renumbers (C-002), a
  closed campaign is cut from STATUS into its history bin with links rewritten (C-003), the
  touched-path set (C-004), the parser's refusals (parametrized) and the coverage check (C-001; a wrapped closed-campaigns
  row is one row; a marker in a code span is prose), and the gate red
  on each of its four classes and green on the compacted tree (C-005).
- `test_v3r_1_rulings.py` — **V3R-1 (2026-08-25; tree pins):** the five owner rulings are
  recorded where the gate reads them — registry `V3-COW-1` (refusal row) and `V3-GEO-1`
  (DECLARED), the queued `V3-VARIANT-SHRED-1`, the north-star matrix rows (COW, types,
  upgrade) and OD-3b, the tier-2 runbook's scoped S3 Tables statement, and the no-obituary
  rule for the unit itself. RP-2 salvage (2026-08-28) retargeted the `V3-COW-1` assertions to
  the narrowed row. RP-3 (2026-08-30) retargeted again: live-DV DELETE merge lifts; UPDATE,
  MERGE, and sequential COW after overwrite stay refused (BACKLOG, 2026-08-25 ruling kept).
  **V3-9 (2026-09-02):** the MOR DML matrix row assertion flipped from "one 🚫, V3-8 measured"
  to "no 🚫, `V3-MOR-1` FIXED plus the dated `V3-DV-1` residual naming fork F-18 and repin
  RP-7", and the STATUS assertions follow the compacted v3 block — including the
  `F-rp3-c7 consumed` substring, which no longer depends on where the line wraps
  (pins: v3-9-mor-predicate-dml-dv/C-005, C-007). **RP-7 (2026-09-02):** the same row now
  asserts `V3-DV-1` FIXED and that STATUS's Known-issues link to it is GONE, so the residual
  cannot be re-opened silently (pins: rp-7-f18-repin/C-003).
  V3-7 (2026-09-02) lifts MERGE; V3-8 (2026-09-02) lifts subquery-`WHERE` DML and the row
  becomes FIXED — the assertions now check the FIXED heading, the discharged ruling, the
  `F-v3-8-update-files` artefact and a 🚫-free north-star COW row
  (pins: v3-8-subquery-where-lineage/C-003; v3-7-merge-lineage/C-003). RP-6 (2026-09-01) lifts UPDATE and sequential
  COW DELETE (pins: rp-6-fork-repin/C-002, C-006). V3-3 (2026-08-30) records the measured keep-refusal: Spark preserves `_row_id`; the engine
  rewrite reassigns (pins: v3-3-dml/C-003). V3-6 (2026-09-01) renames the V3R-1 test to
  `test_v3_geo_1_is_declared_and_shredded_variant_is_rowed_with_v3_6_pins`, retargets the
  `V3-VARIANT-SHRED-1` assertion to the landed §4 row citing the binary-vs-shredded pins,
  and bounds the geo slice before the new section (pins: v3-6-v3-types/C-007);
  ruff-formatted in the same pass.
- `test_dl_2_ledger_grammar.py` — **DL-2 (2026-08-23):** the ledger grammar gate on a scratch
  tree seeded with the script's own `EXCEPTIONS` rows at their ceilings: a clean ledger counts;
  a bad verdict cell, a duplicate id and a row without evidence go red; an unpinned `PROVEN`
  clause and a dead `pins:` citation go red, archived and completed clauses can be cited; the attestation is
  required once no clause is `OPEN` and its shape defects (no artifacts, no justification, a
  missing category, an inconsistent `complete:`) go red; a ledger with no clause table goes
  red; `FINDING:` fields are checked; a raised ceiling or a stale `EXCEPTIONS` row goes red
  against the real tree. Each test cites the DL-2 clause it pins. The DL-1 file's archive-row
  tests likewise cite the DL-3 clauses (the condense rule).
- `test_dl_1_ledger_lifecycle.py` — **DL-1 (2026-08-23):** the ledger lifecycle
  script on a scratch git repository: `archive` moves a `completed/` ledger to
  its dated archive name, rewrites every link to it (fragments kept, code spans
  untouched), re-expresses the ledger's own links, relocates its map row — whole
  into the live bins, condensed to one line (first sentence, `+ `-continuations
  joined) into an archive month map (DL-3) — and stages the lot; idempotent; a ledger not on `main` is left when unnamed (the pickup case)
  and refused when named; `move` to
  `completed`; `archive` is not a `move` target. The provocation proofs of
  `check`: a ledger outside the bins, an archive prefix disagreeing with its
  month, a dead ledger link in a non-map document, and the frozen rule (link
  repair and a prepended errata pass; a prose edit and a deletion fail).
- `test_pyc_6_docstring_presence.py` — **PYC-6 (2026-08-22; the prose-homes pin retargeted to PYC's
  history record by DL-4, 2026-08-25):** five presence
  rules only; style `D` not in py-lint select; tests keep the `D` per-file
  ignore; EXCEPTIONS is 39 keys summing to 136, no `/tests/` path, sorted;
  Ruff pin matches the Makefile; dual-wired `make ci` + ci.yml; on the
  pre-commit hook (conventions stays off).
- `test_pyc_5_close.py` — **PYC-5 (2026-08-22; the prose-homes pin retargeted to PYC's history
  record and its slate copy dropped by DL-4, 2026-08-25):** nested-def EXCEPTIONS empty;
  DATACLASS leftover is dual-wire only; facade tests no longer ignore ANN201;
  conventions guard is not on the pre-commit hook and stays in `make ci` +
  ci.yml.
- `test_pyc_4_parity_harness.py` — **PYC-4 (2026-08-22):** nested-def EXCEPTIONS
  table empty; DATACLASS_EXCEPTIONS is only the dual-wire script; 20 converted
  files import-free of `dataclasses`; every converted `BaseModel` AST-pins
  `extra="forbid"` and not `strict=True`; lifted modules have zero nested `def`s;
  signal-handler / shrink-predicate / spy / dual-wire comparator keep a
  `# nested-def:` pragma *on the def line*; `CensusRow` extra-field refuse + int
  `test_id` refuse; recorded-denominator dummy ids are strings; `repark-parity`
  declares `pydantic>=2.10,<3`; isolated `make py-test` / ci.yml `--with pydantic`
  (C1-Q-001); root Ruff ANN ignores split so parity tests see ANN201/ANN202.
  **PYC-5:** facade ANN201 pin retargeted (ignore dropped).
  Behaviour stays on `test_compare_reports.py` / `test_compat_harness.py`.
- `test_datasets_secrets.py` — **DS-3** secrets fixture: A9 defaults, table-identity
  determinism, manifest class→column coverage in schema order, the needle labels
  re-derived with the `prop_key_is_secret` fold (lowercase, hyphen/dot → underscore,
  underscores stripped for the compact form) **without importing repark**, the
  `bucket_key` `_key` carve-out as a negative control, the hard hygiene fence (every
  value starts with `repark-fake-`; no `AKIA…`/`ghp_…`/`sk-…`/`xoxb-…` shape, no `@`,
  no URL), the one nullable credential column, parquet identity, CSV columns, CLI.
  **Acceptance pin in the module docstring:** reads behave NORMALLY today — the opt-in
  secrets-flagging mechanism is a roadmap feature this fixture predates, so nothing
  here asserts redaction. Facade read pins are DS-4.
- `test_datasets_smartcsv.py` — **DS-3** messy-CSV torture generator: A9 defaults,
  table-identity determinism, both manifest scopes (**column** classes present in
  `small()`; **file** classes provable in the emitted text at 64 rows), the delimiter
  zoo (comma / semicolon / tab / pipe, one file each, byte-equal to `render_csv`),
  BOM + preamble, duplicate header row emitted twice, ragged rows in both directions
  with the short-wins tie (row 137), bool spellings vs yes/no tokens that only look
  boolean, recognized
  vs literal null tokens, currency + decimal width variants, embedded-delimiter
  quoting in every scheme, parquet identity, CLI.
- `test_datasets_manifest_types.py` — **DS-3 rider (from the DS-2 review):** the
  manifest↔schema cross-check over all four labeled families (`schema_inference`,
  `extreme_types`, `secrets`, `smartcsv`). Every manifest-declared type string must
  equal the real Arrow field type after normalizing spacing (`decimal128(24, 21)` vs
  `decimal128(24,21)`) and pyarrow's rendering aliases (`double` → `float64`,
  `date32[day]` → `date32`). Both directions are closed: no manifest row may name a
  column the schema lacks, and no schema field may go unlabeled outside the explicit
  `EXPECTED_UNLABELED` set (`id` in the two DS-2 families). The normalizer is itself
  pinned so a no-op normalizer cannot hide a mismatch, and class ids must be unique.
- `test_datasets_schema_inference.py` — **DS-2** schema-inference generator: manifest
  class→column pin, A9 defaults, `conflict_at` int32→int64 + string/float halves,
  parquet identity, CSV text patterns, CLI `--conflict-at`. **DS-3 rider:** the
  `leading_zero_id` pad width is derived from the requested row count (a fixed `06d`
  loses the leading zero at `row_index >= 1_000_000`, and `MAX_ROWS` is 10M) — pinned
  at the >1M boundary through `leading_zero_width` / `leading_zero_id` with explicit
  widths, never by generating a million rows, plus a helper↔column binding test.
- `test_datasets_extreme_types.py` — **DS-2** extreme-types generator: manifest
  classes, decimal128(24,21), beyond-38 digit strings, uuid5, paragraph length,
  HTML example.com-only, parquet identity, CLI.
- `test_datasets_nested.py` — **DS-1** nested / dynamicFlatten generator: A9 defaults
  (64 / seed 42), table-identity determinism (not raw bytes), parquet + JSON-lines
  re-read under `SCHEMA`, labeled classes (depth ≥ 6, capitalized `Legs`, mixed list
  types, null-typed lists, empty/null list rows), cache symlink + in-repo refuse, CLI
  `--out`. Loads `repark_datasets` via the bench sys.modules loader. Ledger:
  `task/c18-datasets-ledger.md`.
- `test_dynflatten_bed.py` — **PERF-DYNFLATTEN-1** measurement-bed pins: charter
  shapes, dict-encoded capitalized `Name`, 30 % null parents, cartesian sibling
  lists, list widths 1/8/64, parquet round-trip, gate manifest, full-scale skip of
  `list_struct_64`, real-dataset flag/env refuse, in-repo write refuse, the isolation shapes
  kept out of the headline set, and the ranking contract: cost is one fixture's isolated delta
  (never a sum), and a candidate is queued only above 3x the measured noise floor.
  pins: perf-dynflatten-1-measure/C-001, C-002
- `test_compare.py` — equal/unequal frames, order-insensitivity, null handling, schema/row-count
  mismatches, and a **field-nullability difference** (part of the schema signature — a differing
  `nullable` flag with identical name/type/values is a parity failure). **G18 nested invariants:**
  (1) flat-schema `sort_by` path unchanged, (2) nested row-permutation invariance for
  list/struct/map, (3) multiset sensitivity mutation per nested kind, (4) `order_sensitive=True`
  untouched on nested tables; plus list-element-order significance and nested-only schemas.
- `test_compat_harness.py` — C2 / R-PYSPARK-COMPAT unit pins: message-first JVM classification,
  HARNESS narrowness, both denominators (incl. MODULE-TIMEOUT stay-in), `tag_for_pyspark_version`,
  worker env scrub, method-name filter (no prefix steal), `Py4J*` type → NEEDS-JVM,
  `validate_module_short` path-injection refuse, zero-padded tag normalize,
  TimeoutError → MODULE-TIMEOUT (RecordingResult + classify), third-party ImportError → HARNESS
  (incl. site-packages TB + **X1** cache path `repark-pyspark-tests` must not false-FAIL-MISSING;
  **octo C1** site-packages/repark frame + pandas still HARNESS; product `repark.*`
  ModuleNotFoundError + cache path stays FAIL-MISSING),
  unknown status clamp in rank+denominators, module-name dotted IDs, markdown cell escape;
  **C3** `STRETCH_MODULES`/`C3_EXPAND_MODULES` order pin (**U8:** `test_udf` IN);
  **octo C3** `resolve_census_modules` dual-denom pin (`--c3-expand` ignores night-1/`--stretch`);
  C3 series markdown never claims C2 zero-fix /345; ruff-clean import order + format;
  **r20 C4** `C4_EXPAND_MODULES` charter-order pin + `resolve_census_modules(--c4-expand)` dual-denom
  (never blend classic /345 or C3); C4 series/scratch distinct; `_KNOWN_FATAL_TESTS` disjoint
  from C4 cohort + exact-method deselect (not endswith prefix collision); dual-denom markdown %
  pin; filter×fatal dual-denom; testing.utils error rebind pin; PATCH_MAP AssertionError note;
  install_redirect errors-before-factories order; testing.utils rebind via **fake modules**
  (parity isolation — no pyspark/repark); `REPARK_COMPAT_SERIES` worker-only series branding
  pin (no C2 zero-fix on C4 workers; parent ignores leaked env — octo C5); frozen
  `_KNOWN_FATAL_TESTS` exact map pin (octo C6); worker JSON unknown-status clamp.
- **Phase-3 EC-8 additions to `test_compat_harness.py`** (6 tests, new in this repository):
  `CLASSIC_MODULES` charter order + disjointness from both expand cohorts;
  `resolve_census_modules(classic=True)` denominator isolation (hostile `--modules` ignored);
  the fixed precedence `--c4-expand` > `--c3-expand` > `--classic`; the keyword-only default
  that keeps every ported call site unchanged; the **`--stretch` blending pin** (eleven modules,
  not five — the trap `--classic` exists to avoid, documented in executable form); and the CLI
  wiring pin that `--classic` actually reaches the resolver.
  **G-6:** `default_markdown_report_path` pin — markdown defaults under gitignored
  `target/census-reports/` (C-2 alignment), never `task/`.
- `test_compare_reports.py` — the battery for `compat/compare_reports.py`, the census report
  comparator (NEW code; design §6.4). Over synthetic reports: identical → exit 0; each of the
  five delta directions (pass→fail, fail→pass, class-change, appeared, vanished) named and
  exit 1; both denominators re-asserted; deferred subtraction from the baseline side only, with
  the echo, the not-present entry, and the "deferred id appeared in the candidate" finding;
  quarantine excluded both sides; mismatched environment manifests → loud exit 2 **with no diff
  body at all**; `generated_at` / `repark_version` deliberately not gated; sorted-rendering byte
  exactness (a case-only class difference is still a difference); duplicate ids and malformed
  JSON → loud failure; junit mode with skips first-class (skip→pass detected, xfail
  distinguished from skip, manifests required); and the **provoked undeclared subtraction** that
  proves the checked-in ledger file is the only way a row leaves the diff (five plausible
  environment variables set, an `--exclude` flag attempted, the frozen option set asserted).
  Also pins the three properties that keep the manifest gate from being nominal: an external
  `--manifest-*` file may not overwrite a key the report records (the shared-manifest attack that
  makes two different environments render identical); `pandas_version` is refused when it
  **differs** and equally when it is **unrecorded** (a key nobody records compares equal by
  absence); and each report's own recorded denominator block is validated against its own rows —
  including the case where both sides are byte-identical *and* identically wrong, which the
  post-subtraction re-assert alone cannot catch.
- `test_deferred_ledger.py` — **phase-3 PR-5 (EC-4)**: the harness that binds the checked-in
  deferral ledger to the comparator's allowlist. There is exactly one file
  (`task/port/deferred-python-tests.txt`), so byte-identity is pinned by proving the single-file
  property — the comparator's documented acceptance invocation names that path, and its own
  `load_ledger` is what parses it here. Both EC-4 failure directions are closed: every deferred id
  must be a **pin-collected** name (under-subtraction — a row that names nothing removes nothing)
  and must be **absent from the ported tree** (over-subtraction — listed AND ported means a row is
  subtracted from the baseline while still running here). Absence is checked statically via `ast`,
  so the test needs no wheel and runs in the ordinary `make py-test` loop. Also pins that the
  prose ledger names every machine-readable id, so the two halves cannot drift. **PR-5 fixer:**
  a third direction is closed — every deferred id must resolve to a row of the recorded baseline
  JUnit XML *through `load_junit_report`*, the loader the facade cohort's `--junit` gate actually
  uses; the id-space mismatch that assertion catches made the ledger subtract nothing.
- `test_ta_bench_conf.py` — **BH-1 (conductor-19):** default-conf `target_partitions`
  contract for `bench/ta` (omit knob + emit `default`; isolation emits `1` +
  `isolation=single_core`). Helper unit pins + AST scan of the six scripts.
  No engine / numpy / native module.
- `test_w0_window_bench.py` — **W-0:** engine-free pins for the window-shape
  bench (roster equals the charter enumeration, 1e7 unpartitioned constant,
  seeded generator, DuckDB/PySpark pins, thirteen sliding refuses including
  int64 `approx_count_distinct`, fifteen planning-absent names after FNP-7
  removed `try_avg`, result model fields, scratch delete in `finally`, no
  retry on the error path, WIN-SLIDE registry headings).
  **WIN-SLIDE-1 (2026-09-04):** the thirteen moved from `REFUSING_SLIDING_NAMES` (now empty) to
  `RESCANNED_SLIDING_NAMES`; two pins were added —
  `test_every_rescanned_name_has_a_fixed_registry_row` (each name keeps its `WIN-SLIDE-<name>`
  heading, now carrying `FIXED 2026-09-04 (WIN-SLIDE-1)`) and
  `test_the_frozen_sliding_refuse_set_is_empty`.
  pins: w-0-window-bench/C-001, C-002, C-003, C-004, C-007, C-008, C-009, C-010, C-011;
  fnp-7-try-inversions/C-012; win-slide-1/C-008.
- `test_redact.py` — the battery for `compat/redact.py`, the recorded path-redaction transform.
  Its one hard property is that the artifact still parses afterwards, so the two regressions are
  explicit contrasts: a naive text substitution over a traceback-bearing census report emits an
  unescaped quote and stops being JSON, and one over a JUnit XML turns `<scratch>` into an element
  start tag; the parser-based transform cannot do either. Plus key redaction, non-string scalars
  untouched, XML attributes, longest-prefix-wins ordering, malformed-input loud failures, plain
  text passthrough, in-place rewrite idempotence, and the CLI exit codes.

## Pointers

- Up: [../map.md](../map.md)

### PR-4 orchestrator additions
- `test_duplicate_test_id_loads_when_quarantined` / `…_without_quarantine_still_refuses` —
  both directions of the comparator's one duplicate escape (quarantined ids may repeat,
  first row wins), with the fixture recomputing the recorded denominator block over rows AS
  CARRIED — matching the real v1 expand artifact.

### PR-7: --added tests
- `test_added_cell_present_on_candidate_side_only_passes` / `test_added_does_not_subtract_from_the_baseline_side`
  — both directions of the additions mirror (candidate-side subtraction), plus the frozen-option
  pin now includes `--added`.

## Debug

- `test_sqp_1_record.py` is itself byte-frozen by `test_pr_245_revalidation_record.py`.
  `test_cap_1_source_file_line_cap.py` mirrors both size gates' exception tables and their row
  counts: regenerate the tuples in the commit that ratchets a gate, then run this suite
  (`make py-test` — it is not in `preflight`).
| Symptom | First check |
|---|---|
| `test_datasets_manifest_types` reds | A schema field and its `manifest.json` row were edited one-sidedly; the failure names the family and class id |
| `test_datasets_secrets` reds on the hygiene fence | A value stopped starting with `repark-fake-` or picked up a real credential shape — fix the value, never the fence |
| `test_deferred_ledger` reds on "ALSO ported" | A node id is in the txt AND still defined in `python/repark/tests` — excise the test or drop the row |
| `test_deferred_ledger` reds on "absent from the recorded pin collection" | The id does not name a real v1 node; check it against `task/census/baseline-fc3f48102/facade/collected.txt` |
| `test_deferred_ledger` reds on the human summary | `task/port/deferred-tests.md` must name every id in the txt verbatim |
| `test_deferred_ledger` reds on "would subtract nothing" | The id does not survive `junit_node_id` into a row of `…/facade/facade.xml`; check the id form, not the XML (the XML is recorded evidence — never hand-edit it) |

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`.
Escalate to: [../map.md#debug](../map.md).
