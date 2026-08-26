# Charter ledger — REG-1 · the registry says what the pins prove (DEC-2/6/7/8, TZ-8, G3-E8)

**Date:** 2026-08-26 · **Branch:** `docs/reg-1-registry-truth-up` · **Base:** `5f64254` (`main`,
#243) · **Policy:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
§6 "How a row is added, mirrored and retired" (dated FIXED notes, never a silent deletion);
[AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"; the compact-context-docs pickup
ritual (a stale claim in a document agents trust is fixed against the delta that made it stale) ·
**SEPMO path:** STANDARD at base (docs-only; CCC risk tier mechanical → Critic-1 + the claims
Critic) · **Size:** S · **Requested by:** the owner's overnight grant, 2026-08-25 ("get our SQL
parser and query engine in amazing state") — this is the record half of that.

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Why this unit exists.** Tonight's agenda readers (six of them) mis-scoped three units because
the registry and STATUS still describe work that landed weeks ago as open: the DEC-2 / DEC-6 /
DEC-7 / DEC-8 rows read "BACKLOG, intent to FIX" while their pins in
`python/repark/tests/test_decimal128_parity.py` are equalities against Spark (landed by #94 and
#99, `crates/repark-functions/src/decimal_spark.rs`); the TZ-8 row reads "Not FIXED" while
`CAST(ts AS DATE)` / `to_date` / `datediff` ride the session-zone rewrite
(`crates/repark-functions/src/analyzer.rs` `rewrite_timestamp_to_date_cast`, 2026-08-14) and only
`last_day` / `date_add` / `date_sub` over a TIMESTAMP remain (red-on-purpose pin
`crates/repark-spark/tests/session_timezone.rs::last_day_and_date_add_over_a_timestamp_still_refuse`);
the G3-E8 row says "UPDATE IN … remain refused" while the uncorrelated positive
`UPDATE … SET <scalar> WHERE col IN (SELECT …)` executes through `predicate_dml`
(`crates/repark-iceberg/src/write/predicate_dml.rs` `try_allowed_update_in`;
`python/repark/tests/test_dml_subquery_parity.py` identity UPDATE IN, content rows). STATUS
"Known correctness issues" repeats all three. A registry a reader cannot trust costs every
future unit its first hour.

**Out of scope:** any engine change; DEC-9 / DEC-5 nullability (genuinely open — stays BACKLOG);
the TZ-8 residual and B-TZ-3 (engine work, a later unit); the G3-E8 remainder (UPDATE NOT IN /
[NOT] EXISTS / correlated IN; ANY/ALL); deleting any row (§6 forbids silent retirement).

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Registry rows DEC-2, DEC-6, DEC-7, DEC-8 each carry a **FIXED** note per §6 dated by the fix's landing day (2026-08-14, #99; #94 named where the ANSI default is the dependency), naming the landing PR and the equality pin(s) in `python/repark/tests/test_decimal128_parity.py` that prove it; the note is measured: the Actor runs those pins on this tree and records the run; the row's original divergence text stays as history | Tree pin + pin run | **PROVEN** | `test_dec_rows_carry_dated_fixed_notes` |
| C-002 | The TZ-8 row states which half is FIXED (`CAST(ts AS DATE)` / `to_date` / `datediff`, the analyzer rewrite, its pins) and which half remains BACKLOG (`last_day` / `date_add` over TIMESTAMP — `date_sub` was measured to refuse too but is unpinned, so the row does not claim it; B-TZ-3), naming the red-on-purpose pin with its recorded live Spark values; "Not FIXED" is gone | Tree pin + pin run | **PROVEN** | `test_tz8_row_splits_fixed_and_residual` |
| C-003 | The G3-E8 row states the delivered spellings (uncorrelated DELETE IN / NOT IN / [NOT] EXISTS ± correlation / correlated DELETE IN, and uncorrelated positive identity UPDATE IN) and the true remainder (UPDATE NOT IN / [NOT] EXISTS / correlated IN; ANY / ALL everywhere), each side naming its pins; "UPDATE IN … remain refused" is gone | Tree pin + pin run | **PROVEN** | `test_g3e8_row_states_delivered_and_remainder` |
| C-004 | STATUS "Known correctness issues" bullets for decimal128, the session-timezone family and DELETE/UPDATE subquery predicates say the same as the registry (one line of state + link each, per that section's own rule) and nothing else in STATUS changes; STATUS stays under its ceiling | Tree pin + gate | **PROVEN** | `test_status_known_issues_match_the_registry` |
| C-005 | Every test the new notes cite exists at the named path and is green on this tree; every `pins:` citation the unit writes resolves (`make check-ledger-grammar`); DEC-9 still reads BACKLOG | Tree pin + gates | **PROVEN** | `test_cited_pins_exist_and_dec9_stays_open` |
| C-006 | `docs/map.md` (if it lists registry sections), `python/repark-parity/tests/map.md` and `task/ledgers/staging/map.md` in lockstep; no row deleted; §6's form followed (dated note, PR link, pin name) | Tree pin + `make check-map-sync` | **PROVEN** | `test_no_row_deleted_and_maps_in_lockstep` |

## Execution record

**Actor:** SEPMO REG-1, 2026-08-26 — docs-only truth-up, no engine change. Worktree
`docs/reg-1-registry-truth-up` (base `5f64254`). Discipline: MEASURE then WRITE — every note below
was run on this tree before it was written.

**This unit's tree pins (red on the base tree, green after the edits).**
`python/repark-parity/tests/test_reg_1_registry_truth_up.py` — six pins, one per clause. Command:
`PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow --with pytest --with
'pydantic>=2.10,<3' pytest python/repark-parity/tests/test_reg_1_registry_truth_up.py -q`.
- Base tree (registry/STATUS un-truthed): **6 failed** (C-001…C-006 all red).
- After the edits: **6 passed**.

**Per-row behavior-pin runs (the pins each note now cites).**

- **C-001 · DEC-2 / DEC-6 / DEC-7 / DEC-8** — landed by #94 (ANSI default TRUE, U5) and #99 (U4b `/`
  formula, DEC-8 ExprPlanner, DEC-6 checked ± UDF), all in `crates/repark-functions/src/decimal_spark.rs`.
  `cargo test -p repark-spark --lib -- pin_div_same_precision_scale_repark_i128
  pin_overflow_max_decimal38_plus_one_wrong_value_i128 pin_overflow_max_decimal38_plus_one_null_when_ansi_false
  pin_div_by_zero_decimal38_raises_under_default_ansi pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false
  pin_mul_38_20_still_refuses_at_plan` (+ six `g3e8_*` names) → **12 passed; 0 failed; 550 filtered**.
  `cargo test -p repark-functions --lib -- mul_38_20_plans_via_the_expr_planner` → **1 passed; 0
  failed; 232 filtered**. Facade corpus (equality flips): `test_decimal128_parity.py` → **38 passed** (part of the 109-pass targeted facade run below).
  Each named DecimalRow reads `repark is None` (equality) or is a shared-raise — confirmed by reading
  the corpus and by the runs above.

- **C-002 · TZ-8** — CAST/`to_date`/`datediff` half landed by #100 (`rewrite_timestamp_to_date_cast`).
  `cargo test -p repark-spark --test session_timezone -- timestamp_to_date_paths_read_the_session_zone
  native_dataframe_api_cast_to_date_reads_the_session_zone date_valued_shims_take_the_date_in_the_session_zone
  last_day_and_date_add_over_a_timestamp_still_refuse` → **4 passed; 0 failed; 19 filtered** (the
  first three prove NY `2024-06-14` / `13`; the fourth is the red-on-purpose residual). Facade
  partition-audit: `test_partition_value_audit.py` (`tz8_*` rows flipped to equality) → **39 passed** (part of the same 109-pass run).

- **C-003 · G3-E8** — delivered spellings execute, remainder refuses (both in the 12-pass lib run
  above: `g3e8_delete_in_subquery_deletes_exactly_the_matching_row`,
  `g3e8_delete_not_in_subquery_deletes_non_matching_rows`,
  `g3e8_delete_correlated_in_deletes_exactly_the_matching_row`,
  `g3e8_update_in_subquery_rewrites_only_the_matching_row`, `g3e8_delete_subquery_family_all_refuse`,
  `g3e8_update_subquery_family_all_refuse`). Facade: `test_dml_subquery_parity.py` → **31 passed**
  (only `update_not_in_subquery_with_null_key` stays `kind="split"`; every UPDATE IN row is content).

**Live mirror.** No changed row carries a `live-mirror:` key (the keys sit on ID-2/TY-1/TY-2/BL-2/…),
so `python/repark/tests/_live_parity.py` is untouched and
`test_parity_live.py::test_disclosures_mirror_the_registry` is unaffected → **green** (it is one of the 109 passed:
`test_decimal128_parity.py` 38 + `test_dml_subquery_parity.py` 31 + `test_partition_value_audit.py` 39 +
`test_disclosures_mirror_the_registry` 1 = 109 passed, 0 failed, `uv run … pytest … -q`).

**House form (C-001 wording).** DEC-2/6/7/8 follow the landed-fix house form of DEC-1 / DEC-3 / DEC-4:
a dated **FIXED** blockquote note that keeps the row (grep-findable, never a silent deletion); the
corpus rows already flipped to equalities inside #94/#99, so the revert-red evidence is baked in and
the pins are green today, not red-on-purpose. That matches the charter's C-001 as written ("a dated
FIXED note that keeps the row"), so **no C-001 amendment was needed**. TZ-8 and G3-E8 are partial
fixes → kept as `###` rows split into a FIXED half plus a BACKLOG/refused half, the residual pins
(`last_day_and_date_add_over_a_timestamp_still_refuse`, `g3e8_*_family_all_refuse`) red-on-purpose.
Cross-references corrected so the registry does not contradict itself: DEC-3's note (`/` EXCEPTED and
DEC-8 "stays BACKLOG" → both since landed, #99); the two F-Y10 anchor links to the removed DEC-6 /
DEC-7 headings (de-linked to plain text); the TZ-4 note's TZ-8 link (de-linked, "stored zone" → the
FIXED/residual split). STATUS "Known correctness issues" — the three named bullets restate the
registry's new state; nothing else in STATUS changed; STATUS 24,419 B < 25,000 ceiling.

**Gates.**
- `python3 scripts/check_ledger_grammar.py` → **1 finding** ("no COVERAGE_ATTESTATION block"), exit 1
  — expected and the only finding; the attestation is written at unit close, not by the Actor. No
  `EXCEPTIONS` row added.
- `make -k ci` → exit 2, and the sole failure is `check-ledger-grammar` (that one finding). Every
  other gate clean: rust-fmt-check, rust-clippy (repark + repark-python), rust-panic-ban, crate-dag
  (20 edges), lib-rs, rust-file-size (273), lib-py (72), python-conventions (180), docstring-presence
  (159), manifest, ledger-check (143), docs-compaction (STATUS 24,419 B), parity-live dual-wire,
  matrix-test-liveness (93), rust-check, py-lint (ruff `All checks passed`), py-format-check,
  py-lock-check, toml-check, spell-check.
- `make preflight` → exit **2**: it re-runs `ci` and stops at the same expected `check-ledger-grammar`
  finding before `test` / `py-test-facade`; every gate `ci` reaches beforehand is clean. The facade
  suite was therefore measured directly below.
- `make py-test-facade` (wheel via maturin + full facade suite) → **3721 passed, 71 skipped, 0 failed** (exit 0, 214 s) — the full facade suite is green.
- Parity suite (CI's command) `PYTHONPATH=python/repark-parity/src uv run --no-project --with pyarrow
  --with pytest --with 'pydantic>=2.10,<3' pytest python/repark-parity/tests -q` → **386 passed; 0 failed** (the six REG-1 pins are in this run).

**Maps.** `python/repark-parity/tests/map.md` and `task/ledgers/staging/map.md` carry REG-1 in
lockstep (same commit); `docs/map.md` does not enumerate registry rows, so it needed no change.
`make check-map-sync` → 150 maps clean.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-reg-1-close
  agent: Actor
  action: commit the REG-1 docs-only truth-up (registry + STATUS + maps + tree-pin file + ledger record)
  charter_trace: reg-1-registry-truth-up/C-001..C-006
  preconditions:
    - "the four DEC rows are equalities/shared-raises on this tree": SATISFIED (cargo repark-spark --lib 12 passed; repark-functions 1 passed)
    - "TZ-8 CAST/to_date/datediff read the session zone; last_day/date_add over TIMESTAMP still refuse": SATISFIED (session_timezone 4 passed incl. the red-on-purpose residual)
    - "uncorrelated UPDATE IN and correlated DELETE IN execute; the remainder refuses": SATISFIED (g3e8 lib pins in the 12-pass run; source module doc + try_allowed_update_in)
    - "no changed row carries a live-mirror: key": SATISFIED (grep of the registry; keys sit on other rows)
    - "every cited fn/def exists at its named path": SATISFIED (C-005 green)
  success_condition: "the six tree pins are green, make -k ci is clean but for the one expected attestation finding, and the facade + parity suites are green"
  step_risks:
    - "a note cites a renamed/deleted test": HANDLED(C-005 asserts each fn/def exists; the dead DEC-8 name and the renamed TZ-8 pin were replaced)
    - "a removed heading orphans an inbound anchor link": HANDLED(the three inbound links were de-linked to plain text; grep finds no #dec-2/6/7/8 or stored-zone anchor)
    - "STATUS drifts from the registry or breaks its ceiling": HANDLED(three bullets restated to match; 24,419 B < 25,000; docs-compaction clean)
  contingencies:
    - "a facade/parity run reds": EXECUTABLE(revise the offending note to the measured state before commit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

PROCEED.

## Critic pass — findings and attestation (repo SEPMO; CCC mechanical tier: Critic-1 + the claims Critic)

Cycle 1 attacked the cycle-1 tree with two fresh Opus Critics on scratch clones: Critic-1 (quality
+ test-coverage skeptic — seven faithful mutations, every pin live; house form against DEC-1/3/4 and
TZ-6/7; anchors) and Critic-4 (every written claim re-measured: the four PR numbers, 12 + 1 + 23
Rust pins and 109 facade tests green, the G3-E8 split against `predicate_dml`'s allow-list, a
remainder spelling probed through the door). Four findings — one S1 (the FIXED notes dated by the
truth-up, not the landing day — the charter's own parenthetical), one S2 (`date_sub` claimed
without a pin), two S3 — remediated in cycle 2 by the Orchestrator (mechanical record edits,
pins updated). Cycle 2 re-attested on a fresh clone: both remediations verified under mutation,
the landing dates confirmed against `git show`, three S3 test-robustness / record observations —
applied in the departure commit with the pins green. **Verdict: CONVERGED.**

```yaml
COVERAGE_ATTESTATION:
  pr_unit: reg-1-registry-truth-up
  cycle: 2
  risk_tier: mechanical
  critic_engine: ccc
  complete: true
  note: >
    Two-Critic mechanical-tier review per CCC's tier table plus a claims Critic. Cycle 1 (on
    24011a8): a quality + test-skeptic Critic ran seven faithful mutations, every pin red under
    mutation and green on the tree, all pins live; a claims Critic re-measured every cited number
    (12 + 1 + 23 Rust pins, 109 facade, 386 parity). Their findings — DEC-2/6/7/8 FIXED notes
    dated on the truth-up day (2026-08-26) not the fix's landing day; the TZ-8 title/STATUS
    claiming date_sub as an unpinned residual; the decimal128 STATUS bullet missing its registry
    link; and a pre-existing (38,4)-vs-(38,6) pin-name wart — were remediated in a160537: the four
    dates -> 2026-08-14 (#99's landing), date_sub dropped from the TZ-8 title/repark line/TZ-4
    cross-note/STATUS with C-002 amended to say why (measured-but-unpinned, §6), the STATUS link
    added; the pin-name wart left ACCEPTED_FLAGGED for an engine unit. Cycle 2 (this verification,
    on a fresh clone of a160537): both named mutations red the correct pin and restore clean;
    landing dates confirmed by git show (#94 2026-08-13, #99 / #100 2026-08-14); the sole surviving
    date_sub string (line 1522) is the substring in the test name update_subquery; gates green
    (map-sync 150 clean, docs-compaction STATUS 24480 B, ruff clean) and check-ledger-grammar's one
    finding is resolved by this block; all six pins green and mutation-live. Three new S3
    observations, all non-blocking (a pre-existing G4-3 anchor dangle outside the diff; the TZ-8
    date_sub window under-covering the row tail by 178 non-date chars; C-006's substring row check).
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, python/repark-parity/tests/test_reg_1_registry_truth_up.py, task/ledgers/staging/reg-1-registry-truth-up-ledger.md]
    - id: AT-2
      status: N/A
      justification: docs-only truth-up plus one text-reading pin; no function parses external input, so there is no input-validation surface
    - id: AT-3
      status: N/A
      justification: no concurrency, resource or lifecycle surface — Markdown text and a synchronous string-assertion test
    - id: AT-4
      status: N/A
      justification: no shared state or ordering — the pins read files and assert substrings; nothing mutates state or races
    - id: AT-5
      status: N/A
      justification: no security surface (no runtime, no untrusted input); the content-hygiene scan of the added diff lines (git diff 24011a8..a160537, added lines only) is clean — no home paths, IPs, e-mails, or secret patterns
    - id: AT-6
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/map.md, task/ledgers/staging/map.md]
    - id: AT-7
      status: N/A
      justification: no error-handling or failure-path surface — no fallible runtime code is introduced
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/decimal.rs, crates/repark-spark/tests/session_timezone.rs, crates/repark-spark/src/tests/dml.rs, crates/repark-functions/src/decimal_precision.rs, python/repark/tests/test_decimal128_parity.py, python/repark/tests/test_dml_subquery_parity.py, python/repark/tests/test_partition_value_audit.py]
    - id: AT-9
      status: N/A
      justification: no API or compatibility surface — no public signature, schema, or serialized-format change; only docs, a test, and ledger/maps
    - id: AT-10
      status: ATTACKED
      artifacts: [python/repark-parity/tests/test_reg_1_registry_truth_up.py]
```

**Attack notes.** AT-1 (clause walk + the date and date_sub corrections): Walked C-001..C-006 against the tree. C-001 — the four DEC notes read 'FIXED (2026-08-14'; grep for the old 2026-08-26 date returns 0; the date matches #99's true commit date (git show 6fd30b1 = 2026-08-14) and the sibling house form (DEC-1 #84=2026-08-13, TZ-8 #100=2026-08-14). C-002 — TZ-8 title/repark/Rationale/TZ-4 cross-note/STATUS all drop date_sub; the amendment honestly records that date_sub was measured to refuse but is unpinned (§6). C-003 — the G3-E8 row states delivered spellings (uncorrelated DELETE IN/NOT IN, [NOT] EXISTS ±correlation, correlated DELETE IN, uncorrelated identity UPDATE IN via try_allowed_update_in) and the true remainder (UPDATE NOT IN/[NOT] EXISTS, correlated UPDATE IN, ANY/ALL), each side pinned. C-004 — the three STATUS bullets (decimal128, session-timezone, DELETE/UPDATE) match the registry and each links it. C-005/C-006 — every cited test resolves, DEC-9 still reads BACKLOG, no row deleted. MUTATIONS: restoring 2026-08-26 reds C-001; date_sub in the TZ-8 title reds C-002 — both restore clean.

AT-5 (content-hygiene, N/A for security but scanned): git diff 24011a8..a160537 restricted to added lines, grepped for /home//Users//root/, IPv4, e-mail, and AKIA|secret|password|token|api_key|PRIVATE KEY — every grep returned no match. Clean.

AT-6 (record integrity): make check-map-sync = 150 maps clean. test_no_row_deleted confirms DEC-1..9, TZ-8, G3-E8, G3-E8-NULL all survive and the parity/staging maps carry this unit in lockstep. A full intra-doc anchor scan of the registry found one dangling anchor (G4-3: link 'semiant' vs heading slug 'semianti'), but it is untouched by the diff and present at the pre-unit baseline 5f64254 — pre-existing, filed as REG1-C2-001 S3 ACCEPTED_FLAGGED. There are zero (#dec-/#tz-8 inbound anchor links (cross-refs are plain text), so the row->note conversion carries no anchor-dangle risk. The old DEC-6/DEC-7 heading anchors are de-linked (pin asserts).

AT-8 (house form + cited-test contract): each of the 14 cited Rust fns grepped 'fn <name>' = exactly 1 at its named path (6 in decimal.rs, 1 in decimal_precision.rs, 4 in session_timezone.rs, 3 in dml.rs). The facade/parity corpus rows (div_same_precision_scale, mul_38_20_plans_in_spark_refuses_in_repark), the dml parametrization name=update_in_subquery, and both tz8 partition-audit ids all resolve. C4-F3's pin fn name still says 38_4 while its body asserts (38, 6) and DEC-7 says decimal128(38,6) — genuine, pre-existing, ACCEPTED_FLAGGED.

AT-10 (the six pins + mutations): 6 passed clean. Mutation-liveness proven for every pin — date restore reds C-001, date_sub-in-title reds C-002, STATUS-over-25000-ceiling reds C-004, cited-fn rename reds C-005, removing try_allowed_update_in reds C-003; a DEC-9 heading deletion is caught by C-005 (though not by C-006, whose DEC check is a bare id substring — filed REG1-C2-003 S3). Every mutation restored; final run 6 passed with git status clean. The TZ-8 window is 2500 chars against a true 2678-char row (REG1-C2-002 S3, immaterial — the missed 178-char tail holds no date-family token and the window bleeds into no neighbor).

FINDING:
  id: REG1-C1-001
  severity: S1
  category: AT-8
  clause: C-001
  disposition: REMEDIATED
  claim: the DEC-2/6/7/8 FIXED notes were dated by the truth-up (2026-08-26) where the house form — DEC-1/3/4 and this unit's own TZ-8 note — dates by the fix's landing day; the charter's own parenthetical was the source
  evidence: git show -s --date=short fddf1bc (#94, 2026-08-13), 6fd30b1 (#99, 2026-08-14); fix a160537 — the four notes and the C-001 pin read 2026-08-14, the charter clause amended; pin red with 08-26 restored (same finding as C4-F2)

FINDING:
  id: C4-F1
  severity: S2
  category: AT-6
  clause: C-002, C-006
  disposition: REMEDIATED
  claim: the TZ-8 row title and the STATUS bullet claimed `date_sub` over a TIMESTAMP as a residual while the sole residual pin, the crate map and the analyzer doc name only `last_day` / `date_add`
  evidence: Critic-4 measured `date_sub` refuses too, but it is unpinned — §6: an unpinned divergence is prose; fix a160537 drops the claim from the row, the TZ-4 cross-note and STATUS; the C-002 pin asserts it is not claimed; the charter says why

FINDING:
  id: REG1-C1-002
  severity: S3
  category: AT-8
  clause: C-004
  disposition: REMEDIATED
  claim: the decimal128 STATUS bullet named the registry as plain text where its neighbours carry the link
  evidence: fix a160537

FINDING:
  id: C4-F3
  severity: S3
  category: AT-8
  clause: C-005
  disposition: ACCEPTED_FLAGGED
  claim: the pre-existing pin name `pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false` says 38,4 while its body and the registry say (38,6) — a tree wart from #99, not this unit's
  evidence: below the floor; the registry prose and the pin body agree on (38,6); the rename belongs to the next engine unit that touches `decimal.rs`

FINDING:
  id: REG1-C2-001
  severity: S3
  category: AT-6
  clause: C-006
  disposition: REMEDIATED
  claim: a pre-existing inbound anchor to G4-3 dangled by one character (`semiant` for `semianti`), untouched by the diff but in the document this unit trues
  evidence: cycle-2 Critic anchor scan; fixed in the departure commit (one slug character); `make check-map-sync` clean

FINDING:
  id: REG1-C2-002
  severity: S3
  category: AT-10
  clause: C-002
  disposition: REMEDIATED
  claim: the TZ-8 pin bounded its `date_sub` window at a fixed 2,500 characters, 178 short of the row
  evidence: fixed in the departure commit — the window ends at the next `### ` heading; pin green

FINDING:
  id: REG1-C2-003
  severity: S3
  category: AT-6
  clause: C-006
  disposition: REMEDIATED
  claim: the no-row-deleted check asserted DEC ids as bare substrings, so a deleted DEC row whose id survived in a range would pass
  evidence: fixed in the departure commit — each DEC row is asserted by its heading or its FIXED-note opener; a deleted DEC-9 heading now reddens C-006 as well as C-005
