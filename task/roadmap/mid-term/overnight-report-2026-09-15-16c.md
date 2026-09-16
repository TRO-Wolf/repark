# Day report — run 16c (2026-09-15): the SQL door, the type table and the registry backlog

**Unit** overnight-16c · **Orchestrator** Claude (claude-opus-5) · **Window** 2026-09-15 07:59 → 20:15 EDT (stop opening lanes 19:30) · **Lane prefix** `qc-` (build clones `/tmp/qc-conv2`, `/tmp/pc-cv2`) · **Charter** the 1.5 Spark-parity campaign, run 16c slice: the SQL parser, planner and door code, the repark-spark type table, the config carriers, and the registry's BACKLOG and divergence rows.

This report closes when its PR merges. Every measured claim below is dated 2026-09-15 unless stated.

## 1. Census slice — before / after

The census counts the dispositions in the registry `docs/spark-sql-iceberg-parity.md` headings.

| Point | Main | Row headings | BACKLOG | DECLARED | FIXED | Other |
|---|---|---|---|---|---|---|
| Run start | `11ae1595` | 342 | 154 | 69 | 88 | 31 |
| 12:34 | `bee2cde3` | 356 | 158 | 72 | 95 | 31 |
| Report time | `2c7a4d25` | 369 | 158 | 74 | 105 | 32 |

Rows 16c moved to FIXED:
- BL-16, BL-17, BL-18 (#616 DOOR-CONVERGE-1);
- BL-7 (#612 JAVA-DOUBLE-STR-1);
- DC2-CONCAT-1 / REVERSE-1 / SEQUENCE-1 / SPLIT-1, and the FNP-6D `concat(BINARY, BINARY)` residual (#622);
- TYPES-GEO-DDL-1 (#624);
- BL-2, BL-9, BL-10 and BL-12 with #611 (merged 0355ef5e at 19:50).
- JAVA-DOUBLE-FD-1, JAVA-DOUBLE-CAST-SUFFIX-1 and FNP10-JAVA-DOUBLE-TEXT-1 with #633, green and queued at the stop.

New rows from 16c merges:
- ARRAY-LITERAL-CONTAINSNULL-1 and ELEMENT-AT-ALIAS-1 (#616);
- JAVA-DOUBLE-FD-1 (#612);
- JAVA-REGEX-FEATURES-1 (#622).

SQL-door names answering PySpark 4.1.2 on both doors, gained today:
- `base64` / `unbase64`, `hypot`, `abs`, `size` / `cardinality`, `array_contains`, `bin` / `rint`, `ascii`, `length` family (#616);
- DOUBLE / FLOAT text (#612);
- `concat` over arrays and STRING+BINARY, `reverse` over arrays, `sequence`, `split` (#622).

## 2. Per-PR table

| PR | Unit | State | Actor rounds (worker, steps) | Reviews (verdict, cost) | Orchestrator gates |
|---|---|---|---|---|---|
| #616 | DOOR-CONVERGE-1 | **merged ff6003ec** 08:53, tree-equal | 15c rounds (Devin / Grok) | 15c reviews | regate: verify rc 0, facade 7016, parity 757 |
| #612 | JAVA-DOUBLE-STR-1 | **merged 5c5cd2b2** 09:36, tree-equal | 15c rounds (Muse) | 15c reviews | verify rc 0, facade 7042, parity 757 (release native) |
| #620 | IO-JDBC-FORMAT-1 (Q-15B-4) | **closed** as a duplicate of 16b's #621 | orchestrator | — | pin 3 passed, parity 757 |
| #622 | DOOR-CONVERGE-2 (P1 slice) | **merged 96a47bba** 13:29, tree-equal | Muse 470 + 223 + 376 | perf no P1, 4 P2 fixed ($0.55); critic NEEDS_REMEDIATION, claims re-measured by batch 15 ($1.09) | verify rc 0, facade 7671 plus 3 expected pins fixed in fcfb59a0, parity 757 |
| #624 | TYPES-GEO-DDL-1 | **merged 4bd43fc8** 14:25, tree-equal | Muse 149 + 53 | critic NEEDS_REMEDIATION, 2 P2 fixed ($0.84); perf clean ($0.49) | verify rc 0, facade 7680 / 0 failed, parity 757; the departure edit was added after the first queue attempt |
| #611 | FNP-4B | **merged 0355ef5e**, tree-equal | Muse rounds 5 (394), 6 (294), 7 (107), 8 (466) | final critic NEEDS_REMEDIATION, 2 P1 + 2 P2 confirmed by batch 17, all fixed ($0.62); perf no P1, 2 P2 fixed ($0.85) | gate 1 rc 1 (fixed in 23ccd379); gate 2 rc 0; gate 3 on 3c8f5c09: verify rc 0, facade 8197 passed plus 1 wall-clock pin (a load flake, 3/3 on re-run, R-16c-15), parity 757, #613 cohort 384 |
| #633 | JAVA-DOUBLE-FD-1 (+ C-006 CAST suffix P1) | green and queued at the stop (not merged; the owner merges) | Muse 147 (provider outage) + 417 (resumed) | critic NEEDS_REMEDIATION: L-001…L-003 confirmed by batch 19, plus L-004 (`%F`) found by the oracle, all fixed ($0.92); perf no P1, P2-1 and P2-4 fixed ($0.71) | worker: corpus 39,427/39,427, verify, facade 7685; orchestrator: fmt, clippy, java_double tests after R-16c-17 |
| #636 | FNP-13 collation design note (Q-15c-5) | open at the stop (docs only) | orchestrator | — | `make check-map-sync` clean |

**Spend.** Muse Spark 1.3 contributor: 12 rounds, 3,259 model steps, one round lost to a provider outage and resumed on its session. Grok 4.6: 8 reviews, $6.07 — #622 critic $1.09 and perf $0.55; #624 critic $0.84 and perf $0.49; #611 critic $0.62 and perf $0.85; #633 critic $0.92 and perf $0.71. No Devin or GLM rounds. Oracle: batches 12–19, 314 live PySpark 4.1.2 SQL cells plus the 39,427-row JDK 17.0.15 `toString` corpus.

## 3. Oracle record

All batches were recorded by the orchestrator on live PySpark 4.1.2 (JDK 17.0.15) and are kept at `/tmp/oc-worker/qc-oracle/`.

| Batch | Contents | Cells |
|---|---|---|
| 12 | `concat`, `reverse`, `sequence`, `split` | 56 |
| 13 | Geometry / geography DDL | 21 |
| 13 | JDK 17 `Double.toString` / `Float.toString` corpus (82 non-shortest doubles) | 29,451 doubles + 9,976 floats |
| 14 | Bare nullary keywords and the bare-unit `timestampadd` family | 41 |
| 15 | Re-measurement of the #622 critic's claims | 20 |
| 16 | DOOR-CONVERGE-2 remainder | 40 |
| 16 | SET-ANSI snapshot sequence | 12 |
| 17 | Re-measurement of #611's final critic claims | 26 |
| 18 | Collation semantics, for the FNP-13 design note | 55 |
| 19 | Re-measurement of #633's critic claims | 23 |

Corrections found by the oracle:
- **Batch 14:** bare `localtimestamp` refuses on Spark, so EX-FN-25 is RePark over-accepting.
- **Batch 15:** four critic claims needed correcting.
  - L-002: Spark coerces STRING+BINARY `concat` to STRING rather than refusing.
  - L-005: re-scoped. Spark has no array-size cap, so the finding is only RePark's own policy.
  - L-007: overturned. Spark's empty-pattern `split` is per code point.
  - L-001: confirmed.
- **Batch 16:** ANSI binds at frame analysis; the zone binds at analysis except `current_timezone()`, which folds at collect.
- **Batch 17:** on #611, L-001, L-002 and L-004 were confirmed. L-003's expected column names were corrected: Spark names unaliased suffix literals from the value text (`1L` → `1`, `1e200D` → `1.0E200`). L-005 was re-scoped (Spark also refuses `ROWS BETWEEN 1L`), and L-006 was overturned (Spark's DDL rejects `ARRAY<INT NOT NULL>`).
- **Batch 18:** a `spark.sql.session.collation` SET does not change literal comparisons on 4.1.2, and `straße` ≠ `STRASSE` even under `UNICODE_CI`.
- **Batch 19:** it confirmed #633's three critic findings and turned up a fourth defect nobody had claimed — Java's `Formatter` has no `%F`, so Spark refuses `format_string('%F', …)` while RePark printed `NAN`.

## 4. Rulings taken today

### Owner rulings applied (2026-09-15)

| Ruling | Applied where |
|---|---|
| Q-15c-6, Q-15c-7 | #622 |
| Q-15c-4 | #622 R-1 `analyzer.rs` +8; FNP-4B baselines |
| Q-15c-8 | #611 rebase and baseline recount |
| Q-15c-2 | JAVA-DOUBLE-FD-1 card |
| Q-15c-3 | SET-ANSI-RUNTIME-1 card |
| Q-15B-2 | #624 |
| Q-15B-4 | #620, closed as a duplicate of #621 |
| Q-15c-5 | FNP-13 note after FNP-4B |

### Orchestrator rulings under G-2

- **R-16c-1…4:** FNP-4B round 5.
  - R-16c-1: continue the MERGE `t."_file"` hunt.
  - R-16c-2: the `catalog_surface.py` hand-off to 16b, later granted as R-16b-21.
  - R-16c-3: diagnose the HOF display leak before any hand-off.
  - R-16c-4: the worker's base clone may be used for one round only.
- **R-16c-5…8:** #622 round 3.
  - R-16c-5: `.typos.toml` allowlist entry.
  - R-16c-6: pin text in 16a's `test_fn_regexp_extract.py`.
  - R-16c-7: P3s recorded.
  - R-16c-8: no second critic round.
- **R-16c-9:** FNP-4B's out-of-fence edits are carried, with their owners told.
- **R-16c-10:** a hand-off pin on a merging PR becomes strict xfail.
- **R-16c-11:** #622's post-rebase fixes: 16a's FNP-6D pin flips, and `split` leaves `FACADE_ONLY_ROUTINE_NAMES`.
- **Q3 on #622:** the sequence error class follows the oracle (`IllegalArgumentException`); the class mapping is a hand-off to 16b.
- **G-2 on #624:** the `_type_table.py` bridge arm is in the unit's fence.
- **R-16c-12:** an accuracy reflow of an existing required `///` doc block (`Column.sql`, #611) is not a new comment.
- **R-16c-13:** #611's L-006 pin moved out of 16b's `test_catalog_surface_1.py` into the unit's own test file.
- **R-16c-14:** 16a's #613 qualifier-leak pin keeps its `spark.sql` half; its `selectExpr` half flips because #611 converged that door.
- **R-16c-15:** a wall-clock pin that fails under box load (load average 26.7) and passes 3/3 on re-run is a load flake, recorded, not a regression.
- **R-16c-16:** `#[allow(clippy::cast_*)]` on the FD-1 port matches the repository's idiom (72 on main; `#[expect]` is for `disallowed_methods`).
- **R-16c-17:** FD-1's nine new `///` lines on private items were removed by the orchestrator (comment ban).

## 5. Owner questions (with recommendations)

| # | Question | Recommendation |
|---|---|---|
| Q-16c-1 | Spark's `split` / `regexp_*` / `rlike` answer lookahead, lookbehind, backreferences and possessive quantifiers (batch 15 Q15-10…12). The `regex` crate supports none of them, and #622 refuses them loudly (JAVA-REGEX-FEATURES-1). Approve a fallback engine? | **Approve `fancy-regex`** (pure Rust, built on `regex`), used only for patterns that need one of those features, with a backtrack limit. |
| Q-16c-2 | Collation (FNP-13): the `UNICODE*` and ICU locale collations need a collator. Approve ICU4X `icu_collator`? The design note is `docs/design/collation-fnp-13.md`. | **Approve, for slice FNP-13c only.** Pure Rust, with data compiled in. Slices 13a (the carrier, `UTF8_LCASE`, `RTRIM`, precedence) and 13b (string functions) need no dependency. |

## 6. Hand-offs to the other runs

- **16a:**
  - `F.split` facade wiring; the kernel landed in #622.
  - FNP-4B C-026: the selectExpr HOF packing leak.
  - FNP-4B C-029: `F.expr` backtick display.
  - The two carried hunks in `functions.py` / `test_examples_functions_a.py`.
  - The SQL-door skip sets in `test_fnp11a_temporal.py`, which fold into SPARK-SQL-GRAMMAR-1.
- **16b:**
  - The `IllegalArgumentException` class mapping for the sequence illegal-step error (`export_errors.py`).
  - R-16b-21 accepted.
  - The carried `dataframe/**` hunks.
- **16a, also:**
  - #633 flips the JDK-spelling pin in `test_fnp_9_collections_json.py` and closes FNP10-JAVA-DOUBLE-TEXT-1, because `to_json` shares the ported formatter.
  - #622's pin-text change in `test_fn_regexp_extract.py` (R-16c-6).
  - `split` removed from `FACADE_ONLY_ROUTINE_NAMES` (R-16c-11).

## 7. Rust-first roll-call — Python logic left in Python, with the reason

Every kernel, planner rule and type conversion this run shipped is Rust. The Python touched was binding, pins and allowlists:

- **#622 DOOR-CONVERGE-2:** no Python logic. The `concat` / `reverse` / `sequence` / `split` kernels are Rust (`repark-functions`), reached from both doors through the Rust dispatch. The only Python edit is one name removed from `functions_byname.py`'s measured `FACADE_ONLY_ROUTINE_NAMES` tuple (R-16c-11).
- **#624 TYPES-GEO-DDL-1:** a 4-line descriptor decode arm in `_type_table.py`, the thin binding for the new Rust tags. Parsing, the SRID sets and the Arrow mapping are Rust.
- **#633 JAVA-DOUBLE-FD-1:** no Python logic. The `FloatingDecimal` port, the `%f` shim and the suffix-cast kernel are Rust.
- **#611 FNP-4B:** three Python pieces stay Python, all pre-existing and edited in place under run 15b's grants:
  - `_idents.quote_ident` and `_temp_views.py`, the facade's identifier quoting for generated SQL;
  - `dataframe/core.py` `_quote_filter_sql_identifiers`, the filter-string quoter;
  - the aggregate rebind regexes in `dataframe/joins_columns.py` and `plan_collapse.py`.

  Each one only widened its character class to accept backticks; the parsing and rewriting that decide meaning are Rust (`spark_literals.rs`, `spark_rewrites.rs`, `spark_typed.rs`). Moving facade SQL-string quoting into Rust belongs to the FACADE-6+ roll-call, not to this unit. The one-line `catalog_surface.py` `_ddl_type` change is a DDL spelling under run 16b's grant R-16b-21, with the type mapping itself in Rust.

## 8. Lessons (mechanics)

- **A stale native looks like a regression.** FNP-4B round 5's 135 facade failures were almost all a debug-era native. A fresh release rebuild left 8. Rebuild before triaging.
- **Re-measure reviewer claims before the actor sees them.** On #622, the critic's expected answer was wrong on 2 of 7 claims, and a third needed re-scoping.
- **The shared queue file works, but a merged PR's line lingers until its owner deletes it.** Two stale lines were cleared under the 45-minute rule and logged in `queue-notes.txt`.
- **Disk crossed the 250 G line twice (249 G, 245 G).** Clearing finished reviewer clones at once, plus holding new second-build-clone rounds, recovered it without killing a round.
- **A worker opened its own base clone for a bisect** (`/tmp/pc-cv2-base`, 7 G). It was allowed for one round and then removed.
- **Duplicate registry rows across runs:** #620 duplicated 16b's #621. Check the other runs' open docs PRs before filing a row an owner ruling names.
- **Queue before the departure edit, never after.** #624 was queued without the ledger move, withdrawn, and re-queued. The ship scripts now do the departure edit first.
- **`gh pr checks --watch` can return while jobs are still pending right after a push.** The merge scripts now poll until nothing is pending.
- **Re-gate after a large merge lands under a dialect change.** 16b's IO-TEXT-1 added internal-SQL paths, and only a full facade run on the rebased head can show whether double-quoted identifiers survived the Databricks dialect.
- **Absolute wall-clock pins are load-sensitive** with three orchestrators sharing 64 cores. Re-run them before blaming the branch.
- **Muse can leave clippy pedantic debt behind a green pre-commit hook.** The hooks do not run clippy, so the orchestrator's audit greps `#[allow` and runs `make rust-clippy` on every hand-back.
- **Audit the attribution trailer and the ledger location on every hand-back, not just the diff.** One round wrote `Authored-By: muse-worker 16c` instead of the owner's form, and another staged a second ledger beside the one already moved to `completed/`. Both were caught by the audit and fixed before the gate.
- **A queue shared by three runs plus a release PR is the real merge cost.** #611 was gated four times, each time because main moved under it: two docs reports, one large IO unit, one decimal unit and the v1.4.2 release. Only the code-bearing moves earned a re-gate; the docs-only ones were rebased and pushed on the standing gate.
- **Freeing a build clone without breaking a merge chain:** the merge scripts read HEAD from the clone they are handed, so moving a PR's bookkeeping into a 426 M git-only clone let the build clone start the next unit immediately.

## 9. Parked for the next run, ready to start

- **SET-ANSI-RUNTIME-1 (Q-15c-3), the first unit to open.** Round 1 was stopped at 25 minutes with no commits under ruling R-16c-18, to gate #611 a fourth time. Everything it needs is written:
  - card `qc-cards/card-setansi1.md`, with the batch-16 addendum that measured the snapshot semantics (ANSI binds at analysis; the zone binds at analysis except `current_timezone()`; the refusal classes);
  - launch template `qc-cards/round-setansi1-r1.template.md`;
  - the partial work as a named stash in `/tmp/pc-cv2` and a patch under `/tmp/oc-worker/pc-cv2/snapshots/setansi-r1-partial-*`.
  BL-11 (numeric → BINARY with ANSI off) follows it.
- **DOOR-CONVERGE-2b** (decimal-scale `round` / `ceil` / `floor`, `date_part` seconds, array literal widths, element nullability, 3-argument `like`): card `qc-cards/card-conv2-r4.md`, template `round-conv2b-r1.template.md`, oracle batch 16 cells Q16-0…39.
- **SPARK-SQL-GRAMMAR-1:** card `qc-cards/card-grammar1.md` with the batch-14 correction (bare `localtimestamp` refuses on Spark, so EX-FN-25 is RePark over-accepting), template `round-grammar1-r1.template.md`. C-008 and C-010 also retire the SQL-door skip sets in 16a's `test_fnp11a_temporal.py`.
- **The FNP-13 collation design note** (Q-15c-5, after FNP-4B lands): draft at `/tmp/oc-worker/qc-report/fnp13-collation-design-note.draft.md`, with `ship-fnp13-note.sh` ready to open it as a docs PR.
- **JAVA-REGEX-FEATURES-1 and the ICU collator** wait on owner questions Q-16c-1 and Q-16c-2.
