# REVIEW-1 — the Grok critic sweep over the merges since 2026-09-08

**Date:** 2026-09-10 · **Card:** [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
REVIEW-1 · **Base for every round:** `origin/main` at `2fad8135` · **Critic tier:** Grok 4.6
(`grok-worker`, `--role critic-quality` / `critic-logic`), never Opus (critic-tier ruling
2026-09-06) · **Orchestrator:** run 5b of the overnight sequence, 2026-09-10.

**Why this document exists.** Twenty-plus units merged to `main` in the two days to 2026-09-09
under orchestrator audits alone; the SEPMO Critic stage never ran on them. Each round below read
one unit's ledger, its merged diffs and its card, attacked the claims, and wrote a report file.
The full reports live beside this document's rounds in `/tmp/grok-worker/<lane>/`; what follows is
the merged record: every finding, its verdict, and — for the ones the orchestrator re-ran itself
— whether the reproduction holds on this tree.

**How to read a verdict.**

| Verdict | Means |
|---|---|
| CONFIRMED | The critic gave a reproduction command and it demonstrates the defect. |
| RE-RUN | The orchestrator ran the critic's reproduction itself and saw the same result. A CONFIRMED finding without RE-RUN is the critic's word plus its transcript. |
| SUSPECTED | Reasoned from the code, not reproduced. |
| QUESTION | The finding contradicts an owner ruling; it is filed for the owner, not fixed. |

**Environment note that bounds every Python finding below.** The review clones carry no built
wheel. The rounds ran the facade pins on the owner's prebuilt `_native.abi3.so` with the clone's
Python source shadowing it; that binary predates CFG-1, so each round shimmed the two or three
missing native names and said so in its report. A finding whose reproduction needs a shim is
still a finding about the Python source on `main` — the shim replaces a missing binding, not the
behaviour under test — but the fix rounds should re-run each reproduction against a wheel built
from `main`.

## 1. Rounds run

| Unit | PRs reviewed | Role | Turns | Cost | Verdict |
|---|---|---|---|---|---|
| CFG-1 | #440 #445 #447 #451 #455 | `critic-quality` | 29 | $0.50 | 5 CONFIRMED, 1 SUSPECTED |
| CFG-1 | same | `critic-logic` | 39 | $0.60 | 2 CONFIRMED, 1 SUSPECTED, 1 QUESTION |
| DISPLAY-POLARS-1 | #429 #434 #439 #448 #449 | `critic-quality` | 35 | $0.72 | 3 CONFIRMED |
| DF-EAGER-1 | #443 #452 | `critic-logic` | 33 | $0.41 | 2 CONFIRMED |
| SQL-DESCRIBE-1 | #428 | `critic-quality` | 39 | $0.58 | 2 CONFIRMED, 1 QUESTION |
| DF-EXPLAIN-1 | #427 | `critic-logic` | 51 | $0.78 | 1 CONFIRMED |
| PROFILES-1 | #441 | `critic-quality` | 34 | $0.58 | 3 CONFIRMED, 1 SUSPECTED, 1 QUESTION |
| CFG-1 | #440 #445 #447 #451 #455 | `critic-security` | 30 | $0.41 | 2 CONFIRMED, 1 QUESTION |
| DISPLAY-POLARS-1 | #429 #434 #439 #448 #449 | `critic-logic` | 26 | $0.33 | 3 CONFIRMED, 1 QUESTION |
| SQL-DESCRIBE-1 | #428 | `critic-security` | 29 | $0.37 | 0 CONFIRMED, 2 QUESTION |
| DF-EAGER-1 | #443 #452 | `critic-quality` | 22 | $0.29 | 2 CONFIRMED (one raised to high) |
| SQL-DESCRIBE-1 | #428 | `critic-logic` | 48 | $0.77 | 1 CONFIRMED, 2 SUSPECTED, 1 QUESTION |
| PREFLIGHT-PARITY-1 | #433 #438 | `critic-quality` | 18 | $0.19 | **no findings** |
| DF-EXPLAIN-1 | #427 | `critic-quality` | 29 | $0.39 | 2 CONFIRMED |
| LEDGER-READING-1 | #432 | `critic-logic` | 18 | $0.30 | 1 CONFIRMED, 1 QUESTION |
| DOCS-LINKS-1 | #437 | `critic-quality` | 24 | $0.37 | 4 CONFIRMED, 1 QUESTION |
| PROFILES-1 | #441 | `critic-logic` | 24 | $0.35 | 4 CONFIRMED, 1 QUESTION |
| DISPLAY-BRIDGE-1 | #454 | `critic-logic` | 22 | $0.31 | **no findings** |
| BALLISTA-AUDIT-0 | #426 | `critic-quality` | 24 | $0.37 | 4 CONFIRMED |
| DOCS-LINKS-1 | #437 | `critic-logic` | 21 | $0.31 | 2 CONFIRMED |
| LEDGER-READING-1 | #432 | `critic-quality` | 19 | $0.27 | 2 CONFIRMED |
| PREFLIGHT-PARITY-1 | #433 #438 | `critic-logic` | 17 | $0.24 | **no findings** |

A first `critic-logic` round on CFG-1 returned the fabrication pattern the runbook §3 names —
`num_turns` 1, a summary naming eight pytest files that do not exist in the tree, no report file
on disk. It was discarded unread and relaunched with an explicit mandate; the relaunch is the
39-turn round in the table. The pattern cost $0.01 and one detection step: the check that catches
it is `ls` on the report path plus `num_turns`, both of which the runbook already prescribes.

## 2. Findings

Forty-four numbered findings across twenty rounds: 33 CONFIRMED (nine of them re-run by the
orchestrator, all nine holding), 3 SUSPECTED, 8 filed as owner questions. Three rounds over two
units yielded nothing, which is recorded here as a result rather than omitted.

### CFG-1 — the `repark.toml` loader

**Q-1 · The unit's own pins fail on a developer's machine that has `~/.config/repark/repark.toml`.**
CONFIRMED, RE-RUN by the critic with a planted HOME (`cargo test -p repark-core config_file`,
exit 101, 46 passed / 2 failed). `load_without_a_file_is_the_empty_config`
(`crates/repark-core/src/config_file/tests/mod.rs:41`) calls the public `load()`, which reads the
real `HOME`, `cwd` and `REPARK_CONFIG`; the C-023 control session
(`tests/wiring.rs:227`) auto-discovers the home file too, so its "byte-identical to `.config()`"
comparison picks up the extra keys. This violates the unit's own D-8 ("discovery takes its
environment as a parameter, so no pin mutates the process environment"). Severity high — anyone
who follows `docs/guide/repark-toml.md` and writes the file cannot run the suite. Fix: point both
pins at `load_file_config` / `discover` with a stub environment, or set `REPARK_CONFIG=""` on the
control builder.

**Q-2 · The guide quotes a refusal the guide's own example does not produce.** CONFIRMED.
`docs/guide/repark-toml.md:43` puts the database source on `[write.database.postgres.company_db]`
while the quoted error at :55 names `default.database.postgres.company_db`. The loader gates only
the *effective* profile table (`wiring.rs:87–108`), so without `REPARK_ENV=write` the complete
example loads and the quoted transcript belongs to a different file. C-030 claims the guide quotes
only what ran in the clone. Fix: quote the refusal under `REPARK_ENV=write`, and state that an
unselected profile's database table is not consulted until that profile is selected.

**Q-3 · The Pydantic mirror raises `AttributeError` where the loader returns a config error.**
CONFIRMED and **RE-RUN by the orchestrator**: `SessionConfig.model_validate({"batch_size": 1.5})`
raises `AttributeError("'float' object has no attribute 'strip'")` from
`python/repark/src/repark/config.py:99`, not `ValidationError`. Same for a list. Callers that
catch `ValidationError` — the contract the rest of that module keeps — miss it.

**Q-4 · The mirror accepts a name collision the loader refuses.** CONFIRMED by both critics and
**RE-RUN by the orchestrator**: `ProfileConfig.model_validate` accepts `postgres.acme` beside
`trino.acme` (`config.py:195–206` compares catalogs against databases but never databases against
each other), while `sources.rs:177–188` refuses the same document naming both key paths (pin
`a_name_collision_inside_the_database_family_refuses_naming_both_keys`). A user who builds the
file through `repark.config` gets a document `getOrCreate()` rejects.

**Q-5 · `to_toml()` drops an empty overlay profile.** CONFIRMED and **RE-RUN by the
orchestrator**: `ReparkConfig(profiles={"prod": ProfileConfig(), "default": …}).to_toml()` renders
`[default.conf]` only — `prod` disappears, and `REPARK_ENV=prod` on the rendered file then refuses
as an unknown profile. `render_lines` emits subsections and never the profile header
(`config.py:208–247`). Severity low but it silently loses a declared profile.

**Q-6 · The mirror's "digit string" rule is Unicode-wide; the loader's is ASCII.** CONFIRMED and
**RE-RUN by the orchestrator**: `str.isdigit()` at `config.py:99` accepts `"٠١٢"` and `"²"`, which
`to_toml()` then writes; the loader parses session knobs with `trim().parse::<usize>()`
(`wiring.rs:264`), which returns `InvalidDigit` for both. Construction succeeds, loading the
rendered file refuses. Fix: require `value.isascii() and value.isdigit()` after `strip()`, the
rule `_normalize_display_int` already uses.

**Q-7 · File `batch_size = 0` and `.config("repark.batch.size", "0")` take different doors.**
SUSPECTED — the critic could not build the native in its clone. The typed path refuses
(`session.rs:181–185`, "batch_size must be >= 1"), while `0` on the facade is the documented Spark
"no limit" sentinel (`session_configuration.py:391–394`). Worth settling in the same fix round as
Q-3/Q-6, with a pin on both entry points.

**Q-8 · Unset `REPARK_ENV` against a `[prod]`-only file applies nothing.** QUESTION for the owner,
not a fix. C-004 and the card say `None` selects the default table alone, and an *unknown*
`REPARK_ENV` already refuses loud. The critic's lean, which the orchestrator shares: keep the
current behaviour — a warning here would be a new ruling. Filed under REVIEW-1 D-4.

### DISPLAY-POLARS-1 — the polars display default

**Q-9 · An odd `repark.display.max_rows` drops a row; `max_rows = 1` prints an empty table.**
CONFIRMED. At `display.py:287–302`, `head_n + tail_n` sums to `max_rows - 1` for every odd value,
and at the legal value `1` both edges are 0, so `show()` emits `shape: (7, 1)` with no body rows
and no ellipsis — live polars 1.43.2 at `tbl_rows=1` prints the first row and `…`. The keys pin
exercises only the even value 4, so the suite stays green. Severity medium: a legal setting
produces an empty rendering of a non-empty frame.

**Q-10 · D-7's `show(truncate=True)` → `str_len` remap is unpinned.** CONFIRMED by mutation: the
critic deleted the remap at `display.py:75–76` and the named pins stayed green while the default
`show()` began cutting at 20 characters. A behaviour the card decided has no test holding it.

**Q-11 · `DataFrame.show`'s docstring still describes the pre-D-5 counting rule.** CONFIRMED,
severity low (`display.py:55–57`, `core.py:4070–4071`): it says the styled paths always count,
which D-5's probe-first count replaced.

### DF-EAGER-1 — `.eager()` / `.compute()` / `.lazy()`

**Q-12 · `lazy()` after `localCheckpoint()` plans `SELECT * FROM None`.** CONFIRMED and **RE-RUN
by the orchestrator** — the reproduction prints
`AnalysisException: Error during planning: table 'datafusion.public.none' not found`.
`_to_lazy` (`eager.py:82`) reads a set `_eager_shape` as "this frame is eager" and rebuilds the
copy as `SELECT * FROM {frame._cache_view}`; `localCheckpoint` truncates lineage and sets
`_cache_view = None` without clearing `_eager_shape`, so the identifier `None` is interpolated
into SQL. `unpersist` was updated to clear the shape; the checkpoint path
(`core.py:510`) was not. Fix: return `_spawn_preserving_identity(frame._inner)` when the shape is
set, and treat a missing `_cache_view` as "not cache-owned".

**Q-13 · `count()` and the styled `repr` skip a pending `localCheckpoint(eager=False)`.**
CONFIRMED. D-4 forbids a *count query* on an eager frame, not skipping the materialize step:
`_count_rows` (`eager.py:89`) returns `_eager_shape[0]` without calling
`_materialize_cache_if_needed`, so a lazy checkpoint stays pending and the cache view is not
dropped after an action Spark treats as the trigger. `_styled_total_rows` (`display.py:253`)
has the same skip.

**The quality round, run separately on the same unit, raised this to high with a data-loss
path.** Because the cache view stays registered and the checkpoint stays pending, a later
`catalog.clearCache()` unpersists it and restores the source lineage — so a frame read from a CSV
that was deleted after `eager()` collects as `[]` instead of the checkpointed rows. A real action
(`collect`) before `clearCache` keeps the rows, which is the control that makes the diagnosis. Two
independent critic rounds reached this finding from different directions; REVIEW-FIX-4 is the
sweep's highest-priority fix card.

### SQL-DESCRIBE-1 — `DESCRIBE [TABLE] [EXTENDED|FORMATTED]`

**Q-14 · The intercept skips real Iceberg tables whose name is a metadata-table word, so the
reported bug survives for them.** CONFIRMED (predicate level; no end-to-end DESCRIBE of such a
table was run). `describe_show.rs:230–232` requires exactly three name parts and then applies
`is_metadata_table_name` to the *last* part, so a genuine table `cat.ns.files` (or `snapshots`,
`history`, …) returns `None`, the router never intercepts, and
`DESCRIBE TABLE EXTENDED cat.ns.files` still raises the DataFusion `ParseException` the unit
closed. The suffix rule D-1 wanted applies to `t.snapshots` (four-part), not to a three-part name
whose table happens to be spelled that way. A fixture for exactly this collision already exists —
`crates/repark-spark/src/tests/metadata_tables.rs:177` creates `ice.sales.files` — and no describe
pin names it. Fix: apply the metadata check to the suffix position only, and pin
`DESCRIBE EXTENDED ice.sales.files`.

**Q-15 · `Owner` is read from `USER` / `USERNAME` at query time.** CONFIRMED
(`describe_show.rs:413–417`). ADR-0004 ("everything through the Session") forbids environment
reads on the query path; both pins assert only that the value is non-empty, so `"unknown"`,
`"root"` or a server-process user all keep them green. Fix: snapshot the user at session build or
read a session-held config, and pin the value, not its emptiness.

**Q-16 · Does D-1's `<ident>` include a two-part `ns.t` completed from the session default
catalog?** QUESTION for the owner. `try_parse_describe_table` returns `None` unless the name has
exactly three parts, so dbt's `describe extended ns.table` stays refused as
`R-DESCRIBE-TWO-PART`. The critic's lean, which the orchestrator shares: if the owner already
narrowed D-1 to three-part names, record the residue on the DESC-1 registry row and leave the dbt
refusal; otherwise complete short names from the session defaults and pin the dbt spelling.

### DF-EXPLAIN-1 — `explain()` prints a plan

**Q-17 · `explain(True, mode=…)` silently discards `mode`.** CONFIRMED.
`core.py:3353` resolves `mode = "extended" if extended is True else "simple" if mode is None else
str(mode)`, so `explain(True, mode="formatted")` prints the extended indent plan with no
`FORMAT TREE` tree, and `explain(True, mode="codegen")` drops D-2's codegen line. The mirrored
swap also loses a string `extended` whenever `mode` is set. PySpark refuses the both-set shape
outright (`CANNOT_SET_TOGETHER`); this path fails open onto the wrong plan text. None of the seven
unit pins passes both arguments. Fix: refuse the both-set shape the way PySpark does, or honour
`mode`; either way pin it.

### CFG-1 — security round

**Q-18 · `to_toml()` injects TOML through an unsanitized profile name.** CONFIRMED and **RE-RUN by
the orchestrator**. Conf keys and string values are quoted (`_toml_conf_key` / `_toml_text`);
profile names are dropped raw into `f"[{name}.display]"` and its siblings
(`config.py:212, 216, 221, 227`, rendered by `to_toml` at `:244`). A profile name that closes the
header and opens another table renders a document the constructor never validated — the
reproduction writes a full `[default.catalog.stolen]` Glue block with an attacker-chosen
warehouse, and `tomllib` reads it back as exactly that structure. A dotted name (`foo.bar`) is the
same class without hostility: unquoted headers nest. Fix: quote every header segment with
`_toml_text`, or refuse `]`, `.`, quotes and newlines in profile and source names the way catalog
names already refuse `.`.

**Q-19 · A config parse error reprints the offending source line, so a mis-pointed credentials
file leaks a key into the exception.** CONFIRMED (the critic reproduced the toml crate's `Display`
against the same rlib this crate links; the end-to-end `getOrCreate` leg was not run because the
clone's native predates CFG-1). `config_file.rs:62–64` maps `toml::from_str` errors through
`Error::Config(error.to_string())`; `toml_edit::TomlError`'s `Display` prints the failing line, so
`REPARK_CONFIG=~/.aws/credentials` (unquoted, therefore invalid TOML) puts
`aws_access_key_id = AKIA…` into the message the facade raises as `IllegalArgumentException`. Fix:
carry `error.message()` and the position, never the source snippet; or redact the echoed line.

**Q-20 · Discovery plus interpolation plus Glue-at-build is a confused-deputy composition.**
QUESTION for the owner, not a fix — each part is a standing ruling (D-2 discovers `./repark.toml`
at `getOrCreate`, `${VAR}` interpolation is unrestricted, R-21 says a `type = "glue"` block
connects to AWS when the session builds). Together they mean a cloned repository's `repark.toml`
can expand the developer's process environment into catalog properties and reach AWS on ambient
credentials with no further opt-in. The critic's lean, which the orchestrator shares: if the owner
wants a guard, refuse `glue` / `s3tables` from a CWD-discovered file unless `REPARK_CONFIG` named
it, or require an explicit AWS opt-in key — but do not patch it silently against three rulings.

### PROFILES-1 — the `.config()` pass-through probe

**Q-21 · The probe script is not idempotent.** CONFIRMED (`scripts/profiles1_probe.py:92`): the
second run fails with `PATH_ALREADY_EXISTS` because `write.parquet` writes to a fixed path. A
probe that cannot be re-run cannot be re-measured, which is the whole point of a probe.

**Q-22 · Three keys the probe labels UNREAD do refuse invalid values at `getOrCreate`.** CONFIRMED
(`docs/…/probe.md:51`): `concurrency_limit`, `file_scoped_rewrite` and `scan_pruning` are parsed at
session build, so "accepted but unread" overstates the finding for them.

**Q-23 · The probe's summary line miscounts.** CONFIRMED (`probe.md:104`): "five had no subject"
does not account for the nine UNREAD rows.

**Q-24 · `bloom_filter_on_read` is called UNREAD on a fixture with no bloom filters.** SUSPECTED —
the measurement cannot distinguish "unread" from "read and irrelevant on this file".

**Q-25 · R-16's "nine `datafusion.*` keys" does not match what the probe measured.** QUESTION for
the owner. The nine UNREAD rows are four `datafusion.*` keys, three `repark.*` session keys (parsed
at build — see Q-22) and two Iceberg table properties. The critic's lean, which the orchestrator
shares: narrow R-16 and CONF-UNREAD-1 to `enable_page_index`, `bloom_filter_on_read`,
`coalesce_batches` and `write_batch_size`; treat `repark.*` as already wired and `write.*` as table
properties.


### DISPLAY-POLARS-1 — logic round

**Q-26 · `max_rows = 1` renders a body-less table.** CONFIRMED again from the logic side
(`display.py:288`), independently of Q-9 above — the two rounds found the same defect through
different doors, which is the strongest signal in this sweep. Severity high on this round's
reading.

**Q-27 · A four-element list cell is spelled in full; polars ellipsizes at four.**
CONFIRMED (`polars_cells.py:118`): `_polars_nested_at_depth` ellipsizes only when
`len(items) > 4`, so `[0, 1, 2, 3]` renders in full while live polars 1.43.2 renders
`[0, 1, … 3]` from the same Arrow table. D-6 requires byte-identity with
`str(pl.from_arrow(...))`. The unit's own residue R-003 mis-states the threshold ("lists longer
than 4"), and the three oracle pins cap lists at 3, so the boundary was never probed.

**Q-28 · `±999999.0` is spelled fixed; polars switches to `±9.99999e5`.** CONFIRMED, low
(`polars_cells.py:50`) — the same D-6 byte-identity claim, at the float-formatting boundary.

**Q-29 · Did R-005 waive R-11's "duckdb is settled in step 4"?** QUESTION for the owner. Step 4
left the duckdb branch on count-first because the protected pin's duckdb section still pins
`max_rows_per_export = 2`. Either R-005 waives R-11 for duckdb, or a duckdb D-5 follow-up card is
owed.

### SQL-DESCRIBE-1 — security round

No CONFIRMED findings; two questions, one of which is the sweep's most consequential.

**Q-30 · `Table Properties` redacts with Spark's `Utils.redact` predicate, not the
`prop_key_is_secret` predicate D-5 names.** QUESTION (the critic filed it as a question because
D-5's wording and the unit's AT-5 attestation disagree, and the shipped behaviour matches Spark).
**RE-RUN by the orchestrator**: `s3.access-key-id`, `credential` and `basic.auth.user.info` are
`prop_key_is_secret = True` and are *not* matched by the namespace predicate — so
`DESCRIBE TABLE EXTENDED` prints an Iceberg REST credential or an S3 access-key property in
plaintext where `DESCRIBE NAMESPACE EXTENDED` would redact it. The shipped pin uses
`secret_token=hunter2`, which both predicates catch, so it cannot tell them apart. Ruling needed:
either keep Spark's predicate and amend D-5's wording, or switch Table Properties to
`prop_key_is_secret` and accept a documented Spark delta. Whichever way it goes, the pin should
use a key the two predicates disagree about.

**Q-31 · `Owner` at query time (see Q-15) is also a security-round finding**, filed there as a
ruling question: keep Spark's shape via `std::env::var`, or obey ADR-0004 and thread the owner
through the session.


### PREFLIGHT-PARITY-1 — no findings

The `critic-quality` round over #433 and #438 attacked the wiring, the red-first claim, the map
lockstep and D-1/D-2 against R-12 and found nothing. Recorded here because a clean unit is a
result: it is the only unit in this sweep that survived a round untouched.

### DF-EXPLAIN-1 — quality round

**Q-32 · Five of D-2's rows have no pin.** CONFIRMED (`test_df_explain_1.py:41–92`): the cost mode,
the codegen note, `ANALYZE`, the simple-mode physical-only shape and the blank-line separation are
decided by D-2 and asserted by nothing. Q-17's mis-route shipped green through exactly this hole.

### LEDGER-READING-1 — logic round

**Q-33 · The READING exemption fail-opens on a substring.** CONFIRMED
(`scripts/check_ledger_grammar.py:65, 88–91, 293–294`). D-1 makes the exemption the `Path` *header
field* whose value is `READING`; the implementation searches the first 40 lines for the substring
`**Path:** READING` with only a word boundary after it. So a ledger whose real field is
`**Path:** STANDARD` but whose notes quote the marker inside a code span is exempted from rule B,
and `**Path:** READING-FOO` is exempted too (`-` is a non-word character). The critic's control —
the same scratch ledger with the quoted token changed to `LIGHT` — goes red with the standing
rule-B message, which is what proves the substring is doing the work. A gate that can be disarmed
by quoting it in prose is worth fixing even though nothing has exploited it: this sweep's own
FACADE-AUDIT-0 ledger relies on that exemption legitimately.

**Q-34 · D-3's prefix check was never implemented.** QUESTION for the owner, filed by the critic
against the card's text rather than the code.


### DOCS-LINKS-1 — `make check-docs-links`

All four findings are about the gate itself, and all four are invisible to its ten pins because the
fixtures use the plain shapes.

**Q-35 · GitHub-style slugs are computed on raw markdown.** CONFIRMED
(`check_docs_links.py:80`): `heading_anchors` slugs the raw ATX line, so
`## [Usage](README.md)` becomes `usagereadmemd` while GitHub slugs the rendered text to `usage` —
a GitHub-correct `#usage` reds, and the gate accepts an anchor GitHub does not produce. The tree
already carries that heading shape
(`docs/history/hardening-h1/promotion-ledger.md:44` and its siblings). Duplicate suffixing has the
same class of bug: `Foo`, `Foo`, `Foo-1` all contribute `foo-1`, so GitHub's `foo-1-1` reds.

**Q-36 · A `docs:` token anywhere in a ledger is treated as an evidence cell.** CONFIRMED
(`:27`, `:161–163`): the pattern runs on every non-fenced line under `task/ledgers/`, not on table
evidence cells, so prose — including a quoted conventional-commit subject `docs: add changelog` —
fails `make ci`. This one will bite an unrelated unit eventually.

**Q-37 · An unclosed fence fail-opens the rest of the file.** CONFIRMED, low (`:130–134`): there is
no end-of-file balance check, so every later link in that file is silently skipped and not counted.
The tree has zero unbalanced fences today (measured this round).

**Q-38 · Absolute targets red instead of being skipped.** CONFIRMED, low (`:143–149`): C-001 and
`scripts/map.md` list absolute targets as out of scope beside `http(s)` and `mailto`, but a `/…`
target reaches `Path.resolve()` and is reported as resolving outside the repository. No such link
exists in the tree today.

**Q-39 · The gate is not wired into `ci.yml`'s guards job.** QUESTION. `check-docs-compaction`, the
gate this one was seated beside, is dual-wired (`make ci` *and* the guards job); this one is only
in `make ci`. `scripts/map.md` records that as deliberate because the card's Home named the
Makefile only, and the actor was banned from `.github/`. AGENTS.md's standing rule is dual-wiring
unless a row says otherwise, and that row exists — written by the unit itself. The owner's call:
accept the row, or wire the step and delete it.


### PROFILES-1 — logic round

The logic round reached Q-21…Q-25 independently and added one:

**Q-40 · Iceberg `write.*` keys in the table are table properties, not session knobs.** CONFIRMED
(`append.rs:272`): two of the nine UNREAD rows are Iceberg table properties the session was never
going to read, so calling them "accepted but unread session keys" mis-describes them. This is the
measured half of Q-25's ruling question.

**Q-41 · The probe has no NOT MEASURED state.** CONFIRMED (`profiles1_probe.py:29`): an empty
subject maps to UNREAD, so a key the probe could not exercise is indistinguishable from a key the
engine ignores. That is the mechanism behind Q-22 and Q-24, and REVIEW-FIX-8's D-2 should add the
third state rather than only re-labelling three rows.

### DISPLAY-BRIDGE-1 — no findings

Merged during this sweep (#454) and not in REVIEW-1 D-1's list; reviewed anyway because it
touches the display path three other findings live in. The round attacked empty, null and NaN
frames, `explode`, filtered frames, the 10–12 / 19–21 / 25 row boundaries, cached and eager frames
and `max_rows` 1–3: `show()` equals `repr` for uncached `mapInArrow` frames under both styles, the
spark peek bytes are unchanged, and the vertical warning survives. The second clean unit of the
sweep.


### BALLISTA-AUDIT-0 — the audit document

Reviewed as a document, and two of its four findings were independently confirmed by this run's
own Ballista work before the critic reported them — which is the best possible evidence that a
document review is worth doing.

**Q-42 · The session seat is `SessionBuilder`, not `SessionProvider`.** CONFIRMED
(`ballista-audit-2026-09-08.md:144`). BALLISTA-M1-B hit exactly this when it filled the seat: 54.1.0
exports `SessionBuilder` (`Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>`). The
audit's name is what the M1-B card copied, so the card is wrong in the same place.

**Q-43 · REST/axum cannot be feature-gated away, and the scheduler defaults pull AWS.** CONFIRMED
(`:176`). The audit's dependency-surface paragraph understates what depending on
`ballista-scheduler` brings in. Worth a line in `docs/design/distributed-m1.md` before Milestone 2
decides where the scheduler runs.

**Q-44 · The A1 reproduction command is a placeholder, so D-7's `20110` number is not
reproducible.** CONFIRMED (`:317`). A number in an audit that cannot be re-measured is exactly what
the reading-ledger discipline exists to prevent.

**Q-45 · `task/roadmap/epic-term/map.md:91` still says step 3 is pending.** CONFIRMED, low — #426
landed it.

### DOCS-LINKS-1 — logic round

**Q-46 · A same-file `#anchor` is skipped entirely.** CONFIRMED (`check_docs_links.py:141`): the
gate checks anchors only on cross-file links, so a stale same-file fragment ships green — and one
already has. **RE-RUN by the orchestrator:** `docs/spark-sql-iceberg-parity.md:3053` links to
`#v3-cov-8--ctas-derives-…-one`, but the heading at `:3018` gained a
`— **FIXED 2026-09-05, TYPES-1**` suffix, so GitHub's slug for it no longer matches the fragment.
The link is dead on GitHub today and the gate cannot see it. This is the highest-value finding of the two DOCS-LINKS-1 rounds because it is not
hypothetical: the tree is red today under a correct gate.

**Q-47 · A four-space-indented fence also fail-opens.** CONFIRMED (`:25`), same class as Q-37 and
with the same fix.


### LEDGER-READING-1 — quality round

**Q-48 · The unit's own clauses are cited only from a `map.md`.** CONFIRMED
(`python/repark-parity/tests/map.md:391`): C-001…C-004 carry their `pins:` citations in the map,
not in the three tests that actually pin them. That is legal — the ledger-grammar gate reads every
tracked file under `python/`, and the owner's 2026-08-26 adjustment explicitly blesses a citation
in a `map.md` — but for a unit whose whole subject is pin binding it is worth tightening: a reader
who opens the test cannot see which clause it holds.

**Q-49 · The reading fixture puts `docs:` in the Verdict column and `PROVEN` in Evidence.**
CONFIRMED, low (`test_dl_2_ledger_grammar.py:56`): the fixture's columns are transposed relative to
the ledger grammar it is fixture for. It passes because neither column is parsed positionally,
which is itself worth knowing.

### PREFLIGHT-PARITY-1 — logic round, no findings

The second clean round on this unit, from the other role: D-1/D-2 wiring correct, both pins red on
the parent Makefile and green on `HEAD`, the CAP-1 mirror 23 passed. PREFLIGHT-PARITY-1 is the only
unit in the sweep that took two rounds and yielded nothing in either.


## 3. Fix cards

The cards below are the disposition REVIEW-1 D-4 requires: a confirmed finding becomes a card for
a mechanical- or implementation-tier worker, never a patch by the critic. Each card names its
Home, the pin it must land red-first, and the finding it closes. None of them is opened by this
document — they enter the slate order the owner or the next orchestrator sets.

---

### Card REVIEW-FIX-1 — the CFG-1 mirror agrees with the loader (Q-3, Q-4, Q-5, Q-6)

**Home.** `python/repark/src/repark/config.py`, `python/repark/tests/test_config_mirror.py`,
`python/repark/src/repark/map.md` if a line moves. Nothing in `crates/`.

**Decisions.** D-1 The mirror's job is to refuse exactly what the loader refuses: a knob value is
an `int` (not `bool`) or an ASCII digit string after `strip()` — `value.isascii() and
value.isdigit()`, matching `_normalize_display_int` — and anything else raises so pydantic wraps
it as `ValidationError`, never `AttributeError`. D-2 `check_names` tracks `name -> key path` over
every catalog and every database source, so two database kinds sharing a name refuse the way
`sources.rs` `refuse_duplicate_names` does, naming both paths. D-3 `to_toml()` emits `[<profile>]`
for every profile in `profiles`, so an empty overlay survives the round trip.

**Pins (red first).** A float and a list knob raise `ValidationError`; `"٠١٢"` and `"²"` refuse
while `"4096"` and `" 4096 "` are accepted; `postgres.acme` beside `trino.acme` refuses naming
both; an empty `prod` overlay round-trips through `to_toml()` and `tomllib.loads`.

**Tier.** M (one Python module and its test file). **Rounds.** 1.

---

### Card REVIEW-FIX-2 — CFG-1's own pins stop reading the developer's `HOME` (Q-1)

**Home.** `crates/repark-core/src/config_file/tests/mod.rs`,
`crates/repark-core/src/config_file/tests/wiring.rs`, the `config_file/` maps.

**Decisions.** D-1 No pin in the unit calls the public `load()` without a stub environment: the
empty-config pin drives `discover` / `load_file_config` with `home: None` and an explicit
environment, per the unit's own D-8. D-2 The C-023 control session builds with discovery disabled
(`REPARK_CONFIG=""` or the environment parameter), so the "byte-identical to `.config()`" dump
compares the same two things on any machine.

**Pin (red first).** The suite passes with `HOME` pointed at a directory containing
`.config/repark/repark.toml` — today `cargo test -p repark-core config_file` fails 2 of 48 there.

**Tier.** M. **Rounds.** 1.

---

### Card REVIEW-FIX-3 — the polars display honours every legal `max_rows` (Q-9, Q-10, Q-11)

**Home.** `python/repark/src/repark/spark/dataframe/display.py`,
`python/repark/tests/test_display_polars_default.py`, `core.py`'s `show` docstring.

**Decisions.** D-1 `head_n + tail_n == min(n, max_rows)` for every `max_rows >= 1`: the odd row
goes to the head, which is what polars does. D-2 `max_rows = 1` prints the first row and the
ellipsis, never an empty body under a non-zero shape line. D-3 The `show(truncate=True)` →
`str_len` remap gains a pin that fails when the remap is deleted. D-4 The `show` docstring states
the probe-first count D-5 introduced.

**Pins (red first).** `max_rows` 1, 3 and 5 against live polars at the same `tbl_rows`; a mutation
pin over the `str_len` remap.

**Tier.** M. **Rounds.** 1.

---

### Card REVIEW-FIX-4 — the eager frame's checkpoint paths (Q-12, Q-13)

**Home.** `python/repark/src/repark/spark/dataframe/eager.py`,
`python/repark/src/repark/spark/dataframe/core.py` (`_materialize_cache_if_needed`),
`display.py` (`_styled_total_rows`), `python/repark/tests/test_df_eager_1.py`.

**Decisions.** D-1 `_to_lazy` never interpolates `_cache_view` into SQL: an eager frame becomes a
lazy one through `_spawn_preserving_identity(frame._inner)`. A set `_eager_shape` with no
`_cache_view` is not a cache-owned frame. D-2 `count()` and the styled row count keep D-4's ban on
a *count query* while still running the pending-checkpoint materialize, so
`localCheckpoint(eager=False)` is discharged by the next action as Spark discharges it.

**Pins (red first).** `eager().localCheckpoint().lazy()` plans and collects;
`eager().localCheckpoint(eager=False)` then `count()` leaves no pending checkpoint and drops the
cache view; the same through a styled `repr`.

**Tier.** M. **Rounds.** 1.

---

### Card REVIEW-FIX-5 — DESCRIBE intercepts a table named like a metadata table (Q-14, Q-15)

**Home.** `crates/repark-spark/src/describe_show.rs`,
`crates/repark-spark/src/tests/describe_table.rs`, the crate maps.

**Decisions.** D-1 The metadata-table check applies to the suffix position of a four-or-more-part
name, never to the table segment of a three-part name; `cat.ns.files` is a table. D-2 `Owner` is
resolved once when the session is built (or from a session-held configuration) and never from
`std::env::var` on the query path (ADR-0004); the pin asserts the resolved value, not that the
string is non-empty.

**Pins (red first).** `DESCRIBE TABLE EXTENDED ice.sales.files` returns Spark's rows against the
existing `ice.sales.files` fixture; the `Owner` row equals the session-resolved user with the
environment variable changed after the session is built.

**Tier.** I (the router and the Rust describe path). **Rounds.** 1.

---

### Card REVIEW-FIX-6 — `explain()` refuses the both-set shape (Q-17)

**Home.** `python/repark/src/repark/spark/dataframe/core.py` (`_explain_text`, `explain`),
`python/repark/tests/test_df_explain_1.py`.

**Decision.** D-1 Follow PySpark: passing both `extended` and `mode` raises the
`CANNOT_SET_TOGETHER`-shaped error rather than silently choosing one. A string `extended` with no
`mode` keeps working as it does today.

**Pin (red first).** `explain(True, mode="formatted")` raises; `explain(mode="formatted")` still
prints the tree; `explain("formatted")` still prints the tree.

**Tier.** M. **Rounds.** 1.

---

---

### Card REVIEW-FIX-7 — the config mirror cannot inject TOML, and a parse error cannot echo a secret (Q-18, Q-19)

**Home.** `python/repark/src/repark/config.py`, `python/repark/tests/test_config_mirror.py`,
`crates/repark-core/src/config_file.rs`, `crates/repark-core/src/config_file/tests/mod.rs`.

**Decisions.** D-1 Every rendered header segment is quoted with `_toml_text`, or the name is
refused at construction: a profile or source name containing `]`, `.`, a quote or a newline is a
`ValidationError`, the way catalog names already refuse `.`. D-2 A TOML parse failure carries
`error.message()` and the position only — never the source snippet the `toml` crate's `Display`
prints — so a file that is not TOML cannot echo its own lines into the error the facade raises.

**Pins (red first).** A profile name carrying `]\n[default.catalog.stolen]` refuses (or renders as
one quoted header and reads back as one profile); a config error over a file whose second line is
`aws_access_key_id = AKIA_PROBE` contains neither `AKIA_PROBE` nor the line text.

**Tier.** M for the Python half, I for the Rust half; they may be one round.
**Rounds.** 1–2. **Severity.** This is the sweep's only finding class with a security shape.

---

### Card REVIEW-FIX-8 — the PROFILES-1 probe is re-runnable and its table is true (Q-21, Q-22, Q-23)

**Home.** `scripts/profiles1_probe.py`, the probe document under `docs/`, and the CONF-UNREAD-1
card's key list if the owner rules Q-25.

**Decisions.** D-1 The probe writes to a unique temporary directory per run and is idempotent.
D-2 A key that refuses an invalid value at `getOrCreate` is not "accepted but unread"; the table
gains a third state (`VALIDATED`) and `concurrency_limit`, `file_scoped_rewrite` and `scan_pruning`
move into it. D-3 The summary counts agree with the table.

**Pin (red first).** The probe run twice in a row exits 0 both times and its two outputs agree.

**Tier.** M. **Rounds.** 1. **Blocked by** Q-25's ruling for the CONF-UNREAD-1 key list.


---

### Card REVIEW-FIX-9 — the polars renderer matches polars at its boundaries (Q-26, Q-27, Q-28)

**Home.** `python/repark/src/repark/spark/dataframe/polars_cells.py`,
`python/repark/src/repark/spark/dataframe/display.py`,
`python/repark/tests/test_display_polars_default.py`.

**Decisions.** D-1 The list-cell ellipsis threshold is polars': a four-element list renders
`[0, 1, … 3]`. D-2 Float spelling follows polars at the fixed/scientific switch (`±999999.0` →
`±9.99999e5`). D-3 The D-6 oracle pins exercise each boundary — list lengths 3, 4 and 5, and the
float switch either side — against `str(pl.from_arrow(...))` on the same Arrow table, so a
threshold cannot drift unpinned again. This card subsumes REVIEW-FIX-3's `max_rows` half if the
two are worked together.

**Tier.** M. **Rounds.** 1.


---

### Card REVIEW-FIX-10 — the READING exemption is a field, not a substring (Q-33)

**Home.** `scripts/check_ledger_grammar.py`, its test/fixture home, `scripts/map.md`.

**Decision.** D-1 `is_reading` parses the `Path` header *field* — the marker matches only at the
start of a header line, with the value read to the end of the token and compared to `READING`
exactly, so `READING-FOO` and a quoted mention inside a code span do not match.

**Pin (red first).** Three scratch ledgers: the real field (exempt), a `STANDARD` ledger quoting
the marker in prose (not exempt, rule B fires), and `READING-FOO` (not exempt). The first two are
the critic's own reproduction and can be lifted from its report.

**Tier.** M. **Rounds.** 1.

---

### Card REVIEW-FIX-11 — the unpinned `explain()` rows (Q-32, with Q-17)

**Home.** `python/repark/tests/test_df_explain_1.py`, and `core.py` only if a D-2 row is found
wrong once pinned.

**Decision.** D-1 Every D-2 row gets a pin: cost, codegen, `ANALYZE`, simple-mode physical-only,
and the blank-line separation. Land these with REVIEW-FIX-6 so the both-set refusal and the mode
rows are one change.

**Tier.** M. **Rounds.** 1.


---

### Card REVIEW-FIX-12 — the docs-links gate measures what it claims (Q-35, Q-36, Q-37, Q-38)

**Home.** `scripts/check_docs_links.py`, its fixtures and pins, `scripts/map.md`.

**Decisions.** D-1 Anchors are slugged from the *rendered* heading text: link syntax is reduced to
its text before slugging, and duplicate suffixing counts occurrences of the final slug, not of the
base. D-2 The `docs:` evidence-cell pattern applies to table cells under `task/ledgers/**`, not to
prose. D-3 An unbalanced fence at end of file is a finding, not a silent skip. D-4 An absolute
target is skipped as out of scope, like `http(s)` and `mailto`, and `scripts/map.md` says so once.

D-5 A same-file `#anchor` is checked against that file's headings, like a cross-file one — and the
stale `V3-COV-8` fragment at `docs/spark-sql-iceberg-parity.md:3053` that this uncovers is fixed in
the same PR, since the gate goes red on the tree without it (Q-46). D-6 An indented fence opener is
recognised (Q-47).

**Pins (red first).** A fixture whose heading is a link and whose inbound anchor is GitHub's; three
headings that collide on the base slug; a ledger line reading `docs: add changelog` in prose; a
file with an unclosed fence and a broken link after it; a `](/abs/path)` link; a same-file anchor
that does not resolve; a four-space-indented fence.

**Tier.** M. **Rounds.** 1. Every pin is lifted from the critic's reproductions.


## 4. Questions for the owner

| # | Question | Lean |
|---|---|---|
| Q-8 | A discovered `repark.toml` with only `[prod]` and `REPARK_ENV` unset applies nothing. Refuse, warn, or keep? | Keep — C-004 rules that `None` selects the default table alone, and an unknown `REPARK_ENV` already refuses loud. A warning would be a new ruling. |
| Q-16 | Does `DESCRIBE`'s D-1 `<ident>` include a two-part `ns.t` completed from the session default catalog? | If D-1 was narrowed to three-part names, record the residue on the DESC-1 registry row and leave `R-DESCRIBE-TWO-PART`; otherwise complete short names and pin dbt's spelling. |
| Q-20 | Discovery of `./repark.toml` + unrestricted `${VAR}` + Glue-at-build together let a cloned repo's file reach AWS on ambient credentials. Accept, or add a guard? | If a guard is wanted: refuse `glue` / `s3tables` from a CWD-discovered file unless `REPARK_CONFIG` named it, or require an explicit opt-in key. Not a silent fix — three rulings meet here. |
| Q-25 | R-16 names "nine `datafusion.*` keys"; the probe's nine UNREAD rows are four DataFusion keys, three `repark.*` session keys and two Iceberg table properties. Narrow R-16? | Yes — narrow CONF-UNREAD-1 to `enable_page_index`, `bloom_filter_on_read`, `coalesce_batches`, `write_batch_size`. |
| Q-29 | Did R-005 waive R-11's "duckdb is settled in step 4", or is a duckdb D-5 follow-up card owed? | A follow-up card — the duckdb branch is still count-first and its protected pin still fixes `max_rows_per_export = 2`. |
| Q-30 | `DESCRIBE TABLE EXTENDED` redacts with Spark's predicate; D-5's text names `prop_key_is_secret`, which also catches `s3.access-key-id`, `credential` and `basic.auth.user.info`. Which is the contract? | Switch Table Properties to `prop_key_is_secret` and document the Spark delta: printing an S3 access key or a REST credential in plaintext is the worse of the two divergences. |
| Q-34 | LEDGER-READING-1 D-3's prefix check was never implemented. Is it still wanted? | Ask the card's author; it is a documentation-vs-code mismatch, not a defect in what shipped. |
| Q-39 | `check-docs-links` is in `make ci` but not in `ci.yml`'s guards job, unlike the gate it was seated beside. Accept the recorded exception, or dual-wire it? | Dual-wire it: the exception exists only because the card's Home named the Makefile and the worker could not touch `.github/`, which is a process artefact, not a decision. |

## 5. What this sweep has not covered

Twenty rounds landed, plus one discarded fabrication. Not reviewed: the security angle of every unit but CFG-1 and
SQL-DESCRIBE-1, the `critic-logic` half of PREFLIGHT-PARITY-1, and PROFILES-1's remaining role, (DISPLAY-BRIDGE-1 was reviewed even though it is not in D-1's
list, because it merged into the display path mid-sweep). REVIEW-1's "done when" is therefore not
met; the card stays open with this
document as its first instalment.

Every Python reproduction ran against the owner's prebuilt native with per-round shims for the
bindings CFG-1 added; the fix rounds should re-run each one against a wheel built from `main`
before they trust a negative result.

## Pointers

- Up: [map.md](map.md) · The card: [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) REVIEW-1
- The run that produced it: [overnight-report-2026-09-10-b.md](overnight-report-2026-09-10-b.md)
