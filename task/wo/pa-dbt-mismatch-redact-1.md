# PA-DBT-MISMATCH-REDACT-1 (U-5 part 1, audit PA-01): the dbt profile mismatch names keys, never values     grade: B   engine: Muse xhigh, guided   release: 1.5.6

**Status:** order of record for the first of U-5's two units in the Iceberg closeout (closeout
plan row U-5; owner ruling T-5, 2026-10-13: "PA-01 to PA-04 inside the closeout, two Muse units";
ruling 69, 2026-10-14 night: "Both U-5 orders written tonight, launched after triage"). Written by
the orchestrator (Claude, claude-opus-5-5) on 2026-10-14 from
[the production audit](../../docs/cutover/production-audit-2026-10-11.md), finding PA-01. Main at
writing: `37d60eaf`. **Not launched before the 2026-10-15 triage.**

## 0. Why, and what is out of scope

`acquire_session` in `python/dbt-repark/src/dbt/adapters/repark/session.py` refuses a second
profile whose configuration differs from the live session's, and its `DbtRuntimeError` embeds both
`session_key` tuples with `repr`: the full sorted `catalog_properties` and `session_properties`,
secrets included (audit PA-01, S1, executed with sentinel secrets). The refusal is right; its text
discloses secrets into terminal output, job logs and support bundles.

Out of scope, by name: what the refusal refuses (the comparison stays on the full configuration);
the session lifecycle (`_build_session`, `release_session`); RePark's own redaction rules (this
unit consumes them, it does not change them); every other dbt message.

## 1. Rulings already made

- **Audit PA-01's required result** is the charter: "compare full configuration internally, but
  emit only safe identifiers and changed key names. Test the real dbt error and debug paths with
  sentinel secrets in both property dictionaries, including a source URL containing credentials.
  Verify that mismatch refusal still works. This boundary should consume the project's redaction
  policy; a Python exception must not bypass it."
- **Orchestrator, at writing (the owner may overrule at triage):** the message names the live and
  requested `database` and `warehouse` (identifiers, not secrets) and the sorted list of changed
  keys, each prefixed by its dictionary (`catalog_properties.<key>`, `session_properties.<key>`),
  and no value of either dictionary. Where a value must appear at all, it goes through the
  project's redaction (`_native.redact_property_value`, the rule `getAll` uses), never a new rule.
- **Rust first** (owner, 2026-09-14) does not move this: the adapter is Python and the change is a
  message.

## 2. Files

| Path | Change |
|---|---|
| `python/dbt-repark/src/dbt/adapters/repark/session.py` (192 lines) | the mismatch message; a helper that lists changed key names |
| `python/dbt-repark/src/dbt/adapters/repark/connections.py` | `_connection_keys` (line 59 at writing) prints `catalog_properties` in dbt's connection report: render it redacted, or drop it, per step 0's measurement |
| `python/dbt-repark/tests/test_profile_mismatch_redact.py` | new: the pins |
| `python/dbt-repark/tests/map.md`, `python/dbt-repark/src/dbt/adapters/repark/map.md` | rows |
| `task/ledgers/staging/pa-dbt-mismatch-redact-1-ledger.md` | new: the unit ledger |
| `docs/cutover/production-audit-2026-10-11.md` | an amendment line under PA-01, dated, naming the pins (the audit says later measurements amend it with a date) |

## 3. Skeletons

Grade B: none.

## 4. Steps

0. **Measure, no product code.** With two sentinel secrets (`SENTINEL_CAT_SECRET` in
   `catalog_properties`, `SENTINEL_SESSION_SECRET` in `session_properties`) and a source URL
   carrying a password (`postgresql://u:SENTINEL_URL_PW@h/db`) in each dictionary: (a) trigger the
   mismatch on main by changing only `database`, and paste which sentinels appear; (b) run dbt's
   `debug` path (`dbt debug` with a profile, or the adapter's connection display through dbt's own
   entry point) and paste which sentinels appear; (c) paste what `_native.redact_property_value`
   returns for each of the three. Commit the outputs in the ledger.
1. **Pins, red.** `test_profile_mismatch_redact.py`: the mismatch error carries no sentinel, names
   both databases, and names each changed key (one test per dictionary, one for the URL, one where
   only `database` differs); the refusal still raises `DbtRuntimeError` and the live session is
   unchanged; the debug display carries no sentinel. Red on main; paste the count. Commit.
2. **The message.** The fix in `session.py`. Commit.
3. **The debug display.** The fix in `connections.py` (only if step 0 (b) showed a sentinel).
   Commit.
4. **Records.** The ledger (one clause per sentence of §1's charter, each pinned), the dated
   amendment under PA-01, map rows, one line in
   `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`. Commit.

## 5. Gates and their expected output

| Command | Green means |
|---|---|
| `.venv/bin/python -m pytest python/dbt-repark/tests -q` | every test passes; paste the count |
| `uvx ruff@0.15.22 check` and `format --check` on changed Python | exit 0 |
| `make check-docs-links check-docs-compaction check-ledger-grammar`, `make spell-check` | exit 0 |
| `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| the comment gate | 0 hits |

CI green before the verifier: one fresh Opus 5.5 high verifier, scoped to the pins, the sentinel
probes and the debug path (closeout plan row U-5).

## 6. Halt rules

A sentinel that still appears through a path this order does not list (name the path); a change
needed outside §2; a test that needs a real credential, a cloud request or AWS; any decision this
order does not make. Hand back with the question, its premise and a lean.

## 7. Hand-back

`status`, `head`, `commits`, `step0` (the three probes and their output), `pins` (red on main,
green on head), `gates`, `questions`, `out_of_scope_observed`.

Standing rules inherited from [TEMPLATE.md](TEMPLATE.md): no code comments from Anthropic models;
relocated code sheds its comments; map.md lockstep; the scratch clone, never the live worktree;
never push, never open a pull request; the TRO-Wolf identity and exactly one trailer,
`Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
