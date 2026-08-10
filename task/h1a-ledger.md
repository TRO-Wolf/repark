# Unit ledger — H-1a **split A**: session-timezone conf surface, oracle override, differential rows

**Unit:** H-1a of the V2 Engine Hardening campaign
([../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md) "H-1a", decision D7 in
[../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md)) ·
**Date:** 2026-08-10 · **Panel:** FULL (semantics + oracle lenses)

**This ledger covers SPLIT A only.** The brief's split rule reads: *"unit A = the conf surface +
the registry override + the differential rows; unit B = the extraction fix + its Rust pins.
Neither half lands without its own tests."* Split A therefore changes **no evaluated result**: it
adds a validated session-timezone configuration, teaches the live oracle registry to run a
scenario under a non-UTC session, and records the divergence class against real PySpark. The
"§ Split B (reserved)" section at the end is deliberately empty.

---

## 1. What landed

### 1.1 The configuration surface (one key, resolved once, engine-validated)

| Layer | File | What it holds |
|---|---|---|
| Engine | `crates/repark-core/src/session_time_zone.rs` | `SESSION_TIME_ZONE_KEY` (the ONE spelling), `DEFAULT_SESSION_TIME_ZONE = "UTC"`, the validated `SessionTimeZone` type, `resolve_session_time_zone` |
| Engine | `crates/repark-core/src/session.rs` | `build()` resolves the zone **once** and stores it; `ReparkSession::session_time_zone()` reads it back |
| Facade | `python/repark/src/repark/session/session_time_zone.py` | the same key spelling, the `UTC` default, `warn_runtime_session_time_zone_not_applied` |
| Facade | `_funcs.py` / `builder_conf.py` / `session_core.py` | the `conf.get` default; `conf.set`/`conf.unset` accepted-warned-not-stored (D-A5); exclusion from the `getOrCreate` reuse fold |

`ReparkSession.builder.config("spark.sql.session.timeZone", "America/New_York")` → the facade
passes the whole `.config` map to the binding → `ReparkSessionBuilder::configs` → `build()`
validates the zone against Arrow's zone database and stores it. An unknown zone is
`Error::Config` (→ `IllegalArgumentException`) **at `getOrCreate`**, never at query time.

### 1.2 The live-registry per-scenario session-conf override

`Scenario.session_conf` (a tuple of pairs — the dataclass is frozen) applied by each engine's own
mechanism: the shared JVM oracle takes it through `spark_session_conf` (set around the leg,
restore after); repark takes it by BUILDING a session with it, because repark resolves the zone
once at construction. Two scenarios added, one either side of UTC. Size pin **27 → 29** and the
uniqueness pin moved in the same diff, plus a new pin that the non-UTC overrides exist and use the
one key spelling. `docs/port/census.md`'s record of that count moved with them.

### 1.3 The differential rows (record mode, real PySpark 4.1.2)

`python/repark/tests/test_session_timezone_parity.py` — 20 recorded recipes + the
`current_timestamp` type row: **14 for gap G1** (budget 10–14) and **7 for gap G16** (budget 6–8).
18 of the 20 run over scalar literals; the 2 `column_extract_*` rows run over a real tz-aware
TIMESTAMP **column** (a two-row in-memory frame registered as a temp view) under each non-UTC
zone — the brief's own recipe wording, added in the fix pass (§8, finding O-2). Every Spark half
was recorded by running the committed recipe on live PySpark; nothing is hand-computed, and the
driver that records them is committed (`_record_session_timezone_goldens.py`).

---

## 2. Decisions, with rationale

**D-A1 — The engine owns the key and the validation; the facade owns no second validator.**
Two validators drift. The facade forwards the value and surfaces the engine's refusal. Cost,
accepted and recorded: on the `getOrCreate` **reuse** path no new session is built, so an invalid
zone is neither validated nor applied — it falls into the existing engine-knob `unapplied` warning
("engine knobs are fixed at session build"). Nothing is silently wrong; the value simply does not
take, loudly. (Contrast `repark.memory.limit.gb`, whose *range* check is facade-side and therefore
does fire on reuse. Zone-id validity is not a range check — it needs the zone database.)

**D-A2 — The resolved zone lives on `ReparkSession` (repark-core), not in a DataFusion
`ConfigExtension`.** Three candidate homes were considered:

1. a `ConfigExtension` in `repark-functions` (the `repark.sql.maxArrayElements` precedent),
2. DataFusion's own `datafusion.execution.time_zone`,
3. a typed field on the session, resolved in `build()`.

(2) was rejected outright: DataFusion's `now()` reads that option, so setting it would change
`current_timestamp` **in this unit**, which is exactly the evaluated-result change split A must
not make. (1) was rejected for this split because `extensions_options!` derives a *settable*
option path (`SET <prefix>.session_time_zone = …`), which is a second, live spelling of the knob —
and the unit's acceptance gate is "exactly one authoritative spelling". (3) keeps the value
immutable for the session's life and needs no new crate edge: `repark-core` is tier 2 and
`repark-functions` is a tier-3 leaf with no internal deps, so a core→functions edge would be a
forbidden upward edge under `scripts/check_crate_dag.py`. **Consequence handed to split B:** it
must choose how the zone reaches the extractor layer — the natural seam is `SparkExtension`
(which already sees the builder conf map and depends on both crates) constructing zone-aware
shims, or a `ConfigExtension` installed from that same hook. Split B owns that decision; nothing
here forecloses it.

**D-A3 — The default is `UTC`, not the host's local zone.** Spark defaults to the JVM's local
zone. Matching that would (a) make a run's results depend on the host and (b) require a
host-environment read, which `docs/adr/0004-server-prep-disciplines.md` forbids. Declared as a
divergence with a registry row (§6) rather than inherited silently.

**D-A4 — Exactly one spelling, no alternates.** The neighbouring knobs in this repo deliberately
carry two spellings (`repark.sql.maxArrayElements` / `…max_array_elements`;
`repark.batch.size` / `spark.sql.execution.arrow.maxRecordsPerBatch`). This one does not, because
the acceptance gate asks for one and because the Spark spelling has no repark-native counterpart
worth inventing. A lookalike is an unknown `.config` key and configures nothing — pinned by
`lookalike_spellings_are_not_a_second_way_to_set_the_zone` over six near-misses.

**D-A5 — A runtime `spark.conf.set` of the key is ACCEPTED, warned once, and NOT stored.**
*This decision was reversed mid-unit by evidence; both halves are recorded because the reversal is
the useful part.*

*First choice (wrong):* refuse loud, copying the memory-pool knob's "one truth, not two lying
knobs" idiom — `IllegalArgumentException`, build-time only.

*What killed it:* the full facade suite went red on
`test_pyspark_compat_smoke.py::test_compat_smoke_suite_in_subprocess`, whose subprocess runs the
**pinned Apache test**
`pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_dst`.
That test sets this exact key through PySpark's own `sql_conf` context manager
(`.../test_creation.py:139`). A raise there is not a strictness win — it is a drop-in regression on
a *pinned* test, i.e. census movement, on a promise ("change the import line") that outranks the
idiom.

*Final choice:* the `.master(...)` / arrow-batch-sentinel shape (OTH-010) — accept the call, emit
the disclosure once per process, and **do not store the value**, so `conf.get` keeps reporting the
zone the live engine session actually has. `conf.unset` of the key is handled the same way, because
tombstoning it would let `conf.get` fall back to the `UTC` default on a session built with another
zone — the same split-brain by another route. The facade therefore never lies; it declines,
audibly. Declared divergence, registry row in §6.

*Handed to split B:* if the fix routes the zone through DataFusion `ConfigOptions` (see D-A2), a
runtime `conf.set` becomes *implementable* — forward it as a live `SET` and the divergence
disappears entirely. That is a better end state than either option here, and it is split B's to
take or decline deliberately.

**D-A6 — THE ORDERING QUESTION: divergent rows land as explicit current-behavior DISCLOSURES.**
The extraction fix is split B, so `repark == Spark` rows would be red on arrival, and a red gate is
not acceptable. Deleting the rows until B lands would leave the CRITICAL class unmeasured. The
harness's own idiom for this is the live registry's `Disclosure` (pin BOTH halves; a silent
convergence reds), and the facade tree's `test_divergence_*` convention (pin repark's behavior,
record Spark's half as a constant, and say in the message what must be revisited if it changes).
This unit follows both: each divergent row carries repark's actual Arrow output **and** the
recorded Spark output, asserts repark still matches its own half, and asserts the two **still
differ**, naming the in-flight fix. When split B lands, each row flips `repark=None` — and that
flip is precisely the revert-red evidence the testing contract demands, because reverting the fix
turns the equality assertion red again.

*Structural improvement taken over the plain disclosure pattern:* an all-disclosure corpus cannot
distinguish a zone-aware engine from a zone-blind one — every row would still "diverge" if a fix
moved results the WRONG way. So the corpus carries **two control rows** that assert plain
EQUALITY today and must stay equal after the fix: DATE extraction and leap-day DATE arithmetic are
session-zone independent by contract. A fix that pushed the session zone into the DATE path reds
there. `test_session_timezone_row_set_covers_both_gap_budgets` asserts at least one equality row
exists, so the corpus cannot degenerate into all-disclosures later.

**D-A7 — The two live scenarios assert EQUALITY, and are honest about why.** A live scenario
asserts `repark == pinned golden == live Spark`; a timestamp-extraction scenario would therefore
red the live tier today. The two added scenarios pin a real invariant instead — DATE extraction and
DATE arithmetic must NOT move with the session zone — under a non-UTC ORACLE session. They prove
the override reaches the oracle end to end, and they are the pins that catch a zone-blind fix that
over-reaches into the DATE path. The divergent TIMESTAMP rows for the same class live in the
facade corpus as recorded disclosures until split B; converting them into live disclosures is a
natural split-B follow-on.

---

## 3. Evidence per acceptance-gate item

The brief's H-1a gate is written for the WHOLE unit; the items below are the ones split A can
discharge. Items belonging to split B are named as such, not silently claimed.

| Gate item | Verdict | Evidence |
|---|---|---|
| A session-conf grep shows exactly ONE authoritative spelling of the timezone key | **MET** | §4 grep; `lookalike_spellings_are_not_a_second_way_to_set_the_zone` (Rust) + `test_registry_runs_at_least_two_scenarios_under_a_non_utc_oracle` (asserts every override uses the one key) |
| At least two live-tier scenarios run under a non-UTC oracle | **MET** | `date_extractor_under_new_york_session`, `date_math_under_tokyo_session`; full live run green (§5) |
| With the live gate unset they SKIP with a visible reason, never a silent pass | **MET** | JVM-free run: 32 passed, 33 skipped, reason `REPARK_PARITY_LIVE unset …` (§5) |
| The registry's size pin and its name-uniqueness pin are updated in the same diff as the registry change | **MET** | `test_registry_covers_the_mandated_golden_family` 27 → 29 for both pins, same file, same diff; `docs/port/census.md` count moved too |
| 10–14 differential rows vs real PySpark (record mode) for G1 | **MET (14)** | `G1_ROWS` (13 = 11 scalar-literal + 2 column-path) + `test_current_timestamp_type_and_zone_disclosure` (1), budget pinned by `test_session_timezone_row_set_covers_both_gap_budgets` |
| Fold in gap G16: 6–8 additional differential rows | **MET (7)** | `G16_ROWS` |
| Rows assert on the Arrow path, value AND type | **MET** | every row goes through `repark_parity.assert_frames_equal`, whose signature is `(name, type, nullable)` per field, then bit-exact values; no `show` anywhere |
| Every class pinned at all FOUR entry points (native DataFrame / ANSI door / Spark door / facade) | **CLAIMED AS SPLIT B — NOT MET HERE** | Split A pins ONE cell: the facade `sql()` door. It now pins it over a tz-aware timestamp COLUMN as well as over scalar literals (`column_extract_under_new_york_session` / `..._tokyo_session`), so the brief's literal recipe is constructible and constructed — but three of the four cells (native DataFrame API, ANSI door, Spark door) are **unpinned in this split**, deliberately and by declaration, not by silence. The original justification ("until extraction honors the zone the other cells would pin the same absence again") does NOT survive review: pinning an absence per cell is exactly what this corpus does, and the panel demonstrated the DataFrame-API cell diverges identically today. The honest statement is: split B owes the matrix, and owes it WITH the fix. Residual §7.1. |
| The brief's recipe: `year`/`month`/`day`/`hour` over a tz-aware timestamp COLUMN under two non-UTC session zones | **MET** | `column_extract_under_new_york_session` (all four fields move, incl. the YEAR across a boundary) + `column_extract_under_tokyo_session` (only the hour moves — the pair is what distinguishes a session-zone bug from an offset-sign bug); both re-derived against live PySpark 4.1.2 by the committed record driver. Enforced by `test_session_timezone_row_set_covers_both_gap_budgets`, which asserts the column family spans BOTH zones. |
| Reverting the extraction change reds at least one pin per named class | **SPLIT B** | there is no extraction change in split A; the flip of each disclosure row to equality IS that evidence, and it happens in split B |

---

## 4. Gate output (verbatim)

### 4.1 `make ci`

```text
$ make ci
cargo fmt --check
cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods
cargo clippy --locked --workspace --lib --bins --exclude repark-python -- \
        -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used ...
cargo clippy --locked -p repark-python --lib -- \
        -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used ...
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
cargo check --locked --workspace
uvx ruff@0.15.22 check .
All checks passed!
uvx ruff@0.15.22 format --check .
240 files already formatted
uv lock --locked
Resolved 29 packages in 1ms
uvx taplo@0.9.3 format --check
uvx taplo@0.9.3 lint
uvx typos@1.47.2
EXIT=0

NOTE (honest): the FIRST `make ci` run of this unit was RED at `py-lint` — two untracked scratch
scripts (the probe and the record-mode driver) were sitting in the worktree root and ruff lints the
whole tree. They were moved OUT of the worktree (they must never land); a second run was RED once
more on `UP017` (`dt.timezone.utc` -> `dt.UTC`) in the new test file; the third and final run is the
one above. The gate did its job twice.
```

### 4.2 `make test` (Rust workspace)

```text
$ make test          # cargo test --locked --workspace
...
test session_time_zone::tests::absent_key_resolves_to_the_utc_default ... ok
test session_time_zone::tests::iana_zone_id_is_accepted_verbatim ... ok
test session_time_zone::tests::fixed_offset_is_accepted ... ok
test session_time_zone::tests::padded_value_is_trimmed_not_treated_as_a_different_zone ... ok
test session_time_zone::tests::unknown_zone_fails_loud_naming_the_key ... ok
test session_time_zone::tests::blank_value_fails_loud_rather_than_falling_back_to_the_default ... ok
test session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone ... ok
test session_time_zone::tests::bare_session_carries_the_utc_default ... ok
test session_time_zone::tests::builder_conf_reaches_the_built_session ... ok
test session_time_zone::tests::invalid_zone_fails_the_build_not_a_later_query ... ok
test session_time_zone::tests::session_clone_shares_the_resolved_zone ... ok
...
31 test binaries, every one `test result: ok`; 0 `test result: FAILED`
EXIT=0
```

### 4.3 Facade suite

The numbers below are the FIX-PASS re-capture (2026-08-10, post-§8). The actor's original
capture recorded `2593 passed, 36 skipped`; BOTH panel lenses re-ran the identical command on the
identical tree and got `2594 passed, 36 skipped`, twice each. The one-test gap was never
reproducible and is not environment-sensitive — collection is deterministic from the file tree —
so the honest reading is that the actor's number was captured mid-edit, one test before the final
tree. It is corrected here rather than explained away. The fix pass then added 6 tests (2 column
rows + 4 conf-surface pins), which is the whole of the 2594 → 2600 move.

```text
$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests -q
2600 passed, 36 skipped, 46 warnings in 142.79s (0:02:22)
EXIT=0

Run against the parity-live provisioning (the four facade extras PLUS `--extra record`), NOT a bare
`make py-test-facade` sync — deliberately, because `uv sync` is EXACT and the facade-extras-only
sync uninstalls pyspark, which would have silently skipped the Apache compat-smoke subprocess that
found the D-A5 regression. `make py-test-facade` verbatim was run separately (below) once the
recording work no longer needed pyspark.

$ make py-test-facade
uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark
cd python/repark && VIRTUAL_ENV=... uvx maturin@1.14.1 develop
PYTHONPATH=python/repark-parity/src ... uv run --no-project python -m pytest python/repark/tests -q
2564 passed, 46 skipped, 37 warnings in 91.47s (0:01:31)
EXIT=0

The denominator differs from the run above (2564/46 vs 2600/36) for a known reason and not a
regression: this target's `uv sync` is EXACT and does NOT request `--extra record`, so pyspark is
uninstalled and the pyspark-oracle cohort (compat smoke, the UDF/pandas oracle files) skips. That is
the target's normal shape; it is why the recording work above ran against the parity-live
provisioning instead.
```

### 4.4 `bash scripts/check_map_md.sh`

```text
$ bash scripts/check_map_md.sh
map_md EXIT=0
(silent on success; the guard reads the STAGED set, so every touched directory's map.md was staged
alongside its code — crates/repark-core/src, crates/repark-core/src/session_time_zone (NEW dir, new
map.md with a populated ## Debug), python/repark/src/repark/session, python/repark/tests, task)
```

### 4.5 `python3 scripts/check_manifest.py`

```text
$ python3 scripts/check_manifest.py
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index,
the status document and the crate maps
manifest EXIT=0
(run because docs/port/census.md — a declared document — was edited; no manifest field changed)
```

### 4.6 Record mode against live PySpark 4.1.2 (the oracle leg)

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python <record driver>
[G1] year_of_instant_under_new_york_session [America/New_York] PASS
    live spark schema = [('year_part', 'int32', True)]
    live spark rows   = [{'year_part': 2023}]
[G1] month_of_instant_under_new_york_session [America/New_York] PASS
[G1] day_of_instant_under_new_york_session [America/New_York] PASS
[G1] hour_of_instant_under_new_york_session [America/New_York] PASS
    live spark rows   = [{'hour_part': 8}]
[G1] hour_of_instant_under_tokyo_session [Asia/Tokyo] PASS
    live spark rows   = [{'hour_part': 21}]
[G1] year_month_day_of_instant_under_tokyo_session [Asia/Tokyo] PASS
[G1] to_timestamp_of_zone_suffixed_string [America/New_York] PASS
    live spark schema = [('ts', 'timestamp[us, tz=UTC]', True)]
[G1] dst_spring_forward_instant_hour [America/New_York] PASS
[G1] dst_fall_back_repeated_local_hour [America/New_York] PASS
    live spark rows   = [{'before_part': 1, 'after_part': 1}]
[G1] tz_aware_to_naive_round_trip [America/New_York] PASS
[G1] date_trunc_day_across_a_zone_boundary [America/New_York] PASS
    live spark rows   = [{'day_start': datetime.datetime(2024, 6, 14, 4, 0, tzinfo=ZoneInfo('UTC'))}]
[G16] pre_1970_extract_under_new_york_session [America/New_York] PASS
[G16] pre_1970_timestamp_cast_to_bigint [America/New_York] PASS
    live spark rows   = [{'epoch_value': -1800}]
[G16] year_boundary_date_trunc_under_tokyo_session [Asia/Tokyo] PASS
[G16] year_boundary_extract_and_format_under_new_york_session [America/New_York] PASS
[G16] leap_day_extract_under_new_york_session [America/New_York] PASS
[G16] date_extraction_is_session_zone_independent [America/New_York] PASS
[G16] leap_day_date_arithmetic_is_session_zone_independent [Asia/Tokyo] PASS
[G1] current_timestamp live type = timestamp[us, tz=UTC], nullable=False

record mode: 18 rows re-derived, 0 mismatch(es)
```

**Re-captured in the fix pass**, after the driver was committed and the two column rows added:

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=python/repark-parity/src \
    .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py
...
[G1] column_extract_under_new_york_session [America/New_York] PASS
[G1] column_extract_under_tokyo_session [Asia/Tokyo] PASS
...
[G1] current_timestamp live type = timestamp[us, tz=UTC], nullable=False

record mode: 20 rows re-derived, 0 mismatch(es)
EXIT=0
```

The driver imports `ROWS` from the COMMITTED test module and runs each row through the SAME
`run_row` helper the assertions use, so the recorded golden and the asserted recipe cannot drift
apart — there is one recipe, not two copies. It is now COMMITTED
(`python/repark/tests/_record_session_timezone_goldens.py`, §8 finding O-4): the actor left it as
scratch outside the worktree, which made "recorded against live PySpark 4.1.2" unfalsifiable from
inside the repo the moment that session ended. It never edits the corpus — it prints the live
values and exits non-zero, because a driver that rewrites its own oracle launders drift.

### 4.7 The live oracle tier, armed

```text
$ JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 \
    PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests/test_parity_live.py -q
66 passed in 17.54s          # fix-pass re-capture; was 65 before §8 added the override pin
  (29 routine JVM-free scenario legs + 29 live triple legs + 4 live disclosure legs + the flag
   detector + the two registry pins + the `build_repark_engine` override pin added in §8; the two
   non-UTC scenarios assert repark == pinned golden == live Spark under America/New_York and
   Asia/Tokyo respectively)

$ (same command with the gate UNSET)
33 passed, 33 skipped in 0.79s   # was 32/33; the new override pin is JVM-free
  skip reason: "REPARK_PARITY_LIVE unset — the live PySpark oracle tier is skipped (routine CI is
  JVM-free). Set REPARK_PARITY_LIVE=1 with a JVM present (...) to run it." 
```

---

## 5. Provocation proofs

This unit adds no new *mechanical gate* (no lint entry, guard script, CI step or hook), so
`docs/testing.md` "Gate provocation proofs" does not bind it. Three of its claims are nonetheless
detection claims, and a green run proves nothing about detection — so each was provoked both ways.
**None of the provocations is committed**; each was reverted immediately, and the clean run is
captured beside the red one.

### P-1 — "exactly one spelling" (Rust)

*Claim:* a lookalike conf key is not a second way to set the session zone.

**must-FAIL** — plant an all-lowercase alias in `resolve_session_time_zone`
(`.or_else(|| config.get("spark.sql.session.timezone"))`):

```text
$ cargo test --locked -p repark-core session_time_zone
test session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone ... FAILED

---- session_time_zone::tests::lookalike_spellings_are_not_a_second_way_to_set_the_zone stdout ----
thread '...' panicked at crates/repark-core/src/session_time_zone/tests.rs:98:9:
assertion `left == right` failed: "spark.sql.session.timezone" must not be a second spelling of
spark.sql.session.timeZone
  left: "America/New_York"
 right: "UTC"

test result: FAILED. 10 passed; 1 failed; 0 ignored; 0 measured; 90 filtered out
error: test failed, to rerun pass `-p repark-core --lib`
```

**must-PASS** — alias reverted:

```text
$ cargo test --locked -p repark-core session_time_zone
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 90 filtered out; finished in 0.01s
```

### P-2 — the disclosure-convergence detector (Python)

*Claim:* if repark and Spark ever agree on a disclosed row, the row goes RED instead of passing
quietly.

**must-FAIL** — simulate convergence by moving the recorded Spark golden of
`year_of_instant_under_new_york_session` to repark's value (2023 -> 2024):

```text
$ pytest "...::test_session_timezone_row_matches_spark_or_still_diverges[year_of_instant_under_new_york_session]" -q
>       assert _frames_differ(row.repark, row.spark), (
E       AssertionError: year_of_instant_under_new_york_session: repark and Spark have CONVERGED —
E       this disclosure is stale. Do not delete the row: flip it to an equality row (repark=None)
E       and record the convergence. the instant is 2023-12-31 23:30 in New York, so Spark's year is
E       2023; repark extracts in the stored (UTC) zone and answers 2024. Flipped to equality by the
E       session-timezone extraction fix (briefs/v2-engine-hardening.md, H-1a split B).
E       assert False
E        +  where False = _frames_differ(pyarrow.Table\nyear_part: int32\n----\nyear_part: [[2024]],
E                                        pyarrow.Table\nyear_part: int32\n----\nyear_part: [[2024]])
1 failed in 0.44s
```

**must-PASS** — golden restored:

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
1 passed in 0.39s
```

### P-3 — the non-UTC-oracle registry pin (Python)

*Claim:* the registry cannot silently lose its ability to run the oracle in a non-UTC session.

**must-FAIL** — drop `session_conf` from `date_math_under_tokyo_session`:

```text
$ pytest python/repark/tests/test_parity_live.py -q -k registry
>       assert len(overridden) >= 2, "at least two scenarios must run under a non-UTC oracle session"
E       AssertionError: at least two scenarios must run under a non-UTC oracle session
E       assert 1 >= 2
1 failed, 1 passed, 63 deselected in 0.39s
```

**must-PASS** — override restored:

```text
$ pytest python/repark/tests/test_parity_live.py -q -k registry
2 passed, 63 deselected in 0.35s
```

**Tree check.** All three provocations were reverted by restoring a pre-provocation copy of each
file; `git diff` against the staged tree is empty for
`crates/repark-core/src/session_time_zone.rs`, `python/repark/tests/_live_parity.py` and
`python/repark/tests/test_session_timezone_parity.py`, and the final `make ci` / `make test` /
facade runs above are post-revert.

*Amended in the fix pass:* a `git diff` check is NOT sufficient on its own. A stale pytest
assertion-rewrite cache in `python/repark/tests/__pycache__` survived a provocation revert and
produced a twice-consecutive false RED for one panel lens (finding S-8). Every revert in §8.2 was
verified by **sha256 of the file AND a cold `__pycache__`**, and all `*.pyc` under `python/` and
`scripts/` were deleted before the §8.3 gates were run.

---

## 6. Divergence-registry rows (ready to paste — this unit must NOT create the file)

`docs/spark-sql-iceberg-parity.md` is authored by unit **H-1d**, which merges FIRST. The rows below
are this unit's output for it, in the four-column shape the brief mandates (repark behavior /
Spark behavior / the pin / rationale). The orchestrator lands them at assembly.

**Declared-divergence rows**

| Row | repark behavior | Spark behavior | Pin | Rationale |
|---|---|---|---|---|
| **TZ-1 — timestamp extraction ignores the session zone** | `year` / `month` / `dayofmonth` / `hour` / `date_trunc` / `date_format` over a TIMESTAMP resolve in the STORED (UTC) zone; `spark.sql.session.timeZone` does not move them | resolves every one in the session zone (the census measured a four-hour offset) | `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges` — **15 extraction-class disclosure rows** (the module holds 18 disclosure table rows + 2 equality controls; of the 18, two are TZ-4 and one is TZ-5), e.g. `[hour_of_instant_under_new_york_session]`, `[dst_fall_back_repeated_local_hour]`, `[year_boundary_date_trunc_under_tokyo_session]`, and the two COLUMN-path rows `[column_extract_under_new_york_session]` / `[column_extract_under_tokyo_session]` | **IN-FLIGHT FIX** (campaign decision D7; H-1a split B). Recorded as a disclosure so the CRITICAL class is measured while the fix lands; every row flips to an equality assertion with the fix, which is the fix's revert-red evidence. |
| **TZ-2 — the session-zone default is `UTC`** | `spark.conf.get("spark.sql.session.timeZone")` is `UTC` on a session that never set it | the JVM's local zone, so results depend on the host | `test_session_timezone_conf_is_readable_back_and_defaults_to_utc`; `crates/repark-core/src/session_time_zone/tests.rs::absent_key_resolves_to_the_utc_default` | **DECLARED.** A reproducible default beats a host-dependent one, and reading the host zone would be the query-time/construction-time environment read `docs/adr/0004-server-prep-disciplines.md` forbids. A job that wants host-local behavior sets the key. |
| **TZ-3 — a runtime `conf.set` of the session zone is accepted, neither validated nor applied** | the call succeeds, warns (`accepted for source compatibility but NOT applied ... its value is NOT validated`) and stores nothing, so `conf.get` keeps reporting the engine's real zone; `conf.unset` behaves the same way. **The value is not checked at all**: `conf.set(K, "Mars/Olympus_Mons")` is swallowed, where live Spark raises `[INVALID_CONF_VALUE.TIME_ZONE] ... SQLSTATE: 22022`. **The disclosure is once per PROCESS, not per session** (the OTH-010 `_warn_master_once` idiom), so a second session in the same interpreter gets a fully SILENT no-op. | applies the new zone to the live session immediately, and validates it | `test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied` (covers the valid leg, the warning TEXT saying "NOT validated", and the invalid-value leg under `simplefilter("error")` so a second warning would raise); `test_apache_sql_conf_context_manager_round_trips_the_session_zone`; `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise` (the same laxness on the reuse path) | **DECLARED**, and evidence-driven: refusing broke the pinned Apache drop-in test `test_create_dataframe_from_pandas_with_dst`, which sets this key via PySpark's `sql_conf`. Accepting keeps drop-in; not storing keeps `conf.get` honest. The unvalidated half is the accepted COST of keeping exactly one validator (in the engine, at build) — repark is knowingly laxer than PySpark on this key at runtime, and the warning text now says so in as many words rather than leaving a typo'd zone to a silent no-op. Becomes fixable if split B routes the zone through DataFusion `ConfigOptions`. |
| **TZ-4 — TIMESTAMP Arrow export is tz-naive** | `to_arrow()` yields `timestamp[ns]` (or `timestamp[us]` after `date_trunc`) with NO timezone | `toArrow()` yields `timestamp[us, tz=UTC]` | `[to_timestamp_of_zone_suffixed_string]`, `[tz_aware_to_naive_round_trip]`, `test_current_timestamp_type_and_zone_disclosure` | **IN-FLIGHT FIX** — the export-type half of TZ-1's class. A consumer that localizes a tz-naive column silently shifts it, which is why the type is asserted and not only the value. |
| **TZ-5 — `CAST(TIMESTAMP AS BIGINT)` returns nanoseconds** | epoch NANOSECONDS (`-1800000000000` for 1969-12-31T23:30:00Z) | epoch SECONDS (`-1800`) | `[pre_1970_timestamp_cast_to_bigint]` | **NEW, found by this unit; NOT a zone bug** — a cast-unit bug, correctly signed before 1970. Disclosed here; the fix needs its own unit (a 10^9 factor on every timestamp->integer cast is a silently-wrong-result class in its own right). |

**Backlog rows (no pin yet — surfaced by this unit while authoring the corpus)**

| Row | repark behavior | Spark behavior | Status |
|---|---|---|---|
| **B-TZ-1 — `unix_timestamp` is not a Spark-door SQL function** | `SELECT unix_timestamp(...)` fails: `Error during planning: Invalid function 'unix_timestamp'. Did you mean 'to_timestamp'?` | returns epoch seconds | BACKLOG. The facade exposes a Python `F.unix_timestamp`; only the SQL-door spelling is missing, so this is a two-doors coverage hole, not a missing kernel. |
| **B-TZ-2 — `timestamp_seconds` is not a Spark-door SQL function** | `SELECT timestamp_seconds(-1800)` fails: `Invalid function 'timestamp_seconds'` | builds a TIMESTAMP from epoch seconds | BACKLOG, same shape as B-TZ-1 (a `F.timestamp_seconds` exists on the facade). |
| **B-TZ-3 — `date_add(DATE, <integer literal>)` fails to coerce in the SQL door** | `Failed to coerce arguments ... 'date_add(Date32, Int64)' ... You might need to add explicit type casts` | returns a DATE | BACKLOG. Spark's own idiom (`date_add(d, 1)`) does not plan; the DataFrame-API spelling does. |
| **B-TZ-4 — `CAST(TIMESTAMP AS STRING)` returns `string_view` and ISO-`T` formatting** | Arrow type `string_view`; rendered `2024-06-15T12:00:00` in the stored zone | Arrow type `string`; rendered `2024-06-15 08:00:00` in the session zone | BACKLOG (observed while authoring; the round-trip row was deliberately narrowed to the timestamp column so one row does not conflate the zone class with the string-type class). |
| **B-TZ-5 — the SQL `SET` door does not reach the `spark.*` conf namespace** | `spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")` (and the backtick-quoted / no-space spellings) raise `datafusion engine error: Invalid or Unsupported Configuration: Could not find config namespace "spark"` | applies the conf | BACKLOG, surfaced by the panel. **Pre-existing for every `spark.*` key, not introduced by this unit**, and wider than the session zone — it wants its own decision rather than a fold into split B. Recorded because D-A5's drop-in argument covers the Python `conf.set` door and must not be read as covering the SQL door too. |

---

## 7. Residuals, for the verifier and for split B

1. **Four-entry-point matrix is split B's, claimed as split-scope in the §3 gate table.** Split A
   pins ONE cell — the facade `sql()` door — and now pins it over a tz-aware timestamp COLUMN as
   well as over scalar literals. The native-DataFrame / ANSI-door / Spark-door cells are UNPINNED
   here. The original reason given ("until extraction honors the zone the other cells would pin
   the same absence again") was refuted at review: pinning an absence per cell is precisely what
   this corpus does, and the panel demonstrated the DataFrame-API cell diverges identically today
   (`df.select(F.year(F.col('ts')), F.hour(F.col('ts')))` under `America/New_York` → y=[2024,2024],
   h=[3,4]). Split B must add the remaining three cells WITH the fix. Recorded as a claim, not a
   silence.
2. **Reuse-path validation asymmetry** (D-A1): an invalid zone passed to a second `getOrCreate`
   is not validated, because no session is built. It is also not applied — the existing engine-knob
   warning fires and `conf.get` still reports the engine's zone. This is now PINNED both ways:
   a valid-but-different zone by
   `test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone`, an INVALID zone
   by `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise`. Before the fix pass
   this ledger and the actor report both claimed the invalid leg was pinned when it was not (§8,
   finding S-3). `get_or_create`'s own docstring, which promised validation for every engine knob,
   now carves this key out explicitly.
3. **The plumbing seam for split B is deliberately not chosen here** (D-A2). The zone lives on
   `ReparkSession`; `SparkExtension::configure` already receives the builder conf map and depends on
   both `repark-core` and `repark-functions`, so it is the natural place to hand the zone to the
   extractor layer. Note the tier rule: `repark-core` (tier 2) may NOT depend on `repark-functions`
   (tier-3 leaf), so a core-side import is not an option.
4. **`test_dogfood_gaps.py::test_divergence_timestamp_ltz_collect_passthrough` (DIVERGENCE-1)** says
   in its docstring "disclose, do not build session-tz machinery". Decision D7 supersedes that
   instruction. Split A leaves the test green (it changes no evaluated result); split B must
   revisit that disclosure in the same diff as the fix.
5. **`build_repark_engine(session_conf)` stops the process-active session** when — and only when — an
   override is requested, because `getOrCreate` would otherwise hand back a session carrying the
   previous scenario's zone. A future caller that builds two engines inside one test would find the
   first handle dead. Narrow by construction and documented at the call site.
6. **`docs/port/census.md`'s recorded registry count** (27) was updated to 29 with the reason,
   because that file states the count must "only ever move deliberately". It is the only file
   outside this unit's own surface that this unit edits, and it is a doc-only edit.
7. **Three new engine gaps and one type gap were found while authoring** (registry rows TZ-5,
   B-TZ-1..B-TZ-4). None is fixed here. TZ-5 in particular is a silently-wrong-result class
   (a 10^9 factor) that deserves its own unit rather than a fold into split B.
8. **The SQL door cannot set this conf at all**, and the drop-in argument in D-A5 must not be
   read as broader than it is: `spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")` raises
   `Invalid or Unsupported Configuration: Could not find config namespace "spark"` — as does every
   `spark.*` key through the SQL door. **Pre-existing, not introduced here**, and NOT fixed here.
   Split B item: it is a candidate divergence-registry row (working name **B-TZ-5 — the SQL `SET`
   door does not reach the `spark.*` conf namespace**), and it is wider than the session zone, so
   it wants its own decision rather than a fold into the extraction fix. The Python `conf.set`
   door is the one D-A5's evidence actually covers.
9. **The `timeZone` reader option is a different surface.** A grep for `timezone`-shaped strings in
   `python/repark/src` also finds `reader.py` / `_funcs.py` occurrences: those are the
   `spark.read.option("timeZone", ...)` READER option that predates this unit, not a session conf
   key. The one-spelling claim is about the session-conf surface, and the grep in §4 shows exactly
   one string literal per language.

---

## 8. Fix pass (panel findings) — 2026-08-10

The FULL adversarial panel (semantics + oracle lenses) returned **ACCEPT-WITH-NITS** from both
lenses, no BLOCKER, with 6 + 3 MAJORs and 8 NITs. Both lenses independently reproduced the
headline claims — resolved-once, one-spelling, the evidence behind the D-A5 reversal, and the
oracle: one lens re-derived 14 of the recorded Spark halves and the other re-derived all 18 plus
the `current_timestamp` type, from drivers of their own, against live PySpark 4.1.2. Every finding
below is recorded with what was actually DONE, including the three places the dispositions were
followed but the underlying claim in this ledger turned out to have been wrong.

Gates after the fix pass are in §8.3; the provocations the fix pass altered or added are in §8.2.

### 8.1 Finding → action

| # | Sev | Finding | Action |
|---|---|---|---|
| S-1 | MAJOR | The runtime `conf.set` path performs ZERO validation, so repark silently swallows a zone live PySpark refuses — and is fully silent after the first call in the process. | **KEPT accept-warn-don't-store** (the D-A5 evidence stands), made HONEST and PINNED instead of tightened. The warning now states in words that the value is *neither validated nor applied*, that validation happens once at `getOrCreate`, and that the disclosure is once per process. Pinned by `test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied`, which now asserts the warning TEXT contains "its value is NOT validated" and drives an invalid zone (`Mars/Olympus_Mons`) under `simplefilter("error", UserWarning)` so the silent-second-call behavior is pinned, not merely true. Registry row TZ-3 rewritten to disclose the unvalidated half and the once-per-process scope. |
| S-2 | MAJOR | `get_or_create`'s docstring promises validation for every engine knob on the reuse path; the zone is now in that set and is not validated. | Docstring CORRECTED at `session_core.py`: the promise is now scoped to knobs whose validation is facade-side, with an explicit carve-out naming `spark.sql.session.timeZone`, why (zone validity needs the engine's zone database and the repo keeps ONE validator), and the pin that holds it. |
| S-3 | MAJOR | This ledger (§7.2) and ACTOR-REPORT §8.2 both claimed the invalid-zone-on-reuse behavior was "pinned by `test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone`". That test passes a VALID zone. The claim was false. | Pin ADDED: `test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise` — with an active Tokyo session, a second builder carrying `Mars/Olympus_Mons` warns the engine-knob warning, returns the ACTIVE session, does not raise, and leaves `conf.get`/`conf.getAll` on `Asia/Tokyo`. §7.2 rewritten to name both pins. Provoked (§8.2 P-5). |
| S-4 | MAJOR | `python/repark/tests/map.md` still advertised "the **loud refusal** of a runtime `conf.set`" — the behavior the same diff reversed. Two maps in one diff disagreed on the unit's headline behavior. | Reworded to "the accepted-but-neither-validated-nor-applied runtime `conf.set`/`unset` disclosure", and the same entry now also names the reuse laxness, the whitespace normalization and the blank-zone refusal. Swept for other survivors of the reversal: none found. |
| S-5 / O-5 | MAJOR / NIT | `conf.get` does not "ALWAYS report the zone the live engine session actually has": the engine trims, the facade did not, so `.config(K, "  Asia/Tokyo \t")` made the two disagree. | `normalize_session_time_zone_config` added to `session_time_zone.py` and called from `get_or_create` **before** the value is stored or forwarded. Its docstring and both call-site comments state that this is WHITESPACE NORMALIZATION ONLY and that the engine (`SessionTimeZone::parse`) remains the SOLE validator. Module docstring's "ALWAYS" claim rewritten to say what the mechanism is rather than assert an inference. Pinned by `test_padded_zone_is_normalized_so_conf_get_reports_the_engine_zone`; the failure mode of over-reaching is pinned by `test_whitespace_only_zone_still_fails_loud_at_session_build` (`"   "` trims to empty and the ENGINE still refuses — normalization must never turn a refusal into a silent `UTC`). Provoked (§8.2 P-6). |
| S-6 | MAJOR | The zone validator rides an UNDECLARED transitive `arrow/chrono-tz` enable from `datafusion-functions`. Without it `Tz::from_str` accepts only fixed offsets and rejects EVERY IANA id, so a DataFusion feature change would turn `.config(K, "America/New_York")` into a hard build refusal for every user. | Declared: `crates/repark-core/Cargo.toml` now carries `arrow = { workspace = true, features = ["chrono-tz"] }` with the reason inline. `cargo tree -p repark-core -e features -i arrow-array` now lists `repark-core` as a DIRECT enabler of `arrow feature "chrono-tz"` beside `datafusion`. **`Cargo.lock` is byte-unchanged** (sha256 verified before and after; `git diff --cached Cargo.lock` empty) — the feature was already resolved, only the ownership moved. Debug rows added to `crates/repark-core/map.md` and `crates/repark-core/src/session_time_zone/map.md` naming the exact symptom ("every IANA id refused, fixed offsets still work"). |
| O-1 / S-NIT | MAJOR / NIT | The flip-don't-delete guidance sat on `assert _frames_differ(row.repark, row.spark)` — two module CONSTANTS, so no engine change can ever reach it. A real convergence red on the first assertion with a bare `FrameMismatchError`. The docstring credited the wrong assertion. | The disclosure runner now CLASSIFIES the failure before raising, on `actual` (the engine's real output): if `actual` equals the recorded Spark golden it raises the CONVERGED / flip-don't-delete message; otherwise it raises a REGRESSION message pointing at record mode. The constants assertion is KEPT and re-labelled as what it is — the row-well-formedness guard against a half-edited row — with its own message. Docstring now credits the mechanism it actually has. Both directions re-provoked (§8.2 P-2b convergence, P-4 regression), plus the must-PASS. |
| O-2 | MAJOR | No row exercises a tz-aware timestamp COLUMN; all 19 rows are scalar-literal expressions, so the brief's own recipe wording is met by nothing. The stated reason for deferring was refuted. | Column family ADDED and RECORDED from live PySpark 4.1.2: `column_extract_under_new_york_session` and `column_extract_under_tokyo_session` run `year`/`month`/`dayofmonth`/`hour` over a two-row in-memory frame (`createDataFrame` + `createOrReplaceTempView`, schema INFERRED so both engines carry `timestamp[us, tz=UTC]` — a DDL `ts timestamp` string makes repark's column tz-NAIVE and would have weakened the row). New York moves all four fields including the YEAR across a boundary (`[2023,2024]/[12,6]/[31,15]/[23,8]`); Tokyo moves only the hour (`[13,21]`) while repark answers `[2024,2024]/[1,6]/[1,15]/[4,12]` under BOTH zones — which is what separates a session-zone bug from an offset-sign bug. Budget pins moved in the same diff (G1 12 → 14, still inside 10–14), and `test_session_timezone_row_set_covers_both_gap_budgets` now asserts the column family spans both zones. The four-entry-point matrix is stated as split B's obligation in the §3 gate table and residual §7.1 — claimed, never silent. |
| O-3 | MAJOR | `build_repark_engine`'s `active.stop()` branch executes ZERO times under the suite (conftest clears the active session before every test), while its absence silently leaves a scenario on the wrong zone. | Pin ADDED (JVM-free): `test_build_repark_engine_override_stops_the_active_session_and_rebuilds` builds a default engine, asserts it is the process-active session, then requests a Tokyo override and asserts a NEW session was built, that it carries `Asia/Tokyo`, that it is now the active one, and that the previous handle is STOPPED (the documented cost in residual §7.5). Provoked (§8.2 P-7). |
| O-4 | NIT | The record driver was scratch outside the worktree, so "recorded against live PySpark 4.1.2" became unfalsifiable from inside the repo the moment that session ended. | Driver COMMITTED as `python/repark/tests/_record_session_timezone_goldens.py` (not a `test_` module; never collected). It imports `ROWS` from the committed corpus and runs each row through the SAME `run_row` helper the assertions use, so recipe and oracle are one piece of code. Exit 0 = all halves reproduce; non-zero prints the live schema+rows. It never edits the corpus. Invocation is in the corpus module's docstring, in `python/repark/tests/map.md` (Contents, an `I want to…` row and a Debug row) and in §4.6. Re-run in the fix pass: **20 rows re-derived, 0 mismatch(es)**. |
| S-7 / O-6 | NIT | The "verbatim" facade count (2593) does not reproduce; both lenses got 2594 on the identical tree and command. | Corrected in §4.3 with the honest reason: collection is deterministic from the file tree, so this was not environment sensitivity — the number was captured one test before the final tree. Fix-pass re-capture recorded: **2600 passed / 36 skipped** (record provisioning) and **2564 / 46** for `make py-test-facade` verbatim, the +6 being this pass's new tests. |
| S-8 | NIT | Stale pytest assertion-rewrite cache in the worktree produced a twice-consecutive false RED on a disclosure row. | All `*.pyc` under `python/` and `scripts/` deleted at the start of this pass (gitignored; no tracked file touched). Every gate in §8.3 was run with a cold cache. |
| S-9 | NIT | The disclosure is once per PROCESS, so a second session in the same interpreter gets a fully silent no-op. | Kept (the OTH-010 idiom is consistent) and DISCLOSED rather than fixed: stated in the warning text itself, in the function docstring, in `python/repark/src/repark/session/map.md`'s Debug table as its own row, and in registry row TZ-3. |
| S-10 | NIT | The drop-in argument does not hold for the SQL door: `SET spark.sql.session.timeZone=…` explodes (`Could not find config namespace "spark"`), pre-existing for every `spark.*` key. | Recorded as residual §7.8 and as a **split-B candidate registry row B-TZ-5**. Not fixed here: it is wider than the session zone and wants its own decision. The D-A5 evidence covers the Python `conf.set` door, and the ledger now says so. |
| O-7 | NIT | The ready-to-paste TZ-1 row miscounts its own pins ("15 disclosure rows"); the lens computed 13. | Corrected AND re-derived, because the count MOVED in this pass: the corpus now holds 20 rows = 18 disclosure table rows + 2 equality controls; of the 18, two are TZ-4 and one is TZ-5, leaving **15 extraction-class rows**. The number 15 was wrong when it was written (the true value was 13) and is right now only because the two column rows were added — recorded here so nobody reads the unchanged digit as "no finding". Counted mechanically, not by hand. |

### 8.2 Provocations added or re-run in this pass

Every provocation was reverted from a pre-provocation copy and the restore verified by sha256 +
`git diff`. The must-PASS leg is captured beside each must-FAIL.

**P-2b — the disclosure runner detects a REAL engine convergence** (this is the leg O-1 says the
original could not reach). Simulated honestly: the goldens are UNTOUCHED and the RECIPE is changed
so the engine produces exactly the recorded Spark value
(`year(to_timestamp('2024-01-01T04:30:00Z'))` → `year(to_timestamp('2023-06-01T00:00:00Z'))`,
which answers 2023 in UTC = the recorded Spark half).

```text
$ pytest "...::test_session_timezone_row_matches_spark_or_still_diverges[year_of_instant_under_new_york_session]" -q
        try:
            assert_frames_equal(actual, row.repark)
        except FrameMismatchError as mismatch:
            if not _frames_differ(actual, row.spark):
>               raise AssertionError(
E               AssertionError: year_of_instant_under_new_york_session: repark and Spark have
E               CONVERGED — repark now produces the RECORDED SPARK output, so this disclosure is
E               stale. Do not delete the row: flip it to an equality row (repark=None) and record
E               the convergence. the instant is 2023-12-31 23:30 in New York, so Spark's year is
E               2023; repark extracts in the stored (UTC) zone and answers 2024. Flipped to
E               equality by the session-timezone extraction fix (briefs/v2-engine-hardening.md,
E               H-1a split B).
1 failed in 0.45s
```

**P-4 — a plain REGRESSION is reported as a regression, not as a convergence** (the panel's P-A,
re-run against the new runner): move the row's `repark` half to 2023 so the engine matches neither
half.

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
>           raise AssertionError(
E           AssertionError: year_of_instant_under_new_york_session: repark moved OFF its pinned
E           disclosure and does NOT match the recorded Spark golden either — this is a regression,
E           not a convergence. Re-derive both halves in record mode (see this module's docstring)
E           before touching the pin. ...
1 failed in 0.44s
```

**must-PASS** (file restored; sha256
`55dd6bdacee61c0400b997f24569e680f626dc708fd8668786d112026ee81939`):

```text
$ pytest "...[year_of_instant_under_new_york_session]" -q
1 passed in 0.37s
```

**P-5 — the reuse-path pins red when the zone leaves the engine-knob set.** Drop
`*SESSION_TIME_ZONE_KEYS` from `engine_key_set` in `session_core.py`:

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q -k reuse
>       with pytest.warns(UserWarning, match="engine knobs are fixed at session build"):
E       Failed: DID NOT WARN. No warnings of type (<class 'UserWarning'>,) were emitted.
FAILED ...::test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone
FAILED ...::test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise
2 failed, 28 deselected in 0.40s

$ (restored)  pytest ... -q -k reuse
2 passed, 28 deselected in 0.34s
```

**P-6 — the trim normalization is load-bearing.** Remove the
`normalize_session_time_zone_config(self._config)` call from `get_or_create`:

```text
$ pytest python/repark/tests/test_session_timezone_parity.py -q -k "padded or whitespace_only"
>       assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO
E       AssertionError: assert '  Asia/Tokyo \t' == 'Asia/Tokyo'
FAILED ...::test_padded_zone_is_normalized_so_conf_get_reports_the_engine_zone
1 failed, 1 passed, 28 deselected in 0.40s

$ (restored)  pytest ... -q -k "padded or whitespace_only or invalid_zone"
3 passed, 27 deselected in 0.34s
```

Note the second leg stayed GREEN under the provocation: `test_whitespace_only_zone_still_fails_loud_at_session_build`
pins that normalization does not swallow a blank zone, and removing the normalization does not
break that — which is the correct shape (the two tests pin opposite failure modes).

**P-7 — the `active.stop()` branch is load-bearing.** Replace the `active.stop()` inside
`build_repark_engine` with `pass`:

```text
$ pytest "python/repark/tests/test_parity_live.py::test_build_repark_engine_override_stops_the_active_session_and_rebuilds" -q
>       assert second.session is not first.session, "an override must BUILD, never reuse"
E       AssertionError: an override must BUILD, never reuse
E       assert <ReparkSession object at 0x7a8aea883900> is not <ReparkSession object at 0x7a8aea883900>
  UserWarning: Using an existing ReparkSession; some configuration may not apply (engine knobs are
  fixed at session build; unapplied keys: ['spark.sql.session.timeZone']).
1 failed, 1 warning in 0.46s

$ (restored; sha256 280f83551ebf558e61f0c42d93d495d08aee665e0939a2aa5c358636f69a9513)
1 passed in 0.37s
```

That warning line in the RED output is the finding stated as evidence: without the stop, the
override is swallowed by the reuse path and the scenario runs under the previous zone.

P-1 (one spelling, Rust) and P-3 (the non-UTC registry pin) were not altered by this pass and are
unchanged in §5.

### 8.3 Gates after the fix pass

```text
$ make ci
crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates
lib-rs: 9 crate roots clean (no inline test modules; ceilings held)
lib-py: 54 files clean (ceilings held; no-stub rule held)
manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc
          index, the status document and the crate maps
uvx ruff@0.15.22 check .     -> All checks passed!
uvx ruff@0.15.22 format --check .  -> 241 files already formatted
uv lock --locked             -> Resolved 29 packages in 1ms
taplo format --check / lint / typos -> clean
EXIT=0

NOTE (honest): the first `make ci` of this pass was RED at `py-lint` on three ruff findings in the
new code (I001 import order in the record driver, RUF043 an unescaped `match=` pattern, E501 a
101-column line). Fixed, not suppressed; the run above is the second.

$ make test                  # cargo test --locked --workspace, never --all-features
31 test binaries, every one `test result: ok`; 0 `test result: FAILED`; 1279 tests passed
EXIT=0

$ PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests -q
2600 passed, 36 skipped, 46 warnings in 142.79s        # was 2594 pre-fix (+2 column rows,
EXIT=0                                                 # +4 conf-surface pins)

$ make py-test-facade
2564 passed, 46 skipped, 37 warnings in 91.47s
EXIT=0

$ JAVA_HOME=zulu-17 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 pytest python/repark/tests/test_parity_live.py -q
66 passed in 17.54s
$ (gate unset)
33 passed, 33 skipped in 0.79s

$ .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py   # JAVA_HOME set
record mode: 20 rows re-derived, 0 mismatch(es)
EXIT=0

$ bash scripts/check_map_md.sh      -> EXIT=0   (staged-set method; 8 touched dirs' map.md staged)
$ python3 scripts/check_manifest.py -> EXIT=0

$ sha256sum -c (Cargo.lock, taken before the arrow feature declaration) -> Cargo.lock: OK
```

### 8.4 Deviations from the dispositions, stated rather than absorbed

1. **Disposition 1's warning wording kept the substring the existing pins and maps quote.** The
   message now says "…accepted for source compatibility but NOT applied at runtime, and its value
   is NOT validated…", which extends rather than replaces the phrase `pytest.warns(match=…)`,
   `python/repark/src/repark/session/map.md` and registry row TZ-3 all key on. Replacing it would
   have silently changed three documents' quoted text.
2. **Disposition 6's column rows are the facade `sql()` door over a COLUMN, not the DataFrame-API
   door.** The disposition asks for "year/month/day/hour over a tz-aware timestamp COLUMN (a small
   in-memory dataframe)" and separately says the four-entry-point matrix stays split B's. Those two
   are satisfied by one recipe on the existing door; adding a DataFrame-API row would have started
   the matrix this split is explicitly not to start. The frame IS built with `createDataFrame` and
   the column IS `timestamp[us, tz=UTC]` on both engines, so the brief's recipe wording is met.
3. **Disposition 9's TZ-1 count did not need the correction the panel computed.** The lens's
   arithmetic (13) was right for the tree it read; the two column rows this pass adds move it to
   15, which is the number the cell already carried. It is recorded as a corrected count with the
   working shown, so the coincidence is visible instead of looking like the finding was ignored.
4. **Nothing was tightened toward Spark.** Dispositions 1 and the S-1 finding invite a stricter
   runtime `conf.set`; the orchestrator's disposition is accept-warn-don't-store, and the D-A5
   evidence (a pinned Apache drop-in test) still holds. The laxness is now disclosed in the
   warning text, in two maps, in a pin and in the registry row — not removed.

---

## § Split B (reserved)

Deliberately empty. Split B — the extraction fix, its 6–8 Rust extractor-family pins, the
four-entry-point matrix, and the flip of every disclosure row above into an equality row — appends
here.

---

## 9. Assembly note (orchestrator, 2026-08-10)

Landed after H-1d merged (#35). Rebase onto its squash produced three both-added conflicts
(`test_parity_live.py` — the mirror gate beside this unit's two new pins; the two `map.md`
I-want-to tables), all resolved by keeping both sides.

**Registry placement applied the registry's own class vocabulary, not this ledger's §6 table
heading** ("Declared-divergence rows" was loose): TZ-2 and TZ-3 are DECLARED and landed in §4;
TZ-1, TZ-4 and TZ-5 carry intent-to-fix and landed in §7 as BACKLOG rows, whose pins codify
today's behavior so the fix reds them — exactly this unit's disclosure design. B-TZ-1…B-TZ-5
carry **no pin**, so under the registry's §6 admission rule they are not rows: they landed as
§7's "Surfaced, awaiting pins" queue, each pointing back at this ledger for the full observed
behavior. STATUS gained two state lines (the TZ-1/TZ-4 class with split B in flight; TZ-5 open,
own unit) per the state-vs-semantics boundary.
