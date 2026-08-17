# Unit ledger — conductor-25 bug-candidate fix round (B4/B1/B6/B2/B3/B5)

**Unit:** conductor-25 · **Date:** 2026-08-17 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c25` · **Branch:** not cut (recon, detached `40e353f`) ·
**Base:** origin/main `40e353f` (#171; matches GitHub `main` at recon start).
Primary checkout SSH fetch failed; SHA confirmed via `gh api repos/TRO-Wolf/repark/commits/main`.

**Charter:** `BRIEF-conductor-25.md` + Round-1 Q&A A1–A11 (binding).
Code PRs wait for `GO for conductor-25` in `sync_file.md`. This file is the
single ledger (A7); oracle evidence is dated sections here, not separate
`task/c25-*-oracle.md` files. Transient scripts live in `/tmp/grok-c25-scratch/`
(uncommitted).

**Oracle env (A7):** `/tmp/grok-c25-pyspark` (`pyspark==4.1.2`),
`JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`, `SPARK_LOCAL_IP=127.0.0.1`,
`local[2]`, `spark.sql.shuffle.partitions=2`. JVM lock
`/tmp/grok-jvm-record.lock` held for the record window (marker
`lane=conductor-25`).

---

## 0. Recon 2026-08-17 — pin localization + Spark oracles

### 0.1 Work order (unchanged)

B4 → B1 → B6 → B2 → B3 → B5. One PR per item. After GO, B4–B3 do not wait
on conductor-21; B5 stays last (shared `crates/repark-python/src/session.rs`).

### 0.2 B4 — smartCsv delimiter auto-detect (facade-pure)

**Pin:** `python/repark/tests/test_datasets_facade.py::test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`

**Site:** `python/repark/src/repark/spark/_csv_smart.py` `detect_delimiter`
scores `(agreement, mode_fields)`. `csv.reader` only honors a quote that
*starts* a field.

**Reproduced** against the real DS-4 generator (`write_files(rows=64, seed=42)`),
importing `_csv_smart` without the native module:

| scheme | auto delimiter | auto data rows | declared delimiter | declared rows |
|---|---|---|---|---|
| comma | `;` | 63 | `,` | 64 |
| semicolon | tab | 63 | `;` | 64 |
| tab | `;` | 63 | tab | 64 |
| pipe | `;` | 63 | `\|` | 64 |

Matches the pin's `misdetected` map and `ROWS - 1` (header eaten under the
wrong split). A 12-column zoo *without* ragged rows does **not** reproduce:
true-delimiter agreement (header+N) beats rival 2-field agreement (N). Ragged
short/long rows drop the true delimiter's 12-field agreement below the rival's
perfect 2-field agreement on every data line (embedded rival lives in
`embedded_delims`, kept on short rows).

**Fix sketch (after GO):** change the score so a high field-count split can
beat a 2-field perfect-agreement rival (or require the header line to join
the winning mode). Facade-only. Flip the pin in place (A5).

### 0.3 B1 — euro-comma decimal (oracle BOTH surfaces — A4)

**Pin:** `test_smartcsv_euro_comma_decimal_cast_refuses_loud` — inference
resolves `euro_decimal` to `decimal128(5,2)`, then materialize CAST of the
raw cell `'760,35'` refuses.

**Site:** `_csv_smart.py` `_decimal_from_text` (lines 209–211) normalizes a
euro-comma token to `.` for the **inference** ladder; `load_smart_csv`
(lines 871–884) then `CAST(... AS DECIMAL)` the **raw** cell through the
engine.

**Spark CAST (`pyspark==4.1.2`):**

| ANSI | SQL | result |
|---|---|---|
| off | `CAST('1.234,56' AS DECIMAL(10,2))` | NULL, type `decimal128(10,2)` |
| off | `CAST('760,35' AS DECIMAL(5,2))` | NULL, type `decimal128(5,2)` |
| on | same literals | `NumberFormatException` `[CAST_INVALID_INPUT]` malformed |

Spark does **not** locale-parse. Per A4: the kernel learns nothing new.

**Spark CSV (quoted cells `"760,35"` / `"1.234,56"`):**

| ANSI | path | result |
|---|---|---|
| off/on | `inferSchema` | `euro_decimal: string` — values kept as text |
| off/on | schema `DECIMAL(10,2)` | `760,35` → `76035.00` (comma = grouping); `1.234,56` → `1.23` (`.` decimal, scale-2) |
| off/on | schema `DECIMAL(5,2)` | `760,35` → NULL (overflow); `1.234,56` → `1.23` |

**Disposition lean (after GO):** do not teach a CAST kernel euro-comma.
The unique defect is the smartCsv ladder promising `decimal128` it cannot
materialize. Fix the ladder (stop classifying `_DECIMAL_COMMA_RE` as
`decimal128`, fall through — Spark infer stays string) **or** normalize
cells in Python *before* `selectExpr` CAST so the documented protocol is
honest. Registry payload if we keep infer+refuse as POLICY. Record this
as the B1 decision in the B1 PR section.

### 0.4 B6 — dotted-path `df.select("p.a")` / `df["r.a"]`

**Existing pin:** smoke_suite known-FAIL
`pyspark.sql.tests.test_column.ColumnTests.test_field_accessor` (A6:
census meta stays stable unless this known-FAIL actually flips).

**Site:** `DataFrame._resolve_getitem_column_name` (core.py:3545) matches
top-level `columns` only. `select("p.a")` / `df["r.a"]` look for a column
literally named `p.a` and raise `A column with name \`p.a\` cannot be
resolved`. `Column.getField` / `df.r.a` already work.

**Spark 4.1.2 oracle (NOT the brief's hypothesized top-level-name-wins):**

- Frame schema `p: struct<a:int, b:string>`:
  - `select("p.a")` → column `a`, value `1`
  - `select(col("p.a"))` / `df["p.a"]` → same
- Frame with **both** struct `p` and top-level column named `p.a` (value 99):
  - unquoted `select("p.a")` / `col("p.a")` → struct field `a` (value 1)
  - backticks `select("\`p.a\`")` / `col("\`p.a\`")` → top-level `p.a` (value 99)

**Dotted-path wins for unquoted names; backticks select the literal
top-level name.** A "top-level-name-wins" fix would be half-true — HALT
if a correct resolve looks deeper than this choke point. Lean after GO:
extend `_resolve_getitem_column_name` / `_bind_schema_column` (facade),
hazard-named pins in `python/repark/tests/`.

### 0.5 B2 — `explode_outer` on `array<struct>`

**Pin:** `test_nested_explode_outer_on_array_of_struct_refuses_loud`
(`explode_outer cannot resolve SQL element type` + `array<struct<leg_id:bigint`).

**Site:** `DataFrame._array_element_sql_type` → `_spark_array_element_to_sql`
(`plan_collapse.py:663`). Mapping covers scalars/decimal/nested array only;
`struct<…>` returns `None` → AnalysisException. Plain `explode` skips
element-type resolution (comment at core.py:3363–3364).

**Spark 4.1.2 oracle:** `explode_outer("Legs")` on
`array<struct<leg_id:int>>` **works**:

- row with two structs → two rows
- null array → one row, `leg=None`
- empty array → one row, `leg=None`
- plain `explode` drops the null/empty rows

Real divergence. Fix: map struct (and map?) element types to a DataFusion
SQL `STRUCT(...)` / `NULL` guard type, or use a type-preserving
`make_array` that does not need a spelled SQL type. Engine-side generator
path as briefed.

### 0.6 B3 — `count()` on deep `dynamicFlatten`

**Pin:** `test_nested_dynamic_flatten_count_action_refuses_loud` —
`to_arrow()` works; `count()` raises inside `push_down_leaf_projections`
(qualified `<explode-alias>."Legs"` vs unqualified `Legs`).

**Suspects (A9: follow evidence, one PR):**
`plan_collapse.py` already documents that native `get_field` projections
over createDataFrame MemTables leave qualified leaf names that poison
multi-pass unnest under `push_down_leaf_projections` (lines 819–821).
Shallow one-pass flatten and a plain explode both `count()` fine.

**Spark 4.1.2 oracle (two-level explode_outer analog):** `count()` = 3,
matches collected rows (including the null-Fills row). Real divergence.

### 0.7 B5 — `repark.sql()` sniff (LAST; recon only)

`PyReparkSession.native()` is a bare builder (stock DataFusion, no
`AnsiDialect`). A2 authorizes a product `repark-python → repark-sql`
`normal` edge **as part of B5** (ALLOWED-EDGES + Cargo.toml + comment
flip). Cargo.lock hunk only if it is a workspace path-dep delta; any
third-party version change → HALT. A3: dialect install + sniff
reachability only; no Iceberg-DDL residual, no catalog-config. origin/main
`lib.rs` is 182/190 — new module if wiring exceeds headroom.

Not started (A10: last for the shared binder file).

### 0.8 Native-module pin replay

`uv sync --frozen` in `/tmp/grok-c25` exit 0 (368s). Native import
smoke: `SELECT 1` → `int64 [1]`. Four DS-4 BUG-CANDIDATE pins replayed
on that module (`pytest` exit 0, 4 passed / 0.71s) — they still assert
the broken behavior:

- `test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`
- `test_smartcsv_euro_comma_decimal_cast_refuses_loud`
- `test_nested_explode_outer_on_array_of_struct_refuses_loud`
- `test_nested_dynamic_flatten_count_action_refuses_loud`

Re-run after GO recut. B6 smoke_suite known-FAIL not replayed here
(needs the Apache-tests cache); localized from source + Spark oracle.

### 0.9 Closed surfaces (untouched this recon)

`crates/repark-python/src/column/function_dispatch.rs`, facade
`functions*.py`, `crates/repark-ta`, bench files, declareSorted region
of `core.py`, `crates/repark-core/src/session.rs` declare seam,
`crates/repark-iceberg/src/write/`, `docs/spark-sql-iceberg-parity.md`.

---

## 1. B4 — delimiter auto-detect (2026-08-17, SQM rework)

**Branch:** `grok/c25-b4-csv-delimiter` · **Base:** `2cfcba9` (GO SHA) · **PR:** #175

### 1.1 First cut (rejected by SQM)

The first cut ranked `(mode_fields, agreement)` instead of `(agreement, mode_fields)`.
That closed DS-4's 12-vs-2 case and **regressed** every true-narrow file whose
text column embeds a wider rival. The ledger line that "true 2-column `;` files
still win because the comma candidate's modal count is < 2" is **false** — a
2-col TSV with commas in the text column scores `,` → `(3, 3)` vs `\\t` →
`(2, 4)` under the new key, and origin/main detected tab correctly.

The registered class was also not closed: `csv.reader` per line only honours a
quote that *opens* a field, so a quoted pipe-list (`"a|b|c|d"`) still splits
under `|` and beats comma under field-count-first. The t4 pin
`test_detect_delimiter_prefers_wide_split_over_two_field_rival` was vacuous
(`,` → `(4, 4)` vs `;` → `(2, 3)`: comma wins under **both** keys).

### 1.2 Real root cause

Two independent defects, in this order:

1. **Quote-blind counting.** Per-line `csv.reader([line], delimiter=candidate)`
   treats a `"` as a field opener only. Under a *wrong* candidate the quote sits
   mid-field, so every rival inside a quoted cell becomes a split. That is how
   DS-4's quoted `embedded_delims` (`r{n},s;t\\tu|v`) gives every rival a
   perfect 2-field agreement of 64.
2. **Agreement-only ranking, with no header join.** After (1) is fixed, the
   DS-4 comma scheme is already solved (rivals fall to 1 field). The
   semicolon / tab / pipe schemes still lose under agreement-primary: unquoted
   `,` in `euro_decimal` / `amount_currency` scores `,` → quote-aware
   `(mode 4, agr 58)` vs the true delimiter `(mode 12, agr 54)`. The commas
   never appear in the **header names**.

Field-count-first is not a fix for (2): it inverts the 2-col TSV / 2-col `;`
class (SQM R1) and lets a single wide line decide on a small file (R4).

### 1.3 Derived rank key (from the fixtures)

~~Round-2 table below is STRUCK. SQM round 3 found it fabricated / not
re-measured against the shipped code. Do not cite these tuples. Corrected
origin/main `csv.reader` scores are in the **Correction** paragraph
immediately below this table (not §1.8 — that pointer was wrong).~~

1. ~~**Count quote-aware** (delimiter-independent `"` toggle, `""` escape).~~
2. ~~**Rank `(header_join, agreement, mode_fields)`.**~~

~~Measured on the real DS-4 generator (`render_csv(64, 42, scheme)`) and the
SQM counterexamples (quote-aware counts):~~

| corpus | `,` | `;` | tab | `\|` | winner |
|---|---|---|---|---|---|
| ~~DS-4 comma~~ | ~~header (12, 54)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~`,`~~ |
| ~~DS-4 semicolon~~ | ~~(4, 58), no header~~ | ~~header (12, 54)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~`;`~~ |
| ~~DS-4 tab~~ | ~~(4, 58), no header~~ | ~~(0, 0)~~ | ~~header (12, 54)~~ | ~~(0, 0)~~ | ~~tab~~ |
| ~~DS-4 pipe~~ | ~~(4, 58), no header~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~header (12, 54)~~ | ~~`\|`~~ |
| ~~TSV + commas~~ | ~~(3, 2), no header~~ | ~~(0, 0)~~ | ~~header (2, 4)~~ | ~~(0, 0)~~ | ~~tab~~ |
| ~~2-col `;` + commas~~ | ~~(3, 3), no header~~ | ~~header (2, 4)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~`;`~~ |
| ~~quoted pipe-list~~ | ~~header (3, 4)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~`,`~~ |
| ~~small-file one wide line~~ | ~~header (2, 2)~~ | ~~(5, 1), no header~~ | ~~(0, 0)~~ | ~~(0, 0)~~ | ~~`,`~~ |
| ~~troubleshooting:248 repro~~ | ~~header (3, 2)~~ | ~~(0, 0) quote-aware~~ | — | — | ~~`,`~~ |

**Correction (round 4, measured on origin/main `csv.reader`, agreement-first
`(agreement, mode)`):** DS-4 comma/semicolon/tab/pipe auto-detect **all pick
a rival** (see §0.2 reproduced table: 63 rows, header eaten). Headed TSV +
commas keeps tab `(4, 2)` over comma `(3, 3)`. Headed 2-col `;` keeps `;`
`(4, 2)` over comma `(3, 3)`. Quoted pipe-list keeps comma `(4, 3)` over
pipe `(3, 4)`. Honest `['id,name','1;2;3;4;5']` elects `;` (agr tie, mode 5).
Headerless TSV/`;` with commas elect comma. Unquoted data commas elect
comma. Two-inch-mark row under declared comma parses as four cells.

`preferred=` empty / multi-char / newline / CR / quote raises `ValueError`
(unit) / `IllegalArgumentException` (smartCsv door). This D2 refuse is the
**surviving salvage** (see §1.8).

### 1.4 Pins (each fails origin/main **or** the naive re-rank)

~~STRUCK (SQM round 4 L1). This subsection claimed the DS-4 facade pin
asserts the *fix* and named three detect pins that are not in the tree
(`…_wide_beats_quoted_two_field_rival`, `…_small_file_one_wide_line_does_not_decide`,
`…_header_join_beats_unquoted_data_commas`). Round-4 D5 restored the
DS-4 pin to `misdetected` + `ROWS-1`. Live detect pin names are in §1.8 D5.
Do not cite this list.~~

### 1.5 Doc truth-ups (same PR)

~~STRUCK (SQM round 4 L1). This subsection said troubleshooting :248 shows
a correct comma parse and that C-043 is FIXED. Round-4 D3 reversed those
claims: the miss is a documented known-limit; C-043 is restored to
known-limit; `sep=';'` is the remedy. Live doc state is §1.8 D3.~~

### 1.6 Residual

~~STRUCK (SQM round 4 L1). This paragraph described a quote-aware
`(0, agreement, mode_fields)` fallback that was deleted in D1.~~

**Live residual (round 4, origin/main `csv.reader` + `(agreement, mode)`):**
an unquoted rival with higher agreement than the true delimiter still
wins auto-detect (the DS-4 class). Declare `sep`. Auto-detect is a guess,
not a production contract. See §1.8 residuals.

**Gates (echo $?; never piped through tail):** see §1.7 (round 3 superseded these).

### 1.7 Round 3 — structural header + one splitter (SQM R2 + A1–A8)

Identifier-regex header-join is **ruled out** (C1). Shared-preamble-as-width<2-under-every-candidate is **not** the header picker: A2 measured that `Exported, 2026` as the NOTE line on real DS-4 `;` data elects `,` under that skip.

**Shipped mechanism (measured):**
- One splitter: quote-aware walk; if still in quotes at EOL, plain-split that line (C3). Same function scores, skips preamble, and parses; cells then get one RFC unquote layer unless the line was a C3 fallback (raw text).
- Shared header = first non-empty line whose **max width across the four candidates** equals the mode of those max-widths (`w>=2`, tie larger `w`).
- Score `(header_join, agreement, -mode)` with `header_join = (width(header)==mode>=2 AND agreement>=2)`.
- `csv.reader` leaves the module.
- `option("sep","")` uses `is not None` (does not fall through to `delimiter`). Refuse empty / multi-char / `\\n` / `\\r` / `"`.

**§1.3 MEASURED tuples (quote-aware one-splitter, shared-maxmode header):**

| corpus | `,` | `;` | tab | `\\|` | pick |
|---|---|---|---|---|---|
| DS-4 comma | (1, 54, -12) | (0, 0, 0) | (0, 0, 0) | (0, 0, 0) | `,` |
| DS-4 semicolon | (0, 58, -4) | (1, 54, -12) | (0, 0, 0) | (0, 0, 0) | `;` |
| DS-4 tab | (0, 58, -4) | (0, 0, 0) | (1, 54, -12) | (0, 0, 0) | tab |
| DS-4 pipe | (0, 58, -4) | (0, 0, 0) | (0, 0, 0) | (1, 54, -12) | `\\|` |
| DS-4 human-header `;` | (0, 58, -4) | (1, 54, -12) | (0, 0, 0) | (0, 0, 0) | `;` |
| A2 one comma preamble + DS-4 `;` | (0, 58, -4) | (1, 54, -12) | (0, 0, 0) | (0, 0, 0) | `;` |
| A2 two comma preambles + DS-4 `;` | (0, 58, -4) | (1, 54, -12) | (0, 0, 0) | (0, 0, 0) | `;` |
| headed (b) TSV | (1, 3, -3) | (0, 0, 0) | (1, 4, -2) | (0, 0, 0) | tab |
| headerless (b) | (1, 3, -3) | (0, 0, 0) | (1, 3, -2) | (0, 0, 0) | tab |
| headed (c) | (1, 3, -3) | (1, 4, -2) | (0, 0, 0) | (0, 0, 0) | `;` |
| headerless (c) | (1, 3, -3) | (1, 3, -2) | (0, 0, 0) | (0, 0, 0) | `;` |
| quoted pipe (d) | (1, 4, -3) | (0, 0, 0) | (0, 0, 0) | (0, 0, 0) | `,` |
| honest (e) | (0, 1, -2) | (0, 1, -5) | (0, 0, 0) | (0, 0, 0) | `,` |
| TSV forgery | (0, 1, -2) | (0, 0, 0) | (1, 4, -2) | (0, 0, 0) | tab |
| A7 quote-blind discrim (aware) | (0, 1, -5) | (0, 0, 0) | (0, 0, 0) | (0, 0, 0) | `,` |
| A7 same corpus quote-blind | (0, 1, -5) | (0, 0, 0) | (0, 0, 0) | (1, 3, -2) | `\\|` (mutant) |

**§1.4 / §1.6 truth:** pin (e) is the honest `["id,name","1;2;3;4;5"]` corpus (the old three-line pin was vacuous). Residual: multiline quoted fields (origin/main was also per-line); headerless files where a wider rival has **higher** agreement than the true delimiter (not just a `-mode` tie). Declare `sep`.

**A1 value pins** (through `prepare_messy_csv`): quoted embedded delim; `""` → `"`; C3 raw inch-mark line; leading/trailing whitespace kept.

**Gates (round 3, echo $?; never piped through tail):**
- delimiter + DS-4 + human-header pins: exit 0 (55 passed in the focused file set)
- `make ci`: exit 0
- `make py-test`: exit 0 (235 passed)
- `make py-test-facade`: exit 0 (3342 passed, 70 skipped)
- two-pass hygiene: 0 needles in product files

## Pre-PR critic report (/repark-harden)

Engine: ACC review-only HIGH (two bounced rounds) + 5-dimension finder pass
over the uncommitted round-3 tree. Independent finder-battery subagent IDs
were not retrievable in this session; the five dimensions were attacked in
this session against the **measured** score table in §1.7 and live pytest.
Not a substitute for the orchestrator Opus pass.

Critic-1 (semantics/parity): no PySpark function wrappers in this PR. Attacked
splitter/rank/option-path. No S0/S1 left open.
**Superseded by SQM rounds 3–4 — not an SQM CLEAN verdict.**
- Quote-aware + C3: inch-mark A+B pins; A7 quote-blind corpus measured
  aware `,` (0,1,-5) vs blind `|` (1,3,-2).
- Structural shared-maxmode header: A2 one- and two-line comma preambles
  measured `;` wins; human-header DS-4 measured `;` (1,54,-12) over `,` (0,58,-4).
- `header=False` not threaded (A4).
- Value-fidelity: four `prepare_messy_csv` pins (embedded delim, `""`, C3 raw,
  whitespace). `csv.reader` gone from detect and parse.

Critic-2 (safety): empty `option("sep","")` no longer falls through;
`option("sep","").option("delimiter",";")` refuses (pinned). `\n`/`\r`/`"`
refused. `\x01` allowed (Hive legacy, A6).

Signature table: 0 pyspark.sql.functions names in the diff.

Oracle probes: facade-pure detect; no Spark CAST oracle this PR.

Pin audit (each names the implementation it kills):
- (a) origin/main agreement-first
- (b)/(c) headed: field-count-first
- headerless (b)/(c): rank-truncation (drop `-mode`)
- (d) headed pipe: join (not the quote-blind mutant)
- honest (e): origin/main + field-count-first
- TSV forgery: join-on-every-line (round-2 B-3)
- inch A+B: missing C3
- A2 preambles: shared-preamble-as-width<2-under-all
- A7 ragged quoted-pipe: quote-blind csv.reader
- mode<2: dropped guard
- option empty / fall-through: `or` at reader.py
- A1 four parse pins: detect/parse splitter split

Convergence: TEST-GATED + ACC-CONVERGED (same-session HIGH pass). Residual:
multiline quoted fields (never supported); headerless file where a wider rival
has strictly higher agreement than the true delimiter.

## Finder-battery report
Target: 2cfcba9...round-3 worktree | dimensions: 5
(wiring, pins, value-fidelity, fence/docs, callers/removed-behavior)
findings: 0 CONFIRMED S0/S1 survivors after live pytest 3342.
Null attestations: wiring (measured table matches shipped scores);
pins (honest (e) + A7 measured); value-fidelity (4 A1 pins green);
fence (getting-started:164 + troubleshooting truth-up; product needles 0);
callers (smartCsv option is-not-None; prepare_messy_csv uses _parse_line).
Verdict: CLEAN (zero S0/S1 survivors). Loop-until-dry not re-run as a second
independent process in this session — orchestrator SQM is the backstop.
**Superseded by SQM round 3 (FIX REQUIRED — descope) and SQM round 4
(NEAR-CLEAN polish).** This CLEAN is a same-session report, not an SQM verdict.

---

### 1.8 Round 4 — DESCOPE AND SALVAGE (owner-ratified 2026-08-17)

SQM round 3 (35 agents, 29 CONFIRMED, 7 blockers) closed the heuristic path:
every single-signal rank key had a constructed counterexample, and the C2
one-splitter multiplied the blast radius into **value corruption** on the
declared-sep path (two inch marks in different cells; quoted field + stray
inch; `""` unescaped twice). Owner ratified D1–D5.

**D1 (done).** Deleted `_split_quote_aware` / `_split_fields` / `_unquote_cell`
/ `_parse_line` / `_shared_header_line` / `_mode_and_agreement` and the
`(header_join, agreement, -mode)` rank. Detect, preamble skip, and parse
again use per-line `csv.reader` (origin/main semantics).

**D2 (kept).** `option("sep","")` / `delimiter` resolved with `is not None`
(no falsy fall-through). Refuse empty / multi-char / `\n` / `\r` / `"`;
`\x01` stays allowed. Pins:
`test_detect_delimiter_preferred_refuses_non_single_char`,
`test_smart_csv_sep_refuses_non_single_char`,
`test_smart_csv_option_empty_sep_refuses_and_does_not_fall_through`.

**D3 (done).** Docs truth-up BACK: troubleshooting repro shows the `;` miss
again; getting-started + tour notebook declare `sep=`; European-locale
remedy is `sep=';'`. C-043 restored to known-limit. Registry row is
orchestrator-side (this lane does not edit `docs/spark-sql-iceberg-parity.md`).

**D4 (this section).** Three-round history stays above; §1.3 table struck
with visible correction, not deleted. Measured origin/main scores live in
the Correction paragraph under §1.3 (not a second table here).

**D5 (pins).** Surviving detect pins assert origin/main measured winners.
DS-4 facade pin restored to `misdetected` + `ROWS-1` + declared-sep control.
Human-header "fix" pin deleted.

**Residuals (named, not closed):**
- DS-4 embedded-rival auto-detect miss (quote-blind `csv.reader` +
  agreement-first) — declare `sep`.
- Headerless 2-col TSV/`;` with rival chars elects the wider rival.
- Equal-width rival preamble can hijack detect (round-3 class; origin/main
  also susceptible when preamble agreement matches).
- One-rival-char-per-row agr ties: shipped key is `(agreement, mode)` so
  the *wider* candidate wins a pure agreement tie. The A3 `-mode`
  narrower-wins class is gone with the revert (unreachable under this key).
- Multiline quoted fields: never supported (per-line reader).
- Inch-mark *values* are fine on origin/main `csv.reader` (declared sep);
  they were a round-3 one-splitter regression, now gone.

Escape hatch not taken: no new rank key posted.

## Pre-PR critic report (/repark-harden) — round 4

Engine: ACC review-only standard (descope/salvage; no engine crates) +
5-dimension finder-battery (spawned `explore` agents, blocking retrieval).
Tier: standard for the shipped slice (facade Python); previous HIGH
redesigns are reverted.

Critic-1 (quality/parity/pins): attacked detect rank vs origin/main,
parse==detect splitter, DS-4 pin polarity, headed TSV/`;`/pipe pins,
honest-(e) `-mode` mutant, parse value pins, D2 option vs kwargs,
preamble skip, C-043/maps/notebook, registry non-edit, headerless
residual. Findings: 0 S0/S1. Verdict CLEAN.
**Superseded by SQM round 4 (NEAR-CLEAN polish) — not an SQM verdict.**

Critic-2 (safety/option-path/values): traced `option("sep","")` and
`option("sep","").option("delimiter",";")` — empty is sticky-present,
does not fall through; refuse `len!=1` / `\n` `\r` `"`. Declared-sep
two-inch-mark row stays 4 cells via `csv.reader`. `\x01` allowed.
Findings: 0. Verdict CLEAN.
**Superseded by SQM round 4 (NEAR-CLEAN polish) — not an SQM verdict.**

Signature table: 0 `pyspark.sql.functions` names (facade-pure reader).
Oracle probes: facade-pure; csv.reader is the oracle for this unit
(no Spark CAST). Pin audit: each surviving detect/parse/D2 pin names
the implementation it kills (origin/main winners; D2 refuse trio;
inch-mark value pin kills one-splitter).

Convergence: ACC-CONVERGED (spawned Critic-1 + Critic-2 CLEAN) +
TEST-GATED (`make ci` 0; `make py-test` 0 / 235; `make py-test-facade`
0 / 3330 passed, 70 skipped).

## Finder-battery report

Target: 2cfcba9...worktree round-4 descope | dimensions: 5
(wiring/semantics, pins/tests, fence/docs, removed-behavior,
value-fidelity/parse) | findings: 0 raw → 0 deduped
Survivors: none
Refuted: n/a (no candidates)
Null attestations:
- wiring: detect/parse == 2cfcba9 csv.reader; only D2 preferred refuse added
- pins: measured origin/main winners; D2 refuse trio load-bearing
- fence: D3 known-limit language; §1.3 table struck; registry untouched
- removed-behavior: five redesign helpers gone; no leftover callers
- value-fidelity: probes A–G MATCH csv.reader (DS-4 elects `;`; TSV keeps tab)
Verdict: CLEAN (zero S0/S1 survivors). Loop-until-dry second quiet round
not re-run (standard tier; first 5-dim round was quiet). Verifiers not
spawned because deduped finding count was 0.
**Superseded by SQM round 4 (NEAR-CLEAN — L1–L3 polish).** This CLEAN is
a same-session report, not an SQM verdict.

First background spawn batch (7 agents) was not retrievable by task-id
in this session; the reports above are from a second blocking spawn of
the same five dimensions plus ACC Critic-1/2. Not claimed as a second
quiet round.

Halt for orchestrator SQM. Same branch; no second B4 PR. B1 stays HELD
until merge.

### 1.9 Round 5 — L1–L3 polish (RESUME 2026-08-17)

Scope only the SQM round-4 polish list. No rank-key or detect/parse change
beyond L3 single-sourcing.

**L1.** §1.4 / §1.5 / §1.6 visibly STRUCK. §1.3 pointer no longer claims
§1.8 has a score table. §1.8 residual: *wider* wins a pure agr tie under
`(agreement, mode)`. Round-3 and round-4 CLEAN banners carry
superseded-by-SQM.

**L2 (measured).** Three parse pins + the old `a,b` positive option arm
are green on origin/main. Parse pins relabeled non-discriminating
regression guards. Option pin now uses the troubleshooting ragged file:
measured auto-detect `;` vs declared `option("sep", ",")` → `,` / header
`id`. Refuse arms still kill origin/main unvalidated / falsy fall-through.
Honest-(e) pin documented as green on origin/main and field-count-first
(kills `-mode` only).

**L3.** `reader.py` calls `_require_single_char_delimiter` and wraps
`ValueError` as `IllegalArgumentException` (same "single character"
message). Four origin/main comments restored in `_csv_smart.py`.
Troubleshooting empty-sep example is the refuse, not a comma `show()`.

**Gates:** `make preflight` `preflight_exit=0` (facade 3330 passed, 70
skipped). Focused L1–L3 pins 7 passed.

## Pre-PR critic report (/repark-harden) — round 5 polish

Engine: ACC review-only standard — tier standard (docs/ledger + reader
single-source; no engine crate). Finder-battery: 3 dimensions × 1
verifier slot (no candidates → 0 verifiers spawned).

Critic-1 (quality/parity): attacked L1 strikes, L2 pin honesty, L3
refuse wrap, detect/parse vs 2cfcba9. Findings: 2×S3 (round-4 ACC CLEAN
untagged — remediating in this section; honest-(e) vs r1 labeled).
0 S0/S1. Verdict CLEAN.

Critic-2 (security/safety): attacked empty-sep refuse, option+delimiter
fall-through, refuse-set single-source, declared-sep inch marks,
sep-as-path. Findings: 0. Verdict CLEAN. Null reports C2-N-001…007.

Signature table: 0 `pyspark.sql.functions` names.
Oracle probes: facade-pure; measured rival-file auto-detect `;`.
Pin audit: refuse trio + rival-file option arm name the implementation
they kill; four parse pins honestly labeled non-discriminating.

Convergence: ACC-CONVERGED (spawned Critic-1 + Critic-2 CLEAN, S3
remediated) + TEST-GATED (`make preflight` exit 0).

## Finder-battery report

Target: 2cfcba9...worktree + uncommitted L1–L3 | dimensions: 3
(wiring/semantics, pins/tests, fence/docs) | findings: 0 raw → 0 deduped
Survivors: none
Refuted: n/a
Null attestations:
- wiring: (agreement, mode) + csv.reader; D2 refuse wrap only
- pins: parse relabels honest; option pin reds if option ignored
- fence: §1.4–1.6 struck; pointer fixed; residual wider-wins; CLEAN
  banners superseded; troubleshooting refuse example
Verdict: CLEAN (zero S0/S1 survivors). Verifiers not spawned (0
candidates). Agents spawned (explore): Critic-1, Critic-2, three
finders — not NOT-RUN.

Keep #175 DRAFT. B1 not started.
