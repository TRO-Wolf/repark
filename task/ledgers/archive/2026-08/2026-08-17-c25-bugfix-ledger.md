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

---

## 2. DF-2 — dynamicFlatten outer-explode remediation (2026-08-17)

**Branch:** `grok/c25-df2-outer-flatten` off `b628b0f` (#175 merged).
**Charter:** BRIEF-conductor-25.md "Addendum 2026-08-17 — DF-2 WORK ORDER"
+ sync_file amendment (`empty_as_null` default **True**).

### 2.1 Oracles (measured this unit)

**Polars 1.43.2** (`/tmp/grok-c25/.venv`): `DataFrame.explode(..., empty_as_null=)`
on a 3-row GA4-shaped frame (page_view empty items, purchase one item,
session_start NULL items):

| kwarg | event_names kept |
|---|---|
| default (deprecation: will become False in 2.0) | page_view, purchase, session_start |
| `empty_as_null=True` | page_view, purchase, session_start |
| `empty_as_null=False` | purchase, session_start |

Three sequential explodes (params / user_properties / items) agree: True
keeps all three rows; False drops page_view (empty lists) and keeps
session_start (NULL lists).

**Spark 4.1.2** (`/tmp/grok-c25-pyspark`, `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`):
`explode_outer("Legs")` on `array<struct<leg_id,side>>` keeps empty and
NULL as one null-element row; plain `explode` drops them.

**Engine CAST probe (repark native):** `CAST(NULL AS struct<x:bigint,y:string>)`
and `CAST(NULL AS STRUCT<x BIGINT, y VARCHAR>)` both succeed.
`make_array(CAST(NULL AS struct<leg_id:bigint,…,Fills:array<struct<…>>>>)`
+ `unnest` yields a typed null struct. `CAST(NULL AS struct<m:map<string,int>>)`
fails parse — map elements stay refused. `CAST(NULL AS void)` unsupported —
~~void lists keep inner explode.~~ Corrected §2.4: untyped `make_array(NULL)`.

### 2.2 Deliverables

- **D-1:** `_spark_array_element_to_sql` spells `struct<name:TYPE,…>` (nested
  structs + `array<…>` fields). `timestamp_ntz` → `TIMESTAMP`. `map<…>`
  returns `None` (same refuse class).
- **D-2:** `dynamicFlatten(..., empty_as_null: bool = True)`. True →
  `explode_outer`. False → private `explode_keep_null` (NULL-only CASE +
  inner explode). ~~`array<void>` still dropped / inner-exploded (no CAST).~~
  Corrected §2.4: remaining void lists use `explode_outer` /
  `explode_keep_null` via `make_array(NULL)`. `drop_null_lists=True` still
  drops void columns.
- **D-3:** flipped `test_nested_explode_outer_on_array_of_struct_refuses_loud`
  to the value pin; `_nested_full_flatten_rows` now derives the outer
  cartesian (shown in the test body); scalar explode_outer pin untouched.
- **D-4:** in-test GA4 fixture in `test_dynamic_flatten.py` (both flag
  states, leaf count, Arrow types). No `planning/` paths.
- **D-5:** docstring, dataframe-guide flag table, troubleshooting rewrite,
  map.md lockstep. Registry row is orchestrator-side.

B1 not started.

### 2.3 Known residual

`test_nested_dynamic_flatten_count_action_refuses_loud` — flip only if
the optimizer trip goes away under the new plan; otherwise keep.

~~`array<void>` / `NullType` lists: `CAST(NULL AS void)` is unsupported, so
`empty_as_null` cannot build a typed null element. Remaining void lists
(when `drop_null_lists=False`) stay on inner explode (NULL and EMPTY both
drop). Documented on the method, the guide flag table, and
`test_drop_null_lists_false_keeps_null_list_column`. ACC Q-001 (S2)
remediated as honest residual, not a silent ignore.~~
STRUCK (SQM #176 V-2). Silent sibling row-kill is not an acceptable residual.
Corrected residual: `CAST(NULL AS void)` is still unsupported; the NULL-guard
is untyped `make_array(NULL)` (measured: CASE + unnest keep one null element).
Nested `array<array<void>>` and `struct<…:void>` still refuse (no CAST
spelling). Map elements still refuse. `drop_null_lists=True` still drops
void columns.

SEC-001 (S2): struct field names are allowlisted (`[A-Za-z_][A-Za-z0-9_]*`)
before CAST embed; `decimal(p,s)` is `fullmatch`-gated; splitters honor
`()` so `decimal(10,2)` commas do not split fields. Hostile names/tokens
refuse loud.

## Pre-PR critic report (/repark-harden)
Engine: acc review-only standard — tier standard
Critic-1 (quality/parity): attacked explode_outer CAST, empty_as_null True/False
  CASE, void residual, pin discrimination, docs. Findings: Q-001 S2 (void
  lists ignore empty_as_null) WITHDRAWN after honest-docs remediating. 0 S0/S1.
  Test-coverage skeptic table recorded in the critic artifact. Null reports:
  signature parity, map refuse, scalar pin untouched, no planning/ paths.
Critic-2 (security/safety): attacked CAST interpolation, field-name embed,
  decimal prefix. Findings: SEC-001 S2 WITHDRAWN after allowlist + decimal
  fullmatch + paren-depth split. Null reports: secrets, parser DoS, panics,
  destructive ops, _explode_keep_null not exported.
Signature table: 0 pyspark.sql.functions names changed (empty_as_null is a
  repark-extra kwarg on dynamicFlatten only).
Oracle probes: polars 1.43.2 empty_as_null True/False; Spark 4.1.2
  explode_outer on array<struct>; CAST(NULL AS struct<…>); live NTZ
  explode_outer hour=12 / timestamp[us] (finder S1 REFUTED).
Pin audit: ~~struct explode_outer value pin, GA4 both flags, mapper unit,
  map refuse, void residual, sanitizer refuses — each names the impl it kills.~~
  STRUCK (SQM #176 V-1). MEASURED: map-refuse and
  `test_drop_null_lists_false_keeps_null_list_column` were green on BASE
  b628b0f (map already None→refuse; void pin asserted `count()==0` identical
  to BASE inner explode). Corrected in §2.4.
Convergence: ACC-CONVERGED (Critic-1 CLEAN after Q-001; Critic-2 CLEAN after
  SEC-001) + TEST-GATED preflight_exit=0 (3335 passed, 70 skipped).

## Finder-battery report
Target: b628b0f...worktree DF-2 | dimensions: 3
  (wiring/semantics, pins/tests, fence/docs) | findings: 10 raw → 10 deduped
Survivors: none at S0/S1 after verify
Refuted:
  - timestamp_ntz CAST→LTZ hour shift (S1) — live probe: timestamp[us], hour=12
  - full-depth count-only pin — out of D-3 scope (D-4 owns values/False)
  - flipped explode_outer missing payloads — D-3 asked for the scalar pin shape
  - GA4 False name-list-only — D-4 discharged (columns + names + types)
  - GA4 page_view user_properties_key — same CASE as pinned EMPTY items
  - nested web_info EMPTY — same CASE as sibling EMPTY + nested NULL
  - hyphenated field refuse — intended SEC-001 allowlist, not a silent bug
Confirmed (S3 fence, remediating in this commit):
  - tests/map.md "Four" → three remaining BUG-CANDIDATE pins
  - stale "140 rows" in the count() residual docstring
  - dataframe/map.md omitted empty_as_null / explode_keep_null
Null attestations:
  - wiring: True/False CASE, map refuse, void residual documented
  - pins: flip-don't-delete, GA4 both flags, mapper + sanitizer
  - fence: CLOSED surfaces untouched; ceilings held; no planning/ in public files
Verdict: CLEAN (zero S0/S1 survivors). Verifiers spawned (8). Agents spawned
  (explore): Critic-1, Critic-1 re-spot, Critic-2, Critic-2 re-spot, three
  finders, eight verifiers — not NOT-RUN.

SUPERSEDED by SQM #176 ROUND 1 (V-1 vacuous pins, V-2 void-sibling row-kill).
Round-2 critic / finder reports follow §2.4 (this commit).

B1 not started.

### 2.4 SQM #176 ROUND 1 — V-1 / V-2 (2026-08-17)

**V-2 (S2, behavior).** Preference (1) landed: untyped `make_array(NULL)`
CASE arm. MEASURED on f6aed24 before the fix:

| probe | result |
|---|---|
| `make_array(NULL)` | `[{a: [None]}]` list<item: null> |
| `CAST(NULL AS void)` | UnsupportedOperationException |
| CASE empty void → `make_array(NULL)` + unnest | `[{e: None}]` |
| `{props:[] void, items:[SKU]}` `drop_null_lists=False` | 0 rows |
| same, default `drop_null_lists=True` | 1 row, SKU |
| typed-empty sibling contrast | 1 row, SKU |

`_spark_array_element_to_sql("null"|"void")` → `_UNTYPED_NULL_ELEMENT`;
`_select_with_generator` emits `make_array(NULL)` and never interpolates
the sentinel into CAST. Nested `array<null>` / `struct<x:void>` stay
refused. `dynamicFlatten` no longer inner-explodes void lists.

**V-1 (S1, pin honesty).** MEASURED:

| pin | BASE b628b0f | f6aed24 | after V-2 |
|---|---|---|---|
| map-refuse `explode_outer("m")` | same AnalysisException (mapper already None) | same | same — relabeled non-discriminating regression guard |
| `test_drop_null_lists_false_keeps_null_list_column` | `count()==0` | `count()==0` | 1 row, `props` NULL — now discriminates inner-explode |

New discriminating pins: void `explode_outer` value pin; void-sibling SKU
pin (S2 corpus). `empty_as_null=False` EMPTY void still drops; NULL void
+ False keeps SKU (NULL-only CASE). Q-010: NULL-void False keep now pins
input `ArrayType(NullType)` + output Arrow `null`.

## Pre-PR critic report (/repark-harden) — round 2
Engine: acc review-only standard — tier standard
Critic-1 (quality/parity): attacked V-1 pin honesty, V-2 make_array(NULL),
  True/False CASE, docs residual. Cycle 1: Q-001 S2 (doc overclaim False
  keep) REMEDIATED; Q-002 S2 (void pins missing Arrow type) REMEDIATED;
  Q-003 S3 (debug-path nested wrap) REMEDIATED; Q-004 S3 (GA4 False
  payload) REMEDIATED; Q-005 S3 (WHERE redundant) WITHDRAWN (typed-path
  contract). Re-spot: Q-010 S2 (NULL-void input type unpinned)
  REMEDIATED. 0 S0/S1. Verdict CLEAN after remediating.
  Null reports: signature parity, map refuse labeled, scalar pin
  untouched, CLOSED surfaces, True default.
Critic-2 (security/safety): attacked CAST interpolation, sentinel,
  field-name embed, void sibling destruction. Findings: SEC-002 S2
  ACCEPTED_FLAGGED — physical field name `x:decimal(10,2),y` serializes
  as extra type fields in `arrow_type_key` (Rust embed, pre-V-2 D-1
  residual; complete fix is Rust quoting, out of V-1/V-2). Null reports:
  secrets, `_UNTYPED_NULL_ELEMENT` never in CAST, decimal fullmatch,
  classic injection, V-2 leaf sibling row-kill, panics, AWS.
Signature table: 0 pyspark.sql.functions names changed.
Oracle probes: make_array(NULL) + CASE unnest; S2 sibling 0→1 row;
  explode_outer void → pa.null(); CAST(NULL AS void) still unsupported.
Pin audit: void flatten + sibling SKU + void explode_outer + is_null
  each kill inner-explode / missing sentinel; map-refuse + mapper
  map/nested-void `is None` labeled non-discriminating; Q-010 input
  ArrayType pin kills scalar-null impostor.
Convergence: ACC-CONVERGED (Critic-1 CLEAN after Q-001/002/010;
  Critic-2 CLEAN with SEC-002 ACCEPTED_FLAGGED) + TEST-GATED
  preflight_exit=0 (3337 passed, 70 skipped) firsthand.

## Finder-battery report — round 2
Target: b628b0f...worktree DF-2 V-1/V-2 | dimensions: 3
  (wiring/semantics, pins/tests, fence/docs) | findings: 13 raw → 13
  deduped (8 survivors + 5 refuted; the earlier "12 → 12" header
  miscounted the sections below — corrected in §2.5 W-2)
Survivors after verify (ranked):
  [S1/CONFIRMED] F1 ledger pointed at missing round-2 reports — remediating
    in this section (this text).
  [S2/CONFIRMED] F2 explode_outer docstring still said array(NULL) —
    remediating (`make_array(CAST…)` / void `make_array(NULL)`).
  [S2/CONFIRMED] F3 dataframe/map.md F-4 sizes read as live — remediating
    (F-4 snapshot labeled; Debug has 7293/1211).
  [S2/CONFIRMED] F5 spark/map.md DF1 omitted False EMPTY void sibling
    drop — remediating.
  [S3/CONFIRMED] P2 void explode_outer pin lacked input ArrayType —
    remediating.
  [S3/CONFIRMED] P3 mapper map/nested-void `is None` unlabeled BASE-green
    — remediating (labeled).
  [S3/CONFIRMED] P4 web_info pin skips EMPTY — ACCEPTED_FLAGGED (same
    CASE arm as pinned NULL + sibling empty pin).
  [S2/CONFIRMED] SEC-002 type-key name/type split — ACCEPTED_FLAGGED
    (Rust quote; out of V-1/V-2).
Refuted:
  - W1 nested void in struct refuse-kills siblings — D-1 loud residual,
    V-2 is leaf array<void> only.
  - W2 unquoted reserved CAST field names fail explode_outer — engine
    type grammar accepts reserved Word tokens (SQM #176 also live-refuted
    from/to).
  - W3 CASE drops capitalized Fills — mapper spells Fills; flatten
    sibling pin would red.
  - P1 nested void refuse unit-only — same None→AnalysisException as
    map to_arrow pin; BIGINT fail-open impossible.
  - F4 raw SHAs in test prose — no contract forbids MEASURED BASE SHAs
    in pin docstrings.
Null attestations:
  - wiring: leaf make_array(NULL), True/False CASE, drop flags, map refuse
  - pins: V-1 relabel + V-2 discriminators + Q-010 input type
  - fence: CLOSED surfaces; no planning/; ceilings held; pin-audit struck
Verdict: CLEAN after remediating F1–F3/F5/P2/P3 (zero remaining S0/S1).
  Verifiers spawned (7 explore). Finders spawned (3). Not NOT-RUN.

B1 not started.

### 2.5 SQM #176 ROUND 2 — W-1/W-2 (2026-08-17)

**W-1 (S2, behavior).** `DataFrame.schema`'s flat-column mapper
(`spark/dataframe/core.py`) matched only lowercase `"void"` / `"null"`,
and its else-arm fails open to `StringType()`. The engine type key for
EVERY flat void column is the Arrow Debug spelling `"Null"` (capital
N — same spelling the V-2 list pins already carry as
`List(Field { data_type: Null … })`), so `.schema` / `.dtypes` reported
`string` while `to_arrow()` was `pa.null()`. The fail-open was never
explode-specific — MEASURED on BASE, a plain `SELECT NULL AS n` also
reported `('n', 'string')` — the void explode paths this unit opened are
just the newly-pinned route. Fixed narrowly: the arm now
also accepts `"Null"` (no casefold, no chain restructure — every other
standard type key is engine-lowercase).

MEASURED on this round's BASE c38578d (fix hunk stashed) vs fixed:

| probe | BASE c38578d | with W-1 fix |
|---|---|---|
| `logical_schema_fields()` void column key | `'Null'` | `'Null'` (unchanged) |
| `explode_outer(array<void>)` `.schema['e'].dataType` | `StringType()` | `NullType()` |
| same, `.dtypes` entry | `('e', 'string')` | `('e', 'void')` |
| same, `to_arrow()` field type | `null` | `null` (unchanged) |
| `dynamicFlatten(drop_null_lists=False)` `.schema['props']` | `StringType()` | `NullType()` |
| same, `.dtypes` entry | `('props', 'string')` | `('props', 'void')` |
| same, `to_arrow()` field type | `null` | `null` (unchanged) |

Pins extended (both go red with the hunk stashed — MEASURED, 2 failed):
`test_explode_outer_void_array_keeps_null_and_empty` and
`test_drop_null_lists_false_keeps_null_list_column` each now assert
`NullType()` on `.schema[…]` and `(name, "void")` in `.dtypes`. They kill
the StringType fail-open on the Debug-spelled void key.

**W-2 (S3, honesty).** The round-2 finder-battery header read
"12 raw → 12 deduped" while its own body names 13 IDs — survivors
F1, F2, F3, F5, P2, P3, P4, SEC-002 (8) plus refuted W1, W2, W3, P1, F4
(5). Header recounted to "13 raw → 13 deduped (8 survivors + 5 refuted)".
No finding was added or dropped; only the count is corrected.

---

## G3b — GA4 item_params spelling + honesty riders (2026-08-18)

**Lane:** repark · **Executor:** Claude (Opus 5) as ACTOR in an Actor-Critic round ·
**Worktree:** `/tmp/fable-trees/fable-rel031` · **Branch:** `fix/g3b-ga4-item-params` ·
**BASE-of-round:** `95cfaf9` (= merged main). Everything below is UNCOMMITTED.
**Scope:** four confirmed Group-3 findings — D-1 (S1), D-3 (S3), D-4 (S3), D-5 (S3 ruling).
Scratch probes lived in the session scratchpad (uncommitted, outside the tree).

**No Rust was touched** (measured, not assumed: `git diff --stat` lists only
`.cargo/audit.toml`, `deny.toml`, two `docs/` files, two `python/…/src` modules, two test
modules, four `map.md`). So no `make develop` and no `cargo test` were needed; the native
module in `.venv` is the one the round started with.

### D-1 (S1) — nested arrays were spelled postfix `[]`, which the parser re-binds

**Root cause, measured.** `python/repark/src/repark/spark/dataframe/plan_collapse.py` spelled a
nested array as `{inner}[]`. Probed straight at the engine with
`SELECT make_array(CAST(NULL AS <spelling>))`:

| spelling handed to the engine | what the engine parsed it as | faithful? |
|---|---|---|
| `BIGINT[]` | `array<bigint>` | yes |
| `array<BIGINT>` | `array<bigint>` | yes |
| `BIGINT[][]` | `array<array<bigint>>` | yes |
| `array<array<BIGINT>>` | `array<array<bigint>>` | yes |
| `struct<x:VARCHAR,nums:BIGINT[]>` | `struct<x,nums:array<bigint>>` | yes |
| `struct<x:VARCHAR,nums:array<BIGINT>>` | `struct<x,nums:array<bigint>>` | yes |
| `struct<item_id:VARCHAR,item_params:struct<key:VARCHAR,value:struct<sv:VARCHAR>>[]>` | `struct<item_id, item_params:struct<key, value:array<struct<sv>>>>` | **NO — `[]` migrated onto `value`** |
| `struct<item_id:VARCHAR,item_params:array<struct<key:VARCHAR,value:struct<sv:VARCHAR>>>>` | `struct<item_id, item_params:array<struct<key, value:struct<sv>>>>` | yes |

So the postfix form binds correctly only while `inner` does **not** end in `>`. Once it does,
the `[]` walks inward to the innermost field. That made the two arms of the `explode_outer`
`CASE WHEN` rewrite disagree, and the engine refused with `type_coercion` /
"Failed to coerce … CASE WHEN".

**Fix.** New helper `_sql_array_of(inner) -> f"array<{inner}>"`, used by **both** postfix sites —
`_spark_array_element_to_sql` (Spark simpleString path) and `_arrow_debug_type_to_sql`
(Arrow-debug `List(…)` path; the brief asked for that audit and it had the identical hazard).
The angle form round-trips exactly for scalar inners too, so it is applied **uniformly** — one
honest spelling, not a shape-dependent pair.

**Minimal repro, both doors, BASE vs fix (MEASURED):**

| door on `array<struct<item_id, item_params:array<struct<key, value:struct<sv>>>>>` | BASE `95cfaf9` | fixed |
|---|---|---|
| `df.dynamicFlatten()` | `AnalysisException type_coercion` | 4 rows, cols `ev / items_item_id / items_item_params_key / items_item_params_value_sv` |
| `df.select(F.explode_outer("items"))` | `AnalysisException type_coercion` | 3 rows |

**FULL 192-leaf GA4 end-to-end** — schema parsed programmatically from the operator's
`ga4_events_schema.md` tree (31 top-level fields, leaf count asserted `== 192`), five-row
corpus, `dynamicFlatten()` at defaults. Flattened column count came back **192**, matching the
doc's leaf count exactly, and `items_item_params_*` is present with values:

| # | row flavor | description | BASE `95cfaf9` | fixed |
|---|---|---|---|---|
| 1 | `full` | every array non-empty (2 elems), `items[].item_params[]` populated | raises | **16 rows** |
| 2 | `empty_arrays` | every array `[]` | raises | **1 row** |
| 3 | `null_arrays` | every array NULL | raises | **1 row** |
| 4 | `items_no_params` | `items[]` present, `items[].item_params[]` NULL | raises | **1 row** |
| 5 | `all_null_leaves` | arrays present, every scalar leaf NULL | raises | **1 row** |
| | | **survival under defaults** | **0/5** | **5/5** |

BASE fails the whole frame with `type_coercion` naming `"item_params": Struct(… value: List(…))`
on one arm against `"item_params": List(Struct(… value: Struct(…)))` on the other — the migrated
`[]`, verbatim. The `full` row's 16 = 2 `event_params` x 2 `user_properties` x 2 `items` x 2
`item_params`, and `items_item_params_key` carries a real value (not all-NULL).

**Adjacent shapes still correct (MEASURED, fixed tree):** `array<struct<x, nums:array<bigint>>>`
flattens to 3 rows with `a_nums` `int64`; `explode_outer` on `array<array<bigint>>` still returns
the inner list; a 4-deep `array<struct<p:array<struct<q:array<struct<z>>>>>>` flattens to
`t_p_q_z`.

**Refuse class preserved — the brief's explicit rider.** Shapes that still cannot spell refuse
**loud**, with the documented pre-#176 message class, unchanged:
`AnalysisException: explode_outer cannot resolve SQL element type for array column '…'
(engine type 'array<map<string,string>>'); cast the array or use a supported element type`.
Pinned by the new `test_dynamic_flatten_map_element_still_refuses_loud`. The fix widens what
spells; it does **not** fail open on what does not.

**Pins added / changed.**
- `test_dynamic_flatten.py::test_dynamic_flatten_array_of_struct_inside_array_element_struct` —
  the minimal repro, both doors.
- `test_dynamic_flatten.py` GA4 family — `_GA4_ITEM` now carries a **real** `item_params`
  (`ArrayType(_GA4_PARAM)`) with values on the purchase row, `_GA4_COLUMNS` grew the five
  `items_item_params_*` columns. **This fixture gap is how the defect shipped**: the old
  fixture stopped at the scalar item fields, so nothing exercised the shape.
- `test_dynamic_flatten.py::test_dynamic_flatten_scalar_array_inside_array_element_struct` and
  `::test_dynamic_flatten_map_element_still_refuses_loud` — the two riders above.
- `test_explode_rewrite.py::test_nested_array_cast_spelling_round_trips_in_engine` — asserts the
  emitted spelling parses back to the identical type, so the defect class itself is pinned, not
  just this one shape.
- `test_explode_rewrite.py::test_spark_array_element_to_sql_struct_and_map` — the unit pin that
  **locked in the defect** (`…Fills:struct<fill_id:BIGINT>[]>`) now demands the angle form, plus
  four new spelling assertions.

**Mutant check (MEASURED).** Reverting `_sql_array_of` to `f"{inner}[]"`:
4 pins red (`test_dynamic_flatten_ga4_empty_as_null_keeps_export_rows`,
`test_dynamic_flatten_array_of_struct_inside_array_element_struct`,
`test_nested_array_cast_spelling_round_trips_in_engine`,
`test_spark_array_element_to_sql_struct_and_map`) and the 192-leaf GA4 run drops to 0/5.
Restored → 87 passed.

### D-3 (S3) — troubleshooting.md said the wrong cause; the finding's own premise was also wrong

`docs/guide/troubleshooting.md` "count() fails on a deep dynamicFlatten plan" rewritten.

**PARTIAL REFUTATION of the finding as written, with evidence.** The finding asserted
"the polarity is inverted — `count()` SUCCEEDS". **It does not.** Measured on this tree, on the
doc's own repro, on BASE `95cfaf9` **and** on the fixed tree (byte-identical results — D-1 does
not touch this path): `deep.count()` **raises** `push_down_leaf_projections`. The doc's headline
is true. What is false is its **cause** and the completeness of its **remedy** — and on that the
finding is confirmed. Full measured surface:

| operation | result |
|---|---|
| `to_arrow()` / `collect()` / `show()` | 2 rows |
| `filter(...)`, `limit(1)`, `orderBy(...)`, `distinct()`, `drop("Legs_Fills_f")` | works |
| `select("*")`, `select(*deep.columns)`, any 3-of-4 keeping `Tags` | 2 rows |
| `select("Tags")`, `select("Tags","id")`, `select("Legs_leg_id","Tags")` | 2 rows |
| `select("id")`, `select("Legs_leg_id")`, `select("Legs_Fills_f")`, `select("Legs_leg_id","id")` | **raises** |
| `count()`, `agg(F.count(...))` | **raises** |

The exact trigger (measured over all 15 non-empty column subsets): a projection that **drops the
column the LAST explode pass produced** (`Tags` here). `count()`/`agg` are the extreme case —
they drop every column. The doc's old claim "it is `count()` that triggers the rule; the export
path never reaches it" is refuted twice over: the export path raises too once narrowed, and it is
the projection, not the action, that trips the rule.

It is also **order-dependent** (measured): rename `Tags` → `Alpha` so the sibling array explodes
*first*, and **all 15 subsets plus `count()` succeed** on identical data.

**Remedy verified live** (this is what the rewrite now recommends): `cache()` →
`cached.count()` = 2, `cached.select("Legs_Fills_f").to_arrow().to_pylist()` =
`[{'Legs_Fills_f': 1.0}, {'Legs_Fills_f': 1.0}]`, `cached.agg(F.sum("Legs_leg_id"))` = 2. An
Arrow round-trip also works. The engine defect itself is **DEFECT-2, chartered separately and
NOT touched here**; the existing pin
`test_datasets_facade.py::test_nested_dynamic_flatten_count_action_refuses_loud` still holds
unchanged.

### D-4 (S3) — quick-xml ignore rationale did not match the live lockfile

Verified myself with `cargo tree --workspace -i quick-xml@<v> -e normal` and `cargo metadata`:

| copy | pulled by | requirement | via |
|---|---|---|---|
| 0.39.4 | `object_store 0.13.2` | `^0.39.0` | `datafusion 54.1.0` |
| 0.38.4 | `opendal 0.55.0` | `^0.38` | fork's `iceberg-storage-opendal 0.9.1` |
| 0.37.5 | `reqsign 0.16.5` | `^0.37` | `opendal 0.55.0` / `iceberg-storage-opendal 0.9.1` |

**Three** copies, not two. The stale text claimed two, named `object_store 0.13.1` (live is
`0.13.2`), and attributed 0.38.4 to `object_store` — actually `opendal`. Independent
confirmation: `cargo audit` with only `RUSTSEC-2024-0436` ignored reports **6 vulnerabilities**
= 3 copies x 2 advisories, each `Solution: Upgrade to >=0.41.0`.

The *substance* of the ignore survives: `>=0.41.0` is semver-unreachable from **every** path
(`^0.39.0` / `^0.38` / `^0.37`), and exposure is DoS-only against AWS-authored S3 XML. Both
`deny.toml` and `.cargo/audit.toml` rewritten to the measured graph, kept mirrored, with the
verification method and date in the comment. Gates re-run after the edit: `cargo audit` clean,
`cargo deny check advisories` → `advisories ok`.

**Observation for the orchestrator (not fixed here):**
`docs/history/port-v2/p1b-repark-iceberg-ledger.md:124` carries the same stale
"0.38.4 via object_store 0.13.1" text. It is a *historical* ledger recording what was true then,
so I left it alone rather than rewrite history — flagging it in case you want it annotated.

### D-5 (S3 ruling) — rung (1) HONOR was reachable, and is what shipped

**Measured on BASE first.** The silent substitution was **wider than the finding stated**: it is
not only `ArrayType(NullType())`. A bare `NullType()` leaf is substituted too.

| requested (explicit schema) | BASE `95cfaf9` reported | fixed |
|---|---|---|
| `struct<v:void>` | `struct<v:string>` (arrow `string`) | `struct<v:void>` (arrow `null`) |
| `struct<a:array<void>>` | `struct<a:array<string>>` (arrow `list<item: string>`) | `struct<a:array<void>>` (arrow `list<item: null>`) |
| `struct<s:struct<x:array<void>>>` | `struct<s:struct<x:array<string>>>` | `struct<s:struct<x:array<void>>>` |
| `struct<a:array<array<void>>>` | `struct<a:array<array<string>>>` | `struct<a:array<array<void>>>` |
| DDL `"a ARRAY<VOID>"` | `struct<a:array<string>>` | `struct<a:array<void>>` |
| empty-frame seed, same schema | `struct<v:string,a:array<string>>` | `struct<v:void,a:array<void>>` |

No warning, no refuse, on any of them.

**Rung selection.** Rung (1) HONOR was probed before committing to it, by monkeypatching the two
mappers in a scratch process: reported schema, Arrow (`null` / `list<item: null>`), `collect()`,
`count()`, `dynamicFlatten()` and `dynamicFlatten(drop_null_lists=False)`, **and** the
`CAST(NULL AS VOID)` empty-frame seed all worked end to end. Rung (1) is cheap and correct, so
rungs (2)/(3) were not taken.

**Fix** (`python/repark/src/repark/spark/session/_funcs.py`, two hunks):
`_data_type_to_sql_type(NullType())` returns `"VOID"` (was `"VARCHAR"`), and `_sql_type_to_arrow`
maps `VOID` / `NULL` to `pa.null()`. The engine already carried void everywhere else — the DF-2
machinery (`make_array(NULL)`, `drop_null_lists`, the `Null` Debug-key arm from the earlier
W-1 fix) needed no change; ingest was the one dishonest door.

**Pin.** `test_dynamic_flatten.py::test_create_dataframe_honors_requested_void` — reported
schema, `.dtypes`, Arrow types, values, `count()`, nested + DDL doors, empty-frame seed, the
DF-2 flatten behaviours, and a control that `array<string>` is untouched. **Two mutants, both
red (MEASURED):** restoring `return "VARCHAR"`, and separately dropping the `VOID`/`NULL` entries
from `_sql_type_to_arrow`.

**Blast radius: zero.** The full Python suite passes; the only pre-existing text that referenced
the substitution was a docstring in `test_drop_null_typed_list` explaining the workaround, now
updated to point at the new pin.

**Registry question — NOT RUN, orchestrator decision.** No PySpark oracle exists in this
worktree or on this machine (checked `.venv`, `/tmp/grok-c25-pyspark` from the c25 section
above — gone — and a filesystem search; `import pyspark` fails). So I did **not** verify real
Spark's behaviour for an explicit `ArrayType(NullType())` at `createDataFrame`.
My expectation is that Spark honors it and reports `array<void>`, which would make this fix
**convergence** and mean **no registry row is needed** — but that is unverified reasoning, not a
measurement. Per the brief I did not edit the registry. If your oracle shows Spark diverging,
the candidate row text is:

> `createDataFrame` with an explicit `NullType()` / `ArrayType(NullType())` — repark honors the
> requested void end to end (reported schema `void` / `array<void>`, Arrow `null` /
> `list<item: null>`, `CAST(NULL AS VOID)` on the empty-frame seed). Spark <describe measured
> Spark behaviour>. Pin:
> `python/repark/tests/test_dynamic_flatten.py::test_create_dataframe_honors_requested_void`.

### Gates

| gate | result |
|---|---|
| `pytest test_dynamic_flatten.py test_explode_rewrite.py -q` | **87 passed** (BASE: 82) |
| `pytest python/repark/tests -q` | **3393 passed, 70 skipped** (BASE: 3388 / 70; +5 new pins) |
| `make check-lib-py` | `69 files clean (ceilings held; no-stub rule held)` |
| `cargo audit` | clean |
| `cargo deny check advisories` | `advisories ok` |
| `ruff check` / `ruff format` | clean (two test files reformatted after edit) |
| Rust crates touched | **none** — so no `cargo test`, no `make develop` |
| hygiene grep over the diff | **0 matches** for the round's forbidden-token pattern (org names, home paths, account id, AWS ARNs, session/agent URLs, planning-dir references) |
| `docs/spark-sql-iceberg-parity.md` | **untouched** (no ruling above authorised a row) |
| `map.md` lockstep | 4 updated: `python/repark/src/repark/spark/dataframe/`, `python/repark/src/repark/spark/session/`, `python/repark/tests/`, `docs/guide/` |

### NOT RUN (honesty)

- **`make preflight`** — forbidden by the round's hard rules.
- **Real-PySpark oracle for D-5** — no PySpark on this machine (see the registry question above).
  This is the one claim in this section resting on reasoning rather than measurement, and it is
  the reason the registry row is left to the orchestrator.
- **`cargo test` / `make develop`** — deliberately skipped, and the skip is *measured*: no Rust
  source changed. The two Rust-side files edited (`deny.toml`, `.cargo/audit.toml`) are gate
  configs, and both their gates were re-run.
- **`docs/spark-sql-iceberg-parity.md` rows** — none added; no ruling authorised one.
- **The DEFECT-2 projection bug behind D-3** — chartered separately, explicitly not fixed. Only
  the documentation of it was corrected.
- **`docs/history/port-v2/p1b-repark-iceberg-ledger.md`** — its stale quick-xml sentence was left
  as historical record; flagged, not edited.

### G3b residual disclosures (round critic, S3)

- **D-5 side effect, disclosed:** honoring `ArrayType(NullType())` at ingest makes a
  void array NESTED inside an array-element struct reachable by `dynamicFlatten` /
  `explode_outer` — where it hits the DF-2 D-1 nested-void refuse and now refuses
  LOUD (`explode_outer cannot resolve SQL element type …`). Previously the silent
  string substitution masked the shape entirely. Loud refuse over silent wrong is
  the intended trade; same class as the pinned `struct<x:void>` mapper refuse.
- **192-leaf provenance:** the full-GA4 end-to-end table above was built from an
  operator-supplied schema document outside the repo; it is NOT repo-reproducible
  as written. The repo-committed pins carry the load: the real `item_params`
  fixture in `test_dynamic_flatten.py` (the shape that was broken) plus the
  minimal-repro and round-trip pins. The table is evidence of the session run,
  not a repo artifact.

---

## DEFECT-2 — flatten projection pushdown (2026-08-18)

Charter: the projection defect behind G3b/D-3 (documented there, explicitly not fixed) and behind
the Group-3 C-036b BUG-CANDIDATE. Base of round: `5480b2b`. Round shape: Actor/Critic, **cycle 2**
— cycle 1's fix was correct but at the wrong altitude (a blanket optimizer-flag skip) and the
critic MEASURED its cost; §3 records the retraction, the scoped replacement, and the numbers on
both sides of the scope choice.

### 1. Reproduced firsthand (BASE `5480b2b`, MEASURED)

The troubleshooting section's own repro, extended to the full 15-subset matrix and both explode
orders. Whole-frame export = 2 rows in every cell.

```python
rows = [{"id": 1, "Legs": [{"leg_id": 1, "Fills": [{"f": 1.0}]}], "Tags": ["a", "b"]}]
deep = spark.createDataFrame(rows).dynamicFlatten()   # ['Legs_leg_id','Legs_Fills_f','Tags','id']
```

| subset (`.select(*subset).to_arrow()`) | BASE | after fix |
|---|---|---|
| `('Tags',)`, `('Tags','id')`, `('Legs_leg_id','Tags')`, `('Legs_Fills_f','Tags')`, `('Legs_leg_id','Legs_Fills_f','Tags')`, `('Legs_leg_id','Tags','id')`, `('Legs_Fills_f','Tags','id')`, all four | 2 rows | 2 rows |
| `('Legs_leg_id',)` | **RAISE** — Unnest assertion | 2 rows |
| `('Legs_Fills_f',)` | **RAISE** — Unnest assertion | 2 rows |
| `('Legs_leg_id','Legs_Fills_f')` | **RAISE** — Unnest assertion | 2 rows |
| `('id',)` | **RAISE** — qualified/unqualified ambiguity | 2 rows |
| `('Legs_leg_id','id')` | **RAISE** — ambiguity | 2 rows |
| `('Legs_Fills_f','id')` | **RAISE** — ambiguity | 2 rows |
| `('Legs_leg_id','Legs_Fills_f','id')` | **RAISE** — ambiguity | 2 rows |
| `count()` | **RAISE** — Unnest assertion | 2 |
| `agg(F.count(F.lit(1)))` | **RAISE** — Unnest assertion | 2 |

BASE totals: **7 of 15 subsets red** plus `count()`/`agg`. Rename `Tags` → `Alpha` (so the sibling
list explodes in a different position) and BASE is **0 of 15 red** with `count() == 2` — the
order-dependence, reproduced. After the fix both orders are **0 of 15 red**, and every subset's
values are compared cell-for-cell against the whole-frame `to_arrow()` export, not merely checked
for "did not raise".

**Correction to the earlier record (D-3 and the troubleshooting section).** Both said the trigger
is dropping "the column the **LAST** explode pass produced". Measured here, the last explode pass
on this frame is `Legs_Fills` (pass order: `Legs` → `Tags` → `Legs_Fills`), and the failing family
is exactly the 7 subsets that drop **`Tags`** — the *sibling top-level* list, exploded second of
three. The trigger is dropping the output of an explode pass whose `Unnest` node sits *under*
another `Unnest` in the chain. The old wording predicted the wrong 7 subsets; the matrix above is
the correction. Also new here: D-3 recorded only the ambiguity error. There are **two** distinct
failures, split cleanly across the failing subsets (table above).

### 2. Diagnosis — mechanism, not symptom

`push_down_leaf_projections` is **not a repark rule**. It is the pass-2 half of DataFusion 54.1's
leaf-expression extraction (`datafusion-optimizer-54.1.0/src/extract_leaf_expressions.rs`,
`PushDownLeafProjections`, rule #24). It fires whenever a `Projection` carries a
`MoveTowardsLeafNodes` expression — for us, the `get_field` that
`_dynamic_flatten_unnest_structs` emits for every struct field — and walks the plan top-down
trying to relocate that expression toward the scan.

repark's explode lowering (`_select_with_generator`, `python/repark/src/repark/spark/dataframe/
core.py`) builds one scratch-view hop per pass: a native mid `Projection` → temp view
(`SubqueryAlias __repark_expl_<uuid>`) → SQL `SELECT unnest(guard) AS "col", … FROM view`. A
multi-pass `dynamicFlatten` therefore stacks `Unnest` over `Unnest` with `get_field` projections
between them. On that shape the rule has two independent bugs:

1. **`Unnest` assertion.** `try_push_into_inputs` rebuilds the node it is pushing through with
   `node.with_new_exprs(node.expressions(), new_inputs)`. `LogicalPlan::Unnest::expressions()`
   returns the unnest exec column, while `Unnest::with_new_exprs` asserts `expr.is_empty()` →
   `Internal error: Assertion failed: expr.is_empty(): Unnest(Unnest { … })`. The rule has no
   business pushing into an `Unnest` at all; nothing about our plan makes it do so beyond having
   an `Unnest` in the path.
2. **Qualified/unqualified schema ambiguity.** `build_extraction_projection_impl`, merging into an
   existing projection, appends the pass-through columns it needs as `Expr::Column(q.name)`
   resolved against the projection's *input*. Our mid projection carries `q."id" AS "id"` (the
   identity alias `_bind_schema_column` attaches to keep the requested spelling), whose output
   field is **unqualified** `id`. The merge therefore lands qualified `__repark_expl_….id`
   beside unqualified `id` in one `DFSchema` → `Schema error: Schema contains qualified field
   name datafusion.public.__repark_expl_….id and unqualified field name id which would be
   ambiguous`. This is what the "qualified-field-name error" means about the scratch-view chain:
   the scratch views are the *source* of the qualifier, and the rule re-introduces the qualified
   spelling of a column the projection already exposes unqualified.

Why the order-dependence: the rule only trips when the extraction it wants to push has to travel
*through* an inner `Unnest`. When the sibling list explodes first, the `get_field` projections and
the `Unnest` they need to cross line up so the rule either finds nothing to push or bails on its
own schema check. Nothing about the data changes — only where the rule's walk lands.

**Falsified fix hypothesis (MEASURED, kept because it is the altitude question).** Failure (2)
*is* plan-shape reachable: rebuilding the mid projection's pass-through columns without the
identity alias (bare `PyColumn.column(quoted)` when the requested spelling equals the engine
field) removed every ambiguity error. It did **not** fix the defect — the same 7 subsets then
failed with the `Unnest` assertion instead, 7 red either way. Failure (1) has no plan-shape
escape: any multi-pass unnest carrying a struct extract reaches it. So a plan-shape fix would be
half a fix that looks like a whole one; it was abandoned, and the identity alias was left alone
(it is load-bearing for `select("X")` on field `x`).

### 3. The fix, and why that altitude

**Cycle 1 shipped the wrong altitude and the critic was right.** It set
`config.options_mut().optimizer.enable_leaf_expression_pushdown = false` as a core session
default and recorded the perf cost as NOT-RUN. The critic MEASURED that cost — up to 23x on a
filtered wide-struct parquet read — and pointed out that `PushDownLeafProjections` is a public
`OptimizerRule`, so a wrapper that declines only on `Unnest`-carrying plans was directly
available. Both premises verified here: `pub struct PushDownLeafProjections` +
`impl OptimizerRule` at `datafusion-optimizer-54.1.0/src/extract_leaf_expressions.rs:713,721`
inside `pub mod extract_leaf_expressions` (`lib.rs:60`), and `SessionStateBuilder` takes
`with_optimizer_rules`. The cycle-1 sentence *"the skip is unconditional because DataFusion
offers no per-plan disable"* was **false** and is retracted.

**Cycle 2 — the shipped fix.** `crates/repark-core/src/session/df_guards.rs`. The two DF-54.1
guards now sit at two different altitudes, because the two bugs are:

* Guard 1 (scalar subquery) stays a `SessionConfig` default — a whole physical planning mode is
  bad, there is no safe sub-shape, DataFusion's flag is the switch.
* Guard 2 (leaf-projection pushdown) is a **scoped optimizer rule**. `build()` spells out what
  `SessionContext::new_with_config_rt` does and adds one thing:

```rust
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime)
    .with_default_features()
    .with_optimizer_rules(unnest_safe_optimizer_rules())   // DF's list, ONE rule wrapped
    .build();
let context = SessionContext::new_with_state(state);
```

`unnest_safe_optimizer_rules()` is `Optimizer::new().rules` — the exact list
`SessionStateBuilder` installs by itself — with the element named `push_down_leaf_projections`
replaced by `UnnestSafeLeafProjectionPushdown`, which delegates `name()` and `apply_order()` and
gates `rewrite` in **two steps**:

1. **No `Unnest` in the subtree → delegate untouched, errors included.** `apply_order` is
   `TopDown`, so the `plan` a rule sees is the subtree the push can reach; without an `Unnest` in
   it neither bug is reachable. No clone, no catch, and a rule failure still propagates loud, so
   an unrelated upstream bug cannot hide behind this guard.
2. **`Unnest` in the subtree → try the rule; keep the unrewritten plan only if it fails.**

Altitude reasoning:

- The bug is **upstream and optimizer-only**. Both failures are inside a DataFusion rule; with
  the rule declining, the *identical* logical plan executes and returns the *identical* rows.
  Nothing in repark's own lowering is wrong (§2's falsified plan-shape hypothesis).
- **Not a whitelist.** No `count()` case, no per-subset branch. All 15 subsets, both explode
  orders, `count()` and `agg` take one path.
- **Core session default, not a door-extension knob** (design §2 G8): the facade's explode
  rewrite plans through the core session, and an extension-less native session builds the same
  `Unnest` chain from SQL.
- **The scope is by failure, not by shape**, because shape alone is too coarse — see the numbers
  below.

#### PERF — MEASURED this round, replacing cycle 1's NOT-RUN

Machine: this worktree, best-of-3.

**(a) What a blanket flag-off would have cost** (facade, 500k-row parquet, `SELECT s.f1 AS a FROM
t WHERE k > 10`, rule ON vs rule OFF — the OFF column is exactly cycle 1's shipped behavior):

| struct width | rule on | blanket off | ratio |
|---|---|---|---|
| 4 | 0.170s | 0.234s | 1.38x |
| 20 | 0.148s | 0.522s | 3.52x |
| 60 | 0.167s | 1.297s | 7.78x |
| 60, **no** `WHERE` | 0.037s | 0.036s | 0.96x (no delta) |

Same direction, same monotonicity in struct width, and the same "only with the filter" signature
the critic measured (they saw 2.2x / 8.1x / 23.3x on their machine). **The scoped fix pays none
of this**: those plans carry no `Unnest`, so they run the stock rule — pinned as a plan by
`a_plan_without_unnest_keeps_the_stock_leaf_pushdown` (byte-identical to stock DataFusion's
optimized plan).

**(b) What declining by SHAPE would have cost** (Rust, 500k rows × 60 struct fields, stock
`SessionContext` vs `ReparkSession`, same SQL, best-of-3):

| shape | stock DF | repark, decline-by-shape | repark, shipped (try-then-decline) |
|---|---|---|---|
| wide-struct filtered scan **with an unnest alongside** | 9.192s | 107.466s (**11.8x**) | 9.178s (parity) |
| the same scan, no unnest | 0.162s | 0.154s | 0.154s |

That 11.8x is why the wrapper declines on the rule's actual failure rather than on the presence
of an `Unnest`. It is also why the first cycle-2 draft (decline-by-shape) was itself rejected.

**The trade that remains, recorded.** Inside an `Unnest`-carrying subtree the rule's error is
*swallowed*: repark-core carries no logging dependency, so the decline is silent, and the shape
keeps the correct-but-unoptimized plan. It is observable where it matters — `EXPLAIN` shows the
un-pushed plan — and it is the same bargain DataFusion's own `skip_failed_rules` option makes
globally. Bounded to a rule that is a pure optimization, the worst case is a slower plan, never a
wrong one.

**No knob restores the miscompile.** `datafusion.optimizer.enable_leaf_expression_pushdown` is
untouched at DataFusion's default and still disables the whole optimization when set to `false`
(the wrapped rule reads the flag itself) — pinned by
`explicit_conf_can_still_disable_leaf_expression_pushdown`.

### 4. Pins (all new/changed pins, and what each holds)

| pin | holds |
|---|---|
| `session/df_guard_tests.rs::bare_session_keeps_leaf_expression_pushdown_enabled` | the ANTI-BLANKET-SKIP pin: the flag stays at DataFusion's default, so no future round can quietly re-ship cycle 1 |
| `session/df_guard_tests.rs::bare_session_without_extension_scopes_leaf_projection_pushdown` | a bare no-extension `build()` installs the wrapper — under DataFusion's own rule NAME, in DataFusion's own rule ORDER, with every other rule untouched (CORE altitude, design G8) |
| `…::a_plan_without_unnest_keeps_the_stock_leaf_pushdown` | the perf finding pinned as a *plan*: a no-`Unnest` query optimizes byte-identically to stock DataFusion, extraction still hoisted below the filter |
| `…::an_unnest_plan_the_rule_can_rewrite_still_gets_leaf_pushdown` | the try-then-decline altitude: an `Unnest` in the subtree is not by itself a reason to lose the optimization |
| `…::explicit_conf_can_still_disable_leaf_expression_pushdown` | the DataFusion flag still reaches `SessionConfig` through the wrapper |
| `test_dynamic_flatten.py::test_multi_pass_flatten_every_projection_subset_is_green[Tags\|Alpha]` | all 15 subsets, BOTH explode orders, values equal to the whole-frame export |
| `test_dynamic_flatten.py::test_multi_pass_flatten_count_and_agg_are_green[Tags\|Alpha]` | `count()` / `agg` == `to_arrow().num_rows`, both orders |
| `test_dynamic_flatten.py::test_ga4_real_shape_flatten_then_project` | the GA4 `item_params` fixture flatten-then-project, every column + a narrowing multi-column projection |
| `test_dynamic_flatten.py::test_multi_pass_flatten_cache_is_still_a_plain_pattern` | the retired workaround still works as ordinary caching |
| `test_explode_rewrite.py::test_two_pass_explode_chain_survives_a_narrowing_projection` | the same shape **without** `dynamicFlatten` — proves the defect was the plan shape, not the helper |
| `test_datasets_facade.py::test_nested_dynamic_flatten_count_action_is_green` | C-036b flipped in place on the 64-row nested corpus |

**Declared rename (testing.md relocation discipline).** Three test names changed. One is the
BUG-CANDIDATE flip its own docstring instructed; two are cycle-1 pins whose *subject* changed
when the fix moved from a flag to a rule (a pin asserting the flag is `false` cannot survive a
fix that keeps the flag `true`), so they were replaced rather than renamed:

| old | new |
|---|---|
| `test_datasets_facade.py::test_nested_dynamic_flatten_count_action_refuses_loud` | `…::test_nested_dynamic_flatten_count_action_is_green` |
| `session/tests.rs::bare_session_without_extension_carries_df_54_1_leaf_pushdown_guard` (cycle 1) | `…::bare_session_keeps_leaf_expression_pushdown_enabled` + `…::bare_session_without_extension_scopes_leaf_projection_pushdown` (its assertion INVERTS: the flag must now stay enabled) |
| `session/tests.rs::explicit_conf_re_enables_leaf_expression_pushdown` (cycle 1) | `…::explicit_conf_can_still_disable_leaf_expression_pushdown` (the conf's job is the opposite direction now) |

**Honest coverage note.** The GA4 real-shape pin did **NOT** reproduce the defect on BASE
(measured: all 22 single-column projections, `count()` and `agg` were already green there — its
explode order is the lucky one). It is a coverage pin for the real-world shape, not a second
reproduction, and it is labelled that way in its docstring.

### 5. Mutant check (three mutants, MEASURED)

Each mutant was applied to `df_guards.rs`, rebuilt (`cargo test` + `make develop`), and the pins
re-run.

**M1 — the wrapper is never installed (== BASE / stock DataFusion).**

```
FAILED test_dynamic_flatten.py::test_multi_pass_flatten_every_projection_subset_is_green[Tags]
FAILED test_dynamic_flatten.py::test_multi_pass_flatten_count_and_agg_are_green[Tags]
FAILED test_explode_rewrite.py::test_two_pass_explode_chain_survives_a_narrowing_projection
FAILED test_datasets_facade.py::test_nested_dynamic_flatten_count_action_is_green
4 failed, 111 passed
```

…and Rust: `bare_session_without_extension_scopes_leaf_projection_pushdown` FAILED. The `[Alpha]`
order and the GA4 shape stayed green — exactly the cells that never reproduced, which is the
honest signal, not a gap.

**M2 — decline by SHAPE instead of by failure** (the first cycle-2 draft). Rust:
`an_unnest_plan_the_rule_can_rewrite_still_gets_leaf_pushdown` FAILED, everything else green;
Python 115 passed. Correct but 11.8x slower on the shape in §3(b) — which is precisely what that
one pin exists to catch.

**M3 — cycle 1's blanket `enable_leaf_expression_pushdown = false`.** Rust: 4 of the 5 new pins
FAILED (`bare_session_keeps_leaf_expression_pushdown_enabled`,
`bare_session_without_extension_scopes_leaf_projection_pushdown`,
`a_plan_without_unnest_keeps_the_stock_leaf_pushdown`,
`an_unnest_plan_the_rule_can_rewrite_still_gets_leaf_pushdown`); Python 115 passed. Cycle 1 was
correct and slow; the pins now hold that door shut.

Guard restored, `make develop` re-run, all green.

### 6. Docs / workaround retirement

- `docs/guide/troubleshooting.md` — the section is kept as a **fixed entry** (repo convention: the
  pin holds the absence). Retitled `… (FIXED)`, opens with the status line + ledger pointer, keeps
  both real error texts, states the mechanism and the scoped rule, carries the measured perf
  numbers on **both** sides of the scope choice, shows the now-working code, and re-frames
  `cache()` as ordinary caching rather than an escape hatch.
- `examples/notebooks/datasets_tour.ipynb` — the cell that counted through the export path "on
  purpose" now counts both ways and says the defect was fixed.
- `task/c18-datasets-ledger.md` — C-036b and finding 4 carry **SUPERSEDED** riders pointing here
  (historical rows are not rewritten, they are annotated).
- `python/repark/src/repark/spark/dataframe/plan_collapse.py` —
  `_dynamic_flatten_unnest_structs`'s docstring said the `selectExpr` spelling exists to avoid
  poisoning multi-pass unnest under `push_down_leaf_projections`; that rationale is now stale, so
  it is marked stale and the still-live reason (quoted idents for mixed-case/hostile names) is
  named.
- **Stale-pointer sweep (critic S3).** Every reference that pointed the leaf-pushdown guard at
  `crates/repark-core/src/session.rs` now names `crates/repark-core/src/session/df_guards.rs`:
  `python/repark/tests/map.md`, `docs/guide/troubleshooting.md`,
  `python/repark/tests/test_dynamic_flatten.py`,
  `python/repark/src/repark/spark/dataframe/plan_collapse.py`, and this ledger's §3/§5/§7.
  Verified: `grep -rn "session\.rs" ` over the changed files returns no leaf-pushdown row.
- `map.md` lockstep: `python/repark/tests/map.md`, `crates/repark-core/src/map.md` (the bullet now
  describes the two altitudes; the debug-table row routes "nested query got slower" to the real,
  narrow answer), `crates/repark-core/src/session/map.md`, `docs/guide/map.md`,
  `examples/notebooks/map.md`.
- `docs/spark-sql-iceberg-parity.md` — **untouched** (no ruling authorised a row).

### 7. Files changed

| file | change |
|---|---|
| `crates/repark-core/src/session/df_guards.rs` | **new** — guard 1 as a config default (`apply_df_54_1_config_guards`) + guard 2 as the scoped rule (`unnest_safe_optimizer_rules` + `UnnestSafeLeafProjectionPushdown` + `carries_unnest`). Lifted out of `session.rs`'s `build()`, which had crossed clippy `too_many_lines` and then the 1500-line file ceiling; splitting the module is the sanctioned out, not an EXCEPTIONS row |
| `crates/repark-core/src/session.rs` | `build()` calls `apply_df_54_1_config_guards`, then builds the `SessionState` explicitly (`SessionStateBuilder` + `with_optimizer_rules(unnest_safe_optimizer_rules())`) instead of `SessionContext::new_with_config_rt` — one `SessionStateBuilder` import; `apply_datafusion_config_keys` rustdoc names both guards |
| `crates/repark-core/src/session/df_guard_tests.rs` | **new** — the six guard pins (2 cycle-1 pins replaced, 3 new, plus the pre-existing scalar-subquery pin moved here); `tests.rs` was at 1487/1500 and the cohort would have pushed it to 1600, so the module was split rather than an EXCEPTIONS row added |
| `crates/repark-core/src/session/tests.rs` | the DF-54.1 guard cohort moved out to `df_guard_tests.rs` (1449 → 1432 lines) |
| `python/repark/tests/test_dynamic_flatten.py` | 4 pins (2 parametrized ×2 orders) |
| `python/repark/tests/test_explode_rewrite.py` | 1 pin |
| `python/repark/tests/test_datasets_facade.py` | C-036b flipped + renamed |
| `python/repark/src/repark/spark/dataframe/plan_collapse.py` | stale-rationale rider on `_dynamic_flatten_unnest_structs` (docstring only — no behavior) |
| `docs/guide/troubleshooting.md`, `examples/notebooks/datasets_tour.ipynb`, `task/c18-datasets-ledger.md` | workaround retirement / superseded riders |
| `map.md` lockstep | `crates/repark-core/src/map.md`, `crates/repark-core/src/session/map.md`, `python/repark/tests/map.md`, `docs/guide/map.md`, `examples/notebooks/map.md` |

### 8. Suites (exact counts, this tree, MEASURED)

| command | result |
|---|---|
| `pytest test_dynamic_flatten.py test_explode_rewrite.py test_datasets_facade.py -q` | **115 passed** |
| `make py-test-facade` (whole facade suite) | **3400 passed, 70 skipped** (BASE was 3393 / 70; +7 = the 7 new facade pins) |
| `make check-lib-py` | `lib-py: 69 files clean` |
| `cargo test -p repark-core` (the only crate whose source changed) | **145 + 37 + 8 passed, 1 doctest**, 0 failed |
| `cargo test --workspace` | **1920 passed, 0 failed** (BASE 1917; +3 = the net new Rust pins) |
| `make verify` | exit 0 |
| `make preflight` | exit 0 (verify + facade suite + audit + workflow lint; zizmor: no findings) |

### 9. NOT-RUN (declared)

- **`make parity-live`** — needs a JVM; nothing in this round touched a Spark-parity semantic (the
  plans and rows are identical either side of the guard).
- **The packaged-wheel smoke (`wheels.yml`)** — CI-side job, not runnable here; the change is a
  session default, not a boundary/PyO3 seam change, so the `maturin develop` facade suite is the
  right tier for it.
- **`docs/spark-sql-iceberg-parity.md`** — untouched; no ruling authorised a row.
- **A microbenchmark of the wrapper's own overhead.** `carries_unnest` is one short-circuiting
  tree walk per node the `TopDown` rule visits — O(plan²) in the worst case on a plan with no
  `Unnest` anywhere, since nothing short-circuits it — plus a `LogicalPlan` clone on
  `Unnest`-carrying subtrees only. Not isolated: it is *inside* the §3(b) numbers, where repark
  reaches parity with stock DataFusion on both shapes, so on those plans it is below the
  measurement's noise. Not separately quantified, and NOT measured on a pathologically deep plan
  — that is the shape where the quadratic term could surface.
- **The DataFusion upstream issue.** Both failures are upstream bugs worth filing
  (`Unnest::with_new_exprs` vs `LogicalPlan::expressions`; the merge that mixes qualified and
  unqualified spellings in `build_extraction_projection_impl`). Not filed from this round — no
  authority to open an upstream issue on the project's behalf. Flagged for the orchestrator.

### DEFECT-2 round critic S3 riders (applied at close-out)

- The "7.78x" ratio in the width table above is ONE best-of-3 run; the critic's
  rerun of the same recipe reproduced the direction and width-monotonicity but
  not the exact ratio (load-sensitive). All prose citations now say "up to ~8x
  in one measured run"; the table keeps the raw numbers as the run's record.
- The test_dynamic_flatten.py DEFECT-2 module header carried the retracted
  "LAST explode pass" trigger wording; corrected to the measured
  Unnest-under-Unnest trigger.
