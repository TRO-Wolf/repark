---
name: code-quality
version: "2.0"
description: >-
  Portable Python code-quality standards. Load when writing, reviewing, or
  refactoring Python code — Ruff as the single lint/format tool, typing and
  named steps, naming rules, function shape (no nested defs, no unbounded
  recursion), imports, pydantic v2 for structured data, lazy dataframes,
  comment/docstring discipline written for the eventual reader, mandatory
  tests, and the verification gate. Also load when deciding whether a
  convention should be a review duty or a mechanical gate, or when a
  conventions gate fails and the sanctioned outs need to be weighed. It states
  rules and their rationale; it does not run the gate, and where a host repo
  has its own contract, that contract wins.
---

# Python Code Quality Conventions

A portable set of Python code-quality standards, written to apply to any Python
repository. No project-specific paths, tools, or frameworks — pair it with a
per-repo doc ([AGENTS.md](../../../AGENTS.md) here) for placement rules, build
commands, and stack-specific conventions. On any conflict, the per-repo doc wins.
Rust has its own review skill: [../rust-code-quality/SKILL.md](../rust-code-quality/SKILL.md).

Every rule below is marked with **how it is held** — by a **linter**, by a
**purpose-built gate**, or by **review**. That third category is not a lesser
one, but it should be small and deliberate, because a rule held only by review
is a rule that decays at the speed of reviewer attention. When a review-held
rule proves mechanically decidable, graduate it into a gate — §13 is the
procedure.

---

## 1. Tooling / style baseline

**Ruff is the only lint and format tool.** It replaces Black, isort, and Flake8.
Do not run those alongside it — two formatters fighting over the same file is a
diff generator, not a quality gate.

- `ruff format` for formatting (Black-compatible output).
- `ruff check --fix` for linting, including import sorting (the `I` rules replace
  isort) and the pyflakes/pycodestyle rules that replace Flake8.
- Configure once in `pyproject.toml`. `line-length` applies to both the formatter
  and the linter, so the two can never disagree — the Black/Flake8 `E203` and
  `W503` conflict does not exist here and no ignore list is needed for it.

The block below is an **illustrative floor, not a config to impose**. When this
skill lands in a repo that already has a Ruff config, the repo's
`pyproject.toml` is the SSOT — never "align" its existing values (line length,
target version, selected rules, ignores) to this sample. Selecting a new rule
family (`D`, `ANN`) in an existing repo is an arming decision: measure first,
then ratchet — see §13.

```toml
[tool.ruff]
line-length = 88          # example value — keep the host repo's existing one
target-version = "py312"  # example value — match the host repo's floor

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "A", "C4", "SIM", "PTH", "PGH", "ANN", "D"]
ignore = ["ANN401"]  # `Any` is sometimes the honest annotation

[tool.ruff.lint.per-file-ignores]
# Test return types and docstrings are noise, not contract.
"**/tests/**" = ["ANN001", "ANN201", "ANN202", "D"]

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.ruff.lint.isort]
known-first-party = ["<your_package>"]
```

- The rule selection is a floor, not a ceiling. `N` enforces §5 naming, `ANN`
  enforces §3 typing, and `D` enforces the §9 docstring requirement — keep those
  three selected, since they are the rules this document leans on.
- Pin Ruff — an exact version or a tight bounded range (`>=X.Y.Z,<X.Y+1`) — and
  mirror the pin everywhere it appears (pre-commit config, CI config, dev
  dependency group) — bump them together, never independently. Ruff ships rule
  changes on a fast cadence; an unpinned version means CI can fail on a commit
  that touched nothing.
- When a lint rule must be bypassed, use a targeted `# noqa: <RULE>` suppression
  with an explanatory comment on the same line. With `PGH` selected, a bare
  `# noqa` is itself a violation (`PGH004`). **Never `--no-verify`** — if
  pre-commit fails, fix the underlying issue.

**Held by: the linter**, end to end. This whole section exists so that no rule
in it ever needs a reviewer.

## 2. Python language rules

- **EVERYTHING gets a data type — typed out.** Every function signature, every
  public attribute, and every meaningful local variable. See §3.
- **Use pydantic v2 `BaseModel` for all structured data** — configs, API
  payloads, internal records, value objects, grouped function arguments. Do not
  use `dataclasses`, `attrs`, or ad-hoc dicts for these jobs — pydantic validates
  at the boundary; a dataclass just holds whatever it was handed. Use the v2 idioms
  (`model_config` / `Field` / `.model_dump()` / `.model_validate()`) — never the
  v1 ones (`.dict()` / `.parse_obj`). For immutability:
  `model_config = ConfigDict(frozen=True)`.
  The cost, stated honestly: a `BaseModel` validates on construction, which a
  `dataclass` does not — converting an existing container adds a runtime check
  that was not running before. That is usually the bug being found rather than
  the bug being introduced, but it is a behaviour change and it belongs in the
  commit message. *Held by a purpose-built gate*: no linter has this rule; a
  checker that flags `import dataclasses` / `import attrs` outside an
  exceptions table is about thirty lines and is the only thing that keeps the
  rule from decaying.
- Prefer `pathlib.Path` over string paths.
- Use `logging` (not `print`) for any code that runs in production.
- Use f-strings; never `%` formatting or old `.format()` style.
- Never catch bare `Exception` unless you immediately re-raise or log with a full
  traceback. No bare `except:`, no swallowed errors.
- No magic numbers — use named constants or configuration values.
- Error messages must be specific and actionable — not a generic "something went
  wrong."
- If copying logic from one place to another, extract it into a shared function
  instead.
- Credentials come from a secrets manager or connection registry, never from a
  literal in code or config committed to the repo.

## 3. Type everything; name every step

Two rules that trade line count for auditability. Using more lines to break out
the steps is perfectly fine — vertical space is free; a reviewer's working memory
is not. (One caveat: where the host repo caps file length with a thinness gate,
that per-repo ceiling wins — do not cite this section to justify raising one.)

**1. EVERYTHING gets a data type, typed out.** Not just signatures — annotate the
intermediate variables too. An annotated local documents what the expression
produces, makes the reviewer's type-check instant, and turns a wrong assumption
into a type-checker error instead of a runtime surprise three calls later.

**2. Avoid nesting function calls.** Do not bury a comprehension or a call chain
inside another call's argument list. Bind each step to a named, typed variable,
then pass the name. Each intermediate becomes something you can read, print,
debug, and reason about on its own line — an inline nest can only be understood
by mentally executing it.

```python
# Good — the comprehension is bound to a named, typed variable; full-word
# loop variables; the call site reads as one step.
def cleanse_columns(
    df: SparkDataFrame,
) -> SparkDataFrame:
    """Fill NULL ids with the -1 sentinel."""
    cleansed_column_map: dict[str, SparkColumn] = {
        column_name: F.coalesce(F.col(column_name), F.lit(-1))
        for column_name in KEY_COLUMNS
    }
    return df.withColumns(cleansed_column_map)

# Bad — untyped signature, the comprehension is nested inside the call,
# single-letter loop variable.
def cleanse_columns(df):
    """Fill NULL ids with the -1 sentinel."""
    return df.withColumns({c: F.coalesce(F.col(c), F.lit(-1)) for c in KEY_COLUMNS})
```

```python
# Good — the flag map is a named, typed step before the call.
def flag_null_keys(
    df: SparkDataFrame,
) -> SparkDataFrame:
    """BEFORE cleansing: write down which keys were NULL at the source."""
    null_flag_map: dict[str, SparkColumn] = {
        f"{column_name}_was_null": F.col(column_name).isNull()
        for column_name in KEY_COLUMNS
    }
    return df.withColumns(null_flag_map)

# Bad — untyped signature, comprehension nested inside the call.
def flag_null_keys(df):
    """BEFORE cleansing: write down which keys were NULL at the source."""
    return df.withColumns({f"{c}_was_null": F.col(c).isNull() for c in KEY_COLUMNS})
```

```python
# Good — every local annotated; each step on its own line.
def hash_key_columns(
    df: SparkDataFrame,
) -> SparkDataFrame:
    """Build the row hash column from the cleansed key columns."""
    parts_list: list[SparkColumn] = []

    for column_name in KEY_COLUMNS:
        cleansed_column: SparkColumn = F.coalesce(
            F.col(column_name).cast("string"), F.lit(NULL_TOKEN)
        )
        flag_column: SparkColumn = F.col(f"{column_name}_was_null")

        parts_column: SparkColumn = F.when(
            flag_column, F.lit(NULL_TOKEN)
        ).otherwise(cleansed_column)
        parts_list.append(parts_column)

    concatenated_parts: SparkColumn = F.concat_ws("|", *parts_list)
    row_hash_column: SparkColumn = F.sha2(concatenated_parts, SHA2_BIT_LENGTH)
    return df.withColumns({ROW_HASH_COLUMN: row_hash_column})

# Bad — no annotations anywhere, single-letter loop variable, steps fused
# into single expressions, magic number at the call site.
def hash_key_columns(df):
    """Build the row hash column from the cleansed key columns."""
    parts = []
    for c in KEY_COLUMNS:
        cleansed = F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN))
        parts.append(F.when(F.col(f"{c}_was_null"), F.lit(NULL_TOKEN)).otherwise(cleansed))
    return df.withColumns({ROW_HASH_COLUMN: F.sha2(F.concat_ws("|", *parts), 256)})
```

Boundaries, so this does not tip into ceremony:

- **"Meaningful local" is the bar.** A trivially obvious binding
  (`count: int = 0`, the `i` in a bounded loop) does not need an annotation to be
  auditable. A collection, a built expression, anything crossing more than one
  line of logic — annotate it.
- **One level of call is not nesting.** `logging.info(len(rows))` is fine. The
  rule targets a comprehension, a conditional expression, or a multi-call chain
  living inside another call's argument list.
- This rule composes with §5 naming: the intermediates you break out get
  full-word names (`column_name`, `cleansed_column`), never `c` / `tmp`.

**Held by: the linter for signatures** (Ruff `ANN`), **review for the rest** —
no linter checks local annotations or call-nesting depth, so this is where
reviewer attention goes.

## 4. Function length and shape

- **Target: under 100 lines per function.** One responsibility per function — if
  you cannot describe it in a single sentence without "and," it does too much.
- **Triggers to extract a helper**: nesting exceeds three levels, OR the function
  does two distinct things (an "and" in its docstring), OR a block of logic
  deserves its own name to be understood.
- **Splitting is not free** — do not extract a 4-line helper called from one place
  just to hit a line count. Extract when the extracted function's name makes the
  caller easier to read.

### Avoid nested function definitions

A `def` inside a `def` is a function you cannot import, cannot call from a test,
and cannot see in a traceback by name. Break it out to module scope, prefix with
`_` if module-private, pass what it needs as arguments, and give it its own unit
test. A nested helper hides the most test-worthy logic in the file behind an
outer function you can only reach through its full input. It is also rebuilt on
every call of its parent, and — the one that produces the genuinely surprising
bugs — it reads the parent's locals, so changing a local three lines up silently
changes what it computes.

```python
# Bad — key_expr is nested: untestable except through the whole DataFrame call,
# invisible in a traceback by name, and reading df.columns from the closure.
# Single-letter names, untyped locals, and bare `col` / `when` / `lit` imports
# that could mean anything at the call site.
def compute_row_hash_code(
    df: SparkDataFrame,
    hash_columns: Sequence[str],
    column_name: str = ROW_HASH_COLUMN,
) -> SparkDataFrame:
    """sha2 of the key columns. (WHY-rationale docstring elided for the example.)"""
    raw_keys = [c for c in hash_columns if f"{RAW_KEY_PREFIX}{c}" in df.columns]

    def key_expr(c: str) -> SparkColumn:
        cleansed = coalesce(col(c).cast("string"), lit(NULL_TOKEN))
        raw = f"{RAW_KEY_PREFIX}{c}"
        if raw not in df.columns:
            # Derived or renamed key: nothing pre-cleanse to compare against.
            return cleansed
        return when(col(raw).isNull(), lit(NULL_TOKEN)).otherwise(cleansed)

    parts = [key_expr(c) for c in hash_columns]

    if raw_keys and SOURCE_HASH_COLUMN in df.columns:
        source_key_missing = col(f"{RAW_KEY_PREFIX}{raw_keys[0]}").isNull()
        for c in raw_keys[1:]:
            source_key_missing = source_key_missing | col(f"{RAW_KEY_PREFIX}{c}").isNull()
        parts.append(
            when(source_key_missing, col(SOURCE_HASH_COLUMN)).otherwise(lit(""))
        )

    return (
        df.withColumns({
            column_name: sha2(concat_ws("|", *parts), 256)
        })
    )
```

```python
# Good — the helper lives at module scope with a name that says what it builds.
# It takes what it needed from the closure (the column list) as an argument, so
# a unit test can call it with a plain list and pin every branch — derived key,
# raw key present, raw key NULL — without building a DataFrame. Spark functions
# are namespaced (F.col / F.when / F.lit), never bare.
def build_key_hash_column(
    column_name: str,
    dataframe_columns: Container[str],
) -> SparkColumn:
    """Hash contribution for one key column: cleansed value, or the null token."""
    casted_column: SparkColumn = F.col(column_name).cast("string")
    cleansed_column: SparkColumn = F.coalesce(casted_column, F.lit(NULL_TOKEN))

    raw_column_name: str = f"{RAW_KEY_PREFIX}{column_name}"
    if raw_column_name not in dataframe_columns:
        # Derived or renamed key: nothing pre-cleanse to compare against.
        return cleansed_column

    raw_key_is_null: SparkColumn = F.col(raw_column_name).isNull()
    key_hash_part: SparkColumn = F.when(
        raw_key_is_null, F.lit(NULL_TOKEN)
    ).otherwise(cleansed_column)

    return key_hash_part


def compute_row_hash_code(
    df: SparkDataFrame,
    hash_columns: Sequence[str],
    column_name: str = ROW_HASH_COLUMN,
) -> SparkDataFrame:
    """sha2 of the key columns. (WHY-rationale docstring elided for the example.)"""
    raw_key_columns: list[str] = [
        column
        for column in hash_columns
        if f"{RAW_KEY_PREFIX}{column}" in df.columns
    ]

    hash_parts: list[SparkColumn] = [
        build_key_hash_column(column, df.columns) for column in hash_columns
    ]

    if raw_key_columns and SOURCE_HASH_COLUMN in df.columns:
        null_flag_columns: list[SparkColumn] = [
            F.col(f"{RAW_KEY_PREFIX}{column}").isNull()
            for column in raw_key_columns
        ]

        source_key_missing: SparkColumn = F.lit(False)
        for null_flag_column in null_flag_columns:
            source_key_missing = source_key_missing | null_flag_column

        source_mix_part: SparkColumn = F.when(
            source_key_missing, F.col(SOURCE_HASH_COLUMN)
        ).otherwise(F.lit(""))
        hash_parts.append(source_mix_part)

    concatenated_hash_parts: SparkColumn = F.concat_ws("|", *hash_parts)
    row_hash_column: SparkColumn = F.sha2(concatenated_hash_parts, SHA2_BIT_LENGTH)
    return df.withColumns({column_name: row_hash_column})
```

**The sanctioned exceptions**, named so they stop being argued case by case:

1. **A decorator that closes over its own arguments** — `def deco(n):` returning
   an inner `wrapper` is the standard shape, and `functools.wraps` wrappers are
   the same case by another name.
2. **A callback whose closure over local state IS the point** — a signal handler
   bound to a per-call timeout, a predicate handed to a shrink loop, a comparator
   built from a locally computed key. The test is whether passing the state
   explicitly would be more code and less clear, not whether it would be
   possible.
3. **A framework that requires the nesting as its registration shape** —
   decorated inner functions collected by an enclosing builder.

Everything else is a module-level function that has not been moved yet.

**Held by: a purpose-built gate** — Ruff has no rule for nested defs. The gate
allows the sanctioned exceptions through an inline pragma that **requires a
non-empty reason** (an empty pragma is itself a violation), plus a ratcheting
per-file exceptions table for pre-existing debt. The host repo's gate script is
the SSOT for the pragma syntax and the tables — this document never restates
them. Function length and extraction judgment stay with review.

### Avoid recursion unless necessary

Iterate by default. Recursion is permitted only when **all three** hold:

1. The data structure is genuinely recursive (trees, ASTs, nested JSON, directory
   walks with no flat alternative).
2. There is a known bound on depth that makes stack overflow impossible.
3. The iterative version would be substantially harder to read.

When recursion is used, add a doc comment explaining (a) why iteration was
rejected, (b) the depth bound, (c) any tail-call assumptions. Python does **not**
guarantee tail-call optimization; the default recursion limit is 1000 — do not
rely on raising it. For tree walks where depth could exceed a few hundred, prefer
an explicit list-based stack. Any hierarchy built from user-influenced input
needs a depth limit or an explicit stack regardless — unbounded recursion over
external data is a denial-of-service vector, not a style question.

## 5. Naming conventions — all names must carry meaning

Names are the primary interface between the writer and the reader. Bad names cost
more than bad logic because they spread silently through the codebase. Ruff's `N`
(pep8-naming) rules catch the casing violations; the rest of this section is on
you, because no linter can tell `cfg` from `config`.

- **Spell it out.** Never invent an abbreviation for a domain concept. A "double
  valid check" is `double_valid_check` — never `_dvc`. Same for variables,
  functions, methods, types, modules, files.
- **Acronyms only when universally understood**: `HTTP`, `URL`, `JSON`, `SQL`,
  `CSV`, `UUID`, `API`, `IO`. Domain acronyms must clear **two** gates to appear
  in identifiers: (1) widely recognized across the broader domain — not just
  internal team jargon, **AND** (2) expanded verbatim in the docstring on first
  use. If both gates are not cleared, spell the concept out —
  `_retry_on_serialization_conflict`, not `_occ_retry_execute`.
- **No casual abbreviations**: `user`, `config`, `temporary`, `index`, `count`,
  `result` / `response`, `request`, `manager`, `service`, `handle` — never `usr`,
  `cfg`, `tmp`, `idx`, `cnt`, `res`, `req`, `mgr`, `svc`, `hndl`.
- **No single-letter names** except loop indices in clearly bounded numerical
  loops (`i`, `j`, `k`) or established math conventions (`x`, `y`).
- **Booleans read like questions**: `is_valid`, `has_expired`, `should_retry` —
  not `valid` / `expired` / `retry`.
- **Verbs for functions, nouns for values, plurals for collections**:
  `compute_rolling_average()`, `average_window`, `average_windows`.

Self-check: write the full name first, then ask — "would a new hire reading this
file in six months know what this means without context?" If no, keep the full
name.

**Held by: review** (with `N` covering casing only). This is the rule that needs
a human, which is exactly why the mechanical rules around it are worth
automating: reviewer attention is finite, and it should be spent where no tool
can go.

## 6. Imports

- **Import order is Ruff's job, not yours.** `ruff check --fix` applies the `I`
  rules and sorts every import block. Set `known-first-party` in
  `[tool.ruff.lint.isort]` so your own package lands in its own group; do not
  hand-sort, and do not commit a manually reordered block that the next `--fix`
  will undo.
- **Avoid expensive top-level imports in hot-parse paths.** If a module is
  imported by anything that gets loaded frequently (a plugin system, a scheduler,
  a CLI entry point that must start fast), move heavy third-party imports inside
  the functions that use them. A heavy import at module scope is a startup or
  parse-time regression that shows up as latency, not as a test failure. Ruff
  flags in-function imports under `PLC0415`; if that rule is selected, suppress it
  at the import with `# noqa: PLC0415` and one line on which entry point it keeps
  fast.
- **Namespace generic-named functions; never import them bare.** `col`, `when`,
  `lit`, `coalesce` can mean a lot of things — imported bare, the call site gives
  no clue where they came from, and they shadow (or get shadowed by) local names.
  Import the module under its conventional alias and qualify every call:
  `from pyspark.sql import functions as F` → `F.col` / `F.when` / `F.lit`. Same
  idea as `numpy as np` / `polars as pl`. Bare imports are fine only for names
  that are unmistakable on sight (`Path`, `datetime`, a project's own
  `TableSpecModel`).
- **No orphaned imports** — everything imported is used. Ruff's `F401` is the
  check; delete the import rather than silencing it. The one sanctioned
  exception is a deliberate re-export surface (a package `__init__`, an
  API-splitting module) covered by a per-file ignore in the repo's config —
  those ignores are load-bearing API, not debt; do not "clean them up."
- Dependency changes (adding, removing, or bumping a package) are deliberate,
  reviewed changes — never a side effect of another task.
- Before writing code against an external library, verify the API is current and
  not deprecated — especially for fast-moving libraries. Use the exact method
  signature; do not guess parameter names or assume default behavior.

**Held by: the linter** (`I`, `F401`), except the namespacing and
hot-parse-path rules, which are review.

## 7. Prefer pre-built components — build custom only with a reason

Check, in this order, before writing a new class or utility:

1. **The standard library or an installed dependency.** It already exists, is
   tested upstream, and gets maintained without you.
2. **Something already in the repo.** Reusing it is nearly always right; a
   near-duplicate with one field changed is nearly always wrong — add the
   parameter instead.
3. **Only then, a custom implementation.**

**Performance is an acceptable justification** for going custom. So are: an auth
flow the existing option does not support, a transactional / idempotency
guarantee it cannot make, and a dependency the project deliberately refuses to
install. "The existing option's argument names annoyed me" is not one.

**Say the reason in the class docstring** — one or two sentences naming what the
pre-built option could not do. A custom implementation with no such note reads as
reinvention, and the next maintainer will delete it in favor of the standard
option — correctly, on the evidence available to them.

If you build one, it is **production-grade Python or it does not ship**: type
hints throughout, real `__init__` validation that fails loudly and early, no bare
`except:`, no swallowed errors, secrets from a managed source and never a
literal, and idempotent execution wherever a caller may retry it. Plus a
happy-path test and at least one error-path test.

**Held by: review.**

## 8. Lazy dataframes

For any lazy-evaluated dataframe engine (Spark, Polars lazy frames, and their
kin), the plan is the product and every materialization is a cost you chose.

- **Keep the pipeline lazy end to end.** Transformations compose into one plan
  the engine optimizes as a whole. An action (`collect`, `count`, `write`,
  `toPandas`) is a materialization boundary — every one is a deliberate
  decision, placed where the result is actually consumed.
- **No convenience actions mid-pipeline.** A `count()` to log progress or a
  `collect()` to branch on a value executes the entire upstream plan; do it
  twice and the plan runs twice. If a mid-pipeline action is genuinely needed,
  say why in a comment at the call site.
- **Prefer engine-native expressions over row-at-a-time Python.** A Python UDF
  forfeits vectorization and the optimizer's visibility into the expression.
  Reach for a UDF only when the expression API cannot say it, and note the
  reason where the UDF is defined.
- **Cache with a measured reason, and release it.** `cache`/`persist` is an
  optimization claim — it belongs where a subtree is consumed by two or more
  actions, paired with an `unpersist`, and justified in one comment line. An
  unexplained cache is a memory leak with good intentions.
- **Schemas are explicit at boundaries.** Declare or validate the schema at
  read; never rely on inference in a production path — inference makes the
  input's worst row the schema author.

**Held by: review.**

## 9. Write for the eventual reader — comments, docstrings, and prose

Write every markdown paragraph, code comment, piece of documentation, PR
description, and GitHub issue for its **eventual reader**, not for the current
agent conversation. Determine the reader's knowledge, purpose, and likely
questions privately; do not add an audience-analysis section to the artifact
itself.

### DO NOT OVER COMMENT. DO NOT OVER COMMENT.
### DO NOT USE 10 LINES OF COMMENT WHEN 2 WILL DO.
### USE ASD-STE100 SIMPLIFIED TECHNICAL ENGLISH.

Three rules, applied to every comment and docstring:

**1. Comment the WHY, never the WHAT.** `# increment the counter` above `i += 1`
is noise — it ages badly and teaches readers to skim past comments, including the
one that mattered. A clear name plus type hints documents the WHAT.

What earns a comment: a race you prevent, an ordering invariant, a cross-cutting
contract ("this hash must match the initial-load output byte-for-byte"), a
deliberate loud failure, defensive code that looks dead but is not, and the
reason you did not do the obvious thing. Code gets rewritten; the reason it must
not be rewritten a particular wrong way is what the next reader needs.

**2. Use the shortest form that carries the reason.** If 2 lines are enough, do
not write 10. Cut the restatement, the preamble, the second example. Keep the
constraint and the failure mode.

**3. Write comments and docstrings in ASD-STE100 Simplified Technical English.**
A controlled language: readers under time pressure — and non-native English
speakers, and future you at 2 a.m. during an incident — parse simple sentences
correctly and complex ones incorrectly.

| Do | Not |
|---|---|
| One idea per sentence. Max ~20 words. | Multi-clause sentences joined by em-dashes and semicolons. |
| Active voice: "the writer commits the batch". | Passive: "the batch is committed". |
| Present tense: "the retry fails". | "the retry would have failed". |
| One word, one meaning. Pick a term and reuse it. | Rotating synonyms — `row` / `record` / `entry` for one thing. |
| Plain verbs: "use", "read", "fail", "retry". | "leverage", "utilize", "surface", "orchestrate". |
| Say the thing. | Hedging, apology, or narration of your own reasoning. |

Bad: *"It should be noted that, given the potential for concurrent writers to
interleave, it was decided to leverage an idempotency guarantee here."*
Good: *"Two writers can interleave here. The write is idempotent, so a retry is
safe."*

**When in doubt:** delete what a competent reader derives from the code; keep what
they cannot; then cut the keeper by half and check it still says the same thing.

**Docstrings**: every function has one, stating what it does, its inputs, and its
outputs. Non-trivial functions use Google-style sections (`Args:` / `Returns:` /
`Raises:` / `Notes:` — `Notes:` is for invariants and contract sensitivities the
caller needs that fit none of the other three).

**Held by: review**, except docstring presence and shape, which are the linter's
where the repo arms Ruff's `D` rules with `convention = "google"` — arming `D`
in a repo that has not selected it is a §13 decision, not a drive-by.

## 10. Testing — mandatory

**Every new behavior ships with a test in the same commit.** A hard block, not a
strong default. Not required for pure refactors that change no behavior — but the
existing suite must still pass. Where the host repo has its own testing
contract, that contract is stricter and wins.

- **Test names are specifications**: `test_retry_backoff_caps_at_max_attempts`,
  not `test_retry_works`.
- **Coverage shape**: at least one happy-path test AND at least one negative /
  edge-case / error-path test per code path. A single happy-path test does not
  satisfy this.
- **Must fail without the change applied** — a test that passes either way is
  exercising the import, not pinning the behavior.
- **Bug fix → the test pinning the now-fixed behavior lands in the same PR.**
  Bug-fix-without-test is exactly what lets the bug come back.
- Numerically sensitive code (financial calculations, scoring, statistical
  transforms — anything where a rounding change is a real regression) requires
  fixture-based regression tests pinned to exact output.
- **No unexplained skips.** `pytest.skip` and `xfail` need a linked issue;
  commented-out tests and "TODO: add test" placeholders are banned outright.
  Environment-gated skips the repo's testing contract names — an
  `importorskip` on an optional heavy dependency that a dedicated CI tier
  supplies — are a sanctioned pattern, not a violation; do not "fix" them.

**Held by: review, plus whatever gate the host repo arms** (same-commit test
presence is mechanically checkable; most repos hold it by review).

## 11. Verification gate — a task is NOT done until

- [ ] Tests for the change exist in the same commit / PR, named for the behavior
      pinned, with happy path + negative case, and failing without the change.
- [ ] Code runs without errors (run it, do not assume).
- [ ] Tests pass — no skips beyond the sanctioned ones in §10, no ignores, no
      `--no-verify`.
- [ ] `ruff format --check .` reports no changes needed.
- [ ] `ruff check .` passes clean — no new `# noqa`, or each new one carries a
      rule code and a same-line reason.
- [ ] Output matches the expected schema or contract.
- [ ] Null / empty / edge cases handled AND tested.
- [ ] No new warnings or errors in logs.
- [ ] No unintended changes outside the target files — in particular, no
      unrelated files reformatted by a bare `ruff format .`.
- [ ] Imports correct and actually used — no orphaned imports.
- [ ] Project navigation / directory docs reflect the change, where the repo
      maintains them.

## 12. Core principles (condensed)

- **Simplicity first** — minimize blast radius; boring, obvious code over clever.
- **No laziness** — root causes, not temporary fixes; production standards.
- **Minimal impact** — touch only what is necessary; if in doubt, do less and ask.
- **Read before write** — always read the current file state before editing.
- **Fail loudly** — never silently guess; loud failure beats silent drift.
- **Names carry meaning** — never abbreviate domain concepts.
- **Iterate, don't recurse** — recursion only when the structure demands it and
  depth is bounded.
- **No placeholders** — never `# rest of code unchanged` / `...`; write complete
  functions.

## 13. Arming a rule: the decision, and the ratchet

A convention becomes a gate when three things are true: it is mechanically
decidable, its violations are cheap to fix, and it will otherwise be violated
faster than review catches it. If a rule fails the first test, it stays a
review duty and the contract says so out loud.

An armed gate meets an existing codebase that already violates it. The pattern
that works is a **ratchet**, not a flag day:

1. **Measure first.** Count the violations per file with the same parser the
   gate will use. An estimate here produces an exceptions table that is wrong
   on day one.
2. **Seed an exceptions table from the measurement**, keyed by path, carrying
   a per-file ceiling and a reason. The reason states what the code is and
   what the fix would be, so that the row reads as debt rather than as
   sanction.
3. **Ceilings ratchet DOWN only.** Raising one needs a stated reason in the
   commit that raises it. That duty is a convention, not a check, because no
   table can tell whether a reason string is real.
4. **Delete a row rather than zeroing it** when its file converts. A table of
   zeroes is a table nobody reads.
5. **Fail closed on every ambiguity.** An unreadable file, a file that will not
   parse, an empty scan set, and an exceptions key pointing at a path that no
   longer exists are all errors. A guard that silently scans nothing is worse
   than no guard, because it reports success.
6. **Dual-wire it.** A gate wired only into the local `make` target does not
   run in CI; a gate wired only into CI does not run before the commit. Wire
   both, and say in each place that the other exists.

**Prove the gate catches things before trusting it.** Introduce each violation
class deliberately, confirm the gate goes red, confirm the pragma path goes
green, confirm an empty pragma reason goes red, and confirm a stale exceptions
key goes red. A gate that has only ever been observed passing is a gate whose
detection has never been tested.

## 14. When these rules meet existing code

The rules apply to code as it is written and to code as it is touched. They do
not license a tree-wide refactor on their own, and a pure-refactor sweep is a
real risk: it changes working code that the test suite was never written to
pin. Lifting a closure changes what it can see. Converting a container adds
validation that was not running. Renaming a function moves every call site.

The invariant for such a sweep is the one worth stating in the charter: **no
call that worked before returns a different value.** If a change cannot meet
that bar, it is a behaviour change wearing a cleanup's clothes, and it belongs
in its own commit with its own justification.
