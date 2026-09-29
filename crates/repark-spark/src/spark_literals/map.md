# map — repark-spark/src/spark_literals

Child of the Spark front-door canonicalizer
([`spark_literals.rs`](../spark_literals.rs)): the literal-value engine the
`canonicalize` regions call. Split out 2026-09-29 when `spark_literals.rs`
reached 999 of its 1,000 lines; the move is byte-identical code with comments
shed per the owner ruling, and this file carries the shed rationale.
pins: string-literal-escape-1/C-000

## Value rules (as moved; the escape-1 fix follows in the next commit)

The tokenizer runs with `unescape=false`, so every `raw` below is the exact
between-quote text: backslash pairs and doubled quotes arrive unprocessed.

- Default mode unescapes per the SQP-1/FNP-4B table: `\n \t \r \b \Z`,
  `\uXXXX` (with surrogate pairs), `\UXXXXXXXX`, octal `\NNN` only for a
  `0/1` lead plus two octal digits, `\%`/`\_` keeping the backslash for LIKE,
  and any other backslash escape dropping the backslash. A doubled `''`
  collapses; a doubled `""` passes through (the PE-10 defect).
- Verbatim mode (`escapedStringLiterals=true`) collapses `''` and keeps
  backslashes.
- Single-quoted raw literals (`r'…'`) keep their content verbatim as one
  value. Double-quoted raw literals (`r"…"`) have no arm and fall through to
  the downstream parser, which refuses them.

Downstream parses under Generic, whose lexer keeps backslashes literally and
collapses only the active quote type's doubling, so `requote_generic` (wrap
in `'…'`, double every `'`) round-trips every value exactly.

## Contents

- `unescape.rs` — `unescape_spark_literal` (default value),
  `unescape_verbatim_literal` (verbatim value), and the escape applicators
  (`apply_escape`, octal, `\u`/`\U`, Java surrogate artifacts,
  `UNREPRESENTABLE`).
