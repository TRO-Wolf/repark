# map — repark-spark/src/spark_literals

Child of the Spark front-door canonicalizer
([`spark_literals.rs`](../spark_literals.rs)): the literal-value engine the
`canonicalize` regions call. Split out 2026-09-29 when `spark_literals.rs`
reached 999 of its 1,000 lines; the move is byte-identical code with comments
shed per the owner ruling, and this file carries the shed rationale.
pins: string-literal-escape-1/C-000

## Value rules (measured against PySpark 4.1.2, `<pyspark-4.1.2-oracle>`)

The tokenizer runs with `unescape=false`, so every `raw` below is the exact
between-quote text: backslash pairs and doubled quotes arrive unprocessed.

- Default mode unescapes per Spark: `\n \t \r \b \Z`, `\uXXXX` (with surrogate
  pairs), `\UXXXXXXXX`, octal `\NNN` only for a `0/1` lead plus two octal
  digits, `\%`/`\_` keeping the backslash for LIKE, and any other backslash
  escape dropping the backslash. Only the literal's own quote type collapses
  when doubled: `''` inside single-quoted, `""` inside double-quoted (PE-10).
  The other quote passes through untouched.
- Verbatim mode (`escapedStringLiterals=true`) keeps the raw text exactly:
  Spark keeps backslashes AND doubled quotes, so no collapse runs at all.
- Raw literals (`r'…'`, `R"…"`): Spark ends the raw token at the first quote,
  so content after a first `''`/`""` is a following quoted literal. The head
  stays verbatim and the tail unescapes with the same quote's rules; in
  verbatim mode the value is the opening quote plus the raw text with the
  first doubling removed.

Downstream parses under Generic, whose lexer keeps backslashes literally and
collapses only the active quote type's doubling, so `requote_generic` (wrap
in `'…'`, double every `'`) round-trips every value exactly.

Known edges (measured, Spark values recorded in the unit hand-back):
`r`/`R` followed by three or more quotes takes sqlparser's triple path,
which Spark's pair-lexing does not share (`r''''` answers empty on Spark and
refuses here); `F.expr` plans on a sessionless context so it never sees the
verbatim flag; `spark.sql.ansi.doubleQuotedIdentifiers` has no carrier.

## Contents

- `unescape.rs` — `unescape_spark_literal` (default value, quote-aware),
  `literal_value` / `raw_value` / `split_raw_head` (mode and raw dispatch),
  and the escape applicators (`apply_escape`, octal, `\u`/`\U`, Java
  surrogate artifacts, `UNREPRESENTABLE`).
