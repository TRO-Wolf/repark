# map — repark-functions/src/spark_regex_engine

## Purpose

The shared Java-regex compiler behind every regex name (`rlike` / `regexp_like`,
`regexp_extract` / `regexp_extract_all` / `regexp_substr`, `regexp_replace`,
`regexp_count` / `regexp_instr`, `split`): engine selection plus the unified match
operations both engines serve.

## Files

- [`tests.rs`](tests.rs) — engine-routing verdicts, the octal rewrite, the `${}`
  pre-pass, the invalid-pattern message per function name, both overrun tripwires,
  lookbehind normalization, and per-feature value pins. pins: java-regex-features-1/C-001,
  C-002, C-003, C-004, C-005

## Pointers

- Up: [src map](../map.md)
- Engine: [`spark_regex_engine.rs`](../spark_regex_engine.rs)
- Callers: [`spark_regexp.rs`](../spark_regexp.rs),
  [`spark_regexp_match.rs`](../spark_regexp_match.rs), [`spark_split.rs`](../spark_split.rs)
