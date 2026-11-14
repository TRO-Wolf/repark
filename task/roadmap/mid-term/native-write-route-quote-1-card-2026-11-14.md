# Card NATIVE-WRITE-ROUTE-QUOTE-1: the by-name column list in the native write refusal leaves 23 reserved words bare

**Date:** 2026-11-14. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the
NATIVE-DATAFRAME-WRITE-SILENT-1 (PR #1331) delta-2 re-check, finding F7 (`verify-nag-delta2-out/verdict.md`).

**Status:** OPEN, not dispatched. Carded 2026-11-14 by the owner's ruling "Go with defaults" (about 09:50 UTC),
which merges #1331 and cards this as a small follow-up. Severity **S3**: the write fails loudly and changes
nothing; no wrong rows are stored.

## What

The by-name column list in the refusal message leaves 23 reserved words bare: `cluster`, `distribute`, `end`,
`except`, `exclude`, `explain`, `fetch`, `group`, `having`, `intersect`, `into`, `lateral`, `limit`, `minus`,
`offset`, `order`, `returning`, `select`, `sort`, `union`, `values`, `where` and `with`. The named statement then
fails to parse.

## Where

`crates/repark-python/src/session_write_options.rs:290-316` (`TableRoute::ident` and `reads_as_column`). The seat
checks each name with a one-item `SELECT <name>`. The same words are keywords after a comma, inside
`INSERT INTO t (…)` (where `(select` opens a subquery) and after `AS`, so that check passes names the statement
then cannot parse.

The Rust quoting pin (`session_write_options_tests.rs:342-383`) covers only `plain`, `current_date`, `Mixed Case`
and `a"b`.

## Example

A table `(id, "order")` and a frame in the table's own column order, written with `writeTo().append()` or
`saveAsTable` append. The message names `INSERT INTO t (id, order) SELECT id, order FROM <view>`. Followed
literally, that is a parser error (`Expected: end of statement, found: order`), and the table is unchanged. The
Spark door stores the row. The positional `INSERT INTO … SELECT … FROM <view>` of b49403ee handled this column
order, so delta 2 turned a working route into a failing one here.

## Fix direction

Always quote a listed column, or quote any keyword the dialect knows. Pin all 23 words in each position the
message uses: after a comma, inside `INSERT INTO t (…)`, after `AS` in the in-memory replace, and alone.

## Provenance

Found by the #1331 delta-2 re-check (`verify-nag-delta2-out/verdict.md`, F7, class (b), S3). Carded by the owner's
ruling "Go with defaults" (2026-11-14). Linked from
[NATIVE-DATAFRAME-WRITE-SILENT-1](native-dataframe-write-silent-1-card-2026-11-13.md), Delta 2.
