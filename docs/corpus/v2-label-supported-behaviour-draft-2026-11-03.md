# Format-v2 label: supported behaviour, draft (2026-11-03)

CORPUS-1 output 3, second half. A DRAFT for the owner: it states what the v2 label would claim
per finding family, the evidence for each claim, and the exclusions. It claims nothing the cells
do not show; a family whose cells are not built yet says so. Inventory and estimate:
[remaining-work-estimate-2026-11-03.md](remaining-work-estimate-2026-11-03.md).

## Gate

A panic, a wrong read or a wrong write blocks the label whether or not the inventory listed it; cards are for refusal-text, over-refusal and refuse-with-hint classes only.

## Family 1: Hadoop metadata discovery (measured)

Evidence: [hadoop-discovery-cells-2026-11-03.jsonl](hadoop-discovery-cells-2026-11-03.jsonl),
3904 cells; the ported rule ([hadoop-discovery-rule-2026-11-03.md](hadoop-discovery-rule-2026-11-03.md))
predicts Spark's answer on 1910 of 1910 path-read fixtures.

Supported, on the path read (`session.read.format("iceberg").load(<location>)`) of a table whose
metadata files carry a name Java's writer produces (`v<N>.metadata.json`, `v<N>.gz.metadata.json`,
`v<N>.metadata.json.gz`), over the seven writers and the three cleanup states:

| Behaviour | Pass (EQUAL) | Cells |
|---|---|---|
| A current hint reads the newest version | 67 | 67 |
| A hint behind the newest walks forward to it, stopping at the first missing version | 85 | 133 (the other 48 both refuse, text differs: S3) |
| A missing, empty, non-numeric, space-led, CRLF-ended or beyond-`int` hint falls back to the listing's newest version | 382 | 382 |
| A hint ending in a newline reads as the number | 63 | 63 |
| A hint past the newest, or zero, refuses (Spark refuses too) | 0 EQUAL, 126 both refuse | 126 (S3 text only) |

Supported after HADOOP-DISCOVERY-RULE-1 (owner, 2026-11-03; PR #1251): a catalog-style name never takes part in the no-hint listing (D1, 84 of 84 EQUAL); a negative hint refuses with Java's missing-version text (D3, 126 of 126 both refuse; the text is Java's, the exception class is RePark's and is not a parity claim); an unreadable hint, or a directory in place of the hint, lists as Java does (D2, 168 of 252 EQUAL; the other 84 are name-lying fixtures under the D4 exclusion); a hinted version past the newest, or cleaned up, refuses with Java's text (D6, 148 of 148 both refuse). A CRLF-ended hint reads as its number (the dated correction of the rule doc, R2); no measured cell distinguishes the two readings, so the counts above stand.

## Families 2 to 5 (not yet measured by this unit)

| Family | What the label could claim today | Evidence | What the corpus still owes |
|---|---|---|---|
| 2 Names | The AUD-015 heads through `spark.sql`, both catalogs, with and without a same-named temp view | `python/repark/tests/sql/data/ident_dialect_1_cells.jsonl`, 1172 cells (IDENT-DIALECT-1; the 1B round is PR #1245) | about 1130 SQL-only cells: [names-inventory-2026-11-03.md](names-inventory-2026-11-03.md) |
| 3 Nested | No panic on the struct shapes AUD-008 and AUD-016 named; id-less struct reads by position | the fork's nested-projection and struct suites, `map_struct_key_subscript.rs` (29 cells) | about 1200 cells by type, access operation, format and evolution step: [nested-inventory-2026-11-03.md](nested-inventory-2026-11-03.md) |
| 4 Delete files | Parquet position, equality and v3 deletion-vector deletes read back; a dropped equality key reads the survivors | the fork's delete-filter and `eqdel_*` suites; `delete_format_guard.rs` | about 384 Parquet cells: [deletes-inventory-2026-11-03.md](deletes-inventory-2026-11-03.md) |
| 5 Commit outcome | CTAS and RTAS on the Spark door report the committed outcome after a post-publish failure | `crates/repark-spark/tests/post_publish_outcome.rs`, `commit_unknown_doors.rs` (24 cells), `kill_mid_write.rs` (24 cells) | about 144 fault cells across 12 heads and both doors: [commit-inventory-2026-11-03.md](commit-inventory-2026-11-03.md) |

## Exclusions

| Exclusion | Reason it does not invalidate the v2 claim |
|---|---|
| Avro and ORC delete files (AUD-003, AUD-013) | Owner ruling of 2026-11-02: excluded from the v2 label this round, immediate follow-ups. |
| Adopted (owner, 2026-11-03): the `type=hadoop` catalog door on tables written by another process (D8, 84 cells) | RePark's `type=hadoop` is its in-process memory catalog by design (ICE-LOCAL-COMMIT-POINT-1, HADOOP-FACADE-FRESH-SESSION-1); the refusal is loud (`table not found`), never a wrong read, and the path read is the discovery door. |
| Adopted (owner, 2026-11-03): path appends to an existing hadoop table (D7, 1764 cells) | `format("iceberg").save(<path>)` refuses with `TABLE_OR_VIEW_NOT_FOUND` as ICE-WRITE-DF-SAVE-1 states; nothing is written: RePark's metadata directory is unchanged on all 1910 cells, and the Spark judge reads the pre-write state on every fixture but the five checksum controls. |
| Adopted (owner, 2026-11-03): metadata files whose name lies about the payload (D4, 504 cells) | No Java or RePark writer produces a gzip name over plain bytes or a plain name over gzip bytes; RePark reads the payload and answers the version Java selects (ledger lead C-012). |
| Fixed: HADOOP-DISCOVERY-RULE-1 (PR #1251): a catalog-style `NNNNN-<uuid>.metadata.json` inside a hadoop table's `metadata/` with no readable hint (D1, 84 cells; and 21 D3 cells) | Not an exclusion any more. These were wrong reads, which the gate statement above blocked until the unit fixed them, and the owner ruled the fix before the label as one unit with D3, with D2 riding along. No RePark or Spark flow measured so far writes a catalog-style name into a hadoop table's directory (AUD-017). |
| Accepted leniency (owner, 2026-11-03): a version hint written in non-ASCII decimal digits, which Java's `parseInt` accepts and RePark's path read does not (card HADOOP-HINT-UNICODE-DIGITS-1) | No Java or RePark writer produces such a hint. Where the hinted version has a gap after it, the path read can name a different file than Java does, so the leniency is registered here rather than claimed. |
