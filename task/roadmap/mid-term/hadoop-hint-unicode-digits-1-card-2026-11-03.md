# Card HADOOP-HINT-UNICODE-DIGITS-1: a version hint written in non-ASCII decimal digits falls to the listing, where Java parses it

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03, from the Opus 5.5 verifier's finding F1 on HADOOP-DISCOVERY-RULE-1 (PR #1251; ledger clause C-012 of [its ledger](../../ledgers/completed/hadoop-discovery-rule-1-ledger.md)).

**Status:** accepted leniency (owner, 2026-11-03). Grade C (clerk). S3. Not dispatched; no build is owed until the owner rules a port. Not a label blocker: the v2 label draft's exclusions table carries it as an accepted leniency.

**Why:** Java's `HadoopTableOperations.findVersion` reads the hint's first line and calls `Integer.parseInt`. On JDK 21, `parseInt` accepts any Unicode decimal digit: a hint written with the Arabic-Indic two, the fullwidth five or the Thai three parses as 2, 5 and 3. RePark parses the hint with an ASCII-only integer parser, so such a hint falls to the listing. Where the hinted version has a gap after it, RePark then reads a different file than Java does. No Java or RePark writer produces such a hint, and the shape was on main before the unit.

**Site:** `crates/repark-core/src/iceberg_path.rs`, `java_int_version`.

**Measured (Opus 5.5 verifier, probes on JDK 21 and the head wheel; not in the cells table):** probe `unicode_digit_hint_gap`, metadata v1, v2, v3 and v5, hint written with the Arabic-Indic two: RePark rows [3, 4] (v5). Java reads v2 and walks to v3: rows [2, 3].

**Fix options:** (a) port the radix-10 digit classes of `Character.digit` over the Basic Multilingual Plane (Java reads UTF-16 units, so supplementary-plane digits do not parse), pinned; (b) keep the leniency registered in the v2 label draft's exclusions, as the owner ruled.

**Retires:** the card when the owner rules (a) and the port is pinned, or rules (b) and the exclusions table carries the row.

**Relation:** [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), whose listing and hint read this shape touches; the [v2 label draft](../../../docs/corpus/v2-label-supported-behaviour-draft-2026-11-03.md), exclusions table.
