# Card HADOOP-HINT-NEGATIVE-1: a negative version hint on a hadoop table falls back to the listing where Java refuses

**Date:** 2026-10-29. **Filed by:** the clerk of the ICE-HADOOP-GZIP-METADATA-1 verification round 1 (`claude-haiku-5-5`), from the Opus 5.5 verifier's finding S3-4 on PR #1218 (verdict on `94ad2bd8`). The shape is older than that PR and is not a regression of it.

**Status:** open. Grade C (clerk). Not dispatched; no build is owed until the owner charters it.

**Why:** On a `type=hadoop` table, a readable `version-hint.text` that holds a negative integer, such as `-1`, makes the path read fall through to the newest metadata file in the listing and answer its rows. Java's `HadoopTableOperations` reads the hint with `Integer.parseInt`, which accepts `-1`, and the read then refuses with its missing-version error; Spark errors on load. So RePark answers where Java refuses. The verifier reads this as a leniency, not a wrong value. The parity registry's DF-LOAD sentence places a negative hint "like" Java's `findVersion`; that comparison does not hold for this value, and the registry carries a dated correction.

**Site:** `crates/repark-core/src/iceberg_path.rs`, `java_int_version` (about lines 186 to 190). The hint text is parsed as `i32`, and `u64::try_from` drops a negative value, so the function returns no hinted version and the read falls back to the listing (the verifier's measurement).

**Fix shape:** The charter rules first, between two shapes. (a) Match Java: a readable negative hint refuses with the existing hinted-missing text, pinned beside the existing hint pins, and the registry's corrected sentence stays. (b) Keep the leniency: register it in the registry as an accepted divergence with a pin, and remove the "like Java" wording for a negative hint. Before ruling, the charter checks the other fall-through shapes (non-integer, non-UTF-8, out-of-range) against Java's `parseInt`, since only the negative value has been measured.

**Measured (copied from the verifier's record of the audit's probe `rp_table_hint_negative`; the clerk did not re-measure):** hint `-1` over a RePark-written table. RePark answers `[[3]]`; Spark's `load` errors with `Py4JJavaError`.

**Retires:** the card when the charter rules and the negative-hint shape matches the ruling on the probe (a refusal with a pin, or a registered leniency with a pin), and the registry no longer places a negative hint "like" Java.

**Relation:** [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md), whose verification round 1 found the shape and whose registry carries the dated correction; [ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md), which set the hint fall-through rules this card tests; the second audit's AUD-001 fix paragraph, which states that negative hints fall back to the listing like Java ([the report](../../../docs/audits/iceberg-v2-label-independent-reaudit-2026-10-29.md)).
