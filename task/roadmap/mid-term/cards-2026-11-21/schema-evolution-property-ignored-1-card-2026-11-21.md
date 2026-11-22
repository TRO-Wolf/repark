# SCHEMA-EVOLUTION-PROPERTY-IGNORED-1 — RePark ignores the table's schema-evolution-disabled property

**Rating: proposed S2 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-spark-2`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. The witness is the replay's probe file `evolution_star.py`. The audit's explicit-SET witness is wrong; see the Finding.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed).** The schema changed against the table's policy; no data lost.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`. The property is `write.spark.auto-schema-evolution.enabled`, set false, true or unset.

- **[M] The audit's witness is wrong.** The probe form `SET t.extra=s.extra` is refused by Spark with UNRESOLVED_COLUMN whether the property is false, true or unset. So the property is not why Spark refuses that form. The enabled and unset cells in that form could not isolate the property.
- **[M] Spark, star form `UPDATE SET * / INSERT *`.** With the property false, Spark commits (1,'new') and adds no column. With the property true or unset, Spark adds `extra`.
- **[M] RePark, star form.** With the property false, true or unset, RePark adds `extra` and writes (1,'new',7). A cold Spark read shows the `extra` column.
- **[M] RePark, probe form.** RePark adds `extra` and writes (1,'new',7) with the property false, true or unset. Spark refuses this form in all three states, so the form is not a witness for the property.

## Why S2

The card is worded as the replay gives it: RePark ignores the table's schema-evolution-disabled property. The property-gated divergence is real only in the star form. The schema changed against the table's policy, and no data was lost.

## Scope

The star form (`UPDATE SET * / INSERT *`) with the property set false. The probe form is not a witness for this card, because Spark refuses it whatever the property.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
