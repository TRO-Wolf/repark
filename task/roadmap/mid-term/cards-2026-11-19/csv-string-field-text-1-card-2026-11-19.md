# Card CSV-STRING-FIELD-TEXT-1: a CSV read parses a `STRING` field's text into a number, where Spark returns the file's text (`007` reads as `7`)

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — CSV schema timestamp zone unit 0 2026-11-19.md`), §3, kept outside the repository; the side gain is recorded in the [unit 0 bar](../../../wo/csv-schema-timestamp-zone-1/bar.md).

**Status:** filed 2026-11-19; not dispatched. Its own unit, not unit 0 and not unit 1. Its first step measures Spark.

**Severity: S1 by class, label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's §3 ruling: "card it S1 by class: silent, on the default CSV read, and every write stores it (leading zeros in codes and identifiers are the common case)"). Label-blocking by the same test that made unit 0 label-blocking. **For the owner's morning read:** this adds a label-blocking card.

## Finding

**[M]** Identical on main and the head, over a row `007,1.50,1e3,TRUE,..`:

| read | answers |
|---|---|
| `schema("a STRING, b STRING, c STRING, d STRING, ..")` | `7`, `1.5`, `1000.0`, `true` |
| `spark.read.csv(path)`, no schema, no options | `7`, `1.5`, `1000.0`, `true` |
| `option("inferSchema", "false")` | `7`, `1.5`, `1000.0`, `true` |

**[R]** Spark returns the file's text for a `STRING` field and for the default read: `007`, `1.50`, `1e3`, `TRUE`. The builder measured `007` live; Fable did not run Spark for the other three.

**Side gain, not this card:** on the head, a `STRING` field beside a `TIMESTAMP` field answers Spark's text (see the [unit 0 bar](../../../wo/csv-schema-timestamp-zone-1/bar.md)). With no `TIMESTAMP` field the read still answers `7`. This card covers the rest.

**Main:** the three reads answer `7`, `1.5`, `1000.0`, `true`.

## First step

Measure the three reads above on Spark 4.1.2, and the reach: integers, decimals, exponents, booleans and dates.

## Likely seat

**[R]** The all-text scan whenever the read does not infer types, which is `reader.py` again. **[M]** The cast of an integer from text costs about 1.04x, so that seat should meet 1.05x on non-timestamp rows. It would also remove unit 0's second scan.

Up: [map.md](map.md).
