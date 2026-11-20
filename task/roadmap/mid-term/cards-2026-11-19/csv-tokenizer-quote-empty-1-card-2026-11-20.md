# Card CSV-TOKENIZER-QUOTE-EMPTY-1: a doubled quote inside quotes loses its raw form, and quoted empty text under `nullValue` answers NULL where Spark answers the empty string

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §9, kept outside the repository; the cells are in the [domain table §6.4](../../../wo/csv-string-field-text-1/domain-table.md) and the candidate row of the [sketch §8](../../../wo/csv-string-field-text-1/sketch.md).

**Status:** filed 2026-11-20; not dispatched.

**Severity: S2, silent.** Fable §9. **Not label-blocking.** This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

Measured on the default read, header on, `x STRING, k STRING`; main and unit 1 answer alike:

| cell | Spark | main |
|---|---|---|
| `"a""b"` (a doubled quote inside quotes) | `"a""b"`, kept raw | `a"b` |
| `""` (quoted empty) with `nullValue='NA'` | `''` | NULL |

A bare `""` with the default options is NULL on both sides, so it is not a finding.

## Not carded here: `emptyValue`

`""` with `emptyValue='E'` answers `E` on Spark and NULL on main, because the option is dropped silently. Fable §9 rates that S2 as it stands. The wiring unit refuses `emptyValue` at the option gate (any value but `""`), with a pin, and once it is refused the case is S3. This card does not carry it.

## First step

None set by Fable. Fable §9 lists this half with the backslash half as the tokenizer's candidates, so the order of the two cards is for the orchestrator to set.

Up: [map.md](map.md).
