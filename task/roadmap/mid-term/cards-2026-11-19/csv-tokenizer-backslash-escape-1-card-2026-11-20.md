# Card CSV-TOKENIZER-BACKSLASH-ESCAPE-1: a default CSV read shifts the fields of a quoted field that holds `\"`, where Spark keeps the quote

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §9, kept outside the repository. The same shape is in the [domain table §6.4](../../../wo/csv-string-field-text-1/domain-table.md) and the [sketch §8](../../../wo/csv-string-field-text-1/sketch.md).

**Status:** filed 2026-11-20; not dispatched. Its own step 0 comes first. It does not touch tonight's build.

**Severity: S1 by class.** Fable §9: "S1 by class, not S2". It is silent, on the default read, and stored. Spark's writer escapes a quote as `\"` by default, so this is the shape of every Spark-written CSV that holds a quote.

**Label-blocking: yes**, on Fable's lean, acted on under the owner's delegation (about 00:22 UTC on 2026-11-20). **For the owner to confirm.** This is Fable's guidance, not an owner ruling.

## Finding

Measured by Fable on the default read (Spark 4.1.2 live, and main):

| line | Spark | main |
|---|---|---|
| `"a\"b,c",k` | (`a"b,c`, `k`) | (`a\b`, `c"`): the fields shift |
| `"a\"b",k` | (`a"b`, `k`) | (`a\b"`, `k`) |
| `"a\\b",k` | (`a\b`, `k`) | (`a\\b`, `k`) |

## Why setting `escape` is not the fix

With `escape` set to `\`, RePark matches those three lines. It answers `anb` for `"a\nb"` (Spark `a\nb`), and it still differs on both `""` lines, which belong to CSV-TOKENIZER-QUOTE-EMPTY-1. The card needs its own step 0 because of this (Fable §9).

## First step

Its own step 0, before any seat is chosen (Fable §9). No seat is set here.

Up: [map.md](map.md).
