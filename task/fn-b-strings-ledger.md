# Unit ledger — FN-B strings

**Unit:** FN-B · conductor-12 Track T3 A7 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fna` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fna` · **Branch:** `grok/fn-b-strings` ·
**Base (FROZEN):** `a2b385f4113a725a3b013553d2ee99fcf8278cfb` (independent of FN-A).

**Charter:** `FN-MANIFEST.md` FN-B GO-28 + conductor-12 A6/A7.
**SEPMO:** acc + C4. Floor S1. `max_cycles=2`.

## GO / deferred

| Name | Disposition |
|---|---|
| lcase, ucase, char, char_length, character_length | SHIPPED (aliases) |
| substring, substr, left, right | SHIPPED (`substring` on `call_scalar`; left/right SHIM) |
| contains, like, ilike, regexp_like, rlike, regexp | SHIPPED |
| btrim, startswith, endswith | SHIPPED (`starts_with` / `ends_with` / `btrim` arms) |
| printf | SHIPPED as alias of existing `format_string` UOE |
| replace | SHIPPED via escaped `regexp_replace`; pin vs regexp |
| quote | SHIPPED (`concat` + doubled quotes) |
| regexp_extract_all, regexp_substr | **DEFERRED** (charter) |
| split_part, regexp_count, regexp_instr | **DEFERRED** — no `call_scalar` arm; `split` itself is UOE |
| bit_length, octet_length | **DEFERRED** — no byte-length kernel on `call_scalar` |
| to_char, to_varchar | **DEFERRED** — no `call_scalar` arm; `date_format` is not Oracle `to_char` |

`_PRE_SPLIT_ALL` pin move: 207 → 228 (21 shipped). Declared in the PR body.

## ACC

- Risk tier: standard. acc + C4. Floor S1.
- Critic-1 CLEAN. Probed `replace("$")`, `right(s,0)==""`, `quote(NULL) is NULL`.
- S2 display of composed SHIMs (quote/right) ACCEPTED_FLAGGED.
- Critic-2 CLEAN (re.escape is Python-side on the search literal, not row compute).
- C4: 21/21 in `__all__`; deferred absent; no crates/.
- Label: `ACC-CONVERGED`.
