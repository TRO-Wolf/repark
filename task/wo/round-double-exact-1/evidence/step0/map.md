# task/wo/round-double-exact-1/evidence/step0/ — step 0, measurement and inventory

Step 0 of ROUND-DOUBLE-EXACT-1 (2026-11-14), measured on main `954423e0` against Spark 4.1.2 on JDK 17 in
both ANSI modes, before any code and before the inventory critic.

- [domain-table.md](domain-table.md) — 940 values × 22 scales × `round` and `bround` × 4 doors × 5 contexts
  × 2 ANSI modes, with the verdicts per door and context, the targeted halves, the mechanism (unchanged
  against rounding cells), the named cells and the misc cells (NULL and column scales, extreme scales,
  the INT and DECIMAL controls, the card's cells).
- [spark-rule.md](spark-rule.md) — Spark's rule from the `v4.1.2`, Scala 2.13.17 and JDK 17.0.15 sources,
  the measured model check, and what step 1's kernel already yields.
- [inventory.md](inventory.md) — every seat where `round` or `bround` is resolved, planned, folded or
  reached, with file:line, the kernel and "seen: yes / no, because", and how it was searched.
- [probes/](probes/map.md) — the comment-free value generator, runners, model, analysis and renderer.
- [raw/](raw/map.md) — the value set, the JDK 17 scan, the verdict counts and every non-EQUAL SELECT cell.

Up: [../map.md](../map.md).
