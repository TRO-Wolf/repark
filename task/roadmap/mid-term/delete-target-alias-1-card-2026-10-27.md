# DELETE-TARGET-ALIAS-1: an aliased DELETE target fails to resolve its columns (P3 card)

**Filed:** 2026-10-27. **Filed by:** the RP-60 records clerk (`claude-haiku-5-5`), from the Opus 5.5 verifier's round-1 side observation on RP-60 (outside that unit). **Status:** filed, not dispatched. The owner charters it.

**Grade:** not assigned. The first step is a probe; the fix follows the owner's charter.

**Severity:** P3. A DELETE that names its target with an alias fails to resolve the aliased column. The unaliased form answers.

**Measured (2026-10-27, Opus 5.5 verifier, at both the head `f4f753f8` and the base pin `d1304a46`):** `DELETE FROM cat.ns.t AS x WHERE x.d BETWEEN ...` fails with `UNRESOLVED_COLUMN` on `` `x`.`d` `` (SQLSTATE 42703). Spark accepts an aliased DELETE target.

**Why:** not diagnosed. The cause is not measured in round 1.

**Fix as specified:** the DELETE's `WHERE` resolves columns through the target alias, as Spark does, on the door that refuses it.

**Pin:** the aliased and the unaliased forms delete the same rows.

**Out of scope:** the UPDATE and MERGE alias forms, which are not measured in round 1.

**Dispatch:** none. The owner charters it or declines it. Linked from [the RP-60 card](rp-60-posdel-repin-1-card-2026-10-27.md).

Up: [map.md](map.md).
