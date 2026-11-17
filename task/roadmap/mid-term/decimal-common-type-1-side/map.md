# map — task/roadmap/mid-term/decimal-common-type-1-side/

Nine side cards from the step 0 of DECIMAL-COMMON-TYPE-1, measured on main `e50ed17f` against Spark 4.1.2 and recorded in [the domain table](../../../wo/decimal-common-type-1/domain-table.md) and [the sketch](../../../wo/decimal-common-type-1/sketch.md) (§9 and §10). Each card's severity and label status are the owner's ruling "Merge those and defaults." (2026-11-16, ruling B7). The sketch proposed S2 for AVG-DECIMAL-ROUND-1; the ruling sets S1 and label-blocking, and the card carries the ruled value. None is dispatched. Four cards from the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md), filed under the owner's "Defaults." of 2026-11-17, sit beside them.

## Cards

- [AVG-DECIMAL-ROUND-1](avg-decimal-round-1-card-2026-11-16.md) (S1, label-blocking, ruled 2026-11-16; built 2026-11-17, branch `fix/avg-decimal-round-1`, not merged) — **card:** `avg` of a DECIMAL truncates where Spark rounds half up (0.836666 against 0.836667).
- [NVL-DECIMAL-DOUBLE-1](nvl-decimal-double-1-card-2026-11-16.md) (S2, not label-blocking, ruled 2026-11-16) — **card:** `nvl` and `ifnull` over decimals answer a DOUBLE; linked to NVL-TYPE-COERCION-2.
- [LAG-LEAD-COLUMN-DEFAULT-1](lag-lead-column-default-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** `lag` and `lead` with a column default refuse.
- [JOIN-USING-UNALIASED-DERIVED-1](join-using-unaliased-derived-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** `JOIN … USING` over unaliased derived tables refuses.
- [RANGE-DECIMAL-OFFSET-1](range-decimal-offset-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** a `RANGE` frame with a decimal offset refuses.
- [UPDATE-SET-COALESCE-CASE-1](update-set-coalesce-case-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** `UPDATE … SET x = coalesce(CASE …)` refuses.
- [ARRAY-UNION-TYPES-1](array-union-types-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** `array_union` accepts two element types where Spark refuses.
- [DECIMAL-COMMON-TYPE-NATIVE-1](decimal-common-type-native-1-card-2026-11-16.md) (S2, not label-blocking, ruled 2026-11-16) — **card:** the native door's decimal common-type family.
- [LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1](legacy-decimal-truncate-overflow-1-card-2026-11-16.md) (S3, not label-blocking, ruled 2026-11-16) — **card:** the legacy truncate flag's overflow answers NULL in Spark and refuses in RePark.
- [DECIMAL-LITERAL-OVERFLOW-1](decimal-literal-overflow-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, from the build):** a string literal's own cast refuses at planning, where Spark answers NULL with ANSI off.
- [MAP-FILTER-ORDER-1](map-filter-order-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, from the build):** a map built in the select list raises on a row a filter removes; the cause is not diagnosed.
- [FEXPR-DECIMAL-LITERAL-1](fexpr-decimal-literal-1-card-2026-11-17.md) (S1, label-blocking, folded into DOUBLE-DECIMAL-DATAFRAME-1) — **card (2026-11-17, from the build):** `F.expr` types an unsuffixed decimal literal as DOUBLE.
- [DECIMAL-CAST-SELECTIVITY-1](decimal-cast-selectivity-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, from the build):** DataFusion's filter-selectivity assertion on a cast of a BIGINT column with extreme statistics.

Up: [../map.md](../map.md).
