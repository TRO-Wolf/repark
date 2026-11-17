# Card STRUCT-COLLECT-ROW-1: `collect()` returns a struct as a `dict`, where PySpark returns a `Row`

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 5 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 4 ("seen in passing"; measured on main before #1341), and the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md).

**Status:** filed 2026-11-17, not dispatched; S2, not label-blocking (owner, 2026-11-17).

**Severity: S2, not label-blocking (owner, 2026-11-17)** (afternoon ruling 11).

## Finding

A struct value collected by `collect()` comes back as a Python `dict`. PySpark returns a `Row`. Fable saw it in passing and did not check it against the cards.

## Cells

The build's bar records six cells where the struct renders differently: `literal/u_struct` on `t` and `tx`, and `rev/pin/struct_pos`, in both modes. The bar counts them as rendering only: "PySpark returns a Row, RePark a dict; the types and values are equal" with the rendering normalised. The cell is therefore not a wrong answer in the bar's comparison, and the card records that reading rather than settling it.

## Nearest cards

- [MAP-STRUCT-KEY-COLLECT-1](../map-struct-key-collect-1-card-2026-11-05.md) covers a struct used as a map key, where `SELECT *` raises `TypeError: unhashable type: 'dict'`. It is about map keys, not a top-level struct.
- The parity registry's map residues (in [the parity doc](../../../../docs/spark-sql-iceberg-parity.md)) name "a struct inside a map value collects as a `dict` where PySpark answers a `Row`". That is a struct nested in a map value, not a top-level struct.

Neither covers a top-level struct `collect()`, so this card is filed.

Up: [map.md](map.md).

## Append (2026-11-17, afternoon): rated S2, not label-blocking

The owner's afternoon ruling of 2026-11-17 (about 14:05 UTC) rated this card, ruling 11: "STRUCT-COLLECT-ROW-1: S2, not label-blocking." The rating is in the severity and Status lines above, which replace "not rated". The morning's ruling 5 is not edited. Sketch §6 does not cite this card; the rating rests on the card's own six cells (`literal/u_struct` on `t` and `tx`, and `rev/pin/struct_pos`, in both modes), as the Cells section gives them. The [rulings record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md) holds the afternoon section.

Up: [map.md](map.md).
