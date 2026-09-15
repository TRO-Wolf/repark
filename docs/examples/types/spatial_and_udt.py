"""The spatial types and ``UserDefinedType``: display answers and the SRID refusal.

pins: types-bases-1/C-006
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.GeographyType",
    "types.GeometryType",
    "types.UserDefinedType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Check the measured spatial and UDT surface answers."""
    geography = T.GeographyType(4326)
    expect("GeographyType repr", repr(geography), "GeographyType(4326)")
    expect("GeographyType simpleString", geography.simpleString(), "geography(4326)")
    expect(
        "GeographyType json",
        geography.json(),
        '"geography(OGC:CRS84, SPHERICAL)"',
    )
    geometry = T.GeometryType(0)
    expect("GeometryType repr", repr(geometry), "GeometryType(0)")
    expect("GeometryType simpleString", geometry.simpleString(), "geometry(0)")
    expect("GeometryType json", geometry.json(), '"geometry(SRID:0)"')
    try:
        T.GeographyType(3857)
    except Exception as error:
        if "ST_INVALID_SRID_VALUE" not in str(error):
            raise SystemExit(
                f"GeographyType(3857) message {error} lacks ST_INVALID_SRID_VALUE"
            ) from error
    else:
        raise SystemExit("GeographyType(3857) did not refuse")
    expect("UserDefinedType typeName", T.UserDefinedType.typeName(), "userdefinedtype")
    expect("UserDefinedType simpleString", T.UserDefinedType().simpleString(), "udt")


if __name__ == "__main__":
    main()
