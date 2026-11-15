import json
import math
import random
import struct
import sys
from decimal import Decimal, getcontext
from fractions import Fraction
from pathlib import Path

from f32 import f32_neighbor, nearest_f32

getcontext().prec = 200
SCALES = list(range(-3, 19))
rng = random.Random(20261114)

DOUBLE_SPECIAL = [
    "0.0",
    "-0.0",
    "NaN",
    "Infinity",
    "-Infinity",
    "5e-324",
    "-5e-324",
    "2.225073858507201e-308",
    "2.2250738585072014e-308",
    "1e-310",
    "-1e-310",
    "1.7976931348623157e+308",
    "-1.7976931348623157e+308",
]
DOUBLE_LARGE = [
    "1e+15",
    "4503599627370495.5",
    "9007199254740993",
    "1e+16",
    "1.2345678901234568e+16",
    "1e+22",
    "1e+23",
    "1e+300",
    "-1e+300",
    "1e+307",
    "-1e+307",
    "1.5e+300",
    "123456789012345.67",
    "-123456789012345.67",
    "2.82879384806159e+17",
]
DOUBLE_ORDINARY = [
    "1.005",
    "0.285",
    "1.255",
    "2.675",
    "0.125",
    "0.375",
    "2.5",
    "-2.5",
    "0.5",
    "-0.5",
    "1.5",
    "0.49999999999999994",
    "-22.129547339983247",
    "1.0000000000000004",
    "0.30000000000000004",
    "1234.5678",
    "9.995",
    "99.95",
    "0.045",
    "1.45",
    "8.325",
    "1.115",
    "-1.005",
    "-1.255",
    "-2.675",
    "0.1",
    "0.7",
    "3.14159",
    "1e-20",
    "123456789.125",
    "0.0005",
    "1e-05",
    "15.0",
    "25.0",
    "125.0",
    "1250.0",
    "12500.0",
    "-125.0",
    "-0.4",
    "-0.0004",
    "2.2300000000000004",
]
DOUBLE_JDK_NOT_SHORTEST = [
    "5.77408214384925e+16",
    "8.21542955796845e+16",
    "8.47014528212905e+16",
    "1.487126013323565e+17",
    "1.506526864249805e+17",
    "1.653852621443395e+17",
    "1.949133209895265e+16",
    "1.958853329155145e+16",
    "2.059742470994105e+16",
    "5.960464477539063e-08",
    "2.842170943040401e-14",
    "4.656612873077393e-10",
]


def dbits(x: float) -> str:
    return struct.pack(">d", x).hex()


def dtext(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    return repr(x)


def half_points(s: int, small: int, digits: int, neg: int) -> list:
    unit = Decimal(10) ** -s
    k = rng.randint(10 ** (digits - 1), 10**digits - 1)
    return [
        (Decimal(small) + Decimal("0.5")) * unit if s < 0 else Decimal(1) + Decimal(5) * unit / 10,
        (Decimal(k) + Decimal("0.5")) * unit,
        -(Decimal(neg) + Decimal("0.5")) * unit,
    ]


doubles = []


def add_double(text: str, cls: str) -> None:
    v = float(text)
    doubles.append({"text": dtext(v), "bits": dbits(v), "class": cls})


for t in DOUBLE_SPECIAL:
    add_double(t, "special")
for t in DOUBLE_LARGE:
    add_double(t, "large")
for t in DOUBLE_ORDINARY:
    add_double(t, "ordinary")
for t in DOUBLE_JDK_NOT_SHORTEST:
    add_double(t, "jdk-not-shortest")
for s in SCALES:
    for j, h in enumerate(half_points(s, 1, 12 if s >= 0 else 6, 2)):
        d0 = float(h)
        for step in (-3, -2, -1, 0, 1, 2, 3):
            v = d0
            for _ in range(abs(step)):
                v = math.nextafter(v, math.inf if step > 0 else -math.inf)
            add_double(repr(v), f"half/s={s}/h{j}/ulp{step:+d}")

FLOAT_SPECIAL = [
    0.0,
    -0.0,
    math.nan,
    math.inf,
    -math.inf,
    struct.unpack(">f", bytes.fromhex("00000001"))[0],
    -struct.unpack(">f", bytes.fromhex("00000001"))[0],
    struct.unpack(">f", bytes.fromhex("007fffff"))[0],
    struct.unpack(">f", bytes.fromhex("00800000"))[0],
    struct.unpack(">f", bytes.fromhex("7f7fffff"))[0],
    -struct.unpack(">f", bytes.fromhex("7f7fffff"))[0],
]
FLOAT_LARGE = [
    "16777216",
    "16777217",
    "1e10",
    "1e15",
    "1e20",
    "1e30",
    "1.5e30",
    "8388607.5",
    "-8388607.5",
]
FLOAT_ORDINARY = [
    "1.005",
    "0.285",
    "1.255",
    "2.675",
    "0.125",
    "2.5",
    "-2.5",
    "0.5",
    "1.5",
    "-22.129547",
    "1234.5678",
    "9.995",
    "99.95",
    "0.045",
    "1.45",
    "0.1",
    "0.7",
    "3.14159",
    "-1.255",
    "1e-5",
    "15",
    "25",
    "125",
    "-0.4",
]
floats = []


def add_float(x: float, cls: str) -> None:
    floats.append(
        {
            "text": dtext(x),
            "bits": struct.pack(">f", x).hex(),
            "class": cls,
        }
    )


for x in FLOAT_SPECIAL:
    add_float(x, "special")
for t in FLOAT_LARGE:
    add_float(nearest_f32(Fraction(Decimal(t))), "large")
for t in FLOAT_ORDINARY:
    add_float(nearest_f32(Fraction(Decimal(t))), "ordinary")
for s in SCALES:
    for j, h in enumerate(half_points(s, 1, 5 if s >= 0 else 4, 2)):
        f0 = nearest_f32(Fraction(h))
        for step in (-3, -2, -1, 0, 1, 2, 3):
            add_float(f32_neighbor(f0, step), f"half/s={s}/h{j}/ulp{step:+d}")

seen = set()
for kind in (doubles, floats):
    uniq = []
    for r in kind:
        if r["bits"] in seen:
            continue
        seen.add(r["bits"])
        uniq.append(r)
    kind[:] = uniq
    seen = set()
for i, r in enumerate(doubles):
    r["id"] = i
for i, r in enumerate(floats):
    r["id"] = i
with Path(sys.argv[1]).open("w") as fh:
    json.dump({"scales": SCALES, "double": doubles, "float": floats}, fh, indent=0)
print(len(doubles), len(floats))
