import math
import struct
from fractions import Fraction


def f32_bits(x: float) -> int:
    return struct.unpack(">I", struct.pack(">f", x))[0]


def f32_from_bits(b: int) -> float:
    return struct.unpack(">f", struct.pack(">I", b))[0]


def f32_neighbor(x: float, steps: int) -> float:
    b = f32_bits(x)
    if x < 0 or (x == 0 and math.copysign(1.0, x) < 0):
        return -f32_neighbor(-x, -steps)
    return f32_from_bits(b + steps) if b + steps >= 0 else -f32_from_bits(-(b + steps))


def nearest_f32(q: Fraction) -> float:
    if q == 0:
        return 0.0
    if q < 0:
        return -nearest_f32(-q)
    try:
        approx = struct.unpack(">f", struct.pack(">f", float(q)))[0]
    except OverflowError:
        return math.inf
    if math.isinf(approx):
        top = Fraction(f32_from_bits(0x7F7FFFFF))
        half = (Fraction(2) ** 128 - top) / 2
        return math.inf if q >= top + half else float(top)
    b = f32_bits(approx)
    best = None
    for c in (b - 1, b, b + 1):
        if c < 0 or c > 0x7F7FFFFF:
            continue
        v = Fraction(f32_from_bits(c))
        dist = abs(v - q)
        key = (dist, c & 1)
        if best is None or key < best[0]:
            best = (key, c)
    return f32_from_bits(best[1])
