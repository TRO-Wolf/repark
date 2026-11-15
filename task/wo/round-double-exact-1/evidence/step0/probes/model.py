import math
import struct
from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal, getcontext
from fractions import Fraction

from f32 import nearest_f32

getcontext().prec = 3000
getcontext().Emax = 999999
getcontext().Emin = -999999


def show_float(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    return f"{x!r}|{struct.pack('>d', x).hex()}"


def spark_round(jdk_text: str, scale: int, fn: str, src: str) -> str:
    if jdk_text in ("NaN", "Infinity", "-Infinity"):
        return show_float(float(jdk_text))
    mode = ROUND_HALF_UP if fn == "round" else ROUND_HALF_EVEN
    q = Decimal(jdk_text).quantize(Decimal(1).scaleb(-scale), rounding=mode)
    if q == 0:
        return show_float(0.0)
    if src == "FLOAT":
        return show_float(nearest_f32(Fraction(q)))
    return show_float(float(q))
