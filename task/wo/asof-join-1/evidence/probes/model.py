from typing import Any

import asof

KEYS = {"none": (), "k1": (1,), "k1k2": (1, 2)}


def holds(lp: int, rp: int, tol: int | None, exact: bool, direction: str) -> bool:
    if direction == "backward":
        if not (lp >= rp if exact else lp > rp):
            return False
        return tol is None or (rp >= lp - tol if exact else rp > lp - tol)
    if direction == "forward":
        if not (lp <= rp if exact else lp < rp):
            return False
        return tol is None or (rp <= lp + tol if exact else rp < lp + tol)
    if not exact and lp == rp:
        return False
    if tol is None:
        return True
    return abs(lp - rp) <= tol if exact else abs(lp - rp) < tol


def distance(lp: int, rp: int, direction: str) -> int:
    if direction == "backward":
        return lp - rp
    if direction == "forward":
        return rp - lp
    return abs(lp - rp)


def ties(key: dict[str, Any], lrows: Any = None, rrows: Any = None) -> dict[str, list[str] | None]:
    lrows = asof.LEFT if lrows is None else lrows
    rrows = asof.RIGHT if rrows is None else rrows
    cols = KEYS[key["on"]]
    out: dict[str, list[str] | None] = {}
    for lrow in lrows:
        lp = lrow[3]
        best: list[tuple[int, str]] = []
        for rrow in rrows:
            rp = rrow[3]
            if lp is None or rp is None:
                continue
            if any(lrow[c] is None or rrow[c] is None or lrow[c] != rrow[c] for c in cols):
                continue
            if not holds(lp, rp, key["tol"], key["exact"], key["dir"]):
                continue
            best.append((distance(lp, rp, key["dir"]), str(rrow[0])))
        if best:
            low = min(d for d, _ in best)
            out[str(lrow[0])] = sorted(r for d, r in best if d == low)
        elif key["how"] == "left":
            out[str(lrow[0])] = None
    return out
