import json
import math
import struct
from decimal import Decimal
from pathlib import Path


def show_value(v: object) -> str | None:
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        return f"{v!r}|{struct.pack('>d', v).hex()}"
    if isinstance(v, Decimal):
        return f"dec:{v}"
    return repr(v)


def error_record(e: Exception) -> dict:
    cls = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(e, attr, None)
        if fn is not None:
            try:
                cls = fn()
            except Exception:
                cls = None
            if cls:
                break
    head = str(e).strip().splitlines()[0][:240] if str(e).strip() else ""
    java = getattr(e, "java_exception", None)
    if java is not None:
        head = str(java.toString()).strip().splitlines()[0][:240]
    return {"error": type(e).__name__, "class": cls, "head": head}


def rows_by_id(frame: object) -> dict:
    out = {}
    for r in frame.collect():
        out[str(r[0])] = show_value(r[1])
    return out


def write_jsonl(path: str, records: list) -> None:
    with Path(path).open("w") as fh:
        for r in records:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
