import json
import math
import struct
from decimal import Decimal
from pathlib import Path


def type_name(dt: object) -> str:
    if hasattr(dt, "simpleString"):
        return dt.simpleString()
    return str(dt)


def show_value(v: object) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        bits = struct.pack(">d", v).hex()
        return f"{v!r}|{bits}"
    if isinstance(v, Decimal):
        return f"dec:{v}"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(str(show_value(x)) for x in v) + "]"
    if isinstance(v, dict):
        items = sorted((str(k), str(show_value(x))) for k, x in v.items())
        return "{" + ", ".join(f"{k}: {x}" for k, x in items) + "}"
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
    return {"error": type(e).__name__, "class": cls, "head": head}


def run_frame(frame: object) -> dict:
    fields = frame.schema.fields
    t = type_name(fields[-1].dataType)
    rows = frame.collect()
    vals = sorted((show_value(r[len(fields) - 1]) for r in rows), key=lambda x: (x is None, str(x)))
    return {"type": t, "values": vals}


def write_jsonl(path: str, records: list) -> None:
    with Path(path).open("w") as fh:
        for r in records:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
