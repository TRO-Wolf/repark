from __future__ import annotations

import datetime as dt
import decimal
import gzip
import hashlib
import json
import math
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def encode_line(value: Any) -> str:
    """Encode one record as sorted-key ASCII JSON that refuses non-JSON numbers."""
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False)


def cell_digest(cell: dict) -> str:
    """Return the SHA-256 that binds an answer to its exact cell definition."""
    return hashlib.sha256(encode_line(cell).encode()).hexdigest()


def read_lines(path: Path) -> list[str]:
    """Read the non-empty lines of a JSONL file, gzipped when its name ends in .gz."""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            text = stream.read()
    else:
        text = path.read_text(encoding="utf-8")
    return [raw for raw in text.splitlines() if raw.strip()]


def read_records(path: Path) -> dict[str, dict]:
    """Read a JSONL file keyed by id, failing on a duplicate id."""
    rows: dict[str, dict] = {}
    for raw in read_lines(path):
        item = json.loads(raw)
        if item["id"] in rows:
            raise ValueError(f"duplicate ID {item['id']} in {path}")
        rows[item["id"]] = item
    return rows


def write_lines(path: Path, records: Iterable[dict]) -> None:
    """Write records as JSONL through a .partial file, gzipped when the name ends in .gz."""
    partial = path.with_name(path.name + ".partial")
    body = "".join(encode_line(item) + "\n" for item in records)
    if path.suffix == ".gz":
        with gzip.GzipFile(partial, "wb", mtime=0) as stream:
            stream.write(body.encode("utf-8"))
    else:
        partial.write_text(body, encoding="utf-8")
    partial.replace(path)


def canonical(value: Any) -> Any:
    """Encode a Python answer; keep float signs, row fields, exact decimals and bytes."""
    if isinstance(value, float):
        return {"float": "nan" if math.isnan(value) else value.hex()}
    if isinstance(value, decimal.Decimal):
        return {"decimal": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"binary": bytes(value).hex()}
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return {"iso": value.isoformat()}
    if isinstance(value, dt.timedelta):
        return {"duration_us": value // dt.timedelta(microseconds=1)}
    if isinstance(value, dict):
        pairs = [[canonical(key), canonical(item)] for key, item in value.items()]
        return {"map": sorted(pairs, key=encode_line)}
    if hasattr(value, "__fields__"):
        return {"fields": list(value.__fields__), "values": [canonical(item) for item in value]}
    if isinstance(value, (tuple, list)):
        return [canonical(item) for item in value]
    if value is None or isinstance(value, (bool, str, int)):
        return value
    raise TypeError(f"unsupported observation type {type(value).__name__}")
