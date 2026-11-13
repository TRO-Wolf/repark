from __future__ import annotations

import io
import json
import struct
import zlib
from pathlib import Path
from typing import Any

MAGIC = b"Obj\x01"


def _long(stream: io.BytesIO) -> int:
    shift = 0
    accumulator = 0
    while True:
        byte = stream.read(1)[0]
        accumulator |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    return (accumulator >> 1) ^ -(accumulator & 1)


def _bytes(stream: io.BytesIO) -> bytes:
    return stream.read(_long(stream))


def _snappy(data: bytes) -> bytes:
    stream = io.BytesIO(data)
    length = 0
    shift = 0
    while True:
        byte = stream.read(1)[0]
        length |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    out = bytearray()
    while len(out) < length:
        tag = stream.read(1)[0]
        kind = tag & 3
        if kind == 0:
            size = tag >> 2
            if size >= 60:
                extra = size - 59
                size = int.from_bytes(stream.read(extra), "little")
            out += stream.read(size + 1)
            continue
        if kind == 1:
            size = ((tag >> 2) & 7) + 4
            offset = ((tag >> 5) << 8) | stream.read(1)[0]
        elif kind == 2:
            size = (tag >> 2) + 1
            offset = int.from_bytes(stream.read(2), "little")
        else:
            size = (tag >> 2) + 1
            offset = int.from_bytes(stream.read(4), "little")
        for _ in range(size):
            out.append(out[-offset])
    return bytes(out)


def _decompress(codec: str, data: bytes) -> bytes:
    if codec == "null":
        return data
    if codec == "deflate":
        return zlib.decompress(data, -15)
    if codec == "snappy":
        return _snappy(data[:-4])
    raise ValueError(f"unsupported avro codec {codec}")


def _read(stream: io.BytesIO, schema: Any, named: dict[str, Any]) -> Any:
    if isinstance(schema, str):
        if schema in named:
            return _read(stream, named[schema], named)
        return _primitive(stream, schema)
    if isinstance(schema, list):
        return _read(stream, schema[_long(stream)], named)
    kind = schema["type"]
    if kind == "record":
        named[schema["name"]] = schema
        return {field["name"]: _read(stream, field["type"], named) for field in schema["fields"]}
    if kind == "array":
        items: list[Any] = []
        while True:
            count = _long(stream)
            if count == 0:
                return items
            if count < 0:
                count = -count
                _long(stream)
            items.extend(_read(stream, schema["items"], named) for _ in range(count))
    if kind == "map":
        entries: dict[str, Any] = {}
        while True:
            count = _long(stream)
            if count == 0:
                return entries
            if count < 0:
                count = -count
                _long(stream)
            for _ in range(count):
                key = _bytes(stream).decode()
                entries[key] = _read(stream, schema["values"], named)
    if kind == "fixed":
        named[schema["name"]] = schema
        return stream.read(schema["size"]).hex()
    if kind == "enum":
        return schema["symbols"][_long(stream)]
    return _primitive(stream, kind)


def _primitive(stream: io.BytesIO, kind: str) -> Any:
    if kind == "null":
        return None
    if kind == "boolean":
        return stream.read(1) == b"\x01"
    if kind in ("int", "long"):
        return _long(stream)
    if kind == "float":
        return "f32:" + stream.read(4)[::-1].hex()
    if kind == "double":
        return "f64:" + stream.read(8)[::-1].hex()
    if kind == "bytes":
        return _bytes(stream).hex()
    if kind == "string":
        return _bytes(stream).decode()
    raise ValueError(f"unknown avro type {kind}")


def read_avro(path: Path) -> list[Any]:
    stream = io.BytesIO(path.read_bytes())
    if stream.read(4) != MAGIC:
        raise ValueError(f"{path} is not an avro container")
    metadata = _read(stream, {"type": "map", "values": "bytes"}, {})
    schema = json.loads(bytes.fromhex(metadata["avro.schema"]))
    codec = bytes.fromhex(metadata.get("avro.codec", b"null".hex())).decode()
    sync = stream.read(16)
    records: list[Any] = []
    while stream.tell() < len(stream.getbuffer()):
        count = _long(stream)
        block = io.BytesIO(_decompress(codec, _bytes(stream)))
        records.extend(_read(block, schema, {}) for _ in range(count))
        if stream.read(16) != sync:
            raise ValueError(f"{path} sync marker mismatch")
    return records


def bound_bits(hex_value: str | None) -> str | None:
    if hex_value is None:
        return None
    raw = bytes.fromhex(hex_value)
    if len(raw) == 8:
        return "f64:" + raw[::-1].hex()
    if len(raw) == 4:
        return "f32:" + raw[::-1].hex()
    return hex_value


def _pairs(value: Any) -> dict[int, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {int(key): item for key, item in value.items()}
    return {int(item["key"]): item["value"] for item in value}


def current_entries(table_root: Path) -> list[dict[str, Any]]:
    metadata_dir = table_root / "metadata"
    candidates = sorted(metadata_dir.glob("*.metadata.json"), key=lambda p: p.stat().st_mtime_ns)
    metadata = json.loads(candidates[-1].read_text())
    snapshot_id = metadata.get("current-snapshot-id")
    snapshots = {item["snapshot-id"]: item for item in metadata.get("snapshots", [])}
    if snapshot_id is None or snapshot_id < 0 or snapshot_id not in snapshots:
        return []
    out: list[dict[str, Any]] = []
    manifest_list = _local(snapshots[snapshot_id]["manifest-list"], table_root)
    for manifest in read_avro(manifest_list):
        for entry in read_avro(_local(manifest["manifest_path"], table_root)):
            if entry["status"] == 2:
                continue
            data_file = entry["data_file"]
            out.append(
                {
                    "file": Path(data_file["file_path"]).name,
                    "content": data_file.get("content", 0),
                    "record_count": data_file["record_count"],
                    "partition": data_file.get("partition"),
                    "nan_value_counts": _pairs(data_file.get("nan_value_counts")),
                    "lower_bounds": {
                        key: bound_bits(item)
                        for key, item in _pairs(data_file.get("lower_bounds")).items()
                    },
                    "upper_bounds": {
                        key: bound_bits(item)
                        for key, item in _pairs(data_file.get("upper_bounds")).items()
                    },
                }
            )
    return sorted(out, key=lambda item: item["file"])


def _local(location: str, table_root: Path) -> Path:
    text = location.removeprefix("file://").removeprefix("file:")
    path = Path(text)
    if path.exists():
        return path
    marker = "/metadata/"
    return table_root / "metadata" / text.split(marker, 1)[1]


def double_bits(value: float) -> str:
    return struct.pack(">d", value).hex()
