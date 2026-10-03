from __future__ import annotations

import weakref
from typing import Any

from repark import _native

_NODES: weakref.WeakKeyDictionary[Any, tuple[Any, ...]] = weakref.WeakKeyDictionary()


def _resolve(frame: Any) -> Any:
    entry = _NODES.get(frame)
    if entry is None:
        raise RuntimeError("internal error: frame has no lineage node")
    if entry[0] == "node":
        return entry[1]
    if entry[0] == "root":
        node = _native.frame_root(entry[1])
    else:
        node = _native.frame_derived(
            entry[1], entry[2]._frame_node, [other._frame_node for other in entry[3]]
        )
    _NODES[frame] = ("node", node)
    registry = frame._alive_token.setdefault("frame_registry", weakref.WeakValueDictionary())
    registry[node.id] = frame
    return node


def _frame_renews(frame: Any) -> bool:
    entry = _NODES.get(frame)
    if entry is None:
        raise RuntimeError("internal error: frame has no lineage node")
    if entry[0] == "node":
        return bool(entry[1].renews)
    if entry[0] == "root":
        return False
    seen: set[int] = set()
    stack: list[Any] = [frame]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        entry = _NODES.get(current)
        if entry is None:
            raise RuntimeError("internal error: frame has no lineage node")
        if entry[0] == "node":
            if entry[1].renews:
                return True
        elif entry[0] == "derived":
            stack.append(entry[2])
            stack.extend(entry[3])
    return False


def _assign(frame: Any, value: Any) -> None:
    if value is None:
        _NODES[frame] = ("root", frame._inner)
    elif isinstance(value, tuple):
        _NODES[frame] = ("derived", frame._inner, value[0], value[1])
    else:
        _NODES[frame] = ("node", value)


def _bind_frame_node(frame_class: type) -> None:
    frame_class._frame_node = property(_resolve, _assign)
