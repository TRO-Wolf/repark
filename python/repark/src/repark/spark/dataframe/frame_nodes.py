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
        node = _native.frame_root(frame._inner)
    else:
        node = _native.frame_derived(
            frame._inner, entry[1]._frame_node, [other._frame_node for other in entry[2]]
        )
    _NODES[frame] = ("node", node)
    registry = frame._alive_token.setdefault("frame_registry", weakref.WeakValueDictionary())
    registry[node.id] = frame
    return node


def _assign(frame: Any, value: Any) -> None:
    if value is None:
        _NODES[frame] = ("root",)
    elif isinstance(value, tuple):
        _NODES[frame] = ("derived", value[0], value[1])
    else:
        _NODES[frame] = ("node", value)


def _bind_frame_node(frame_class: type) -> None:
    frame_class._frame_node = property(_resolve, _assign)
