from __future__ import annotations

import re

FENCE_PATTERN = re.compile(r"^ {0,3}(?:```|~~~)")


def _fence_mark(line: str) -> tuple[str, int, bool] | None:
    if FENCE_PATTERN.match(line) is None:
        return None
    stripped = line.lstrip()
    char = stripped[0]
    length = len(stripped) - len(stripped.lstrip(char))
    if char == "`" and "`" in stripped[length:]:
        return None
    return char, length, not stripped[length:].strip()


def _find_run(line: str, size: int, start: int) -> int:
    index = line.find("`" * size, start)
    while index != -1:
        if (index == 0 or line[index - 1] != "`") and (
            index + size >= len(line) or line[index + size] != "`"
        ):
            return index
        index = line.find("`" * size, index + 1)
    return -1


def _blank_spans(line: str) -> str:
    chars = list(line)
    index = 0
    while index < len(chars):
        if chars[index] != "`":
            index += 1
            continue
        size = 1
        while index + size < len(chars) and chars[index + size] == "`":
            size += 1
        closer = _find_run(line, size, index + size)
        if closer == -1:
            index += size
            continue
        for blank in range(index, closer + size):
            chars[blank] = " "
        index = closer + size
    return "".join(chars)


def _masked_lines(text: str) -> list[tuple[str, str]]:
    """Each line paired with a copy whose code spans are blanked, fenced blocks blanked whole."""
    pairs: list[tuple[str, str]] = []
    fence: tuple[str, int] | None = None
    in_comment = False
    for line in text.splitlines(keepends=True):
        mark = None if in_comment else _fence_mark(line)
        if fence is None and mark is not None:
            fence = (mark[0], mark[1])
            pairs.append((line, " " * len(line)))
            continue
        if fence is not None:
            if mark is not None and mark[0] == fence[0] and mark[1] >= fence[1] and mark[2]:
                fence = None
            pairs.append((line, " " * len(line)))
            continue
        cleaned = _blank_spans(line)
        pieces: list[str] = []
        cursor = 0
        while True:
            if in_comment:
                end = cleaned.find("-->", cursor)
                if end == -1:
                    pieces.append(" " * (len(cleaned) - cursor))
                    break
                pieces.append(" " * (end + 3 - cursor))
                in_comment = False
                cursor = end + 3
            else:
                start = cleaned.find("<!--", cursor)
                if start == -1:
                    pieces.append(cleaned[cursor:])
                    break
                pieces.append(cleaned[cursor:start] + "    ")
                in_comment = True
                cursor = start + 4
        pairs.append((line, "".join(pieces)))
    return pairs
