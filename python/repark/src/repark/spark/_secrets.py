"""Credential masking for facade refusal text, delegated to ``repark_common::redaction``."""

from __future__ import annotations

import copy
import sys
import traceback
from collections.abc import Callable


def mask_credentials(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _native.mask_value_credentials(value)


def mask_url_userinfo(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _native.mask_url_userinfo(value)


def register_config_value(value: object) -> None:
    if not isinstance(value, str):
        return
    from repark import _native

    _native.register_config_value(value)


def scrub_exception(error: BaseException) -> BaseException:
    from repark import _native

    links = _chain_links(error)
    mask = _native.mask_user_visible
    changed = {id(link) for link in links if _link_text_changes(link, mask)}
    if not changed:
        return error
    parents: dict[int, list[BaseException]] = {}
    for link in links:
        for child in _link_children(link):
            parents.setdefault(id(child), []).append(link)
    frontier = [link for link in links if id(link) in changed]
    while frontier:
        for parent in parents.get(id(frontier.pop()), []):
            if id(parent) not in changed:
                changed.add(id(parent))
                frontier.append(parent)
    copies: dict[int, BaseException] = {}
    for link in links:
        if id(link) in changed and not isinstance(link, BaseExceptionGroup):
            copies[id(link)] = _copy_link(link, mask)
    _copy_groups(links, changed, copies, mask)
    for link in links:
        if id(link) not in changed:
            continue
        fresh = copies[id(link)]
        cause = link.__cause__
        if isinstance(cause, BaseException):
            fresh.__cause__ = copies.get(id(cause), cause)
        context = link.__context__
        if isinstance(context, BaseException):
            fresh.__context__ = copies.get(id(context), context)
        fresh.__traceback__ = link.__traceback__
        fresh.__suppress_context__ = link.__suppress_context__
    return copies[id(error)]


def scrub_user_failure(error: BaseException) -> tuple[str, BaseException]:
    from repark import _native

    return _native.mask_user_visible(traceback.format_exc()), scrub_exception(error)


def _chain_links(error: BaseException) -> list[BaseException]:
    links: list[BaseException] = []
    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending:
        link = pending.pop()
        if id(link) in seen:
            continue
        seen.add(id(link))
        links.append(link)
        pending.extend(_link_children(link))
    return links


def _link_children(link: BaseException) -> list[BaseException]:
    children = [link.__cause__, link.__context__]
    if isinstance(link, BaseExceptionGroup):
        children.extend(link.exceptions)
    return [child for child in children if isinstance(child, BaseException)]


def _link_text_changes(link: BaseException, mask: Callable[[str], str]) -> bool:
    if isinstance(link, OSError):
        fields = (link.strerror, link.filename, link.filename2)
        if any(isinstance(field, str) and mask(field) != field for field in fields):
            return True
    if isinstance(link, BaseExceptionGroup) and mask(link.message) != link.message:
        return True
    notes = _link_notes(link) or []
    if any(isinstance(note, str) and mask(note) != note for note in notes):
        return True
    return any(isinstance(item, str) and mask(item) != item for item in link.args)


def _link_notes(link: BaseException) -> list[object] | None:
    try:
        notes = getattr(link, "__notes__", None)
    except Exception:
        return None
    return list(notes) if isinstance(notes, list | tuple) else None


def _copy_link(link: BaseException, mask: Callable[[str], str]) -> BaseException:
    if isinstance(link, OSError):
        fresh = _rebuild_os_error(link, mask)
    else:
        fresh = _copy_plain_link(link, mask)
    _mask_message_parameters(link, fresh, mask)
    return _mask_notes(link, fresh, mask)


def _copy_groups(
    links: list[BaseException],
    changed: set[int],
    copies: dict[int, BaseException],
    mask: Callable[[str], str],
) -> None:
    pending = [link for link in links if id(link) in changed and id(link) not in copies]
    while pending:
        ready = [
            group
            for group in pending
            if isinstance(group, BaseExceptionGroup)
            and all(id(sub) in copies or id(sub) not in changed for sub in group.exceptions)
        ]
        if not ready:
            for link in pending:
                copies[id(link)] = _mask_notes(link, _masked_stand_in(link, mask), mask)
            return
        for group in ready:
            copies[id(group)] = _copy_group(group, copies, mask)
        pending = [link for link in pending if id(link) not in copies]


def _copy_group(
    group: BaseExceptionGroup[BaseException],
    copies: dict[int, BaseException],
    mask: Callable[[str], str],
) -> BaseException:
    subs = [copies.get(id(sub), sub) for sub in group.exceptions]
    message = mask(group.message)
    try:
        fresh: BaseException = type(group)(message, subs)
    except Exception:
        fresh = BaseExceptionGroup(message, subs)
    return _mask_notes(group, fresh, mask)


def _mask_notes(
    link: BaseException, fresh: BaseException, mask: Callable[[str], str]
) -> BaseException:
    notes = _link_notes(link)
    if notes is None:
        return fresh
    masked = [mask(note) if isinstance(note, str) else note for note in notes]
    try:
        fresh.__notes__ = masked
    except Exception:
        stand_in = _masked_stand_in(link, mask)
        stand_in.__notes__ = masked
        return stand_in
    return fresh


def _mask_message_parameters(
    link: BaseException, fresh: BaseException, mask: Callable[[str], str]
) -> None:
    from repark.errors import _PySparkErrorMixin

    if not isinstance(link, _PySparkErrorMixin) or not isinstance(fresh, _PySparkErrorMixin):
        return
    params = getattr(link, "_message_parameters", None)
    if params is not None:
        fresh._message_parameters = {
            key: mask(value) if isinstance(value, str) else value for key, value in params.items()
        }
    fresh._contexts = list(getattr(link, "_contexts", []))


def _copy_plain_link(link: BaseException, mask: Callable[[str], str]) -> BaseException:
    masked_args = tuple(mask(item) if isinstance(item, str) else item for item in link.args)
    try:
        fresh = copy.copy(link)
    except Exception:
        return _fallback_copy(link, mask, masked_args)
    fresh.args = masked_args
    return fresh


def _fallback_copy(
    link: BaseException, mask: Callable[[str], str], masked_args: tuple[object, ...]
) -> BaseException:
    try:
        fresh = type(link).__new__(type(link), *masked_args)
        fresh.args = masked_args
        if isinstance(link, OSError) and isinstance(fresh, OSError):
            fresh.errno = link.errno
            for field in ("strerror", "filename", "filename2"):
                value = getattr(link, field)
                if isinstance(value, str):
                    setattr(fresh, field, mask(value))
        return fresh
    except Exception:
        return _masked_stand_in(link, mask)


def _masked_stand_in(link: BaseException, mask: Callable[[str], str]) -> BaseException:
    from repark.errors import PySparkException

    try:
        text = str(link)
    except Exception:
        return PySparkException(type(link).__name__)
    return PySparkException(mask(text))


def _rebuild_os_error(error: OSError, mask: Callable[[str], str]) -> BaseException:
    masked_strerror = mask(error.strerror) if isinstance(error.strerror, str) else error.strerror
    masked_filename = mask(error.filename) if isinstance(error.filename, str) else error.filename
    masked_second = mask(error.filename2) if isinstance(error.filename2, str) else error.filename2
    if error.strerror is None and error.filename is None and error.filename2 is None:
        return _copy_plain_link(error, mask)
    winerror = error.winerror if sys.platform == "win32" else None
    try:
        if error.filename is None and error.filename2 is None and winerror is None:
            return type(error)(error.errno, masked_strerror)
        if error.filename2 is None and winerror is None:
            return type(error)(error.errno, masked_strerror, masked_filename)
        return type(error)(error.errno, masked_strerror, masked_filename, winerror, masked_second)
    except Exception:
        return _copy_os_fields_link(error, masked_strerror, masked_filename, masked_second, mask)


def _copy_os_fields_link(
    error: OSError,
    masked_strerror: str | None,
    masked_filename: str | None,
    masked_second: str | None,
    mask: Callable[[str], str],
) -> BaseException:
    masked_args = tuple(mask(item) if isinstance(item, str) else item for item in error.args)
    try:
        fresh = copy.copy(error)
    except Exception:
        return _fallback_copy(error, mask, masked_args)
    fresh.args = masked_args
    if isinstance(error.strerror, str):
        fresh.strerror = masked_strerror
    if isinstance(error.filename, str):
        fresh.filename = masked_filename
    if isinstance(error.filename2, str):
        fresh.filename2 = masked_second
    return fresh


__all__ = [
    "mask_credentials",
    "mask_url_userinfo",
    "register_config_value",
    "scrub_exception",
    "scrub_user_failure",
]
