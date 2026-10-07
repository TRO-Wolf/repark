"""Credential masking for facade refusal text, delegated to ``repark_common::redaction``."""

from __future__ import annotations

import contextlib
import copy
import sys
import traceback
from collections.abc import Callable


def mask_credentials(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _mask_total(_native.mask_value_credentials, value)


def mask_url_userinfo(value: object) -> object:
    if not isinstance(value, str):
        return value
    from repark import _native

    return _mask_total(_native.mask_url_userinfo, value)


def mask_user_visible(text: str) -> str:
    from repark import _native

    return _mask_total(_native.mask_user_visible, text)


def _mask_total(native: Callable[[str], str], text: str) -> str:
    try:
        return native(text)
    except UnicodeEncodeError:
        replaced = text.encode("utf-8", "surrogatepass").decode("utf-8", "replace")
        masked = native(replaced)
        return text if masked == replaced else masked


def register_config_value(value: object) -> None:
    if not isinstance(value, str):
        return
    from repark import _native

    _native.register_config_value(value)


def scrub_exception(error: BaseException) -> BaseException:
    try:
        return _scrub_chain(error, mask_user_visible)
    except Exception:
        return _total_stand_in(error)


def scrub_user_failure(error: BaseException) -> tuple[str, BaseException]:
    try:
        detail = mask_user_visible(traceback.format_exc())
    except Exception:
        detail = type(error).__name__
    return detail, scrub_exception(error)


def _total_stand_in(error: BaseException) -> BaseException:
    stand_in = _masked_stand_in(error, mask_user_visible)
    with contextlib.suppress(Exception):
        stand_in.__traceback__ = error.__traceback__
    return stand_in


def _scrub_chain(error: BaseException, mask: Callable[[str], str]) -> BaseException:
    links = _chain_links(error)
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
    if type(fresh) is type(link):
        _carry_attributes(link, fresh, mask)
    _mask_message_parameters(link, fresh, mask)
    return _mask_notes(link, fresh, mask)


def _carry_attributes(
    link: BaseException, fresh: BaseException, mask: Callable[[str], str]
) -> None:
    with contextlib.suppress(TypeError):
        attributes = vars(fresh)
        for key, value in vars(link).items():
            attributes[key] = mask(value) if isinstance(value, str) else value
    for name in _slot_names(type(link)):
        try:
            value = getattr(link, name)
        except Exception:
            continue
        with contextlib.suppress(Exception):
            setattr(fresh, name, mask(value) if isinstance(value, str) else value)


def _slot_names(cls: type) -> list[str]:
    names: list[str] = []
    for klass in cls.__mro__:
        slots = klass.__dict__.get("__slots__", ())
        if not isinstance(slots, str | list | tuple | dict):
            continue
        for name in (slots,) if isinstance(slots, str) else slots:
            if name in ("__dict__", "__weakref__"):
                continue
            if name.startswith("__") and not name.endswith("__"):
                name = f"_{klass.__name__.lstrip('_')}{name}"
            names.append(name)
    return names


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
        if isinstance(fresh, Exception) and not isinstance(group, Exception):
            fresh = _masked_stand_in(group, mask)
    if type(fresh) is type(group):
        _carry_attributes(group, fresh, mask)
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
        text = mask(str(link))
    except Exception:
        text = type(link).__name__
    if isinstance(link, Exception):
        return PySparkException(text)
    return _base_only_stand_in(type(link), text)


def _base_only_stand_in(cls: type[BaseException], text: str) -> BaseException:
    for base in cls.__mro__:
        if not issubclass(base, BaseException) or issubclass(base, Exception):
            continue
        if base.__module__ != "builtins":
            continue
        try:
            return base(text)
        except Exception:
            continue
    return BaseException(text)


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
    "mask_user_visible",
    "register_config_value",
    "scrub_exception",
    "scrub_user_failure",
]
