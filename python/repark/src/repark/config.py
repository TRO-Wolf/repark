"""Typed construction mirror for repark.toml files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator, model_validator

DatabaseKind = Literal["postgres", "sqlserver", "trino"]

_DISPLAY_KEYS: tuple[str, ...] = ("style", "max_rows", "max_cols", "str_len")

_BARE_KEY: re.Pattern[str] = re.compile(r"[A-Za-z0-9_-]+\Z")
_DOTTED_KEY: re.Pattern[str] = re.compile(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\Z")


def _toml_text(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    escaped = escaped.replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r")
    return f'"{escaped}"'


def _refuse_header_name(name: str, what: str) -> None:
    for mark in ("]", ".", '"', "'", "\n", "\r"):
        if mark in name:
            raise ValueError(f"{what} {name!r} must not contain {mark!r}")


def _toml_key(key: str) -> str:
    if _BARE_KEY.fullmatch(key) is not None:
        return key
    return _toml_text(key)


def _toml_conf_key(key: str) -> str:
    if _DOTTED_KEY.fullmatch(key) is not None:
        return key
    return _toml_text(key)


def _render_scalar(value: str | int) -> str:
    if isinstance(value, str):
        return _toml_text(value)
    return str(value)


def _refuse_bool(value: object, what: str) -> None:
    if isinstance(value, bool):
        raise ValueError(f"{what} must not be a boolean")


class DisplayConfig(BaseModel):
    """One [<profile>.display] table (the DISPLAY-POLARS-1 keys)."""

    model_config = ConfigDict(extra="forbid")

    style: str | int | None = Field(default=None)
    max_rows: str | int | None = Field(default=None)
    max_cols: str | int | None = Field(default=None)
    str_len: str | int | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def check_display(cls, raw: object) -> object:
        """Refuse boolean display values before union coercion hides them."""
        if isinstance(raw, dict):
            for key in _DISPLAY_KEYS:
                if key in raw:
                    _refuse_bool(raw[key], f"display key {key!r}")
        return raw

    def render_lines(self, header: str) -> list[str]:
        """Render this table under one TOML header."""
        lines: list[str] = [header]
        for key in _DISPLAY_KEYS:
            value: str | int | None = getattr(self, key)
            if value is None:
                continue
            lines.append(f"{key} = {_render_scalar(value)}")
        return lines


class SessionConfig(BaseModel):
    """One [<profile>.session] table (the three builder knobs)."""

    model_config = ConfigDict(extra="forbid")

    memory_limit_gb: int | str | None = Field(default=None)
    batch_size: int | str | None = Field(default=None)
    target_partitions: int | str | None = Field(default=None)

    @field_validator("memory_limit_gb", "batch_size", "target_partitions", mode="before")
    @classmethod
    def check_knob(cls, value: object) -> object:
        """Keep non-negative integers and digit strings, refuse the rest."""
        if value is None:
            return None
        _refuse_bool(value, "session knob")
        if isinstance(value, int):
            if value < 0:
                raise ValueError("session knob must be a non-negative integer")
            return value
        if value.strip().isdigit():
            return value
        raise ValueError("session knob must be a non-negative integer")

    def render_lines(self, header: str) -> list[str]:
        """Render this table under one TOML header."""
        lines: list[str] = [header]
        entries: tuple[tuple[str, int | str | None], ...] = (
            ("memory_limit_gb", self.memory_limit_gb),
            ("batch_size", self.batch_size),
            ("target_partitions", self.target_partitions),
        )
        for key, value in entries:
            if value is None:
                continue
            lines.append(f"{key} = {_render_scalar(value)}")
        return lines


class CatalogBlock(BaseModel):
    """One [<profile>.catalog.<name>] block (type plus string properties)."""

    model_config = ConfigDict(extra="allow")

    type: StrictStr | None = Field(default=None)

    @model_validator(mode="after")
    def check_block(self) -> Self:
        """Refuse empty blocks and non-string properties."""
        extras: dict[str, object] = dict(self.model_extra or {})
        if self.type is not None:
            extras["type"] = self.type
        if not extras:
            raise ValueError("catalog block carries no properties")
        for prop, value in extras.items():
            if not isinstance(value, str):
                raise ValueError(f"catalog property {prop!r} must be a string")
        return self

    def render_lines(self, header: str) -> list[str]:
        """Render this block under one TOML header."""
        lines: list[str] = [header]
        if self.type is not None:
            lines.append(f"type = {_toml_text(self.type)}")
        extras: dict[str, object] = dict(self.model_extra or {})
        for prop in sorted(extras):
            value = extras[prop]
            assert isinstance(value, str)
            lines.append(f"{_toml_key(prop)} = {_toml_text(value)}")
        return lines


class DatabaseSource(BaseModel):
    """One [<profile>.database.<kind>.<name>] block (string properties)."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def check_props(self) -> Self:
        """Refuse non-string connection properties."""
        for prop, value in dict(self.model_extra or {}).items():
            if not isinstance(value, str):
                raise ValueError(f"connection property {prop!r} must be a string")
        return self

    def render_lines(self, header: str) -> list[str]:
        """Render this block under one TOML header."""
        lines: list[str] = [header]
        for prop in sorted(self.model_extra or {}):
            value = (self.model_extra or {})[prop]
            assert isinstance(value, str)
            lines.append(f"{_toml_key(prop)} = {_toml_text(value)}")
        return lines


class ProfileConfig(BaseModel):
    """One [profile] section (display, session, conf, catalog, database)."""

    model_config = ConfigDict(extra="forbid")

    display: DisplayConfig | None = Field(default=None)
    session: SessionConfig | None = Field(default=None)
    conf: dict[str, str | int] = Field(default_factory=dict)
    catalog: dict[str, CatalogBlock] = Field(default_factory=dict)
    database: dict[DatabaseKind, dict[str, DatabaseSource]] = Field(default_factory=dict)

    @field_validator("conf", mode="before")
    @classmethod
    def check_conf(cls, value: object) -> object:
        """Refuse boolean conf values (the loader only takes strings and integers)."""
        if isinstance(value, dict):
            for key, entry in value.items():
                _refuse_bool(entry, f"conf key {key!r}")
        return value

    @model_validator(mode="after")
    def check_names(self) -> Self:
        """Refuse header-breaking catalog and source names plus name collisions."""
        for name in self.catalog:
            _refuse_header_name(name, "catalog name")
        for names in self.database.values():
            for name in names:
                _refuse_header_name(name, "database source name")
        source_names: set[str] = set()
        for names in self.database.values():
            source_names.update(names)
        for name in self.catalog:
            if name in source_names:
                raise ValueError(f"name {name!r} is both a catalog and a database source")
        return self

    def render_lines(self, name: str) -> list[str]:
        """Render this profile section with fully qualified TOML headers."""
        lines: list[str] = []
        root = _toml_text(name)
        if self.display is not None:
            lines.extend(self.display.render_lines(f"[{root}.{_toml_text('display')}]"))
        if self.session is not None:
            lines.extend(self.session.render_lines(f"[{root}.{_toml_text('session')}]"))
        if self.conf:
            lines.append(f"[{root}.{_toml_text('conf')}]")
            for key in self.conf:
                lines.append(f"{_toml_conf_key(key)} = {_render_scalar(self.conf[key])}")
        for catalog_name in sorted(self.catalog):
            header = f"[{root}.{_toml_text('catalog')}.{_toml_text(catalog_name)}]"
            lines.extend(self.catalog[catalog_name].render_lines(header))
        for kind in sorted(self.database):
            for source_name in sorted(self.database[kind]):
                header = (
                    f"[{root}.{_toml_text('database')}.{_toml_text(kind)}."
                    f"{_toml_text(source_name)}]"
                )
                lines.extend(self.database[kind][source_name].render_lines(header))
        return lines


class ReparkConfig(BaseModel):
    """A whole repark.toml file (profile name to profile section)."""

    model_config = ConfigDict(extra="forbid")

    profiles: dict[str, ProfileConfig] = Field(default_factory=dict)

    @field_validator("profiles", mode="before")
    @classmethod
    def check_profile_names(cls, value: object) -> object:
        """Refuse header-breaking profile names before any header renders them."""
        if isinstance(value, dict):
            for name in value:
                _refuse_header_name(name, "profile name")
        return value

    def to_toml(self) -> str:
        """Render the file text a user could write as repark.toml."""
        lines: list[str] = []
        for name in self.profiles:
            lines.extend(ProfileConfig.render_lines(self.profiles[name], name))
        if not lines:
            return ""
        return "\n".join(lines) + "\n"

    def save(self, path: str | Path) -> Path:
        """Write the rendered file to path and return the path."""
        target = Path(path)
        target.write_text(self.to_toml(), encoding="utf-8")
        return target
