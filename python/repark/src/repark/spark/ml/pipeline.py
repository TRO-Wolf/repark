"""Spark-shaped pipeline composition and staged ML persistence publication."""

from __future__ import annotations

import contextlib
import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

from repark.errors import IllegalArgumentException, PySparkTypeError, UnsupportedOperationException
from repark.spark.ml.base import Estimator, Model, Transformer, _require_repark_dataframe
from repark.spark.ml.param import Param, Params, TypeConverters
from repark.spark.ml.util import MLReadable, MLReader, MLWritable, MLWriter

REPARK_ML_FORMAT = "repark-ml"
REPARK_ML_VERSION = 1

# Persistence loads only allowlisted facade stages, never delegated extensions.
_ML_STAGE_MODULE_PREFIX = "repark.spark.ml."
_ML_STAGE_MODULE_DENY = ("repark.spark.ml.ext",)
_SAFE_STAGE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _pyspark_version_provenance() -> str:
    """Return the installed PySpark version.

    Return ``unknown`` when the version is missing and ``unavailable`` when import fails.
    """
    try:
        import pyspark

        return str(getattr(pyspark, "__version__", "unknown"))
    except ImportError:
        return "unavailable"


class Pipeline(Estimator["PipelineModel"], MLReadable, MLWritable):
    """Estimator that chains stages."""

    def __init__(self, *, stages: list[Any] | None = None) -> None:
        """Initialize an optional stage list."""
        super().__init__()
        self.stages: Param[list[Any]] = Param(
            self,
            "stages",
            "a list of pipeline stages",
            TypeConverters.identity,
        )
        self._setDefault(stages=[])
        if stages is not None:
            self.setStages(stages)

    def setStages(self, value: list[Any]) -> Pipeline:
        """Validate and set pipeline stages."""
        if not isinstance(value, list):
            raise PySparkTypeError(f"stages must be a list, got {type(value).__name__}")
        for index, stage in enumerate(value):
            if not isinstance(stage, (Estimator, Transformer)):
                raise PySparkTypeError(
                    f"stages[{index}] must be Estimator or Transformer, got {type(stage).__name__}"
                )
        return self._set(stages=list(value))

    def getStages(self) -> list[Any]:
        """Return a copy of the stage list."""
        return list(self.getOrDefault(self.stages))

    def _fit(self, dataset: Any) -> PipelineModel:
        """Fit and apply stages from left to right."""
        frame = _require_repark_dataframe(dataset, verb="Pipeline.fit")
        stages = self.getStages()
        transformers: list[Transformer] = []
        current = frame
        for stage in stages:
            if isinstance(stage, Estimator):
                model = stage.fit(current)
                transformers.append(model)
                current = model.transform(current)
            elif isinstance(stage, Transformer):
                transformers.append(stage)
                current = stage.transform(current)
            else:
                raise PySparkTypeError(
                    f"Pipeline stage must be Estimator or Transformer, got {type(stage).__name__}"
                )
        model = PipelineModel(stages=transformers)
        model._source_pipeline_uid = self.uid  # type: ignore[attr-defined]
        return model

    def copy(self, extra: dict[Param[Any], Any] | None = None) -> Pipeline:
        """Copy the pipeline and its stages."""
        that = super().copy(extra)
        that.setStages([stage.copy() for stage in self.getStages()])
        return that

    def write(self) -> MLWriter:
        """Return a writer for this unfitted pipeline."""
        return _PipelineWriter(self)

    @classmethod
    def read(cls) -> MLReader:
        """Return a reader for an unfitted pipeline."""
        return _PipelineReader()


class PipelineModel(Model, MLReadable, MLWritable):
    """Fitted pipeline containing ordered transformers."""

    def __init__(self, stages: list[Transformer] | None = None) -> None:
        """Initialize fitted or passthrough stages."""
        super().__init__()
        self.stages = list(stages or [])
        self._source_pipeline_uid: str | None = None

    def _transform(self, dataset: Any) -> Any:
        """Apply stages from left to right."""
        frame = _require_repark_dataframe(dataset, verb="PipelineModel.transform")
        current = frame
        for stage in self.stages:
            current = stage.transform(current)
        return current

    def copy(self, extra: dict[Param[Any], Any] | None = None) -> PipelineModel:
        """Copy the model and its stages."""
        that = PipelineModel(stages=[stage.copy() for stage in self.stages])
        that.uid = self.uid
        that._source_pipeline_uid = self._source_pipeline_uid
        if extra:
            for param, value in extra.items():
                name = param.name if isinstance(param, Param) else str(param)
                if hasattr(that, "_set"):
                    that._set(**{name: value})
        return that

    def write(self) -> MLWriter:
        """Return a persistence writer."""
        return _PipelineModelWriter(self)

    @classmethod
    def read(cls) -> MLReader:
        """Return a persistence reader."""
        return _PipelineModelReader()


class _PipelineWriter(MLWriter):
    """Write unfitted pipeline metadata."""

    def saveImpl(self, path: str) -> None:
        """Write stage metadata and publish the staged directory."""
        target = Path(path)
        staging = _begin_atomic_save(target, overwrite=self.should_overwrite)
        try:
            pipeline: Pipeline = self.instance
            stages_meta = []
            for index, stage in enumerate(pipeline.getStages()):
                stages_meta.append(
                    {
                        "index": index,
                        "uid": getattr(stage, "uid", f"stage_{index}"),
                        "class": _stage_class_path(stage),
                        "params": _params_to_jsonable(stage),
                        "fitted": False,
                    }
                )
            metadata = {
                "format": REPARK_ML_FORMAT,
                "version": REPARK_ML_VERSION,
                "kind": "Pipeline",
                "uid": pipeline.uid,
                "pyspark_version": _pyspark_version_provenance(),
                "stages": stages_meta,
            }
            (staging / "metadata.json").write_text(
                json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
            )
            _commit_atomic_save(staging, target, overwrite=self.should_overwrite)
        except Exception:
            _abort_atomic_save(staging)
            raise


class _PipelineReader(MLReader):
    """Read unfitted pipeline metadata."""

    def load(self, path: str) -> Pipeline:
        """Load metadata and rebuild registered stages."""
        target = Path(path)
        metadata = _read_metadata(target)
        if metadata.get("kind") != "Pipeline":
            raise IllegalArgumentException(f"expected kind Pipeline, got {metadata.get('kind')!r}")
        stages = [_rebuild_stage(entry, fitted=False) for entry in metadata.get("stages", [])]
        pipeline = Pipeline(stages=stages)
        pipeline.uid = metadata.get("uid", pipeline.uid)
        return pipeline


class _PipelineModelWriter(MLWriter):
    """Write fitted pipeline models."""

    def saveImpl(self, path: str) -> None:
        """Write metadata and fitted parameters, then publish the staged tree.

        Fitted state is validated before staging so unsupported stages leave no tree.
        """
        model: PipelineModel = self.instance
        fitted_payloads = [_fitted_state(stage) for stage in model.stages]
        target = Path(path)
        staging = _begin_atomic_save(target, overwrite=self.should_overwrite)
        try:
            stages_meta: list[dict[str, Any]] = []
            for index, (stage, fitted_payload) in enumerate(
                zip(model.stages, fitted_payloads, strict=True)
            ):
                raw_uid = getattr(stage, "uid", f"stage_{index}")
                uid = _sanitize_stage_uid(raw_uid, index=index)
                stage_dir = _safe_stage_dir(staging, f"stages/{index}_{uid}")
                stage_dir.mkdir(parents=True, exist_ok=True)
                stage_meta = {
                    "index": index,
                    "uid": uid,
                    "class": _stage_class_path(stage),
                    "params": _params_to_jsonable(stage),
                    "fitted": True,
                    "fitted_keys": sorted(fitted_payload.keys()),
                }
                (stage_dir / "metadata.json").write_text(
                    json.dumps(stage_meta, indent=2) + "\n", encoding="utf-8"
                )
                fitted_dir = stage_dir / "fitted"
                fitted_dir.mkdir(exist_ok=True)
                _write_fitted_parquet(fitted_dir / "params.parquet", fitted_payload)
                stages_meta.append(
                    {
                        "index": index,
                        "uid": uid,
                        "class": stage_meta["class"],
                        "relative_path": f"stages/{index}_{uid}",
                    }
                )
            metadata = {
                "format": REPARK_ML_FORMAT,
                "version": REPARK_ML_VERSION,
                "kind": "PipelineModel",
                "uid": model.uid,
                "source_pipeline_uid": model._source_pipeline_uid,
                "pyspark_version": _pyspark_version_provenance(),
                "stages": stages_meta,
            }
            (staging / "metadata.json").write_text(
                json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
            )
            _commit_atomic_save(staging, target, overwrite=self.should_overwrite)
        except Exception:
            _abort_atomic_save(staging)
            raise


class _PipelineModelReader(MLReader):
    """Read fitted pipeline models."""

    def load(self, path: str) -> PipelineModel:
        """Rebuild stages from metadata and fitted parameters."""
        target = Path(path)
        metadata = _read_metadata(target)
        if metadata.get("kind") != "PipelineModel":
            raise IllegalArgumentException(
                f"expected kind PipelineModel, got {metadata.get('kind')!r}"
            )
        stages: list[Transformer] = []
        for entry in metadata.get("stages", []):
            if entry.get("relative_path"):
                relative = str(entry["relative_path"])
            else:
                stage_index = int(entry.get("index", 0))
                safe_uid = _sanitize_stage_uid(entry.get("uid"), index=stage_index)
                relative = f"stages/{stage_index}_{safe_uid}"
            stage_dir = _safe_stage_dir(target, relative)
            stage_meta = json.loads((stage_dir / "metadata.json").read_text(encoding="utf-8"))
            fitted = _read_fitted_parquet(stage_dir / "fitted" / "params.parquet")
            stage = _rebuild_stage(stage_meta, fitted=True, fitted_state=fitted)
            if not isinstance(stage, Transformer):
                raise IllegalArgumentException(
                    f"loaded stage {entry.get('uid')} is not a Transformer"
                )
            stages.append(stage)
        model = PipelineModel(stages=stages)
        model.uid = metadata.get("uid", model.uid)
        model._source_pipeline_uid = metadata.get("source_pipeline_uid")
        return model


def _prepare_path(target: Path, *, overwrite: bool) -> None:
    """Legacy helper. New writers should use atomic save helpers.

    This helper replaces a path before recreation and can delete an existing tree.
    """
    if target.exists():
        if not overwrite:
            raise IllegalArgumentException(
                f"path already exists: {target} (use write().overwrite().save(...))"
            )
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    target.mkdir(parents=True, exist_ok=True)


def _begin_atomic_save(target: Path, *, overwrite: bool) -> Path:
    """Create an empty sibling staging directory for an atomic save."""
    if target.exists() and not overwrite:
        raise IllegalArgumentException(
            f"path already exists: {target} (use write().overwrite().save(...))"
        )
    parent = target.parent if target.parent.as_posix() not in {"", "."} else Path.cwd()
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f".{target.name}.repark-ml-staging-{uuid.uuid4().hex[:12]}"
    if staging.exists():
        if staging.is_dir():
            shutil.rmtree(staging)
        else:
            staging.unlink()
    staging.mkdir(parents=True, exist_ok=False)
    return staging


def _commit_atomic_save(staging: Path, target: Path, *, overwrite: bool) -> None:
    """Publish staging while retaining directory targets during replacement.

    A directory moves aside and restores only while the target stays absent. Existing files are
    unlinked before rename, so file replacement has a TOCTOU loss window and no restore source.
    Cleanup and restoration can race with another target mutation.
    """
    if not staging.is_dir():
        raise IllegalArgumentException(f"atomic save staging missing: {staging}")
    if target.exists():
        if not overwrite:
            _abort_atomic_save(staging)
            raise IllegalArgumentException(
                f"path already exists: {target} (use write().overwrite().save(...))"
            )
        if not target.is_dir():
            try:
                target.unlink()
            except OSError as exc:
                _abort_atomic_save(staging)
                raise IllegalArgumentException(
                    f"atomic save could not replace non-directory path {target}: {exc}"
                ) from exc
            try:
                staging.rename(target)
            except OSError as exc:
                _abort_atomic_save(staging)
                raise IllegalArgumentException(
                    f"atomic save could not publish staging to {target}: {exc}"
                ) from exc
            return
        aside = target.parent / f".{target.name}.repark-ml-aside-{uuid.uuid4().hex[:12]}"
        try:
            target.rename(aside)
        except OSError as exc:
            _abort_atomic_save(staging)
            raise IllegalArgumentException(
                f"atomic save could not move aside existing path {target}: {exc}"
            ) from exc
        try:
            staging.rename(target)
        except OSError as exc:
            with contextlib.suppress(OSError):
                if not target.exists() and aside.exists():
                    aside.rename(target)
                elif aside.exists():
                    if aside.is_dir():
                        shutil.rmtree(aside, ignore_errors=True)
                    else:
                        with contextlib.suppress(OSError):
                            aside.unlink()
            _abort_atomic_save(staging)
            raise IllegalArgumentException(
                f"atomic save could not publish staging to {target}: {exc}"
            ) from exc
        if aside.exists():
            if aside.is_dir():
                shutil.rmtree(aside, ignore_errors=True)
            else:
                with contextlib.suppress(OSError):
                    aside.unlink()
        return
    try:
        staging.rename(target)
    except OSError as exc:
        _abort_atomic_save(staging)
        raise IllegalArgumentException(
            f"atomic save could not publish staging to {target}: {exc}"
        ) from exc


def _abort_atomic_save(staging: Path) -> None:
    """Remove staging after a failed save when possible."""
    if staging.exists():
        if staging.is_dir():
            shutil.rmtree(staging, ignore_errors=True)
        else:
            with contextlib.suppress(OSError):
                staging.unlink()


def _read_metadata(target: Path) -> dict[str, Any]:
    """Read and validate top-level metadata."""
    meta_path = target / "metadata.json"
    if not meta_path.is_file():
        raise IllegalArgumentException(f"missing metadata.json under {target}")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if metadata.get("format") != REPARK_ML_FORMAT:
        raise IllegalArgumentException(
            f"unsupported ML format {metadata.get('format')!r}; expected {REPARK_ML_FORMAT!r}"
        )
    if int(metadata.get("version", -1)) != REPARK_ML_VERSION:
        raise IllegalArgumentException(
            f"unsupported repark-ml version {metadata.get('version')!r}; "
            f"expected {REPARK_ML_VERSION}"
        )
    return metadata


def _stage_class_path(stage: Any) -> str:
    """Return a stage's dotted class path."""
    return f"{type(stage).__module__}.{type(stage).__name__}"


def _params_to_jsonable(stage: Any) -> dict[str, Any]:
    """Return defined stage parameters in JSON-friendly form."""
    if not isinstance(stage, Params):
        return {}
    result: dict[str, Any] = {}
    for param in stage.params:
        if stage.isDefined(param):
            value = stage.getOrDefault(param)
            result[param.name] = _jsonable(value)
    return result


def _jsonable(value: Any) -> Any:
    """Convert a parameter value to JSON-compatible data."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return repr(value)


def _fitted_state(stage: Any) -> dict[str, Any]:
    """Return serializable fitted metadata and reject unsupported models."""
    if hasattr(stage, "_ml_fitted_state") and callable(stage._ml_fitted_state):
        payload = stage._ml_fitted_state()
        if not isinstance(payload, dict):
            raise IllegalArgumentException("_ml_fitted_state must return a dict")
        return {str(key): _jsonable(value) for key, value in payload.items()}
    if isinstance(stage, Model):
        module_name = type(stage).__module__ or ""
        class_name = type(stage).__name__
        if module_name.startswith("repark.spark.ml.ext") or module_name.startswith("repark.ml.ext"):
            raise UnsupportedOperationException(
                f"PipelineModel.save: stage {class_name!r} cannot be persisted "
                f"(save not supported for ext estimators; repark.ml.ext persistence v1 "
                f"is STOP-loud; booster-bytes format is stretch-only)"
            )
        raise UnsupportedOperationException(
            f"PipelineModel.save: stage {class_name!r} has no _ml_fitted_state; "
            f"cannot hollow-publish fitted model without serializable params"
        )
    return {}


def _write_fitted_parquet(path: Path, payload: dict[str, Any]) -> None:
    """Write fitted parameters as one Parquet row.

    An empty sentinel row preserves a readable file when fitted state has no fields.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not payload:
        table = pa.table({"_empty": pa.array([True])})
        pq.write_table(table, path)
        return
    columns: dict[str, list[Any]] = {}
    for key, value in payload.items():
        if isinstance(value, (list, dict)):
            columns[key] = [json.dumps(value)]
        else:
            columns[key] = [value]
    table = pa.table(columns)
    pq.write_table(table, path)


def _read_fitted_parquet(path: Path) -> dict[str, Any]:
    """Read fitted parameters from Parquet."""
    import pyarrow.parquet as pq

    if not path.is_file():
        return {}
    table = pq.read_table(path)
    if table.num_rows == 0:
        return {}
    if set(table.column_names) == {"_empty"}:
        return {}
    row = {name: table.column(name)[0].as_py() for name in table.column_names}
    decoded: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, str) and value[:1] in "[{":
            try:
                decoded[key] = json.loads(value)
                continue
            except json.JSONDecodeError:
                pass
        decoded[key] = value
    return decoded


def _rebuild_stage(
    entry: dict[str, Any],
    *,
    fitted: bool,
    fitted_state: dict[str, Any] | None = None,
) -> Any:
    """Rebuild a stage from class path, parameters, and fitted state."""
    class_path = entry.get("class", "")
    params = entry.get("params") or {}
    stage = _instantiate_stage(class_path, params, fitted=fitted, fitted_state=fitted_state or {})
    if "uid" in entry and hasattr(stage, "uid"):
        stage.uid = entry["uid"]
    return stage


def _sanitize_stage_uid(uid: Any, *, index: int) -> str:
    """Return a path-safe stage uid, using an index fallback."""
    text = str(uid) if uid is not None else ""
    if _SAFE_STAGE_SEGMENT.fullmatch(text) and ".." not in text:
        return text
    fallback = f"stage_{index}"
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("._-")
    if cleaned and _SAFE_STAGE_SEGMENT.fullmatch(cleaned) and ".." not in cleaned:
        return cleaned
    return fallback


def _safe_stage_dir(root: Path, relative: str) -> Path:
    """Resolve a safe stage path below ``root`` and reject traversal."""
    if not isinstance(relative, str) or not relative.strip():
        raise IllegalArgumentException("pipeline stage relative_path must be a non-empty string")
    normalized = relative.replace("\\", "/").strip("/")
    if not normalized:
        raise IllegalArgumentException(f"pipeline stage relative_path is empty: {relative!r}")
    parts = normalized.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise IllegalArgumentException(
            f"pipeline stage relative_path must not contain '.'/'..' segments: {relative!r}"
        )
    if any(not _SAFE_STAGE_SEGMENT.fullmatch(part) for part in parts):
        raise IllegalArgumentException(
            f"pipeline stage relative_path has unsafe segment: {relative!r}"
        )
    candidate = (root / Path(*parts)).resolve()
    root_resolved = root.resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as error:
        raise IllegalArgumentException(
            f"pipeline stage path escapes model root: {relative!r}"
        ) from error
    return candidate


def _assert_allowed_stage_class_path(class_path: str) -> tuple[str, str]:
    """Validate facade class-path syntax and its allowlist before later import checks."""
    if not isinstance(class_path, str) or not class_path or "/" in class_path or "\\" in class_path:
        raise UnsupportedOperationException(
            f"cannot rebuild stage class {class_path!r} (invalid class path)"
        )
    module_name, sep, class_name = class_path.rpartition(".")
    if not sep or not module_name or not class_name:
        raise UnsupportedOperationException(
            f"cannot rebuild stage class {class_path!r} (no module path; register or implement "
            f"_ml_from_save)"
        )
    if not class_name.isidentifier():
        raise UnsupportedOperationException(
            f"cannot rebuild stage class {class_path!r} (invalid class name)"
        )
    if not module_name.startswith(_ML_STAGE_MODULE_PREFIX):
        raise UnsupportedOperationException(
            f"cannot rebuild stage class {class_path!r}: module must be under "
            f"{_ML_STAGE_MODULE_PREFIX!r} (import allowlist; octo C2-SEC-002)"
        )
    for denied in _ML_STAGE_MODULE_DENY:
        if module_name == denied or module_name.startswith(denied + "."):
            raise UnsupportedOperationException(
                f"cannot rebuild stage class {class_path!r}: module {module_name!r} is not "
                f"loadable via pipeline persistence (denied prefix {denied!r})"
            )
    for part in module_name.split("."):
        if not part.isidentifier():
            raise UnsupportedOperationException(
                f"cannot rebuild stage class {class_path!r}: invalid module segment {part!r}"
            )
    return module_name, class_name


def _instantiate_stage(
    class_path: str,
    params: dict[str, Any],
    *,
    fitted: bool,
    fitted_state: dict[str, Any],
) -> Any:
    """Import and construct a registered or allowlisted stage."""
    registry = _stage_registry()
    if class_path in registry:
        return registry[class_path](params=params, fitted=fitted, fitted_state=fitted_state)
    module_name, class_name = _assert_allowed_stage_class_path(class_path)
    try:
        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
    except (ImportError, AttributeError) as error:
        raise UnsupportedOperationException(
            f"cannot import stage class {class_path!r}: {error}"
        ) from error
    if hasattr(cls, "_ml_from_save"):
        return cls._ml_from_save(params=params, fitted=fitted, fitted_state=fitted_state)
    raise UnsupportedOperationException(
        f"stage class {class_path!r} does not implement _ml_from_save; cannot load"
    )


def _stage_registry() -> dict[str, Any]:
    """Return built-in stage rebuild callables."""
    return {
        "repark.spark.ml.pipeline._ConstantColumnModel": _rebuild_constant_column_model,
        "repark.spark.ml.pipeline._ConstantColumnEstimator": _rebuild_constant_column_estimator,
    }


class _ConstantColumnEstimator(Estimator["_ConstantColumnModel"]):
    """Estimator used to persist and test a constant-column stage."""

    def __init__(self, output_col: str = "const", value: float = 1.0) -> None:
        """Initialize the output column and literal value."""
        super().__init__()
        self.output_col = output_col
        self.value = float(value)

    def _fit(self, dataset: Any) -> _ConstantColumnModel:
        """Validate the frame and return a model holding the literal."""
        _require_repark_dataframe(dataset, verb="_ConstantColumnEstimator.fit")
        model = _ConstantColumnModel(output_col=self.output_col, value=self.value)
        model.uid = self.uid.replace("Estimator", "Model") if "Estimator" in self.uid else self.uid
        return model

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Return the estimator's empty fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> _ConstantColumnEstimator:
        """Rebuild an estimator from saved parameters."""
        return cls(
            output_col=str(params.get("output_col", "const")),
            value=float(params.get("value", 1.0)),
        )


class _ConstantColumnModel(Model):
    """Model that adds a literal output column through the plan."""

    def __init__(self, output_col: str = "const", value: float = 1.0) -> None:
        """Initialize the fitted literal and output name."""
        super().__init__()
        self.output_col = output_col
        self.value = float(value)

    def _transform(self, dataset: Any) -> Any:
        """Add the literal output column lazily."""
        from repark.spark.functions import lit

        frame = _require_repark_dataframe(dataset, verb="_ConstantColumnModel.transform")
        return frame.withColumn(self.output_col, lit(self.value))

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Return fitted parameters without training rows."""
        return {"output_col": self.output_col, "value": self.value}

    def copy(self, extra: dict[Param[Any], Any] | None = None) -> _ConstantColumnModel:
        """Copy the model."""
        that = _ConstantColumnModel(output_col=self.output_col, value=self.value)
        that.uid = self.uid
        return that

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> _ConstantColumnModel:
        """Rebuild a model from saved parameters."""
        payload = fitted_state or params
        return cls(
            output_col=str(payload.get("output_col", "const")),
            value=float(payload.get("value", 1.0)),
        )


def _rebuild_constant_column_model(
    *,
    params: dict[str, Any],
    fitted: bool,
    fitted_state: dict[str, Any],
) -> _ConstantColumnModel:
    """Rebuild a constant-column model."""
    return _ConstantColumnModel._ml_from_save(
        params=params, fitted=fitted, fitted_state=fitted_state
    )


def _rebuild_constant_column_estimator(
    *,
    params: dict[str, Any],
    fitted: bool,
    fitted_state: dict[str, Any],
) -> _ConstantColumnEstimator:
    """Rebuild a constant-column estimator."""
    return _ConstantColumnEstimator._ml_from_save(
        params=params, fitted=fitted, fitted_state=fitted_state
    )


__all__ = [
    "REPARK_ML_FORMAT",
    "REPARK_ML_VERSION",
    "Pipeline",
    "PipelineModel",
]
