"""Global configuration helpers for Sparkless."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict, Optional

DEFAULT_BACKEND = "polars"
ENV_BACKEND_KEY = "SPARKLESS_BACKEND"
ENV_FEATURE_FLAGS_KEY = "SPARKLESS_FEATURE_FLAGS"
ENV_PROFILE_TOGGLE = "SPARKLESS_PROFILE"

_ENV_FEATURE_PREFIX = "SPARKLESS_FEATURE_"
_FEATURE_FLAG_DEFAULTS: Dict[str, bool] = {
    "enable_performance_profiling": False,
    "enable_polars_vectorized_shortcuts": False,
    "enable_expression_translation_cache": False,
    "enable_adaptive_execution_simulation": False,
}


def resolve_backend_type(explicit_backend: Optional[str] = None) -> str:
    """Resolve the backend type using overrides, environment variables, and defaults."""

    candidate = explicit_backend or os.getenv(ENV_BACKEND_KEY) or DEFAULT_BACKEND
    candidate_normalized = candidate.strip().lower()

    from sparkless.backend.factory import BackendFactory

    BackendFactory.validate_backend_type(candidate_normalized)
    return candidate_normalized


@lru_cache(maxsize=1)
def _load_feature_flag_overrides() -> Dict[str, bool]:
    """Load feature flag overrides from environment variables."""

    overrides: Dict[str, bool] = {}

    # Allow simple JSON blob for multiple flags.
    raw_json = os.getenv(ENV_FEATURE_FLAGS_KEY)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                overrides.update(
                    {
                        str(key): bool(value)
                        for key, value in parsed.items()
                        if isinstance(value, (bool, int, str))
                    }
                )
        except json.JSONDecodeError:
            # Ignore bad JSON and fall back to prefix-based overrides.
            pass

    # Support individual overrides via SPARKLESS_FEATURE_<FLAG>=1/0.
    for key, value in os.environ.items():
        if not key.startswith(_ENV_FEATURE_PREFIX):
            continue
        flag_name = key[len(_ENV_FEATURE_PREFIX) :].lower()
        overrides[flag_name] = _coerce_bool(value)

    # Convenience toggle: enabling profiling via SPARKLESS_PROFILE=1.
    if ENV_PROFILE_TOGGLE in os.environ:
        overrides.setdefault(
            "enable_performance_profiling", _coerce_bool(os.environ[ENV_PROFILE_TOGGLE])
        )

    return overrides


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def get_feature_flags() -> Dict[str, bool]:
    """Return merged feature flag map (defaults + overrides)."""

    merged = dict(_FEATURE_FLAG_DEFAULTS)
    overrides = _load_feature_flag_overrides()
    for key, raw_value in overrides.items():
        flag = key.lower()
        merged[flag] = bool(raw_value)
    return merged


def is_feature_enabled(flag_name: str) -> bool:
    """Return True when the given feature flag is enabled."""

    flag = flag_name.lower()
    flags = get_feature_flags()
    return bool(flags.get(flag, False))


def describe_feature_flags() -> Dict[str, bool]:
    """Expose feature flags primarily for debugging or documentation."""

    return get_feature_flags()
