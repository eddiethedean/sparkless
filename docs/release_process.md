# Release process (Sparkless v4)

This document describes how to cut a release (e.g. v4.0.0). Completion criteria for Phase 8 allow "release steps documented or executed"; actual PyPI publish can be deferred to maintainer discretion.

## Prerequisites

- Tests pass with Robin: `SPARKLESS_TEST_BACKEND=robin python -m pytest tests/unit/ -v --tb=no -q` (or `bash tests/run_all_tests.sh`).
- CHANGELOG and version are updated (see below).

## Steps

1. **Update CHANGELOG**: Ensure [CHANGELOG.md](../CHANGELOG.md) has a section for the release version (e.g. `## 4.0.0 — YYYY-MM-DD`) and set the date when releasing.
2. **Set version**: In [pyproject.toml](../pyproject.toml) set `version = "4.0.0"` (or target version). In [sparkless/_version.py](../sparkless/_version.py) set the fallback `__version__ = "4.0.0"` to match.
3. **Tag**: Create an annotated tag, e.g. `git tag -a v4.0.0 -m "Release 4.0.0"`. The tag name must match the version (with a `v` prefix).
4. **Publish**: Push the tag to trigger the GitHub Actions workflow: `git push origin v4.0.0`. The workflow [.github/workflows/publish.yml](../.github/workflows/publish.yml) verifies that the tag version matches `pyproject.toml` and then builds and publishes to PyPI (when configured). Alternatively, use the workflow_dispatch input to specify the version.

## Verification

- The publish workflow extracts the version from the tag and checks it against `pyproject.toml`. Ensure they match to avoid upload failures.
- After publish, confirm the package appears on PyPI and that `pip install sparkless==4.0.0` works.
