# Typing Remediation Tracker

## Baseline (2025-11-07)

Command: `mypy sparkless tests` (captured in `mypy-baseline.log`)

### Error Themes

- **Type aliases & helpers** – invalid runtime aliases and missing generics in
  `sparkless/compat/datetime.py`, `sparkless/backend/polars/schema_registry.py`,
  `tests/tools/*`, etc. (~35 findings)
- **Interface contract drift** – mixins overriding abstract methods with
  incompatible signatures across `sparkless/core/interfaces`,
  `sparkless/functions/core`, and `sparkless/dataframe/*`. (>120 findings)
- **Optional / None handling** – unguarded optionals and union misuse in
  `sparkless/dataframe/lazy.py`, `grouped/base.py`, and the expression
  evaluator. (~40 findings)
- **Missing annotations & attr-defined** – untyped vars/functions and mistaken
  attribute usage in storage/materializer modules and tests. (~15 findings)

### Next Steps

1. Normalize aliases and helper annotations.
2. Align interface and mixin signatures with their abstractions.
3. Harden optional handling hotspots.
4. ~~Iterate on residual errors and wire mypy into CI.~~ **Done** – CI runs `mypy sparkless tests` in `.github/workflows/ci.yml` (lint-and-type job).

Status will be updated as each theme is addressed.

## Progress Log

- **2025-11-07** — Normalised type aliases in `sparkless/compat/datetime.py`
  (now using standard 3.9 generics without runtime fallbacks) and cleaned helper annotations in
  `tests/tools/` and `sparkless/backend/polars/{schema_registry,storage}.py`.
  Refreshed `mypy-baseline.log` after changes (still pending interface/optional
  work).
- **2026-02** — Mypy confirmed wired into CI; exception-handling narrowing in
  `lazy.py`, `delta.py`, and Polars backend (no new type ignores). Further
  reduction of `# type: ignore` remains incremental (interface/optional themes).

