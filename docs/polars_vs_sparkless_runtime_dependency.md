# Polars vs Sparkless: Runtime / Backend Dependency Comparison

This document compares how the **Polars** Python package depends on **polars-runtime-32** with how **Sparkless** depends on **robin-sparkless**.

---

## 1. Dependency declaration

| Aspect | Polars (py-polars) | Sparkless |
|--------|--------------------|-----------|
| **Package** | `polars` → `polars-runtime-32` | `sparkless` → `robin-sparkless` |
| **Constraint** | Exact: `polars-runtime-32 == 1.38.1` | Minimum: `robin-sparkless>=0.8.4` |
| **Optional?** | No (required). Optional *variants*: `rt64`, `rtcompat` as extras | No (required in v4) |
| **Extras** | `rt64`, `rtcompat` switch to a different runtime package | None for Robin (it’s always required) |

- **Polars** pins the runtime to the same version as the main package so the Rust ABI and Python wrapper stay in sync.
- **Sparkless** uses a minimum version so newer compatible robin-sparkless releases are accepted without changing sparkless.

---

## 2. What the dependency provides

| Aspect | Polars | Sparkless |
|--------|--------|-----------|
| **Role** | Binary extension only: the Rust/PyO3 engine. No Polars API. | Full execution engine: PySpark-like API (SparkSession, DataFrame, Column, etc.) implemented in Rust/Polars. |
| **Content** | Pre-compiled native module (`_polars_runtime_32._polars_runtime`) built with maturin. | Python package `robin_sparkless` exposing SparkSession, DataFrame, functions, etc. |
| **Ownership** | Same org (pola-rs), same repo (polars monorepo under `py-polars/runtime/`). | Separate project/repo; Sparkless consumes it as an external dependency. |

---

## 3. How the main package uses it

### Polars

- **Load path:** `polars._plr` is the public name for the Rust API. In `_plr.py`, the package:
  1. Tries runtime packages in order (compat, 64, 32) unless `POLARS_FORCE_PKG` / `POLARS_PREFER_PKG` say otherwise.
  2. Imports e.g. `_polars_runtime_32._polars_runtime` and then does `sys.modules["polars._plr"] = plr` so `polars._plr` is the native module.
- **Version check:** The Rust module’s version must equal the Python package version (`PKG_VERSION`); otherwise Polars skips that runtime or raises.
- **No fallback:** If no matching runtime is found, Polars raises `ImportError`. There is no “pure Python” or alternate backend.

### Sparkless

- **Load path:** Robin is imported only inside backend code (e.g. `sparkless/backend/robin/materializer.py`, `plan_executor.py`):
  - `try: import robin_sparkless except ImportError: robin_sparkless = None`
  - Helpers like `_robin_available()` check `robin_sparkless is not None`.
- **When used:** Backend factory and Robin modules use `_robin_available()` (or the try/except) before calling Robin. If Robin is missing, the factory raises a clear error (“Install with: pip install robin-sparkless”).
- **No version check:** Sparkless does not assert a specific robin-sparkless version at import; it relies on the `>=0.8.4` constraint in `pyproject.toml`.

---

## 4. Architecture in one sentence

- **Polars:** One API (the `polars` package); the runtime is the *implementation* of that API (binary only). No alternate backends.
- **Sparkless:** One API (the `sparkless` package); Robin is one *backend* implementing that API (materializer, storage, plan executor). In v4 it’s the only supported backend, but the abstraction (e.g. `DataMaterializer`, `BackendFactory`) is built for multiple backends.

---

## 5. Build and release

| Aspect | Polars | Sparkless |
|--------|--------|-----------|
| **Runtime source** | In same repo: `py-polars/runtime/polars-runtime-32` (and 64, compat). Template generates per-runtime packages. | Not in Sparkless repo; robin-sparkless is built and released elsewhere. |
| **Publishing** | Main `polars` and `polars-runtime-32` (and other runtimes) are published separately; versions are aligned (e.g. 1.38.1). | Sparkless and robin-sparkless have independent versioning; Sparkless only requires `>=0.8.4`. |
| **Wheel content** | `polars` wheel: mostly pure Python + small `_plr.py` loader. No Rust. Heavy binaries live in the runtime wheel. | `sparkless` wheel: pure Python. Robin’s wheel contains the Rust extension. |

---

## 6. Summary table

| | Polars | Sparkless |
|---|--------|-----------|
| **Dependency** | `polars-runtime-32 == X.Y.Z` | `robin-sparkless>=0.8.4` |
| **Purpose** | Provide Polars’ Rust engine (binary only) | Provide PySpark-like execution engine (full API) |
| **Same repo?** | Yes (monorepo) | No |
| **Version sync** | Exact match required | Min version only |
| **Import style** | Loader in `_plr.py` replaces `polars._plr` with runtime module | Lazy `import robin_sparkless` in backend code + availability checks |
| **Optional at install?** | No (required). Optional *runtime variants* via extras | No (required in v4) |

---

## References

- Polars: [py-polars pyproject.toml](https://github.com/pola-rs/polars/blob/main/py-polars/pyproject.toml), [py-polars/src/polars/_plr.py](https://github.com/pola-rs/polars/blob/main/py-polars/src/polars/_plr.py), [py-polars/runtime/](https://github.com/pola-rs/polars/tree/main/py-polars/runtime).
- Sparkless: `pyproject.toml`, `sparkless/backend/factory.py`, `sparkless/backend/robin/materializer.py`, `sparkless/backend/robin/plan_executor.py`.
