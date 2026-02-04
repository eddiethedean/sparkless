"""Robin (robin-sparkless) backend for Sparkless.

This backend delegates DataFrame execution to the robin_sparkless Python module
(Rust/Polars engine via PyO3). Install with: pip install robin_sparkless
(or build from robin-sparkless repo with maturin develop --features pyo3).

See docs/backend_selection.md and SPARKLESS_INTEGRATION_ANALYSIS in the
robin-sparkless repo for viability and op coverage.
"""

from .materializer import RobinMaterializer
from .storage import RobinStorageManager
from .export import RobinExporter

__all__ = ["RobinMaterializer", "RobinStorageManager", "RobinExporter"]
