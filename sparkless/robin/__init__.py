"""
Robin (robin-sparkless) execution integration.

Single execution path: no backend or materializer abstraction.
Execution and catalog delegate to the Robin session directly.
"""

from sparkless.robin.execution import execute_via_robin

__all__ = [
    "execute_via_robin",
]
