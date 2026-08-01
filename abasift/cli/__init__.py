"""The ``abasift`` command line: one YAML, one machine, one report.

Everything a terminal sees lives here and nowhere else — argument parsing
(:mod:`~abasift.cli.main`), the startup banner (:mod:`~abasift.cli.banner`), and colour
plus log formatting (:mod:`~abasift.cli.term`). Nothing in the data plane imports this
package, so the framework stays usable as a library with no opinion about stdout.
"""

from __future__ import annotations

from .banner import run_banner, run_results
from .main import main
from .term import ColorFormatter, color_enabled, paint, setup_logging

__all__ = [
    "ColorFormatter",
    "color_enabled",
    "main",
    "paint",
    "run_banner",
    "run_results",
    "setup_logging",
]
