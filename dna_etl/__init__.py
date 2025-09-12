"""
DNA ETL Framework - A standardized ETL framework for Axpo.

This package provides standardized patterns, components, and tooling 
to build reliable, observable, and scalable ETL pipelines.
"""

try:
    # Try to get version from setuptools_scm
    from ._version import __version__
except ImportError:
    # Fallback version if setuptools_scm hasn't run yet
    __version__ = "0.0.0+unknown"

__author__ = "Axpo Trading & Sales Ltd"
__email__ = "support@axpo.com"

# Import main components
from .core import Pipeline, ETLBase, get_version

__all__ = [
    "__version__",
    "__author__", 
    "__email__",
    "Pipeline",
    "ETLBase",
    "get_version",
]