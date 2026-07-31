"""Loaders — node 0 of every DAG.

A loader's entire job is normalising one vendor's directory layout into canonical named
streams (``video/main``, ``imu/main``, ``annotation/task``) carrying ``LazyRaw`` handles.
Nothing is downloaded here; enumeration is metadata-only.
"""

from .flat_dir import FlatDirLoader
from .egoverse import EgoverseDjiLoader

__all__ = ["FlatDirLoader", "EgoverseDjiLoader"]
