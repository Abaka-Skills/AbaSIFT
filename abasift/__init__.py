"""AbaSift — distributed QC framework for egocentric vendor datasets.

A job is one pipeline YAML run on one machine. Work distribution is external.
See ``doc/design.md`` for the contract.
"""

from . import decoders  # imported for effect: registers the built-in decoders
from .data import ArtifactUnion, Batch, Sample
from .errors import AbaSiftError, DecodeError, ExecutorError, MissingStream, PipelineError
from .executor import Executor, run_pipeline
from .kernel import ArtifactExt, Kernel, Mutation, MutatingKernel, SampleKernel, SourceKernel
from .lazy import LazyRaw, register_decoder
from .payloads import ImuTrack, VideoFrames, VideoMeta
from .pipeline import Pipeline
from .report import Check, Report, ReportExt, ReportView

__version__ = "0.1.0"

__all__ = [
    "AbaSiftError",
    "ArtifactExt",
    "ArtifactUnion",
    "Batch",
    "Check",
    "DecodeError",
    "decoders",
    "Executor",
    "ExecutorError",
    "ImuTrack",
    "Kernel",
    "LazyRaw",
    "MissingStream",
    "Mutation",
    "MutatingKernel",
    "Pipeline",
    "PipelineError",
    "Report",
    "ReportExt",
    "ReportView",
    "Sample",
    "SampleKernel",
    "SourceKernel",
    "VideoFrames",
    "VideoMeta",
    "register_decoder",
    "run_pipeline",
    "__version__",
]
