"""Exception taxonomy.

Two families, by blast radius:

* ``PipelineError`` / ``ExecutorError`` — the *job* is broken (bad YAML, unimportable
  kernel, artifact key conflict). Fail fast and loud; a human must fix the config.
* ``DecodeError`` and anything a kernel raises on one sample — a *QC finding*. The
  executor turns it into ``status: error`` for that sample and keeps going.
"""


class AbaSiftError(Exception):
    """Base for everything this framework raises."""


class PipelineError(AbaSiftError):
    """Malformed pipeline definition. Raised at load time, before any I/O."""


class ExecutorError(AbaSiftError):
    """The executor's own invariants were violated (e.g. artifact key conflict)."""


class DecodeError(AbaSiftError):
    """A ``LazyRaw`` could not be turned into a usable object.

    Per-sample finding, never fatal: unreadable data is itself a quality defect.
    """


class MissingStream(DecodeError):
    """The container exists but the requested track is not in it."""
