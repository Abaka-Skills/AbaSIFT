"""Shared fixtures. Synthesized videos for the offline suite, real credentials for the
``s3``-marked integration suite (skipped when ``test/s3.json`` is absent)."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
S3_JSON = ROOT / "test" / "s3.json"

#: What the synthesized clips are. Every test that does arithmetic on a clip reads these
#: rather than restating the ffmpeg arguments below.
CLIP_SECONDS = (2, 5, 10)
CLIP_FPS = 10
CLIP_SIZE = (160, 120)  # width, height


def load_node(videos, **params) -> dict:
    """The `FlatDirLoader` node every offline pipeline starts with."""
    return {
        "name": "load",
        "kernel": "abasift.loaders.FlatDirLoader",
        "params": {"root": str(videos), "batch_size": 2, **params},
        "inputs": [],
    }


def run_nodes(nodes, job_id: str = "test"):
    """Run a node list and hand back what a test asserts on: the report and the artifacts."""
    from abasift import Executor, Pipeline

    ex = Executor(Pipeline.from_dict({"job_id": job_id, "nodes": nodes}))
    return ex.run(), ex.artifacts


@pytest.fixture(scope="session")
def ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if not exe:
        pytest.skip("ffmpeg not on PATH")
    return exe


@pytest.fixture(scope="session")
def videos(tmp_path_factory, ffmpeg) -> Path:
    """A directory of clips with known durations plus one deliberately corrupt file."""
    d = tmp_path_factory.mktemp("videos")
    for secs in CLIP_SECONDS:
        subprocess.run(
            [
                ffmpeg, "-v", "error", "-y",
                "-f", "lavfi",
                "-i", f"testsrc=size={CLIP_SIZE[0]}x{CLIP_SIZE[1]}:rate={CLIP_FPS}:duration={secs}",
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                str(d / f"clip_{secs}s.mp4"),
            ],
            check=True,
        )
    (d / "broken.mp4").write_bytes(b"this is not a media container")
    return d


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path):
    """Never let a test touch the developer's real worker disk cache."""
    from abasift.cache import DiskCache, set_disk_cache

    set_disk_cache(DiskCache(tmp_path / "cache"))
    yield
    set_disk_cache(None)


@pytest.fixture(scope="session")
def s3_conf() -> dict:
    if not S3_JSON.exists():
        pytest.skip(f"{S3_JSON} not present (integration tests need vendor credentials)")
    return json.loads(S3_JSON.read_text())


@pytest.fixture
def s3_env(s3_conf, monkeypatch) -> dict:
    """Put the vendor credentials on the standard AWS chain (env), never in YAML.

    The framework itself only ever reads the standard chain; this fixture is the only
    place in the codebase that touches ``test/s3.json``.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s3_conf["access_key"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s3_conf["secret_key"])
    monkeypatch.setenv("AWS_DEFAULT_REGION", s3_conf["region"])
    return {"root": f"s3://{s3_conf['bucket']}/{s3_conf['path']}".rstrip("/")}
