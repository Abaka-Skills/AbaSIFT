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

#: Known durations for the acceptance test.
CLIP_SECONDS = (2, 5, 10)


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
                "-f", "lavfi", "-i", f"testsrc=size=160x120:rate=10:duration={secs}",
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
