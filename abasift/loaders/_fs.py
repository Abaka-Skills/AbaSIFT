"""fsspec listing helpers shared by the loaders.

Small, but written once on purpose: every vendor loader needs "list this prefix and tell me
name + size", and every one of them would otherwise re-encode the same two fsspec quirks —
that backends return either a list or a path-keyed dict, and that entries must be filtered
to real files.
"""

from __future__ import annotations

ORDERS = ("name", "size")


def check_order(order: str) -> str:
    """Validate a loader's ``order`` param at construction, not at enumeration."""
    if order not in ORDERS:
        raise ValueError(f"order must be one of {ORDERS}, got {order!r}")
    return order


def list_files(fs, base: str, recursive: bool = True) -> list[dict]:
    """Every file under ``base`` as ``{"name", "size"}`` — one listing call, no per-file stat.

    Sizes come from the listing itself, so a loader never pays a round trip per object.
    """
    detail = fs.find(base, detail=True) if recursive else fs.ls(base, detail=True)
    entries = detail.values() if isinstance(detail, dict) else detail
    return [
        {"name": e["name"], "size": int(e.get("size") or 0)}
        for e in entries
        if e.get("type", "file") == "file"
    ]
