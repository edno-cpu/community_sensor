#!/usr/bin/env python3
"""
Atomic file operations for safe writing on the Pi.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def atomic_rename(src: Path, dst: Path) -> None:
    """
    Atomically replace dst with src (or move src -> dst if dst doesn't exist).
    Uses os.replace, which is atomic on POSIX filesystems.
    """
    src = Path(src)
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)


def append_lines(path: Path, lines: Iterable[str]) -> None:
    """
    Append one or more lines to a text file, ensuring data reaches disk.

    This is deliberately simple: open → write → flush+fsync → close.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as f:
        for line in lines:
            if not line.endswith("\n"):
                line = line + "\n"
            f.write(line)
        f.flush()
        os.fsync(f.fileno())
