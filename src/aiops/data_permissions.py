"""Readable generated data: directories 0755, regular files 0644."""

import os
from pathlib import Path


def data_directory(path: Path) -> None:
    path = Path(path)
    missing = []
    parent = path
    while not parent.exists():
        missing.append(parent)
        parent = parent.parent
    path.mkdir(parents=True, exist_ok=True, mode=0o755)
    for directory in [*reversed(missing), path]:
        directory.chmod(0o755)


def data_file(path: Path, flags: int) -> int:
    descriptor = os.open(path, flags, 0o644)
    try:
        os.fchmod(descriptor, 0o644)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor
