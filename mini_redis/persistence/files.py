import os

from pathlib import Path

def fsync_dir(path: Path) -> None:
    """fsync a directory, so a rename inside it survives power loss."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)