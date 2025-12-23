from pathlib import Path
from dataclasses import dataclass

@dataclass(frozen=True)
class Paths:
    cache: Path
    externally: Path
    processed: Path
    raw: Path
    root: Path
    reports: Path
def make_paths(root: Path)-> Paths:
    data = root/ "data"
    return Paths(cache = data/"cache",
                externally=data/"externally",
                processed=data/"processed",
                raw=data/"raw",
                root = root,
                reports=root/"reports",)