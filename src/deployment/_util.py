from pathlib import Path

def _require_asset(path: Path, description: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{description} not found at {path}")
    return path
    