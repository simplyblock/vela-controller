from pathlib import Path

from .._util import Identifier


from .settings import settings


def _require_asset(path: Path, description: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{description} not found at {path}")
    return path

def deployment_namespace(branch_id: Identifier) -> str:
    """Return the Kubernetes namespace for a branch using `<prefix>-<branch_id>` format."""

    branch_value = str(branch_id).lower()
    prefix = settings.deployment_namespace_prefix
    if prefix:
        return f"{prefix}-{branch_value}"
    return branch_value
    