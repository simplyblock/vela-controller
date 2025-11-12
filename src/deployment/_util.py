from .._util import Identifier
from .settings import get_settings


def deployment_namespace(branch_id: Identifier) -> str:
    """Return the Kubernetes namespace for a branch using `<prefix>-<branch_id>` format."""

    branch_value = str(branch_id).lower()
    prefix = get_settings().deployment_namespace_prefix
    if prefix:
        return f"{prefix}-{branch_value}"
    return branch_value
