import logging
from typing import Final

from ._util import Identifier, StatusType
from .api.models.branch import BranchServiceStatus
from .deployment import get_db_vmi_identity
from .deployment.kubernetes.kubevirt import get_virtualmachine_status
from .exceptions import VelaDeploymentError

logger = logging.getLogger(__name__)

_VM_STATUS_TO_BRANCH_STATUS: Final[dict[StatusType, BranchServiceStatus]] = {
    "Stopped": "STOPPED",
    "Provisioning": "STARTING",
    "Starting": "STARTING",
    "Running": "ACTIVE_HEALTHY",
    "Paused": "STOPPED",
    "Stopping": "STOPPING",
    "Terminating": "DELETING",
    "CrashLoopBackOff": "ACTIVE_UNHEALTHY",
    "Migrating": "UPDATING",
    "Unknown": "UNKNOWN",
    "ErrorUnschedulable": "ERROR",
    "ErrImagePull": "ERROR",
    "ImagePullBackOff": "ERROR",
    "ErrorPvcNotFound": "ERROR",
    "DataVolumeError": "ERROR",
    "WaitingForVolumeBinding": "STARTING",
    "WaitingForReceiver": "STARTING",
    "UNKNOWN": "UNKNOWN",
}


def _map_vm_status_to_branch_status(status: StatusType) -> BranchServiceStatus:
    mapped = _VM_STATUS_TO_BRANCH_STATUS.get(status)
    if mapped is None:
        logger.warning("Unmapped VM status '%s'; returning UNKNOWN", status)
        return "UNKNOWN"
    return mapped


async def get_branch_status(branch_id: Identifier) -> BranchServiceStatus:
    try:
        namespace, vmi_name = get_db_vmi_identity(branch_id)
        vm_status = await get_virtualmachine_status(namespace, vmi_name)
    except VelaDeploymentError:
        logger.exception("Failed to query VM status for branch %s", branch_id)
        return "UNKNOWN"
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Unexpected error resolving branch status for %s", branch_id)
        return "UNKNOWN"

    return _map_vm_status_to_branch_status(vm_status)
