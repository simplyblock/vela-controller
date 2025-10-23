import logging

from ._util import Identifier

# from .deployment.kubernetes.kubevirt import get_virtualmachine_status
from .api.models.branch import BranchServiceStatus, BranchStatus
from .exceptions import VelaDeploymentError


async def get_branch_status(_branch_id: Identifier) -> BranchServiceStatus:
    # namespace, vmi_name = get_db_vmi_identity(branch_id)
    try:
        # status = await get_virtualmachine_status(namespace, vmi_name)
        status: BranchServiceStatus = "ACTIVE_HEALTHY"
    except VelaDeploymentError:
        logging.exception("Failed to query VM status")
        status = "UNKNOWN"
        _service_health = BranchStatus(
            database="UNKNOWN",
            realtime="UNKNOWN",
            storage="UNKNOWN",
            meta="UNKNOWN",
            rest="UNKNOWN",
        )
    return status
