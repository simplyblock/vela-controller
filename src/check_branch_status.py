from .exceptions import VelaDeploymentError
from .deployment import get_deployment_status, get_db_vmi_identity
#from .deployment.kubernetes.kubevirt import get_virtualmachine_status
from .api.models.branch import Branch, BranchStatus
import logging

async def get_branch_status(branch: Branch):

    #namespace, vmi_name = get_db_vmi_identity(branch.id)
    try:
        #status = await get_virtualmachine_status(namespace, vmi_name)
        status="ACTIVE_HEALTHY"
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