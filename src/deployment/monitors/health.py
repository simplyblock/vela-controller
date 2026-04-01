import asyncio
import logging
import re
from datetime import datetime, timedelta

from pydantic import BaseModel
from ulid import ULID

from ..kubernetes._util import custom_api_client
from ..kubernetes.neonvm import NeonVM, Phase
from ..settings import get_settings

logger = logging.getLogger(__name__)


async def _check_port(ip, port, timeout=1.0):
    try:
        await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=timeout)
        return True
    except (TimeoutError, ConnectionRefusedError, OSError) as e:
        logger.debug(f"Health check failed for {ip}:{port} - {e}")
        return False


class VMStatus(BaseModel):
    phase: Phase | None
    services: dict[str, bool] | None = None


async def check_vm_status(vm: NeonVM, timeout: float = 0.5) -> VMStatus:
    phase = vm.status.phase if vm.status is not None else None
    vm_ip = vm.status.pod_ip if vm.status is not None else None
    online = vm_ip is not None and phase in {Phase.running, Phase.pre_migrating, Phase.migrating, Phase.scaling}
    ports = {port.name: port.port for port in vm.spec.guest.ports if port.protocol == "TCP"}
    return VMStatus(
        phase=phase,
        services=dict(
            zip(
                ports.keys(),
                await asyncio.gather(*(_check_port(vm_ip, port, timeout=timeout) for port in ports.values())),
                strict=True,
            )
        )
        if online
        else None,
    )


class VMMonitor:
    _NAMESPACE_PATTERN = re.compile(rf"^{get_settings().deployment_namespace_prefix}-(?P<id>[0-9a-hjkmnp-tv-z]{{26}})$")

    def __init__(self, interval: timedelta = timedelta(seconds=2), timeout: float = 0.5):
        self._statuses: dict[ULID, VMStatus] = {}
        self._interval = interval
        self._timeout = timeout

    async def run(self):
        logger.info("Started VM monitor")
        async with custom_api_client() as custom_api:
            while True:
                try:
                    start = datetime.now()

                    vms = {
                        ULID.from_str(match.group("id").upper()): NeonVM.model_validate(item)
                        for item in (
                            await custom_api.list_cluster_custom_object(
                                group="vm.neon.tech",
                                version="v1",
                                plural="virtualmachines",
                            )
                        )["items"]
                        if (match := re.match(self._NAMESPACE_PATTERN, item["metadata"]["namespace"])) is not None
                    }

                    self._statuses = dict(
                        zip(
                            vms.keys(),
                            await asyncio.gather(*(check_vm_status(vm, timeout=self._timeout) for vm in vms.values())),
                            strict=True,
                        )
                    )

                    elapsed = datetime.now() - start
                    if (idle_time := self._interval - elapsed) > timedelta():
                        await asyncio.sleep(idle_time.total_seconds())
                    else:
                        logger.warning("VM monitor execution exceeded interval")

                except Exception:  # noqa: BLE001
                    logger.exception("Execution failed")

                except asyncio.CancelledError:
                    self._statuses = {}
                    logger.info("Cancelled VM monitor")
                    raise

    def status(self, id_: ULID) -> VMStatus | None:
        return self._statuses.get(id_)


vm_monitor = VMMonitor()
