import asyncio
import logging
import re
from datetime import datetime, timedelta

from kubernetes_asyncio import client
from pydantic import BaseModel
from ulid import ULID

from ..kubernetes.neonvm import NeonVM, Phase

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


class VMMonitor:
    _NAMESPACE_PATTERN = re.compile(r"^vela-(?P<id>[0-9a-hjkmnp-tv-z]{26})$")

    def __init__(self, interval: timedelta = timedelta(5), timeout: float = 0.5):
        self._statuses: dict[ULID, VMStatus] = {}
        self._interval = interval
        self._timeout = timeout

    async def _check_status(self, vm: NeonVM):
        phase = vm.status.phase if vm.status is not None else None
        vm_ip = vm.status.pod_ip if vm.status is not None else None
        online = vm_ip is not None and phase in {Phase.running, Phase.pre_migrating, Phase.migrating, Phase.scaling}
        ports = {port.name: port.port for port in vm.spec.guest.ports if port.protocol == "TCP"}

        return VMStatus(
            phase=phase,
            services=dict(
                zip(
                    ports.keys(),
                    await asyncio.gather(*(_check_port(vm_ip, port, timeout=self._timeout) for port in ports.values())),
                    strict=True,
                )
            )
            if online
            else None,
        )

    async def run(self):
        logger.info("Started VM monitor")
        async with client.ApiClient() as api:
            custom_api = client.CustomObjectsApi(api)
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
                        ).items
                        if (match := re.match(self._NAMESPACE_PATTERN, item["metadata"]["namespace"])) is not None
                    }

                    self._statuses = dict(
                        zip(
                            vms.keys(),
                            await asyncio.gather(*(self._check_status(vm) for vm in vms.values())),
                            strict=True,
                        )
                    )

                    elapsed = datetime.now() - start
                    if (idle_time := self._interval - elapsed) > timedelta():
                        await asyncio.sleep(idle_time.total_seconds())
                    else:
                        logger.warning("VM monitor execution exceeded interval")

                except asyncio.CancelledError:
                    self._statuses = {}
                    logger.info("Cancelled VM monitor")
                    raise
                except Exception:  # noqa: BLE001
                    pass

    def status(self, id_: ULID) -> VMStatus | None:
        return self._statuses.get(id_)


vm_monitor = VMMonitor()
