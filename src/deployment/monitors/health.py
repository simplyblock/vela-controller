import asyncio
import logging
import re
from datetime import datetime, timedelta

from kubernetes_asyncio import client, watch
from pydantic import BaseModel
from ulid import ULID

from ..kubernetes.neonvm import NeonVM, Phase

logger = logging.getLogger(__name__)

NAMESPACE_PATTERN = re.compile(r"^vela-(?P<id>[0-9a-hjkmnp-tv-z]{26})$")


async def _check_port(ip, port, timeout=1.0):
    try:
        await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=timeout)
        return True
    except (TimeoutError, ConnectionRefusedError, OSError) as e:
        logger.debug(f"Health check failed for {ip}:{port} - {e}")
        return False


def _is_online(phase: Phase | None):
    return phase is not None and phase in {Phase.running, Phase.pre_migrating, Phase.migrating, Phase.scaling}


class VMStatus(BaseModel):
    phase: Phase | None
    services: dict[str, bool] | None = None


class VMMonitor:
    def __init__(self, interval: timedelta = timedelta(5), timeout: float = 0.5):
        self._statuses: dict[ULID, VMStatus] = {}
        self._interval = interval
        self._timeout = timeout

    async def run(self):
        monitors: dict[ULID, asyncio.Task] = {}
        async with asyncio.TaskGroup() as tg, client.ApiClient() as api:
            custom_api = client.CustomObjectsApi(api)

            async for event in watch.Watch().stream(
                custom_api.list_cluster_custom_object, group="vm.neon.tech", version="v1", plural="virtualmachines"
            ):
                if (match := re.match(NAMESPACE_PATTERN, event["object"]["metadata"]["namespace"])) is None:
                    continue

                id_ = ULID.from_str(match.group("id").upper())
                vm = NeonVM.model_validate(event["object"])
                phase = vm.status.phase if vm.status is not None else None

                if id_ not in self._statuses:
                    self._statuses[id_] = VMStatus(phase=phase)
                else:
                    self._statuses[id_].phase = phase

                monitoring = id_ in monitors

                # Ideally we'd use the overlay network (extraNetIP),
                # that is not routed reachable from this pod though.
                vm_ip = vm.status.pod_ip if vm.status is not None else None
                if vm_ip is None:
                    continue

                if ((not monitoring) and event["type"] in {"ADDED", "MODIFIED"}) and _is_online(phase):
                    monitors[id_] = tg.create_task(
                        self._monitor_vm(
                            id_,
                            vm_ip,
                            services={port.name: port.port for port in vm.spec.guest.ports if port.protocol == "TCP"},
                        ),
                        name=f"VM {id_} monitor",
                    )
                elif monitoring and (
                    (event["type"] == "DELETED") or ((event["type"] == "MODIFIED") and not _is_online(phase))
                ):
                    monitors[id_].cancel()
                    del monitors[id_]

                if event["type"] == "DELETED":
                    del self._statuses[id_]

    def status(self, id_: ULID) -> VMStatus | None:
        return self._statuses.get(id_)

    async def _monitor_vm(self, id_: ULID, ip: str, services: dict[str, int]):
        logger.info(f"Started VM {id_} monitor")
        try:
            while True:
                start = datetime.now()

                self._statuses[id_].services = dict(
                    zip(
                        services.keys(),
                        await asyncio.gather(
                            *(_check_port(ip, port, timeout=self._timeout) for port in services.values())
                        ),
                        strict=False,
                    )
                )

                elapsed = datetime.now() - start
                if (idle_time := self._interval - elapsed) > timedelta():
                    await asyncio.sleep(idle_time.total_seconds())
                else:
                    logger.warning(f"VM {id_} monitor execution exceeded interval")

        except asyncio.CancelledError:
            logger.info(f"Stopped VM {id_} monitor")
            if id_ in self._statuses:  # When the VM is deleted this may already be gone
                self._statuses[id_].services = {}
            raise


vm_monitor = VMMonitor()
