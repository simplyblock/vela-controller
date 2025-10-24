import asyncio

from ...exceptions import VelaKubernetesError


async def wait_for_condition(
    *,
    fetch,
    is_ready,
    timeout: float,
    poll_interval: float,
    not_found_message: str | None,
    timeout_message: str,
):
    """
    Poll `fetch` until `is_ready` returns True or the timeout elapses.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while True:
        result = await fetch()
        if result is None:
            if is_ready(result):
                return result
            if loop.time() >= deadline:
                raise VelaKubernetesError(not_found_message or timeout_message)
        elif is_ready(result):
            return result

        if loop.time() >= deadline:
            raise VelaKubernetesError(timeout_message)
        await asyncio.sleep(poll_interval)
