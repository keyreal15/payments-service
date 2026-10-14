import asyncio
import logging
from typing import Protocol

logger = logging.getLogger(__name__)

STOP_TIMEOUT = 10.0


class BackgroundWorker(Protocol):
    async def run(self) -> None: ...

    def stop(self) -> None: ...


def start_worker(name: str, worker: BackgroundWorker) -> asyncio.Task[None]:
    task = asyncio.create_task(worker.run(), name=name)
    task.add_done_callback(_report_unexpected_exit)
    return task


async def stop_worker(worker: BackgroundWorker, task: asyncio.Task[None]) -> None:
    worker.stop()
    try:
        await asyncio.wait_for(task, timeout=STOP_TIMEOUT)  # по таймауту задача отменяется
    except TimeoutError:
        logger.warning("%s did not stop within %.0fs and was cancelled", task.get_name(), STOP_TIMEOUT)
    except Exception:
        logger.exception("%s finished with an error", task.get_name())


def _report_unexpected_exit(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.critical("Background task %s crashed", task.get_name(), exc_info=task.exception())
