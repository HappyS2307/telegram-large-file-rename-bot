from dataclasses import dataclass, field
import asyncio


@dataclass
class RenameJob:
    job_id: str
    user_id: int
    chat_id: int
    source_message_id: int
    original_name: str
    target_name: str
    cancelled: bool = False
    status: str = "queued"
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = field(default=None, repr=False)


class JobManager:
    def __init__(self, max_concurrent: int):
        self.jobs = {}
        self._sem = asyncio.Semaphore(max_concurrent)

    def add(self, job):
        self.jobs[job.job_id] = job

    def cancel(self, job_id):
        job = self.jobs.get(job_id)
        if not job:
            return False
        job.cancelled = True
        job.cancel_event.set()
        return True

    def active_for(self, user_id):
        return [
            j for j in self.jobs.values()
            if j.user_id == user_id and j.status in {"queued", "running"}
        ]

    async def run(self, job, worker):
        async with self._sem:
            if job.cancelled:
                job.status = "cancelled"
                return
            job.status = "running"
            try:
                await worker()
                job.status = "done"
            except asyncio.CancelledError:
                job.status = "cancelled"
                raise
            except Exception:
                job.status = "failed"
                raise
