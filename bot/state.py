from dataclasses import dataclass, field
import asyncio

@dataclass
class RenameJob:
    job_id: str
    user_id: int
    chat_id: int
    source_message_id: int
    original_name: str
    source_username: str | None = None
    target_name: str | None = None
    thumbnail_message_id: int | None = None
    cancelled: bool = False
    status: str = "queued"
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = field(default=None, repr=False)

class BulkCollector:
    def __init__(self, user_id, prefix, start, timeout=30):
        self.user_id = user_id
        self.prefix = prefix
        self.next_number = start
        self.timeout = timeout
        self.messages = []
        self.task = None
        self.done = asyncio.Event()

    def add(self, message):
        self.messages.append(message)

    def stop(self):
        self.done.set()

    def ordered(self):
        return list(self.messages)


class JobManager:
    def __init__(self, max_concurrent):
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
        if job.task and not job.task.done():
            job.task.cancel()
        return True

    def active_for(self, user_id):
        return [j for j in self.jobs.values()
                if j.user_id == user_id and j.status in {"queued", "running"}]

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
