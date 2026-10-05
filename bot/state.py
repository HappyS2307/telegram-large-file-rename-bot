from dataclasses import dataclass
import asyncio
@dataclass
class RenameJob:
    job_id:str; user_id:int; source_message_id:int; original_name:str; target_name:str|None=None; thumbnail_message_id:int|None=None; cancelled:bool=False; status:str='queued'
class JobManager:
    def __init__(self,max_concurrent): self.jobs={}; self._sem=asyncio.Semaphore(max_concurrent)
    def add(self,job): self.jobs[job.job_id]=job
    def cancel(self,job_id):
        j=self.jobs.get(job_id)
        if not j:return False
        j.cancelled=True;return True
    def active_for(self,user_id): return [j for j in self.jobs.values() if j.user_id==user_id and j.status in {'queued','running'}]
