import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()
def parse_admins(v): return {int(x.strip()) for x in v.split(',') if x.strip()}
@dataclass(frozen=True)
class Config:
    api_id:int; api_hash:str; bot_token:str; admin_ids:set[int]; user_session_name:str; log_level:str; max_concurrent_jobs:int; chunk_size_mb:int; temp_dir:str
def load_config():
    a=os.getenv('API_ID','').strip(); h=os.getenv('API_HASH','').strip(); t=os.getenv('BOT_TOKEN','').strip()
    if not a or not h or not t: raise RuntimeError('API_ID, API_HASH and BOT_TOKEN are required')
    return Config(int(a),h,t,parse_admins(os.getenv('ADMIN_IDS','')),os.getenv('USER_SESSION_NAME','rename_user'),os.getenv('LOG_LEVEL','INFO'),max(1,int(os.getenv('MAX_CONCURRENT_JOBS','1'))),max(1,int(os.getenv('CHUNK_SIZE_MB','16'))),os.getenv('TEMP_DIR','/tmp/telegram-rename'))
