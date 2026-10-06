import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def parse_admins(value: str) -> set[int]:
    return {int(x.strip()) for x in value.split(",") if x.strip()}


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    admin_ids: set[int]
    user_session_string: str
    log_level: str
    max_concurrent_jobs: int
    chunk_size_mb: int


def load_config() -> Config:
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    session = os.getenv("USER_SESSION_STRING", "").strip()

    if not api_id or not api_hash or not session:
        raise RuntimeError("API_ID, API_HASH and USER_SESSION_STRING are required")

    admins = parse_admins(os.getenv("ADMIN_IDS", ""))
    if not admins:
        raise RuntimeError("ADMIN_IDS must contain at least one Telegram user ID")

    return Config(
        api_id=int(api_id),
        api_hash=api_hash,
        admin_ids=admins,
        user_session_string=session,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_concurrent_jobs=max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "1"))),
        chunk_size_mb=max(1, min(512, int(os.getenv("CHUNK_SIZE_MB", "16")))),
    )
