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
    bot_token: str
    bot_api_base: str
    bridge_chat_id: int
    admin_ids: set[int]
    user_session_string: str
    log_level: str
    max_concurrent_jobs: int


def load_config() -> Config:
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    session = os.getenv("USER_SESSION_STRING", "").strip()
    bridge = os.getenv("BRIDGE_CHAT_ID", "").strip()

    if not api_id or not api_hash or not session:
        raise RuntimeError("API_ID, API_HASH and USER_SESSION_STRING are required")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")
    if not bridge:
        raise RuntimeError("BRIDGE_CHAT_ID is required")

    admins = parse_admins(os.getenv("ADMIN_IDS", ""))
    if not admins:
        raise RuntimeError("ADMIN_IDS must contain at least one Telegram user ID")

    base = os.getenv("BOT_API_BASE", "https://api.telegram.org").rstrip("/")

    return Config(
        api_id=int(api_id),
        api_hash=api_hash,
        bot_token=bot_token,
        bot_api_base=base,
        bridge_chat_id=int(bridge),
        admin_ids=admins,
        user_session_string=session,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_concurrent_jobs=max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "1"))),
    )
