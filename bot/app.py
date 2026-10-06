import asyncio
import logging
import uuid
from io import BytesIO

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from .config import load_config
from .state import JobManager, RenameJob
from .transfer import TransferEngine, TransferCancelled

CFG = load_config()
JOBS = JobManager(CFG.max_concurrent_jobs)
THUMBS = {}
USER_CLIENT = None


def safe_name(name: str) -> str:
    name = (name or "").replace("\\", "_").replace("/", "_")
    name = " ".join(name.strip().split())
    return name[:240] or "renamed_file"


def extension(name: str) -> str:
    base = (name or "").rsplit("/", 1)[-1]
    return "." + base.rsplit(".", 1)[1] if "." in base else ""


def target_from_caption(caption: str, original: str) -> str | None:
    text = (caption or "").strip()
    if not text:
        return None
    if text.lower().startswith("/rename"):
        text = text[7:].strip()
    if not text:
        return None
    name = safe_name(text)
    if not extension(name):
        name += extension(original)
    return name


def is_media(message):
    return bool(message and (message.document or message.video or message.audio))


def original_name(message):
    if getattr(message, "file", None):
        return getattr(message.file, "name", None) or "file.bin"
    if message.video:
        return "video.mp4"
    if message.audio:
        return "audio.mp3"
    return "file.bin"


async def load_thumbnail(user_id):
    ref = THUMBS.get(user_id)
    if not ref:
        return None
    msg = await USER_CLIENT.get_messages(ref[0], ids=ref[1])
    if not msg or not msg.photo:
        return None
    thumb = BytesIO()
    await USER_CLIENT.download_media(msg, file=thumb)
    thumb.seek(0)
    return thumb


async def process_job(job: RenameJob, status_message):
    async def stage(text):
        try:
            await status_message.edit(text)
        except Exception:
            pass

    await stage(
        f"Queued.\\nSource: {job.original_name}\\nTarget: {job.target_name}\\nStarting..."
    )

    source = await USER_CLIENT.get_messages(job.chat_id, ids=job.source_message_id)
    if not source or not is_media(source):
        raise RuntimeError("Source message no longer contains a transferable file.")

    size = int(getattr(source.file, "size", 0) or 0)
    if size <= 0:
        raise RuntimeError("Telegram did not provide a valid file size.")

    await stage(
        f"Preparing...\\n{job.original_name}\\nSize: {size / 1024 / 1024:.1f} MB"
    )

    thumb = await load_thumbnail(job.user_id)
    engine = TransferEngine(USER_CLIENT, CFG.chunk_size_mb)

    async def progress(current, total, downloaded):
        percent = min(100, int(current * 100 / total)) if total else 0
        filled = int(percent * 18 / 100)
        bar = "█" * filled + "░" * (18 - filled)
        await stage(
            f"Renaming / Uploading...\\n[{bar}] {percent}%\\n"
            f"Upload: {current / 1024 / 1024:.1f} / {total / 1024 / 1024:.1f} MB\\n"
            f"Download: {downloaded / 1024 / 1024:.1f} MB"
        )

    await engine.rename_stream(
        source=source,
        target_name=job.target_name,
        cancel_event=job.cancel_event,
        destination=job.chat_id,
        reply_to=job.source_message_id,
        progress_callback=progress,
        thumb=thumb,
    )

    await stage(
        f"Completed.\\nFrom: {job.original_name}\\nTo: {job.target_name}"
    )


async def run_job(job, status_message):
    try:
        await JOBS.run(job, lambda: process_job(job, status_message))
    except asyncio.CancelledError:
        try:
            await status_message.edit("Cancelled.")
        except Exception:
            pass
    except TransferCancelled:
        try:
            await status_message.edit("Cancelled.")
        except Exception:
            pass
    except Exception as exc:
        logging.exception("Job failed: %s", job.job_id)
        try:
            await status_message.edit(f"Failed: {type(exc).__name__}: {exc}")
        except Exception:
            pass


def make_job(message, target):
    return RenameJob(
        job_id=uuid.uuid4().hex[:12],
        user_id=message.sender_id,
        chat_id=message.chat_id,
        source_message_id=message.id,
        original_name=original_name(message),
        target_name=safe_name(target),
    )


async def queue_job(source, target, status=None):
    job = make_job(source, target)
    JOBS.add(job)
    if status is None:
        status = await source.reply(
            f"Queued.\\nFrom: {job.original_name}\\nTo: {job.target_name}"
        )
    job.task = asyncio.create_task(run_job(job, status))
    return job


async def handle_command(event):
    text = (event.raw_text or "").strip()
    lower = text.lower()

    if lower in {"/start", "/help"}:
        await event.reply(
            "Auto Rename is ON.\\n\\n"
            "Send a video/file/audio with the desired filename as its caption.\\n"
            "Example: My Anime S01E01.mp4\\n\\n"
            "Or reply to a file with /rename NewName.ext\\n"
            "/setthumb — reply to a photo\\n"
            "/cancel — cancel your active jobs\\n"
            "/status — engine status"
        )
        return True

    if lower == "/status":
        me = await USER_CLIENT.get_me()
        await event.reply(
            "Auto Rename: ON\\n"
            f"Transfer account: {me.first_name or ''}\\n"
            f"Premium: {'Yes' if getattr(me, 'premium', False) else 'No'}\\n"
            f"Engine: MTProto streaming, {CFG.chunk_size_mb} MB chunks\\n"
            "Railway disk: no full-file buffering\\n"
            f"Active jobs: {len(JOBS.active_for(event.sender_id))}"
        )
        return True

    if lower == "/cancel":
        jobs = JOBS.active_for(event.sender_id)
        for job in jobs:
            JOBS.cancel(job.job_id)
        await event.reply(
            f"Cancellation requested for {len(jobs)} job(s)." if jobs else "No active job."
        )
        return True

    if lower.startswith("/setthumb"):
        source = await event.get_reply_message()
        if not source or not source.photo:
            await event.reply("Reply to a photo with /setthumb")
        else:
            THUMBS[event.sender_id] = (source.chat_id, source.id)
            await event.reply("Thumbnail saved for auto-renaming.")
        return True

    if lower.startswith("/rename"):
        source = await event.get_reply_message()
        target = text[7:].strip()
        if not source or not target or not is_media(source):
            await event.reply("Reply to a video/file/audio with /rename NewName.ext")
            return True
        status = await event.reply("Queued...")
        job = await queue_job(source, safe_name(target), status)
        logging.info("Manual rename queued job=%s", job.job_id)
        return True

    return False


async def incoming_handler(event):
    if event.sender_id not in CFG.admin_ids:
        return

    message = event.message
    text = (event.raw_text or "").strip()

    if text.startswith("/"):
        await handle_command(event)
        return

    if not is_media(message):
        return

    original = original_name(message)
    target = target_from_caption(text, original)
    if not target:
        await event.reply(
            "File received. Add the desired filename as the caption and resend.\\n"
            f"Example: {original}"
        )
        return

    logging.info(
        "Auto rename received chat=%s message=%s from=%s: %s -> %s",
        message.chat_id, message.id, event.sender_id, original, target
    )
    await queue_job(message, target)


async def main():
    global USER_CLIENT

    logging.basicConfig(
        level=getattr(CFG.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    USER_CLIENT = TelegramClient(
        StringSession(CFG.user_session_string), CFG.api_id, CFG.api_hash
    )
    await USER_CLIENT.connect()

    if not await USER_CLIENT.is_user_authorized():
        raise RuntimeError("USER_SESSION_STRING is invalid or expired.")

    me = await USER_CLIENT.get_me()
    logging.info("Auto Rename account: id=%s username=%s", me.id, me.username)
    logging.info("Auto Rename is ready; waiting for admin media")

    USER_CLIENT.add_event_handler(
        incoming_handler, events.NewMessage(incoming=True)
    )
    await USER_CLIENT.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
