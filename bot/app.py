import asyncio
import logging
import uuid
from io import BytesIO

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_config
from .security import reject_if_not_admin
from .state import JobManager, RenameJob, BulkCollector
from .transfer import TransferEngine

CFG = load_config()
JOBS = JobManager(CFG.max_concurrent_jobs)
THUMBS = {}
BULKS = {}
router = Router()
BOT = None
USER_CLIENT = None


def safe_name(name):
    return name.replace("\\", "_").replace("/", "_").strip()[:240] or "renamed_file"


def extension(name):
    return "." + name.rsplit(".", 1)[1] if "." in name else ""


async def do_rename(user_id, chat_id, source_message_id, target_name, cancel_event, status=None):
    source_mt = await USER_CLIENT.get_messages(chat_id, ids=source_message_id)
    if not source_mt or not getattr(source_mt, "media", None):
        raise RuntimeError("Transfer account cannot access this media file.")

    thumb = None
    thumb_ref = THUMBS.get(user_id)
    if thumb_ref:
        thumb_msg = await USER_CLIENT.get_messages(thumb_ref[0], ids=thumb_ref[1])
        if thumb_msg and thumb_msg.photo:
            thumb = BytesIO()
            await USER_CLIENT.download_media(thumb_msg, file=thumb)
            thumb.seek(0)

    engine = TransferEngine(USER_CLIENT)

    async def progress(current, total, downloaded):
        if not status:
            return
        percent = min(100, int(current * 100 / total)) if total else 0
        filled = int(percent * 16 / 100)
        bar = "█" * filled + "░" * (16 - filled)
        try:
            await BOT.edit_message_text(
                f"Transferring...\n[{bar}] {percent}%\n"
                f"Upload: {current / 1024 / 1024:.1f} MB / {total / 1024 / 1024:.1f} MB\n"
                f"Downloaded: {downloaded / 1024 / 1024:.1f} MB",
                chat_id, status.message_id
            )
        except Exception:
            pass

    return await engine.rename_stream(
        source_mt, target_name, cancel_event,
        progress_callback=progress, thumb=thumb
    )


async def run_job(job, status):
    async def worker():
        await do_rename(
            job.user_id, job.chat_id, job.source_message_id,
            job.target_name, job.cancel_event, status
        )
        try:
            await BOT.edit_message_text(
                f"Completed.\nFrom: {job.original_name}\nTo: {job.target_name}",
                job.chat_id, status.message_id
            )
        except Exception:
            pass

    try:
        await JOBS.run(job, worker)
    except asyncio.CancelledError:
        try:
            await BOT.edit_message_text("Cancelled.", job.chat_id, status.message_id)
        except Exception:
            pass
    except Exception as exc:
        from .transfer import TransferCancelled
        if isinstance(exc, TransferCancelled) or job.cancelled:
            try:
                await BOT.edit_message_text("Cancelled.", job.chat_id, status.message_id)
            except Exception:
                pass
            return
        logging.exception("Job failed: %s", job.job_id)
        try:
            await BOT.edit_message_text(
                f"Failed: {type(exc).__name__}: {exc}",
                job.chat_id, status.message_id
            )
        except Exception:
            pass


@router.message(Command("start"))
async def start(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    await message.answer(
        "Large File Rename Bot\n\n"
        "/rename NewName.ext — reply to a file\n"
        "/bulk PREFIX START — start bulk collector\n"
        "/add — add replied file\n"
        "/bulkdone — queue bulk files\n"
        "/setthumb — reply to photo\n"
        "/cancel — cancel active jobs\n"
        "/status — transfer account status\n"
        "/help"
    )


@router.message(Command("help"))
async def help_cmd(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    await message.answer(
        "MTProto streaming rename engine.\n"
        "Files are not stored completely on Railway disk.\n\n"
        "Bulk: /bulk Episode 1, then reply to files with /add, then /bulkdone."
    )


@router.message(Command("status"))
async def status_cmd(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    me = await USER_CLIENT.get_me()
    premium = bool(getattr(me, "premium", False))
    limit = "4 GB" if premium else "2 GB"
    await message.answer(
        f"Transfer account: {'Premium' if premium else 'Free'}\n"
        f"Telegram upload limit: {limit} per file\n"
        "Engine: MTProto streaming, 512 KB parts\n"
        "Railway disk: full file is not buffered"
    )


@router.message(Command("setthumb"))
async def setthumb(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    source = message.reply_to_message
    if not source or not source.photo:
        await message.answer("Reply to a photo with /setthumb")
        return
    THUMBS[message.from_user.id] = (message.chat.id, source.message_id)
    await message.answer("Thumbnail saved.")


@router.message(Command("bulk"))
async def bulk(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Usage: /bulk PREFIX START")
        return
    try:
        start = int(args[2])
    except ValueError:
        await message.answer("START must be a number.")
        return
    source = message.reply_to_message
    if not source or not source.document:
        await message.answer("Reply /bulk to the first file.")
        return
    BULKS[message.from_user.id] = BulkCollector(
        message.from_user.id, args[1], start
    )
    BULKS[message.from_user.id].add(source)
    await message.answer(
        f"Bulk collector started. Prefix: {args[1]} | Next: {start}\n"
        "Reply each additional file with /add. Finish with /bulkdone."
    )


@router.message(Command("add"))
async def bulk_add(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    collector = BULKS.get(message.from_user.id)
    source = message.reply_to_message
    if not collector or not source or not source.document:
        await message.answer("No active bulk collector.")
        return
    collector.add(source)
    await message.answer(f"Added #{len(collector.messages)}: {source.document.file_name}")


@router.message(Command("bulkdone"))
async def bulk_done(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    collector = BULKS.pop(message.from_user.id, None)
    if not collector:
        await message.answer("No active bulk collector.")
        return

    jobs = []
    for index, source in enumerate(collector.ordered()):
        if not source or not source.document:
            continue
        original = source.document.file_name
        target = safe_name(
            f"{collector.prefix} {collector.next_number + index:02d}{extension(original)}"
        )
        job = RenameJob(
            uuid.uuid4().hex[:12], message.from_user.id, message.chat.id,
            source.message_id, original, target
        )
        JOBS.add(job)
        jobs.append(job)

    await message.answer(f"Bulk queued: {len(jobs)} files. Processing in exact sequence.")

    for index, job in enumerate(jobs, start=1):
        status = await message.answer(
            f"Processing {index}/{len(jobs)}...\n"
            f"{job.original_name} → {job.target_name}"
        )
        await run_job(job, status)


@router.message(Command("cancel"))
async def cancel(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    jobs = JOBS.active_for(message.from_user.id)
    for job in jobs:
        JOBS.cancel(job.job_id)
    await message.answer("Cancellation requested." if jobs else "No active job.")


@router.message(Command("rename"))
async def rename(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids): return
    source = message.reply_to_message
    target = message.text.partition(" ")[2].strip()
    if not source or not target:
        await message.answer("Reply to a video/file with /rename NewName.ext")
        return
    media = source.document or source.video or source.audio
    original = (
        getattr(getattr(media, "file_name", None), "strip", lambda: None)()
        if media else None
    ) or getattr(getattr(source, "document", None), "file_name", None)
    if not original:
        original = "video.mp4" if source.video else "audio.mp3" if source.audio else None
    if not media:
        await message.answer("Please reply to a Telegram video, file, or audio.")
        return

    job = RenameJob(
        uuid.uuid4().hex[:12], message.from_user.id, message.chat.id,
        source.message_id, original, safe_name(target)
    )
    JOBS.add(job)
    status = await message.answer(
        f"Queued.\nFrom: {original}\nTo: {job.target_name}\nStarting..."
    )
    job.task = asyncio.create_task(run_job(job, status))


async def main():
    global BOT, USER_CLIENT
    logging.basicConfig(
        level=getattr(logging, CFG.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    BOT = Bot(CFG.bot_token)
    USER_CLIENT = TelegramClient(
        StringSession(CFG.user_session_string), CFG.api_id, CFG.api_hash
    )
    await USER_CLIENT.connect()
    if not await USER_CLIENT.is_user_authorized():
        raise RuntimeError("USER_SESSION_STRING is invalid or expired.")

    me = await USER_CLIENT.get_me()
    logging.info("MTProto transfer account: id=%s username=%s", me.id, me.username)

    dp = Dispatcher()
    dp.include_router(router)
    try:
        await dp.start_polling(BOT)
    finally:
        await USER_CLIENT.disconnect()
        await BOT.session.close()


if __name__ == "__main__":
    asyncio.run(main())
