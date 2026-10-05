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
from .state import JobManager, RenameJob
from .transfer import TransferEngine

CFG = load_config()
JOBS = JobManager(CFG.max_concurrent_jobs)
THUMBS = {}
router = Router()


def safe_name(name):
    return name.replace("\\", "_").replace("/", "_").strip()[:240] or "renamed_file"


@router.message(Command("start"))
async def start(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return
    await message.answer(
        "Large File Rename Bot\n\n"
        "/rename NewName.ext — reply to a file\n"
        "/bulk PREFIX START — bulk mode\n"
        "/setthumb — reply to a photo\n"
        "/cancel — cancel active job\n"
        "/help — commands"
    )


@router.message(Command("help"))
async def help_cmd(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return
    await message.answer(
        "MTProto streaming engine enabled.\n"
        "Complete files are not stored on Railway disk."
    )


@router.message(Command("setthumb"))
async def setthumb(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return
    source = message.reply_to_message
    if not source or not source.photo:
        await message.answer("Reply to a photo with /setthumb")
        return
    THUMBS[message.from_user.id] = (message.chat.id, source.message_id)
    await message.answer("Thumbnail saved. It will be used for the next /rename job.")

@router.message(Command("bulk"))
async def bulk(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer(
            "Usage: /bulk PREFIX START\n"
            "Reply this command to the first file, then send the remaining files together.\n"
            "Example: /bulk Episode 1"
        )
        return
    prefix = args[1]
    try:
        start = int(args[2])
    except ValueError:
        await message.answer("START must be a number.")
        return
    source = message.reply_to_message
    if not source or not source.document:
        await message.answer("Reply /bulk to the first file.")
        return
    await message.answer(
        "Bulk mode is reserved for the queued multi-file collector. "
        "Use /rename for a single file."
    )

@router.message(Command("cancel"))
async def cancel(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return
    jobs = JOBS.active_for(message.from_user.id)
    for job in jobs:
        JOBS.cancel(job.job_id)
    await message.answer("Cancellation requested." if jobs else "No active job.")


@router.message(Command("rename"))
async def rename(message: Message):
    if await reject_if_not_admin(message, CFG.admin_ids):
        return

    source = message.reply_to_message
    target = message.text.partition(" ")[2].strip()
    if not source or not target:
        await message.answer("Reply to a file with: /rename NewName.ext")
        return

    original = source.document.file_name if source.document else None
    if not original:
        await message.answer("Please reply to a Telegram document/file.")
        return

    job = RenameJob(
        uuid.uuid4().hex[:12],
        message.from_user.id,
        message.chat.id,
        source.message_id,
        original,
        safe_name(target),
    )
    JOBS.add(job)
    status = await message.answer(
        f"Queued.\nFrom: {original}\nTo: {job.target_name}\n\nStarting MTProto transfer..."
    )

    async def worker():
        source_mt = await USER_CLIENT.get_messages(
            message.chat.id, ids=source.message_id
        )
        if not source_mt or not source_mt.document:
            raise RuntimeError("Transfer account cannot access this source message.")

        engine = TransferEngine(USER_CLIENT)
        thumb = None
        thumb_ref = THUMBS.get(message.from_user.id)
        if thumb_ref:
            thumb_msg = await USER_CLIENT.get_messages(thumb_ref[0], ids=thumb_ref[1])
            if thumb_msg and thumb_msg.photo:
                thumb = BytesIO()
                await USER_CLIENT.download_media(thumb_msg, file=thumb)
                thumb.seek(0)

        async def progress(current, total, downloaded):
            percent = min(100, int(current * 100 / total)) if total else 0
            filled = int(percent * 12 / 100)
            bar = "█" * filled + "░" * (12 - filled)
            try:
                await BOT.edit_message_text(
                    f"Transferring...\n[{bar}] {percent}%\n"
                    f"Downloaded: {downloaded / 1024 / 1024:.1f} MB",
                    message.chat.id, status.message_id
                )
            except Exception:
                pass

        result = await engine.rename_stream(
            source_mt, job.target_name, job.cancel_event,
            progress_callback=progress, thumb=thumb
        )

        await USER_CLIENT.send_message(message.chat.id, result)
        await BOT.edit_message_text(
            f"Completed.\nFrom: {original}\nTo: {job.target_name}",
            message.chat.id,
            status.message_id,
        )

    async def runner():
        try:
            await JOBS.run(job, worker)
        except asyncio.CancelledError:
            try:
                await BOT.edit_message_text(
                    "Cancelled.", message.chat.id, status.message_id
                )
            except Exception:
                pass
        except Exception as exc:
            logging.exception("Rename failed: %s", job.job_id)
            try:
                await BOT.edit_message_text(
                    f"Failed: {type(exc).__name__}: {exc}",
                    message.chat.id,
                    status.message_id,
                )
            except Exception:
                pass

    job.task = asyncio.create_task(runner())


async def main():
    global BOT, USER_CLIENT

    logging.basicConfig(
        level=getattr(CFG.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    BOT = Bot(CFG.bot_token)
    USER_CLIENT = TelegramClient(
        StringSession(CFG.user_session_string),
        CFG.api_id,
        CFG.api_hash,
    )

    await USER_CLIENT.connect()
    if not await USER_CLIENT.is_user_authorized():
        raise RuntimeError("USER_SESSION_STRING is invalid or expired.")

    dp = Dispatcher()
    dp.include_router(router)

    try:
        await dp.start_polling(BOT)
    finally:
        await USER_CLIENT.disconnect()
        await BOT.session.close()


if __name__ == "__main__":
    asyncio.run(main())
