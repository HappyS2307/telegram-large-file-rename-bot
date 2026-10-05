import asyncio
import logging
import uuid

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
        result = await engine.rename_stream(
            source_mt, job.target_name, job.cancel_event
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
