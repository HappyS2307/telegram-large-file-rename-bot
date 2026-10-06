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
TRANSFER_USER_ID = None
BOT_USER_ID = None


def safe_name(name):
    return name.replace("\\", "_").replace("/", "_").strip()[:240] or "renamed_file"


def extension(name):
    return "." + name.rsplit(".", 1)[1] if "." in name else ""


async def do_rename(user_id, chat_id, source_message_id, target_name, cancel_event, status=None, source_username=None):
    async def stage(text):
        if not status:
            return
        try:
            await BOT.edit_message_text(text, chat_id, status.message_id)
        except Exception:
            pass

    await stage("Queued.\\nStep 1/3: Reading source message...")
    logging.info(
        "Rename job: reading source chat=%s message=%s target=%s",
        chat_id, source_message_id, target_name
    )
    relay_message_id = None
    try:
        source_mt = await USER_CLIENT.get_messages(chat_id, ids=source_message_id)
    except ValueError as exc:
        logging.warning("Rename job: MTProto cannot resolve source chat=%s: %s", chat_id, exc)
        source_mt = None

    # Bot API and MTProto are separate sessions. A private Bot API message
    # does not exist in the MTProto user's chat history. Relay the message
    # into the transfer account's private chat, then read that copied message
    # through MTProto. This is server-side; Railway never downloads the file.
    if not source_mt:
        if not TRANSFER_USER_ID or not BOT_USER_ID:
            raise RuntimeError("Transfer relay is not initialized.")
        try:
            relay = await BOT.copy_message(
                chat_id=TRANSFER_USER_ID,
                from_chat_id=chat_id,
                message_id=source_message_id,
            )
            relay_message_id = relay.message_id
            bot_entity = await USER_CLIENT.get_entity(BOT_USER_ID)
            source_mt = await USER_CLIENT.get_messages(
                bot_entity, ids=relay_message_id
            )
            logging.info(
                "Rename job: source relayed to transfer account message=%s",
                relay_message_id
            )
        except Exception as exc:
            raise RuntimeError(
                "Transfer account cannot access the source chat. "
                "Open the bot once from the MTProto transfer account "
                "and send /start, then retry."
            ) from exc

    if not source_mt or not getattr(source_mt, "media", None):
        raise RuntimeError("Transfer account cannot access this media file.")

    if not (source_mt.document or source_mt.video or source_mt.audio):
        raise RuntimeError("Source is not a Telegram video, file, or audio.")

    size = int(getattr(source_mt.file, "size", 0) or 0)
    logging.info(
        "Rename job: source resolved type=%s size=%s",
        type(source_mt.media).__name__, size
    )
    await stage(
        f"Queued.\\nStep 2/3: Source ready ({size / 1024 / 1024:.1f} MB).\\n"
        "Starting transfer..."
    )

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

    destination = source_mt.chat_id
    reply_to = source_mt.id
    if source_username:
        try:
            destination = await USER_CLIENT.get_entity(source_username)
            reply_to = None
        except Exception as exc:
            if relay_message_id:
                raise RuntimeError(
                    "Source sender has a username but the transfer account "
                    "cannot send to that account."
                ) from exc

    logging.info("Rename job: entering TransferEngine.rename_stream")
    try:
        result = await engine.rename_stream(
            source_mt, target_name, cancel_event,
            progress_callback=progress, thumb=thumb,
            destination=destination, reply_to=reply_to
        )
        logging.info("Rename job: TransferEngine completed successfully")
        return result
    finally:
        if relay_message_id:
            try:
                await BOT.delete_message(TRANSFER_USER_ID, relay_message_id)
            except Exception:
                logging.warning(
                    "Rename job: failed to remove relay message=%s",
                    relay_message_id
                )


async def run_job(job, status):
    async def worker():
        await do_rename(
            job.user_id, job.chat_id, job.source_message_id,
            job.target_name, job.cancel_event, status, job.source_username
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
            source.message_id, original, getattr(source.from_user, "username", None), target
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
        source.message_id, original, getattr(source.from_user, "username", None), safe_name(target)
    )
    JOBS.add(job)
    status = await message.answer(
        f"Queued.\nFrom: {original}\nTo: {job.target_name}\nStarting..."
    )
    job.task = asyncio.create_task(run_job(job, status))


async def main():
    global BOT, USER_CLIENT, TRANSFER_USER_ID, BOT_USER_ID
    logging.basicConfig(
        level=getattr(logging, CFG.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    BOT = Bot(CFG.bot_token)
    bot_me = await BOT.get_me()
    BOT_USER_ID = bot_me.id
    USER_CLIENT = TelegramClient(
        StringSession(CFG.user_session_string), CFG.api_id, CFG.api_hash
    )
    await USER_CLIENT.connect()
    if not await USER_CLIENT.is_user_authorized():
        raise RuntimeError("USER_SESSION_STRING is invalid or expired.")

    me = await USER_CLIENT.get_me()
    TRANSFER_USER_ID = me.id
    logging.info(
        "MTProto transfer account: id=%s username=%s",
        me.id, me.username
    )

    dp = Dispatcher()
    dp.include_router(router)
    try:
        await dp.start_polling(BOT)
    finally:
        await USER_CLIENT.disconnect()
        await BOT.session.close()


if __name__ == "__main__":
    asyncio.run(main())
