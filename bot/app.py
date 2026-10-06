import asyncio
import json
import logging
import re
import uuid
from io import BytesIO

import aiohttp
from PIL import Image
from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_config
from .state import JobManager, RenameJob, UserSettings
from .transfer import TransferEngine, TransferCancelled

CFG = load_config()
JOBS = JobManager(CFG.max_concurrent_jobs)
SETTINGS: dict[int, UserSettings] = {}
BOT = None
HTTP = None
USER_CLIENT = None


def safe_name(name: str) -> str:
    name = (name or "").replace("\\", "_").replace("/", "_")
    name = " ".join(name.strip().split())
    return name[:240] or "renamed_file"


def extension(name: str) -> str:
    base = (name or "").rsplit("/", 1)[-1]
    return "." + base.rsplit(".", 1)[1] if "." in base else ""


def ensure_extension(name: str, original: str) -> str:
    name = safe_name(name)
    if not extension(name):
        name += extension(original)
    return name


def increment_episode(name: str) -> str:
    stem, ext = (name.rsplit(".", 1) + [""])[:2] if "." in name else (name, "")
    pattern = re.compile(r"(?i)(episode|ep|e)(\s*[-._ ]?\s*)(\d{1,5})(?!\d)")
    matches = list(pattern.finditer(stem))
    if matches:
        m = matches[-1]
        number = int(m.group(3)) + 1
        replacement = f"{m.group(1)}{m.group(2)}{number:0{len(m.group(3))}d}"
        stem = stem[:m.start()] + replacement + stem[m.end():]
    else:
        nums = list(re.finditer(r"\d+", stem))
        if nums:
            m = nums[-1]
            number = int(m.group()) + 1
            replacement = f"{number:0{len(m.group() )}d}"
            stem = stem[:m.start()] + replacement + stem[m.end():]
        else:
            stem += " 2"
    return safe_name(stem + (("." + ext) if ext else ""))


def settings_for(user_id: int) -> UserSettings:
    return SETTINGS.setdefault(user_id, UserSettings())


def keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Single Mode", "callback_data": "mode:single"},
                {"text": "Bulk Mode", "callback_data": "mode:bulk"},
            ],
            [
                {"text": "Set Name", "callback_data": "help:name"},
                {"text": "Set Thumbnail", "callback_data": "help:thumb"},
            ],
            [
                {"text": "Cancel Processing", "callback_data": "cancel:all"},
                {"text": "Status", "callback_data": "status"},
            ],
        ]
    }


def cancel_keyboard(job_id):
    return {"inline_keyboard": [[{"text": "Cancel Processing", "callback_data": f"cancel:{job_id}"}]]}


async def bot_api(method, payload=None):
    url = f"{CFG.bot_api_base}/bot{CFG.bot_token}/{method}"
    async with HTTP.post(url, json=payload or {}) as response:
        data = await response.json(content_type=None)
        if not data.get("ok"):
            raise RuntimeError(f"Bot API {method} failed: {data}")
        return data["result"]


async def edit_status(chat_id, message_id, text, markup=None):
    try:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
        if markup is not None:
            payload["reply_markup"] = json.dumps(markup)
        await bot_api("editMessageText", payload)
    except Exception:
        pass


async def send_status(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if markup is not None:
        payload["reply_markup"] = json.dumps(markup)
    return await bot_api("sendMessage", payload)


async def copy_message(chat_id, from_chat_id, message_id):
    return await bot_api("copyMessage", {
        "chat_id": chat_id,
        "from_chat_id": from_chat_id,
        "message_id": message_id,
    })


async def answer_callback(callback_id, text=""):
    try:
        await bot_api("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": text,
        })
    except Exception:
        pass


def media_info(message):
    if message.get("video"):
        f = message["video"]
        return "video", f.get("file_name") or "video.mp4", int(f.get("file_size") or 0)
    if message.get("document"):
        f = message["document"]
        return "document", f.get("file_name") or "file.bin", int(f.get("file_size") or 0)
    if message.get("audio"):
        f = message["audio"]
        return "audio", f.get("file_name") or "audio.mp3", int(f.get("file_size") or 0)
    return None, None, 0


def compute_target(user_id, original):
    st = settings_for(user_id)
    if not st.base_name:
        return None
    if st.mode == "bulk":
        if st.next_name:
            target = ensure_extension(st.next_name, original)
            st.next_name = increment_episode(target)
            return target
        target = ensure_extension(st.base_name, original)
        st.next_name = increment_episode(target)
        return target
    return ensure_extension(st.base_name, original)


async def make_thumbnail(user_id):
    st = settings_for(user_id)
    if not st.thumbnail_bridge_message_id:
        return None
    msg = await USER_CLIENT.get_messages(
        CFG.bridge_chat_id, ids=st.thumbnail_bridge_message_id
    )
    if not msg or not msg.photo:
        return None
    raw = BytesIO()
    await USER_CLIENT.download_media(msg, file=raw)
    raw.seek(0)
    image = Image.open(raw).convert("RGB")
    image.thumbnail((320, 320))
    out = BytesIO()
    quality = 82
    while quality >= 45:
        out.seek(0)
        out.truncate(0)
        image.save(out, format="JPEG", quality=quality, optimize=True)
        if out.tell() <= 19000:
            break
        quality -= 7
    out.seek(0)
    out.name = "thumb.jpg"
    return out


async def process_job(job):
    status = lambda text, markup=None: edit_status(
        job.chat_id, job.status_message_id, text, markup
    )

    thumb = None
    try:
        await status(
            f"⏳ Queued\n\n"
            f"Original: {job.original_name}\n"
            f"New name: {job.target_name}",
            cancel_keyboard(job.job_id),
        )

        bridge = await USER_CLIENT.get_messages(
            CFG.bridge_chat_id, ids=job.bridge_message_id
        )
        if not bridge:
            raise RuntimeError("Bridge message was not found.")

        size = int(getattr(bridge.file, "size", 0) or 0)
        await status(
            f"⏳ Processing\n\n"
            f"Name: {job.target_name}\n"
            f"Size: {size / 1024 / 1024:.1f} MB\n"
            f"Mode: {'Bulk' if settings_for(job.user_id).mode == 'bulk' else 'Single'}",
            cancel_keyboard(job.job_id),
        )

        thumb = await make_thumbnail(job.user_id)
        engine = TransferEngine(USER_CLIENT)

        async def progress(phase, current, total, downloaded):
            percent = min(100, int(current * 100 / total)) if total else 0
            filled = int(percent * 20 / 100)
            bar = "█" * filled + "░" * (20 - filled)
            phase_label = "Downloading" if phase == "download" else "Uploading"
            await status(
                f"⚙️ {phase_label}\n\n"
                f"[{bar}] {percent}%\n"
                f"{phase_label}: {current / 1024 / 1024:.1f} / {total / 1024 / 1024:.1f} MB\n"
                f"Output: {job.target_name}",
                cancel_keyboard(job.job_id),
            )

        output = await engine.rename_stream(
            source=bridge,
            target_name=job.target_name,
            cancel_event=job.cancel_event,
            destination=CFG.bridge_chat_id,
            reply_to=None,
            progress_callback=progress,
            thumb=thumb,
        )

        output_id = getattr(output, "id", None)
        if not output_id:
            raise RuntimeError("Output message ID was not returned.")

        await copy_message(job.chat_id, CFG.bridge_chat_id, output_id)
        await status(
            f"✅ Completed\n\n"
            f"{job.target_name}",
        )
    except (TransferCancelled, asyncio.CancelledError):
        job.status = "cancelled"
        await status("🛑 Processing cancelled.")
    except Exception as exc:
        logging.exception("Job failed: %s", job.job_id)
        await status(f"❌ Failed\n\n{type(exc).__name__}: {exc}")
    finally:
        if thumb is not None:
            thumb.close()


async def run_job(job):
    try:
        await JOBS.run(job, lambda: process_job(job))
    except Exception:
        logging.exception("Job runner failed: %s", job.job_id)


async def queue_media(user_id, chat_id, source_message_id, original_name):
    st = settings_for(user_id)
    target = compute_target(user_id, original_name)
    if not target:
        await send_status(
            chat_id,
            "⚠️ Pehle filename set karo.\n\n"
            "Example:\n/setname Solo Leveling S03E03.mp4",
            keyboard(),
        )
        return

    bridge_result = await copy_message(
        CFG.bridge_chat_id, chat_id, source_message_id
    )
    bridge_id = bridge_result["message_id"]

    status = await send_status(
        chat_id,
        f"📥 Queued\n\n{original_name}\n→ {target}",
        cancel_keyboard("pending"),
    )

    job = RenameJob(
        job_id=uuid.uuid4().hex[:12],
        user_id=user_id,
        chat_id=chat_id,
        source_message_id=source_message_id,
        bridge_message_id=bridge_id,
        original_name=original_name,
        target_name=target,
        status_message_id=status["message_id"],
    )
    # Replace the temporary cancel button with the real job id.
    await edit_status(
        chat_id,
        status["message_id"],
        f"📥 Queued\n\n{original_name}\n→ {target}",
        cancel_keyboard(job.job_id),
    )
    JOBS.add(job)
    job.task = asyncio.create_task(run_job(job))


async def handle_command(message):
    user_id = message["from"]["id"]
    chat_id = message["chat"]["id"]
    text = (message.get("text") or "").strip()
    parts = text.split(maxsplit=1)
    command = parts[0].split("@")[0].lower()

    if command == "/start":
        st = settings_for(user_id)
        await send_status(
            chat_id,
            "🎬 Auto Rename Bot\n\n"
            f"Mode: {'BULK' if st.mode == 'bulk' else 'SINGLE'}\n"
            f"Name: {st.base_name or 'Not set'}\n"
            f"Thumbnail: {'Set' if st.thumbnail_bridge_message_id else 'Not set'}\n\n"
            "1. Set filename first\n"
            "2. Set thumbnail if needed\n"
            "3. Choose Single or Bulk\n"
            "4. Send your videos/files",
            keyboard(),
        )
    elif command == "/help":
        await send_status(chat_id, await command_help_text(user_id), keyboard())
    elif command == "/setname":
        if len(parts) < 2:
            await send_status(chat_id, "Usage:\n/setname Solo Leveling S03E03.mp4")
            return
        st = settings_for(user_id)
        st.base_name = safe_name(parts[1])
        st.next_name = None
        await send_status(chat_id, f"✅ Filename set:\n{st.base_name}", keyboard())
    elif command == "/single":
        st = settings_for(user_id)
        st.mode = "single"
        st.next_name = None
        await send_status(chat_id, "✅ Single mode enabled.", keyboard())
    elif command == "/bulk":
        st = settings_for(user_id)
        st.mode = "bulk"
        st.next_name = None
        await send_status(chat_id, "✅ Bulk mode enabled. Episode numbers will auto-increment.", keyboard())
    elif command == "/setthumb":
        reply = message.get("reply_to_message")
        if not reply or not reply.get("photo"):
            await send_status(chat_id, "Reply to a photo with /setthumb.")
            return
        copied = await copy_message(CFG.bridge_chat_id, chat_id, reply["message_id"])
        settings_for(user_id).thumbnail_bridge_message_id = copied["message_id"]
        await send_status(chat_id, "✅ Thumbnail saved. It will be applied to output videos.", keyboard())
    elif command == "/cancel":
        count = JOBS.cancel_user(user_id)
        await send_status(chat_id, f"🛑 Cancellation requested for {count} active job(s).", keyboard())
    elif command == "/status":
        st = settings_for(user_id)
        active = JOBS.active_for(user_id)
        await send_status(
            chat_id,
            "📊 Status\n\n"
            f"Mode: {st.mode.upper()}\n"
            f"Name: {st.base_name or 'Not set'}\n"
            f"Thumbnail: {'Set' if st.thumbnail_bridge_message_id else 'Not set'}\n"
            f"Active jobs: {len(active)}",
            keyboard(),
        )


async def handle_callback(callback):
    user_id = callback["from"]["id"]
    if user_id not in CFG.admin_ids:
        await answer_callback(callback["id"], "Not authorized.")
        return
    data = callback.get("data", "")
    message = callback.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    if data == "mode:single":
        st = settings_for(user_id)
        st.mode = "single"
        st.next_name = None
        await answer_callback(callback["id"], "Single mode enabled.")
        await edit_status(chat_id, message["message_id"], "✅ Single mode enabled.", keyboard())
    elif data == "mode:bulk":
        st = settings_for(user_id)
        st.mode = "bulk"
        st.next_name = None
        await answer_callback(callback["id"], "Bulk mode enabled.")
        await edit_status(chat_id, message["message_id"], "✅ Bulk mode enabled. Episode numbers auto-increment.", keyboard())
    elif data == "help:name":
        await answer_callback(callback["id"])
        await send_status(chat_id, "Set the filename before sending files:\n\n/setname Solo Leveling S03E03.mp4")
    elif data == "help:thumb":
        await answer_callback(callback["id"])
        await send_status(chat_id, "Send a photo, then reply to that photo with /setthumb.")
    elif data == "cancel:all":
        count = JOBS.cancel_user(user_id)
        await answer_callback(callback["id"], f"Cancelled {count} job(s).")
    elif data == "status":
        await answer_callback(callback["id"])
        active = JOBS.active_for(user_id)
        await send_status(chat_id, f"Active jobs: {len(active)}", keyboard())
    elif data.startswith("cancel:"):
        job_id = data.split(":", 1)[1]
        if job_id == "pending":
            await answer_callback(callback["id"], "Job is being queued.")
            return
        job = JOBS.jobs.get(job_id)
        if not job or job.user_id != user_id:
            await answer_callback(callback["id"], "Job not found.")
            return
        JOBS.cancel(job_id)
        await answer_callback(callback["id"], "Cancellation requested.")


async def process_update(update):
    if "callback_query" in update:
        await handle_callback(update["callback_query"])
        return

    message = update.get("message")
    if not message:
        return
    user = message.get("from") or {}
    user_id = user.get("id")
    if user_id not in CFG.admin_ids:
        return

    if message.get("text", "").startswith("/"):
        await handle_command(message)
        return

    kind, original, size = media_info(message)
    if not kind:
        return

    st = settings_for(user_id)
    if not st.base_name:
        await send_status(
            message["chat"]["id"],
            "⚠️ Filename not set. Use /setname before sending files.",
            keyboard(),
        )
        return

    await queue_media(user_id, message["chat"]["id"], message["message_id"], original)


async def poll_bot():
    offset = 0
    while True:
        try:
            updates = await bot_api("getUpdates", {
                "offset": offset,
                "timeout": 30,
                "allowed_updates": ["message", "callback_query"],
            })
            for update in updates:
                offset = max(offset, update["update_id"] + 1)
                try:
                    await process_update(update)
                except Exception:
                    logging.exception("Update failed: %s", update.get("update_id"))
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Bot polling error")
            await asyncio.sleep(3)


async def setup_bot_commands():
    commands = [
        {"command": "start", "description": "Bot start/menu aur current settings"},
        {"command": "help", "description": "Commands aur unke functions dekho"},
        {"command": "setname", "description": "Output filename preset karo"},
        {"command": "single", "description": "Single mode ON karo"},
        {"command": "bulk", "description": "Bulk mode + episode auto-numbering ON karo"},
        {"command": "setthumb", "description": "Reply ki photo ko thumbnail set karo"},
        {"command": "cancel", "description": "Apne active processing jobs cancel karo"},
        {"command": "status", "description": "Current mode, filename aur jobs status dekho"},
    ]
    await bot_api("setMyCommands", {"commands": commands})


async def command_help_text(user_id):
    st = settings_for(user_id)
    return (
        "🎬 Auto Rename Bot — Commands\\n\\n"
        "/start — Main menu + current settings\\n"
        "/help — Ye complete command list\\n"
        "/setname <name> — Output filename preset\\n"
        "   Example: /setname Solo Leveling S03E03.mp4\\n"
        "/single — Har file ko same preset name se process karo\\n"
        "/bulk — First episode se numbering auto-increment karo\\n"
        "   Example: S03E03 → S03E04 → S03E05\\n"
        "/setthumb — Photo par reply karke thumbnail set karo\\n"
        "/cancel — Saare active/queued jobs cancel karo\\n"
        "/status — Mode, filename, thumbnail aur active jobs\\n\\n"
        f"Current mode: {st.mode.upper()}\\n"
        f"Filename: {st.base_name or 'Not set'}\\n"
        f"Thumbnail: {'Set' if st.thumbnail_bridge_message_id else 'Not set'}"
    )


async def main():
    global USER_CLIENT, HTTP
    logging.basicConfig(
        level=getattr(logging, CFG.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    HTTP = aiohttp.ClientSession()
    USER_CLIENT = TelegramClient(
        StringSession(CFG.user_session_string), CFG.api_id, CFG.api_hash
    )
    await USER_CLIENT.connect()

    if not await USER_CLIENT.is_user_authorized():
        raise RuntimeError("USER_SESSION_STRING is invalid or expired.")

    me = await USER_CLIENT.get_me()
    logging.info("MTProto backend account: id=%s username=%s", me.id, me.username)
    logging.info("Bot-first Auto Rename starting. Bridge chat=%s", CFG.bridge_chat_id)

    try:
        await bot_api("deleteWebhook", {"drop_pending_updates": False})
        await setup_bot_commands()
        await asyncio.gather(
            poll_bot(),
            USER_CLIENT.run_until_disconnected(),
        )
    finally:
        await HTTP.close()
        await USER_CLIENT.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
