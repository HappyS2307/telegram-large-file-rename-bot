import asyncio,logging,uuid
from pathlib import Path
from aiogram import Bot,Dispatcher,Router
from aiogram.filters import Command
from aiogram.types import Message
from telethon import TelegramClient
from .config import load_config
from .security import reject_if_not_admin
from .state import JobManager,RenameJob
CFG=load_config(); JOBS=JobManager(CFG.max_concurrent_jobs); router=Router()
def safe_name(n): return n.replace('\\','_').replace('/','_').strip()[:240] or 'renamed_file'
@router.message(Command('start'))
async def start(message:Message):
    if await reject_if_not_admin(message,CFG.admin_ids):return
    await message.answer('Large File Rename Bot\n\n/rename — rename a replied file\n/cancel — cancel active job\n/help — commands')
@router.message(Command('help'))
async def help_cmd(message:Message):
    if await reject_if_not_admin(message,CFG.admin_ids):return
    await message.answer('Reply to a file with /rename NewName.ext. Large-file transfer and thumbnail layers will be enabled after the MTProto path is validated.')
@router.message(Command('cancel'))
async def cancel(message:Message):
    if await reject_if_not_admin(message,CFG.admin_ids):return
    jobs=JOBS.active_for(message.from_user.id)
    for j in jobs:JOBS.cancel(j.job_id)
    await message.answer('Cancellation requested.' if jobs else 'No active job.')
@router.message(Command('rename'))
async def rename(message:Message):
    if await reject_if_not_admin(message,CFG.admin_ids):return
    src=message.reply_to_message; target=message.text.partition(' ')[2].strip()
    if not src or not target: await message.answer('Reply to a file with: /rename NewName.ext');return
    original=src.document.file_name if src.document else None
    if not original: await message.answer('Please reply to a Telegram document/file.');return
    j=RenameJob(uuid.uuid4().hex[:12],message.from_user.id,src.message_id,original,safe_name(target));JOBS.add(j)
    await message.answer(f'Queued.\nFrom: {original}\nTo: {j.target_name}')
async def main():
    logging.basicConfig(level=getattr(logging,CFG.log_level.upper(),logging.INFO))
    Path(CFG.temp_dir).mkdir(parents=True,exist_ok=True)
    bot=Bot(CFG.bot_token);dp=Dispatcher();dp.include_router(router)
    client=TelegramClient(CFG.user_session_name,CFG.api_id,CFG.api_hash)
    logging.info('Bot started; MTProto client prepared')
    try: await dp.start_polling(bot)
    finally: await client.disconnect();await bot.session.close()
if __name__=='__main__':asyncio.run(main())
