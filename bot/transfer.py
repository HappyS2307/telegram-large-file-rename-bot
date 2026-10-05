import asyncio
import contextlib
import time
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.tl.custom.message import Message


class TransferCancelled(Exception):
    pass


@dataclass
class TransferStats:
    downloaded: int = 0


class TelegramStream:
    def __init__(self, client: TelegramClient, source: Message, part_size: int,
                 cancel_event: asyncio.Event, queue_chunks: int = 4):
        self.client = client
        self.source = source
        self.part_size = part_size
        self.cancel_event = cancel_event
        self.queue = asyncio.Queue(maxsize=queue_chunks)
        self.buffer = bytearray()
        self.eof = False
        self.producer_task = None
        self.error = None
        self.stats = TransferStats()

    @property
    def name(self):
        return getattr(self.source.file, "name", None) or "file.bin"

    async def start(self):
        self.producer_task = asyncio.create_task(self._produce())
        return self

    async def _put(self, item):
        while True:
            if self.cancel_event.is_set():
                raise TransferCancelled()
            try:
                await asyncio.wait_for(self.queue.put(item), timeout=1)
                return
            except asyncio.TimeoutError:
                continue

    async def _produce(self):
        try:
            async for chunk in self.client.iter_download(
                self.source.media,
                request_size=self.part_size,
                chunk_size=self.part_size,
            ):
                if self.cancel_event.is_set():
                    raise TransferCancelled()
                if chunk:
                    self.stats.downloaded += len(chunk)
                    await self._put(chunk)
            await self._put(None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = exc
            with contextlib.suppress(Exception):
                await self._put(exc)

    async def read(self, size=-1):
        while size < 0 or len(self.buffer) < size:
            if self.eof:
                break
            item = await self.queue.get()
            if item is None:
                self.eof = True
                break
            if isinstance(item, Exception):
                self.eof = True
                raise item
            self.buffer.extend(item)
            if size < 0:
                break

        if size < 0:
            data = bytes(self.buffer)
            self.buffer.clear()
        else:
            data = bytes(self.buffer[:size])
            del self.buffer[:size]

        if self.cancel_event.is_set():
            raise TransferCancelled()
        return data

    async def close(self):
        if self.producer_task:
            self.producer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.producer_task


class TransferEngine:
    PART_SIZE = 512 * 1024

    def __init__(self, client: TelegramClient):
        self.client = client

    async def rename_stream(self, source, target_name, cancel_event, progress_callback=None, thumb=None):
        if not source or not getattr(source, "media", None):
            raise ValueError("Source message does not contain transferable media.")

        media = source.media
        if not (source.document or source.video or source.audio):
            raise ValueError("Source message must be a Telegram video, file, or audio.")

        size = int(getattr(source.file, "size", 0) or 0)
        if size <= 0:
            raise ValueError("Telegram did not provide a valid file size.")

        parts = (size + self.PART_SIZE - 1) // self.PART_SIZE
        if parts > 8000:
            raise ValueError(
                "File is too large for the current 8000-part MTProto upload ceiling. "
                "A 4,000,000,000-byte file needs about 7,630 parts at 512 KB."
            )

        stream = await TelegramStream(
            self.client, source, self.PART_SIZE, cancel_event
        ).start()
        last_update = 0.0

        async def on_upload(current, total):
            nonlocal last_update
            now = time.monotonic()
            if progress_callback and (now - last_update >= 2 or current >= total):
                last_update = now
                await progress_callback(int(current), int(total), stream.stats.downloaded)

        try:
            if cancel_event.is_set():
                raise TransferCancelled()
            result = await self.client.send_file(
                source.chat_id,
                stream,
                file_size=size,
                file_name=target_name,
                force_document=True,
                thumb=thumb,
                progress_callback=on_upload,
                reply_to=source.id,
            )
            if cancel_event.is_set():
                raise TransferCancelled()
            if progress_callback:
                await progress_callback(size, size, stream.stats.downloaded)
            return result
        finally:
            await stream.close()
