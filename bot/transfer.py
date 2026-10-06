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
    def __init__(
        self,
        client: TelegramClient,
        source: Message,
        part_size: int,
        cancel_event: asyncio.Event,
        queue_chunks: int = 4,
    ):
        self.client = client
        self.source = source
        self.part_size = part_size
        self.cancel_event = cancel_event
        self.queue = asyncio.Queue(maxsize=queue_chunks)
        self.buffer = bytearray()
        self.eof = False
        self.producer_task = None
        self.stats = TransferStats()

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
            with contextlib.suppress(Exception):
                await self._put(exc)

    async def read(self, size=-1):
        while (size < 0 or len(self.buffer) < size) and not self.eof:
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
    def __init__(self, client: TelegramClient, part_size_mb: int = 16):
        self.client = client
        self.part_size = min(512, max(1, part_size_mb)) * 1024 * 1024

    async def rename_stream(
        self,
        source,
        target_name,
        cancel_event,
        destination,
        reply_to=None,
        progress_callback=None,
        thumb=None,
    ):
        if not source or not getattr(source, "media", None):
            raise ValueError("Source message does not contain transferable media.")

        if not (source.document or source.video or source.audio):
            raise ValueError("Source must be a Telegram video, file, or audio.")

        size = int(getattr(source.file, "size", 0) or 0)
        if size <= 0:
            raise ValueError("Telegram did not provide a valid file size.")

        stream = await TelegramStream(
            self.client, source, self.part_size, cancel_event
        ).start()

        last_update = 0.0

        async def on_upload(current, total):
            nonlocal last_update
            now = time.monotonic()
            if progress_callback and (now - last_update >= 2 or current >= total):
                last_update = now
                await progress_callback(
                    int(current),
                    int(total),
                    stream.stats.downloaded,
                )

        try:
            if cancel_event.is_set():
                raise TransferCancelled()

            result = await self.client.send_file(
                destination,
                stream,
                file_size=size,
                file_name=target_name,
                force_document=True,
                thumb=thumb,
                progress_callback=on_upload,
                reply_to=reply_to,
            )

            if cancel_event.is_set():
                raise TransferCancelled()

            if progress_callback:
                await progress_callback(size, size, stream.stats.downloaded)

            return result
        finally:
            await stream.close()
