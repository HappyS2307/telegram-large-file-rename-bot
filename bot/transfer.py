import asyncio
import contextlib
import time
from dataclasses import dataclass
from io import BytesIO

from telethon import TelegramClient
from telethon.tl.custom.message import Message


class TransferCancelled(Exception):
    pass


@dataclass
class TransferStats:
    downloaded: int = 0


class TelegramStream:
    # Telegram MTProto file requests are capped at 512 KiB per request.
    PART_SIZE = 512 * 1024

    def __init__(self, client: TelegramClient, source: Message,
                 cancel_event: asyncio.Event, queue_chunks: int = 32):
        self.client = client
        self.source = source
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
                request_size=self.PART_SIZE,
                chunk_size=self.PART_SIZE,
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
    def __init__(self, client: TelegramClient):
        self.client = client

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
            self.client, source, cancel_event
        ).start()

        last_update = 0.0

        async def on_upload(current, total):
            nonlocal last_update
            now = time.monotonic()
            if progress_callback and (now - last_update >= 2 or current >= total):
                last_update = now
                await progress_callback(
                    int(current), int(total), stream.stats.downloaded
                )

        # Preserve the original media type and video attributes. This is what
        # makes an MP4 remain a Telegram video instead of becoming a document.
        attributes = None
        if source.video and source.document:
            attributes = []
            for attr in source.document.attributes:
                from telethon.tl.types import DocumentAttributeFilename
                if isinstance(attr, DocumentAttributeFilename):
                    attributes.append(DocumentAttributeFilename(file_name=target_name))
                else:
                    attributes.append(attr)

        try:
            if cancel_event.is_set():
                raise TransferCancelled()

            result = await self.client.send_file(
                destination,
                stream,
                file_size=size,
                file_name=target_name,
                force_document=not bool(source.video),
                mime_type=getattr(source.file, "mime_type", None),
                attributes=attributes,
                supports_streaming=bool(
                    source.video and any(
                        getattr(a, "supports_streaming", False)
                        for a in (source.document.attributes if source.document else [])
                    )
                ),
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
