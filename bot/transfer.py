import asyncio
import contextlib
import os
import tempfile
import time
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.tl.custom.message import Message
from telethon.tl.types import DocumentAttributeFilename


class TransferCancelled(Exception):
    pass


@dataclass
class TransferStats:
    downloaded: int = 0
    uploaded: int = 0


class TransferEngine:
    """Sequential MTProto transfer engine.

    Phase 1: download the complete source message to a temporary file.
    Phase 2: upload that file to the destination.
    Only the temporary file path is retained; the full file is never loaded
    into RAM.
    """

    PART_SIZE = 512 * 1024

    def __init__(self, client: TelegramClient, temp_dir: str | None = None):
        self.client = client
        self.temp_dir = temp_dir or tempfile.gettempdir()
        os.makedirs(self.temp_dir, exist_ok=True)

    async def _download_to_file(self, source: Message, path: str,
                                size: int, cancel_event: asyncio.Event,
                                progress_callback=None):
        downloaded = 0
        last_update = 0.0

        with open(path, "wb") as fp:
            async for chunk in self.client.iter_download(
                source.media,
                request_size=self.PART_SIZE,
                chunk_size=self.PART_SIZE,
            ):
                if cancel_event.is_set():
                    raise TransferCancelled()
                if chunk:
                    fp.write(chunk)
                    downloaded += len(chunk)

                    now = time.monotonic()
                    if progress_callback and (
                        now - last_update >= 2 or downloaded >= size
                    ):
                        last_update = now
                        await progress_callback(
                            "download",
                            downloaded,
                            size,
                            downloaded,
                        )

        if downloaded != size:
            raise RuntimeError(
                f"Download size mismatch: expected {size}, got {downloaded}"
            )

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

        fd, temp_path = tempfile.mkstemp(
            prefix="telegram-rename-",
            suffix=".upload",
            dir=self.temp_dir,
        )
        os.close(fd)

        try:
            if cancel_event.is_set():
                raise TransferCancelled()

            # Phase 1: complete download before upload starts.
            await self._download_to_file(
                source,
                temp_path,
                size,
                cancel_event,
                progress_callback,
            )

            if cancel_event.is_set():
                raise TransferCancelled()

            # Phase 2: upload only after the local download is complete.
            last_update = 0.0

            async def on_upload(current, total):
                nonlocal last_update
                if cancel_event.is_set():
                    raise TransferCancelled()

                now = time.monotonic()
                if progress_callback and (
                    now - last_update >= 2 or current >= total
                ):
                    last_update = now
                    await progress_callback(
                        "upload",
                        int(current),
                        int(total),
                        size,
                    )

            attributes = None
            if source.video and source.document:
                attributes = []
                for attr in source.document.attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        attributes.append(
                            DocumentAttributeFilename(file_name=target_name)
                        )
                    else:
                        attributes.append(attr)

            result = await self.client.send_file(
                destination,
                temp_path,
                file_size=size,
                file_name=target_name,
                force_document=not bool(source.video),
                mime_type=getattr(source.file, "mime_type", None),
                attributes=attributes,
                supports_streaming=bool(source.video),
                thumb=thumb,
                progress_callback=on_upload,
                reply_to=reply_to,
            )

            if cancel_event.is_set():
                raise TransferCancelled()

            if progress_callback:
                await progress_callback("done", size, size, size)

            return result
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path)
