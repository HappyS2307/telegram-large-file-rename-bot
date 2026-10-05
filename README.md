# Telegram Large File Rename Bot

MTProto-based admin bot for large-file rename and thumbnail workflows.

Planned: 4GB+ transfer support, rename, custom thumbnails, bulk sequence naming, progress, cancellation, retries, and multiple admins.

The implementation will use streaming/chunked I/O and will not assume a 2GB Railway disk can hold a complete 4GB file.

Never commit `.env` or Telegram session files.
