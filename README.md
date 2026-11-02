# Pixeldrain -> Telegram on Render

This package keeps the original Colab uploader workflow and restores the original per-topic progress database naming, with Neon used as a durable remote copy.

## Progress tracking: one database per topic name

The local SQLite filename follows the original Colab convention exactly:

`<clean_topic_name>_pd_to_tg_progress.db`

For example, topic name `Jane_Doe` produces:

`jane_doe_pd_to_tg_progress.db`

The service also creates a **separate PostgreSQL database in Neon** with the matching name (without `.db`):

`jane_doe_pd_to_tg_progress`

Inside each local SQLite file and its matching Neon database, the original `progress` table columns and `pixeldrain_id` primary key are retained. The `topic_map` table is stored alongside it. Different Render services using different topic names therefore connect to different Neon databases rather than sharing one global progress table.

## How syncing works

1. Every progress change is committed to local SQLite first.
2. The uploader does not wait for Neon when marking a file as pending, downloaded, uploaded, or failed.
3. A background task attempts a full local-to-Neon sync every 20 seconds.
4. Each sync opens a fresh PostgreSQL connection, writes the local rows in a transaction, and retries failed connections up to three times.
5. At startup, Neon progress and topic mappings are restored into local SQLite. Local records are then pushed back to Neon, so records left unsynchronised by a previous temporary outage are retried.
6. The batch performs a final sync when processing finishes.

If Neon disconnects, the uploader continues using its local SQLite database. Once Neon is reachable again, a later sync retries the outstanding local state.

## Neon environment variables

Set both connection strings in Render:

- `DATABASE_URL` — Neon direct connection string. Used to create the per-topic databases and for direct database connections.
- `DATABASE_URL_POOLED` — Neon pooled connection string. Kept as a fallback if the direct URL is unavailable.

The Neon role must have permission to create databases. The service attempts to create the matching per-topic database automatically. If Neon rejects `CREATE DATABASE`, the log will say so and local SQLite tracking will continue, but remote syncing for that topic will not work until the matching database is created and accessible.

## Other Render environment variables

Required:

- `TG_API_ID`
- `TG_API_HASH`
- `TG_BOT_TOKEN`
- `DESTINATION_CHAT_ID`
- `PIXELDRAIN_LIST_URL` (or `SOURCE_DB_OR_JSON`)
- `PIXELDRAIN_API_KEY` (if required by Pixeldrain)
- `DATABASE_URL` and/or `DATABASE_URL_POOLED`

Optional:

- `PROGRESS_DIR` — defaults to `/tmp/pd_to_tg_tracking`
- `TEMP_DIR` — defaults to `/tmp/pd_to_tg_temp`
- `SESSION_FILE_NAME` — defaults to `/tmp/telegram_bot.session`
- `MAX_FILES_TO_PROCESS` — `0` means all files
- `FILTER_USERNAME`
- `FILTER_SERVICE`
- `DOWNLOAD_WORKERS` — default `2`
- `SKIP_ALREADY_DONE_BY_THIS_TOOL` — default `true`

## Render storage limitation

On a free Render instance, `/tmp` is ephemeral. Local SQLite protects progress during a Neon outage while that instance remains alive, but an instance restart/redeploy can erase local changes that have not yet synced. The 20-second sync interval reduces that exposure; it cannot eliminate it. A persistent disk mounted at `PROGRESS_DIR` would preserve local SQLite across restarts where the Render plan supports it.

## Batch and health behavior

The batch starts once when the service process starts. The HTTP server exposes `/` and `/health` for Render health checks and stays alive after the batch finishes. Restart/redeploy the service to run the batch again. An uptime ping keeps the web service active where possible, but does not guarantee uninterrupted execution.
