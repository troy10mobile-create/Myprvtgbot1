# Pixeldrain -> Telegram on Render

This is a minimal environment migration of the supplied Google Colab batch script.
The Pixeldrain list is processed once when the Render service starts. The HTTP
server only provides `/` and `/health` so Render can see that the Web Service is
alive; it does not change the Telegram/Pixeldrain workflow.

## Files

- `main.py` — adapted batch script
- `requirements.txt` — Python dependencies
- `Dockerfile` — installs FFmpeg and Python dependencies

## Render setup

Create a **Web Service** from this public GitHub repository.

- Runtime/environment: Docker
- Instance type: Free
- Dockerfile path: `./Dockerfile`
- Health check path (optional): `/health`

Set these environment variables in Render:

| Variable | Value |
|---|---|
| `TG_API_ID` | Your Telegram API ID |
| `TG_API_HASH` | Your Telegram API hash |
| `TG_BOT_TOKEN` | A newly generated bot token |
| `DESTINATION_CHAT_ID` | Destination supergroup/forum ID |
| `PIXELDRAIN_LIST_URL` | Your Pixeldrain list URL |
| `PIXELDRAIN_API_KEY` | Pixeldrain API key, if required |

Optional variables:

- `MAX_FILES_TO_PROCESS` — `0` means all files
- `FILTER_USERNAME`
- `FILTER_SERVICE`
- `DOWNLOAD_WORKERS` — default `2`
- `SKIP_ALREADY_DONE_BY_THIS_TOOL` — default `true`
- `SOURCE_DB_OR_JSON` — path to a DB/JSON file included in the repository/container

Do not put secrets in `main.py`, README, or GitHub commits.

## Important Free-tier behavior

Render Free Web Services spin down after 15 minutes without inbound traffic.
They have 750 free instance-hours per workspace per calendar month and an
ephemeral filesystem. Downloaded files and the Telegram session file can
disappear on restart, redeploy, or spin-down. Upload progress and Telegram topic
mappings are stored in Neon PostgreSQL instead of the local filesystem.

If you want to reduce idle spin-down during a long batch, an external uptime
checker can request `https://YOUR-SERVICE.onrender.com/health` every 10-14
minutes. This does **not** guarantee uninterrupted execution: Render can still
restart the service, and the workspace's monthly free-hour/bandwidth limits
still apply. The external uptime checker does not provide persistent storage;
Neon is what preserves the upload tracking records.

## Batch behavior

The batch begins automatically when the service process starts. After the batch
finishes, the health server remains running. To run the batch again, manually
restart/redeploy the service. The Neon progress records remain available after
that restart, so successfully uploaded files can still be skipped.


## Neon PostgreSQL progress tracking

Add `DATABASE_URL_POOLED` to the Render environment using Neon's pooled connection string. You may also add `DATABASE_URL` as a direct-connection fallback. On startup, the app creates the `progress` and `topic_map` tables if they do not already exist. Existing SQLite progress files are not automatically imported; the Neon database starts with a clean tracking history. Downloaded media stays in `/tmp`, while upload statuses and Telegram topic IDs persist in Neon.
