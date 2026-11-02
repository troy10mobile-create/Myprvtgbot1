"""
Pixeldrain -> Telegram batch uploader, adapted from the user's working Colab script.

Behavior:
- Processes the configured Pixeldrain list (or optional local DB/JSON) once at startup.
- Keeps the original download/topic/progress/upload workflow.
- Exposes a tiny HTTP health endpoint for Render.
- Remains running after the batch finishes so Render sees a healthy web service.

IMPORTANT:
- Each topic name has its own local SQLite progress file and matching Neon database.
- Local SQLite is written first; Neon is restored/synchronised with retries.
- Downloaded media remains temporary in /tmp and is removed after upload.
"""

import os
import re
import json
import asyncio
import sqlite3
import time
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit, quote

import psycopg2
from psycopg2 import sql
import subprocess
import base64
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime

import aiohttp
import aiofiles
from telethon import TelegramClient
from telethon.tl.functions.messages import CreateForumTopicRequest
from telethon.tl.types import DocumentAttributeVideo
from aiofasttelethonhelper import fast_upload


# ============================================================
# CONFIGURATION — set these in Render > Environment
# ============================================================

def env_int(name, default):
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default

def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

SOURCE_DB_OR_JSON = os.getenv("SOURCE_DB_OR_JSON", "").strip()
PIXELDRAIN_LIST_URL = os.getenv(
    "PIXELDRAIN_LIST_URL", "https://pixeldrain.com/l/M2YSA2M1"
).strip()

MAX_FILES_TO_PROCESS = env_int("MAX_FILES_TO_PROCESS", 0)
FILTER_USERNAME = os.getenv("FILTER_USERNAME", "").strip()
FILTER_SERVICE = os.getenv("FILTER_SERVICE", "").strip()

PIXELDRAIN_API_KEY = os.getenv("PIXELDRAIN_API_KEY", "").strip()

api_id = env_int("TG_API_ID", 0)
api_hash = os.getenv("TG_API_HASH", "").strip()
BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
DESTINATION_CHAT_ID = env_int("DESTINATION_CHAT_ID", 0)

# You said not to worry about session persistence. This path is ephemeral.
SESSION_FILE_NAME = os.getenv("SESSION_FILE_NAME", "/tmp/telegram_bot.session")

MAX_TELEGRAM_SIZE = 1900 * 1024 * 1024
DOWNLOAD_WORKERS = env_int("DOWNLOAD_WORKERS", 2)
SKIP_ALREADY_DONE_BY_THIS_TOOL = env_bool("SKIP_ALREADY_DONE_BY_THIS_TOOL", True)

# Downloads remain temporary; upload history and topic mappings live in Neon.
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp/pd_to_tg_temp")
PROGRESS_DIR = os.getenv("PROGRESS_DIR", "/tmp/pd_to_tg_tracking")
DATABASE_URL_POOLED = os.getenv("DATABASE_URL_POOLED", "").strip()
DATABASE_URL_DIRECT = os.getenv("DATABASE_URL", "").strip()
# Prefer Neon pooled connections for ordinary queries; use the direct URL to
# provision a per-topic PostgreSQL database when needed.
DATABASE_URL = DATABASE_URL_POOLED or DATABASE_URL_DIRECT
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(PROGRESS_DIR, exist_ok=True)

# These are selected after the source list title has been loaded. The names
# deliberately follow the original Colab naming convention exactly.
ACTIVE_USERNAME = "unknown"
ACTIVE_DB_NAME = "unknown_pd_to_tg_progress"
ACTIVE_DB_PATH = os.path.join(PROGRESS_DIR, "unknown_pd_to_tg_progress.db")
ACTIVE_NEON_URL = ""
ACTIVE_NEON_DATABASE_READY = False
ACTIVE_NEON_TABLES_READY = False
ACTIVE_NEON_RESTORE_DONE = False
LOCAL_NEON_DIRTY = False


def validate_config():
    missing = []
    for key, value in {
        "TG_API_ID": api_id,
        "TG_API_HASH": api_hash,
        "TG_BOT_TOKEN": BOT_TOKEN,
        "DESTINATION_CHAT_ID": DESTINATION_CHAT_ID,
    }.items():
        if not value:
            missing.append(key)
    if not DATABASE_URL:
        missing.append("DATABASE_URL_POOLED (or DATABASE_URL)")
    if not PIXELDRAIN_LIST_URL and not SOURCE_DB_OR_JSON:
        missing.append("PIXELDRAIN_LIST_URL (or SOURCE_DB_OR_JSON)")
    if missing:
        raise RuntimeError("Missing Render environment variable(s): " + ", ".join(missing))


# ============================================================
# PROGRESS TRACKING — original per-topic SQLite + matching Neon database
# ============================================================

def clean_progress_username(username):
    clean = "".join(c for c in str(username) if c.isalnum() or c in ("-", "_"))
    clean = clean.strip().lower() or "unknown"
    return clean


def _database_url_for_name(base_url, database_name):
    parts = urlsplit(base_url)
    return urlunsplit((parts.scheme, parts.netloc, "/" + quote(database_name), parts.query, parts.fragment))


def _local_connection():
    conn = sqlite3.connect(ACTIVE_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _neon_connect():
    if not ACTIVE_NEON_URL:
        raise RuntimeError("Neon database URL has not been selected")
    # Always create a fresh connection. If Neon or the network has closed an
    # earlier connection, the next attempt establishes a new TCP connection.
    return psycopg2.connect(
        ACTIVE_NEON_URL,
        connect_timeout=15,
        application_name="pixeldrain-telegram-uploader",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )


def _with_neon_retry(operation, label="Neon operation"):
    if not ACTIVE_NEON_URL:
        return False
    last_error = None
    for attempt in range(1, 4):
        conn = None
        try:
            conn = _neon_connect()
            result = operation(conn)
            conn.commit()
            return result if result is not None else True
        except Exception as exc:
            last_error = exc
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(f"{label} attempt {attempt}/3 failed: {type(exc).__name__}: {exc}", flush=True)
            if attempt < 3:
                time.sleep(attempt * 2)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    print(f"{label} deferred; local SQLite remains authoritative. Last error: {last_error}", flush=True)
    return False


def _provision_neon_database(database_name):
    """Create a real PostgreSQL database matching this topic's SQLite file."""
    admin_url = DATABASE_URL_DIRECT or DATABASE_URL_POOLED
    if not admin_url:
        print("No Neon connection string configured; using local SQLite only.", flush=True)
        return False
    admin_url = _database_url_for_name(admin_url, "neondb")
    conn = None
    try:
        conn = psycopg2.connect(admin_url, connect_timeout=20, application_name="pd-tg-db-provisioner")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                print(f"Created Neon database: {database_name}", flush=True)
            else:
                print(f"Neon database already exists: {database_name}", flush=True)
        return True
    except Exception as exc:
        # A concurrent Render service may have created it between the check
        # and CREATE DATABASE. Re-check once before treating it as unavailable.
        try:
            if conn is not None:
                conn.close()
                conn = None
            check = psycopg2.connect(admin_url, connect_timeout=15)
            try:
                with check.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
                    if cur.fetchone():
                        return True
            finally:
                check.close()
        except Exception:
            pass
        print(f"Could not provision Neon database {database_name}: {type(exc).__name__}: {exc}", flush=True)
        print("The uploader will continue with its local SQLite database; Neon sync will retry.", flush=True)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _init_local_tables():
    with _local_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS progress (
                pixeldrain_id TEXT PRIMARY KEY,
                filename TEXT,
                service TEXT,
                username TEXT,
                status TEXT DEFAULT 'pending',
                local_path TEXT,
                error TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS topic_map (
                destination_chat_id INTEGER NOT NULL,
                topic_name TEXT NOT NULL,
                topic_id INTEGER NOT NULL,
                updated_at TEXT,
                PRIMARY KEY (destination_chat_id, topic_name)
            )
        """)


def _init_neon_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS progress (
                pixeldrain_id TEXT PRIMARY KEY,
                filename TEXT,
                service TEXT,
                username TEXT,
                status TEXT DEFAULT 'pending',
                local_path TEXT,
                error TEXT,
                updated_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS topic_map (
                destination_chat_id BIGINT NOT NULL,
                topic_name TEXT NOT NULL,
                topic_id BIGINT NOT NULL,
                updated_at TEXT,
                PRIMARY KEY (destination_chat_id, topic_name)
            )
        """)


def _neon_upsert_progress(row):
    def write(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO progress
                    (pixeldrain_id, filename, service, username, status,
                     local_path, error, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (pixeldrain_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    filename = COALESCE(EXCLUDED.filename, progress.filename),
                    service = COALESCE(EXCLUDED.service, progress.service),
                    username = COALESCE(EXCLUDED.username, progress.username),
                    local_path = EXCLUDED.local_path,
                    error = EXCLUDED.error,
                    updated_at = EXCLUDED.updated_at
            """, tuple(row[key] for key in (
                "pixeldrain_id", "filename", "service", "username", "status",
                "local_path", "error", "updated_at"
            )))
    return _with_neon_retry(write, "Neon progress sync")


def sync_all_local_to_neon():
    """Mirror the complete local progress DB in one retryable Neon transaction."""
    global ACTIVE_NEON_DATABASE_READY, ACTIVE_NEON_TABLES_READY, ACTIVE_NEON_RESTORE_DONE
    global LOCAL_NEON_DIRTY
    if not ACTIVE_NEON_URL:
        return False
    # Also recover if Neon database creation or table initialization failed at
    # startup while the service was temporarily unable to reach Neon.
    if not ACTIVE_NEON_DATABASE_READY:
        ACTIVE_NEON_DATABASE_READY = _provision_neon_database(ACTIVE_DB_NAME)
        if not ACTIVE_NEON_DATABASE_READY:
            return False
    if not ACTIVE_NEON_TABLES_READY:
        ACTIVE_NEON_TABLES_READY = bool(
            _with_neon_retry(_init_neon_tables, "Neon table initialization retry")
        )
        if not ACTIVE_NEON_TABLES_READY:
            return False
    if not ACTIVE_NEON_RESTORE_DONE:
        ACTIVE_NEON_RESTORE_DONE = _restore_neon_to_local()
        if not ACTIVE_NEON_RESTORE_DONE:
            return False
    with _local_connection() as conn:
        rows = conn.execute("SELECT * FROM progress").fetchall()
    values = [tuple(row[key] for key in (
        "pixeldrain_id", "filename", "service", "username", "status",
        "local_path", "error", "updated_at"
    )) for row in rows]

    def write_batch(conn):
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO progress
                    (pixeldrain_id, filename, service, username, status,
                     local_path, error, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (pixeldrain_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    filename = COALESCE(EXCLUDED.filename, progress.filename),
                    service = COALESCE(EXCLUDED.service, progress.service),
                    username = COALESCE(EXCLUDED.username, progress.username),
                    local_path = EXCLUDED.local_path,
                    error = EXCLUDED.error,
                    updated_at = EXCLUDED.updated_at
            """, values)
    progress_ok = (
        bool(_with_neon_retry(write_batch, "Neon batch sync"))
        if values else True
    )

    with _local_connection() as conn:
        topic_rows = conn.execute("SELECT * FROM topic_map").fetchall()
    if topic_rows:
        topic_values = [tuple(row[key] for key in (
            "destination_chat_id", "topic_name", "topic_id", "updated_at"
        )) for row in topic_rows]
        def write_topics(conn):
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO topic_map
                        (destination_chat_id, topic_name, topic_id, updated_at)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (destination_chat_id, topic_name) DO UPDATE SET
                        topic_id=EXCLUDED.topic_id,
                        updated_at=EXCLUDED.updated_at
                """, topic_values)
        topics_ok = bool(_with_neon_retry(write_topics, "Neon topic-map sync"))
    else:
        topics_ok = True
    all_ok = progress_ok and topics_ok
    if all_ok:
        LOCAL_NEON_DIRTY = False
    return all_ok


def _restore_neon_to_local():
    if not ACTIVE_NEON_URL:
        return False
    def read(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pixeldrain_id, filename, service, username, status,
                       local_path, error, updated_at FROM progress
            """)
            return cur.fetchall()
    rows = _with_neon_retry(read, "Neon progress restore")
    if rows is False:
        return False
    def read_topics(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT destination_chat_id, topic_name, topic_id, updated_at
                FROM topic_map
            """)
            return cur.fetchall()
    topic_rows = _with_neon_retry(read_topics, "Neon topic-map restore")
    if topic_rows is False:
        return False
    with _local_connection() as conn:
        for row in rows or []:
            conn.execute("""
                INSERT INTO progress
                    (pixeldrain_id, filename, service, username, status,
                     local_path, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pixeldrain_id) DO NOTHING
            """, row)
        for row in topic_rows:
            conn.execute("""
                INSERT INTO topic_map
                    (destination_chat_id, topic_name, topic_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(destination_chat_id, topic_name) DO NOTHING
            """, row)
    return True


def init_progress_db(username=None):
    global ACTIVE_NEON_TABLES_READY, ACTIVE_NEON_RESTORE_DONE
    _init_local_tables()
    if ACTIVE_NEON_URL:
        ACTIVE_NEON_TABLES_READY = bool(
            _with_neon_retry(_init_neon_tables, "Neon table initialization")
        )
        if ACTIVE_NEON_TABLES_READY:
            ACTIVE_NEON_RESTORE_DONE = _restore_neon_to_local()
            # Push any local rows left unsynchronised by a previous outage.
            if ACTIVE_NEON_RESTORE_DONE:
                sync_all_local_to_neon()


def get_progress(username, pd_id):
    with _local_connection() as conn:
        row = conn.execute(
            "SELECT status, filename, error FROM progress WHERE pixeldrain_id=?",
            (pd_id,),
        ).fetchone()
    return tuple(row) if row else None


def mark_progress(username, pd_id, status, filename=None, service=None,
                  local_path=None, error=None):
    now = datetime.now().astimezone().isoformat()
    # Commit locally first. A Neon outage cannot interrupt the actual upload.
    with _local_connection() as conn:
        conn.execute("""
            INSERT INTO progress
                (pixeldrain_id, filename, service, username, status,
                 local_path, error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pixeldrain_id) DO UPDATE SET
                status=excluded.status,
                filename=COALESCE(excluded.filename, progress.filename),
                service=COALESCE(excluded.service, progress.service),
                username=COALESCE(excluded.username, progress.username),
                local_path=excluded.local_path,
                error=excluded.error,
                updated_at=excluded.updated_at
        """, (pd_id, filename, service, username, status, local_path, error, now))
    global LOCAL_NEON_DIRTY
    LOCAL_NEON_DIRTY = True


def print_progress_summary(username):
    with _local_connection() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM progress GROUP BY status ORDER BY status"
        ).fetchall()
    if not rows:
        print(f"Progress DB for {username} is empty", flush=True)
        return
    print(f"Progress tracking for [{username}]", flush=True)
    for status, count in rows:
        print(f"   {status}: {count}", flush=True)


def get_user_progress_db(username):
    return ACTIVE_DB_PATH


def get_saved_topic_id(chat_id, topic_name):
    with _local_connection() as conn:
        row = conn.execute(
            "SELECT topic_id FROM topic_map WHERE destination_chat_id=? AND LOWER(topic_name)=LOWER(?)",
            (chat_id, topic_name),
        ).fetchone()
    if row:
        return row[0]
    if not ACTIVE_NEON_URL:
        return None
    def read(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT topic_id FROM topic_map WHERE destination_chat_id=%s AND LOWER(topic_name)=LOWER(%s)",
                (chat_id, topic_name),
            )
            result = cur.fetchone()
            return result[0] if result else None
    result = _with_neon_retry(read, "Neon topic lookup")
    return result if result is not False else None


def save_topic_id(chat_id, topic_name, topic_id):
    now = datetime.now().astimezone().isoformat()
    with _local_connection() as conn:
        conn.execute("""
            INSERT INTO topic_map (destination_chat_id, topic_name, topic_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(destination_chat_id, topic_name) DO UPDATE SET
                topic_id=excluded.topic_id, updated_at=excluded.updated_at
        """, (chat_id, topic_name, topic_id, now))
    global LOCAL_NEON_DIRTY
    LOCAL_NEON_DIRTY = True
    if ACTIVE_NEON_URL:
        def write(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO topic_map (destination_chat_id, topic_name, topic_id, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (destination_chat_id, topic_name) DO UPDATE SET
                        topic_id=EXCLUDED.topic_id, updated_at=EXCLUDED.updated_at
                """, (chat_id, topic_name, topic_id, now))
        _with_neon_retry(write, "Neon topic sync")


# ============================================================
# SMART TOPIC EXTRACTION FROM LIST TITLE
# ============================================================

def topic_from_list_title(title: str) -> str:
    if not title:
        return "unknown"
    t = title.strip()
    for suffix in [
        " archive (final)",
        " cumst archive (final)",
        " archive",
        " cumst archive",
        " (final)",
    ]:
        if t.lower().endswith(suffix.lower()):
            t = t[:-len(suffix)].strip()
            break
    if "_" in t:
        parts = t.split("_", 1)
        return parts[1] if len(parts) > 1 else t
    return t or "unknown"


# ============================================================
# LOAD FROM DB / JSON
# ============================================================

def load_from_db_or_json(source_path):
    files = []
    if source_path.lower().endswith(".json"):
        with open(source_path, encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            pd_id = item.get("pixeldrain_id")
            if not pd_id:
                continue
            if FILTER_USERNAME and str(item.get("username", "")).lower() != FILTER_USERNAME.lower():
                continue
            if FILTER_SERVICE and str(item.get("service", "")).lower() != FILTER_SERVICE.lower():
                continue
            files.append({
                "pixeldrain_id": pd_id,
                "filename": item.get("filename") or f"{pd_id}.bin",
                "service": item.get("service") or "unknown",
                "username": item.get("username") or "unknown",
                "force_official": False,
            })
    else:
        conn = sqlite3.connect(source_path)
        conn.row_factory = sqlite3.Row
        query = """
            SELECT pixeldrain_id, filename, service, username
            FROM files
            WHERE pixeldrain_id IS NOT NULL AND pixeldrain_id != ''
        """
        params = []
        if FILTER_USERNAME:
            query += " AND LOWER(username) = ?"
            params.append(FILTER_USERNAME.lower())
        if FILTER_SERVICE:
            query += " AND LOWER(service) = ?"
            params.append(FILTER_SERVICE.lower())
        rows = conn.execute(query, params).fetchall()
        conn.close()
        for row in rows:
            files.append({
                "pixeldrain_id": row["pixeldrain_id"],
                "filename": row["filename"] or f"{row['pixeldrain_id']}.bin",
                "service": row["service"] or "unknown",
                "username": row["username"] or "unknown",
                "force_official": False,
            })
    return files


# ============================================================
# PIXELDRAIN LIST
# ============================================================

def _pd_auth_header():
    if not PIXELDRAIN_API_KEY:
        return {}
    token = base64.b64encode(f":{PIXELDRAIN_API_KEY}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def extract_list_id(url_or_id: str) -> str:
    url_or_id = (url_or_id or "").strip()
    match = re.search(r"pixeldrain\.com/l/([A-Za-z0-9_-]+)", url_or_id, re.I)
    if match:
        return match.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]+", url_or_id):
        return url_or_id
    return ""


async def load_from_pixeldrain_list(list_url_or_id: str):
    list_id = extract_list_id(list_url_or_id)
    if not list_id:
        raise ValueError("Could not extract Pixeldrain list ID")

    url = f"https://pixeldrain.com/api/list/{list_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        **_pd_auth_header(),
    }

    timeout = aiohttp.ClientTimeout(total=90, sock_connect=20, sock_read=60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as sess:
        async with sess.get(url) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"List API failed ({resp.status}): {body[:200]}")
            data = await resp.json()

    list_title = data.get("title") or ""
    topic_name = topic_from_list_title(list_title)
    print(f"List title : {list_title}", flush=True)
    print(f"Topic name : {topic_name}", flush=True)

    files = []
    for item in data.get("files") or []:
        pd_id = item.get("id")
        if not pd_id:
            continue
        name = item.get("name") or f"{pd_id}.bin"
        availability = (item.get("availability") or "").lower()
        views = item.get("views") or 0
        downloads = item.get("downloads") or 0
        force_official = (
            "rate_limited" in availability
            or "captcha" in availability
            or (downloads > 0 and views * 3 < downloads)
        )
        files.append({
            "pixeldrain_id": pd_id,
            "filename": name,
            "service": "pixeldrain_list",
            "username": topic_name,
            "force_official": force_official,
        })

    print(f"Loaded {len(files)} files from list {list_id}", flush=True)
    return files, topic_name


# ============================================================
# PIXELDRAIN DOWNLOAD
# ============================================================

BYPASS_MIRRORS = [
    "https://cdn.pixeldrain.eu.cc/{id}?download",
    "https://cdn.pixeldrain.eu.cc/{id}?download",
]
OFFICIAL_URL = "https://pixeldrain.com/api/file/{id}?download"


async def download_from_pixeldrain(pd_id: str, local_path: str,
                                   force_official: bool = False, filename: str = ""):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        **_pd_auth_header(),
    }

    if force_official:
        candidates = [OFFICIAL_URL.format(id=pd_id)]
        print("  -> Using official API (list metadata indicates restriction)", flush=True)
    else:
        candidates = [url.format(id=pd_id) for url in BYPASS_MIRRORS]
        candidates.append(OFFICIAL_URL.format(id=pd_id))

    last_error = None
    partial = local_path + ".part"

    for url in candidates:
        for attempt in range(2):
            try:
                timeout = aiohttp.ClientTimeout(
                    total=None, sock_connect=20, sock_read=300
                )
                async with aiohttp.ClientSession(
                    headers=headers, timeout=timeout
                ) as sess:
                    async with sess.get(url, allow_redirects=True) as resp:
                        if resp.status == 404:
                            last_error = "File not found (404)"
                            break
                        if resp.status == 403:
                            body = await resp.text()
                            last_error = f"403 Forbidden - {body[:150]}"
                            break
                        if resp.status == 429:
                            last_error = "HTTP 429 rate limited"
                            break
                        if resp.status == 500:
                            last_error = "HTTP 500"
                            if attempt == 0:
                                await asyncio.sleep(2)
                                continue
                            break
                        if resp.status != 200:
                            last_error = f"HTTP {resp.status}"
                            break

                        # Start a clean partial file for each successful response.
                        try:
                            if os.path.exists(partial):
                                os.remove(partial)
                        except OSError:
                            pass

                        async with aiofiles.open(partial, "wb") as f:
                            async for chunk in resp.content.iter_chunked(512 * 1024):
                                await f.write(chunk)

                        if not os.path.exists(partial) or os.path.getsize(partial) == 0:
                            try:
                                os.remove(partial)
                            except OSError:
                                pass
                            last_error = "Empty download"
                            break

                        os.replace(partial, local_path)
                        print(f"  Downloaded via {urlparse(url).netloc}", flush=True)
                        return True, None

            except Exception as exc:
                last_error = str(exc)
                break

    try:
        if os.path.exists(partial):
            os.remove(partial)
    except OSError:
        pass
    return False, last_error or "All mirrors failed"


# ============================================================
# SPLIT / TELEGRAM HELPERS
# ============================================================

def split_if_needed(path, max_size=MAX_TELEGRAM_SIZE):
    size = os.path.getsize(path)
    if size <= max_size:
        return [path]

    print(f"Splitting {os.path.basename(path)} ({size / 1024 / 1024:.1f} MB)", flush=True)
    base, ext = os.path.splitext(path)

    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", path,
            ],
            capture_output=True, text=True, check=True,
        )
        duration = float(result.stdout.strip())
    except Exception:
        duration = 0

    if duration <= 0:
        print("Could not determine media duration; cannot split this file.", flush=True)
        return [path]

    target_bytes = 1800 * 1024 * 1024
    segment_time = max(60, int((target_bytes / size) * duration))
    out_pattern = f"{base}_part%03d{ext}"

    subprocess.run(
        [
            "ffmpeg", "-y", "-i", path, "-c", "copy", "-map", "0",
            "-f", "segment", "-segment_time", str(segment_time),
            "-reset_timestamps", "1", out_pattern,
        ],
        capture_output=True, text=True,
    )

    directory = os.path.dirname(path) or "."
    prefix = os.path.basename(base) + "_part"
    parts = sorted(
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if name.startswith(prefix) and name.endswith(ext)
    )
    good = [part for part in parts if os.path.getsize(part) <= max_size]
    return good if good else [path]


async def create_or_get_topic(client, chat_id, topic_name, topic_cache):
    if topic_name in topic_cache:
        return topic_cache[topic_name]

    saved_id = get_saved_topic_id(chat_id, topic_name)
    if saved_id is not None:
        topic_cache[topic_name] = saved_id
        return saved_id

    try:
        forum = await client.get_entity(chat_id)
        result = await client(
            CreateForumTopicRequest(peer=forum, title=topic_name)
        )
        for update in result.updates:
            if hasattr(update, "id"):
                topic_id = update.id
                save_topic_id(chat_id, topic_name, topic_id)
                topic_cache[topic_name] = topic_id
                print(f"Created topic: {topic_name} (ID: {topic_id})", flush=True)
                return topic_id
    except Exception as exc:
        print(f"Topic create failed: {exc}", flush=True)
    return None


def get_video_duration(path):
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", path,
            ],
            capture_output=True, text=True, check=True,
        )
        if result.stdout:
            return int(float(result.stdout.strip()))
    except Exception:
        pass
    return 0


def generate_thumbnail(path):
    thumb = path + "_thumb.jpg"
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-i", path, "-ss", "00:00:01",
                "-vframes", "1", "-vf", "scale=320:-1", thumb,
            ],
            capture_output=True, text=True,
        )
        if os.path.exists(thumb) and os.path.getsize(thumb) > 0:
            return thumb
    except Exception:
        pass
    return None


async def upload_to_telegram(client, chat_id, topic_id, file_path, caption):
    thumb = None
    try:
        file_handle = await fast_upload(client, file_path)
        ext = os.path.splitext(file_path)[1].lower()

        if ext in [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"]:
            duration = get_video_duration(file_path)
            thumb = generate_thumbnail(file_path)
            try:
                await client.send_file(
                    chat_id,
                    file_handle,
                    caption=caption,
                    reply_to=topic_id,
                    thumb=thumb,
                    attributes=[
                        DocumentAttributeVideo(
                            duration=duration, w=0, h=0, supports_streaming=True
                        )
                    ],
                    force_document=False,
                )
            except Exception:
                await client.send_file(
                    chat_id, file_handle, caption=caption,
                    reply_to=topic_id, force_document=True,
                )
        else:
            await client.send_file(
                chat_id, file_handle, caption=caption,
                reply_to=topic_id, force_document=True,
            )
        return True
    except Exception as exc:
        print(f"Telegram upload error: {exc}", flush=True)
        return False
    finally:
        if thumb and os.path.exists(thumb):
            try:
                os.remove(thumb)
            except OSError:
                pass


# ============================================================
# PROCESS ONE
# ============================================================

async def process_one(item, client, chat_id, topic_cache, semaphore):
    pd_id = item["pixeldrain_id"]
    filename = item["filename"]
    service = item["service"]
    username = item["username"]
    force_official = item.get("force_official", False)

    async with semaphore:
        prog = get_progress(username, pd_id)
        if SKIP_ALREADY_DONE_BY_THIS_TOOL and prog and prog[0] == "uploaded":
            print(f"Already done by this tool: {filename}", flush=True)
            return True

        safe_name = re.sub(r'[<>:"/\\\\|?*]', "_", filename)
        local_path = os.path.join(TEMP_DIR, f"{pd_id}_{safe_name}")

        print(f"\nDownloading: {filename} (Pixeldrain ID: {pd_id})", flush=True)
        mark_progress(username, pd_id, "pending", filename=filename, service=service)

        ok, err = await download_from_pixeldrain(
            pd_id, local_path, force_official=force_official, filename=filename
        )
        if not ok:
            print(f"Download failed: {err}", flush=True)
            mark_progress(
                username, pd_id, "failed", filename=filename,
                service=service, error=err,
            )
            return False

        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"Downloaded ({size_mb:.1f} MB)", flush=True)
        mark_progress(
            username, pd_id, "downloaded", filename=filename,
            service=service, local_path=local_path,
        )

        topic_id = await create_or_get_topic(
            client, chat_id, username, topic_cache
        )
        parts = split_if_needed(local_path)
        all_ok = True

        for index, part_path in enumerate(parts):
            part_name = os.path.basename(part_path)
            caption = f"#{service}_{username}\n{part_name}"
            if len(parts) > 1:
                caption += f"\n(Part {index + 1}/{len(parts)})"

            print(f"Uploading: {part_name}", flush=True)
            ok = await upload_to_telegram(
                client, chat_id, topic_id, part_path, caption
            )
            if not ok:
                all_ok = False
                break

        # Remove generated segments and the original temporary download.
        for part_path in parts:
            if part_path != local_path:
                try:
                    os.remove(part_path)
                except OSError:
                    pass
        try:
            os.remove(local_path)
        except OSError:
            pass

        if all_ok:
            mark_progress(
                username, pd_id, "uploaded",
                filename=filename, service=service,
            )
            return True

        mark_progress(
            username, pd_id, "failed", filename=filename,
            service=service, error="telegram upload failed",
        )
        return False


# ============================================================
# BATCH MAIN — same startup/batch behavior as the Colab script
# ============================================================

async def main():
    validate_config()

    print("=" * 60, flush=True)
    print("Pixeldrain -> Telegram (Render migration)", flush=True)
    print("=" * 60, flush=True)

    files = []
    active_username = "unknown"

    if PIXELDRAIN_LIST_URL:
        print(f"Using Pixeldrain List: {PIXELDRAIN_LIST_URL}", flush=True)
        files, active_username = await load_from_pixeldrain_list(
            PIXELDRAIN_LIST_URL
        )
    elif SOURCE_DB_OR_JSON:
        if not os.path.exists(SOURCE_DB_OR_JSON):
            raise FileNotFoundError(f"Source not found: {SOURCE_DB_OR_JSON}")
        print(f"Using DB/JSON: {SOURCE_DB_OR_JSON}", flush=True)
        files = load_from_db_or_json(SOURCE_DB_OR_JSON)
        if files:
            active_username = files[0].get("username", "unknown")
    else:
        raise RuntimeError("Provide PIXELDRAIN_LIST_URL or SOURCE_DB_OR_JSON")

    global ACTIVE_USERNAME, ACTIVE_DB_NAME, ACTIVE_DB_PATH, ACTIVE_NEON_URL
    global ACTIVE_NEON_DATABASE_READY, ACTIVE_NEON_TABLES_READY, ACTIVE_NEON_RESTORE_DONE
    global LOCAL_NEON_DIRTY
    ACTIVE_USERNAME = clean_progress_username(active_username)
    # Match the Colab filename convention: <clean username>_pd_to_tg_progress.db
    ACTIVE_DB_NAME = f"{ACTIVE_USERNAME}_pd_to_tg_progress"
    ACTIVE_DB_PATH = os.path.join(PROGRESS_DIR, f"{ACTIVE_DB_NAME}.db")
    if DATABASE_URL:
        # Use the direct endpoint for database-specific connections when it is
        # available; fall back to the pooler only if the direct URL is absent.
        neon_base_url = DATABASE_URL_DIRECT or DATABASE_URL_POOLED
        ACTIVE_NEON_URL = _database_url_for_name(neon_base_url, ACTIVE_DB_NAME)
        ACTIVE_NEON_DATABASE_READY = _provision_neon_database(ACTIVE_DB_NAME)
        ACTIVE_NEON_TABLES_READY = False
        ACTIVE_NEON_RESTORE_DONE = False
    LOCAL_NEON_DIRTY = False
    print(f"Progress database name: {ACTIVE_DB_NAME}", flush=True)
    print(f"Local progress DB: {ACTIVE_DB_PATH}", flush=True)
    print(f"Neon database: {ACTIVE_DB_NAME}", flush=True)

    init_progress_db(active_username)
    print_progress_summary(active_username)
    print(f"Total files available: {len(files)}", flush=True)

    if not files:
        print("Nothing to process.", flush=True)
        return

    if MAX_FILES_TO_PROCESS > 0:
        files = files[:MAX_FILES_TO_PROCESS]
        print(f"Limited to first {len(files)} file(s)", flush=True)
    else:
        print(f"No limit - processing all {len(files)} files", flush=True)

    client = TelegramClient(SESSION_FILE_NAME, api_id, api_hash)
    try:
        await client.start(bot_token=BOT_TOKEN)
        me = await client.get_me()
        print(f"Telegram connected: {me.first_name}", flush=True)

        topic_cache = {}
        semaphore = asyncio.Semaphore(DOWNLOAD_WORKERS)
        results = []

        # Kept sequential like the original script. DOWNLOAD_WORKERS controls
        # the semaphore limit, but the original loop itself processes one item
        # at a time; this avoids changing the working behavior.
        for item in files:
            result = await process_one(
                item, client, DESTINATION_CHAT_ID, topic_cache, semaphore
            )
            results.append(result)

        ok_count = sum(1 for result in results if result)
        fail_count = len(results) - ok_count
        print("Flushing local progress to Neon...", flush=True)
        await asyncio.to_thread(sync_all_local_to_neon)

        print("\n" + "=" * 60, flush=True)
        print("BATCH DONE", flush=True)
        print(f"Success: {ok_count}", flush=True)
        print(f"Failed : {fail_count}", flush=True)
        print_progress_summary(active_username)
        print(f"Per-list progress DB: {get_user_progress_db(active_username)}", flush=True)
        print("=" * 60, flush=True)
    finally:
        await client.disconnect()


# ============================================================
# RENDER HTTP HEALTH SERVER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            body = b"OK - Pixeldrain to Telegram service is running\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        # Keep routine health checks out of the logs.
        return


def start_health_server():
    port = env_int("PORT", 10000)
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Health endpoint listening on port {port}", flush=True)
    return server


async def service():
    start_health_server()

    async def periodic_neon_sync():
        while True:
            await asyncio.sleep(20)
            try:
                if ACTIVE_NEON_URL and (
                    LOCAL_NEON_DIRTY
                    or not ACTIVE_NEON_DATABASE_READY
                    or not ACTIVE_NEON_TABLES_READY
                    or not ACTIVE_NEON_RESTORE_DONE
                ):
                    await asyncio.to_thread(sync_all_local_to_neon)
            except Exception as exc:
                print(f"Periodic Neon sync will retry: {type(exc).__name__}: {exc}", flush=True)

    asyncio.create_task(periodic_neon_sync())

    # Start the batch exactly once per process start. If it fails, log the
    # exception but keep the HTTP service alive so Render can report health.
    async def run_batch():
        try:
            await main()
        except Exception as exc:
            print(f"FATAL batch error: {type(exc).__name__}: {exc}", flush=True)

    asyncio.create_task(run_batch())

    # A Web Service must keep its process alive after the batch completes.
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(service())
