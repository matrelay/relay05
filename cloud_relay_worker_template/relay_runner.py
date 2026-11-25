# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 MATRIX CLOUD RELAY SHIFT WORKER (GitHub Actions 10Gbps Multi-Userbot Runner)
=============================================================================
File: relay_runner.py

Purpose:
  Connects to the central PostgreSQL database queue on Render, streams
  restricted media from Telegram source groups, and re-uploads to target groups.
  Runs entirely on GitHub's unmetered 10Gbps Azure pipe to eliminate Render bandwidth.

Privacy & Security Guarantees:
  1. Zero Leakage: Session strings are pulled directly from PostgreSQL over TLS
     into RAM. No session strings are hardcoded in GitHub, workflow files, or logs.
  2. Parallel Multi-Userbot Pool: Spawns 2 concurrent workers per active userbot
     session in the queue so all selected userbots transfer together simultaneously.
  3. Caption & Reply Preservation: Preserves captions (with formatting entities) or
     stripped/custom captions configured in Target Pairs / Auto-Pair, and links
     replied messages in the target group via `message_mappings`.
  4. Sanitized Logs: Never prints session strings, chat IDs, phone numbers, or captions.
  5. 5-Hour Shift Cap & 15-Minute Idle Sleep + Instant Disk Cleanup.
=============================================================================
"""

import os
import re
import sys
import html
import json
import time
import shutil
import tempfile
import asyncio
import psycopg2
try:
    import requests
except ImportError:
    requests = None
from telethon import TelegramClient, errors
try:
    from telethon import types, utils as tg_utils
except ImportError:
    types = None
    tg_utils = None
from telethon.sessions import StringSession

# Enforce real-time unbuffered logging in CI/CD environments
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

import builtins
_builtin_print = builtins.print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _builtin_print(*args, **kwargs)

MAX_SHIFT_SECONDS = 18000       # 5 Hours maximum continuous run
IDLE_TIMEOUT_SECONDS = 900      # 15 Minutes idle shutdown if queue stays empty
HEARTBEAT_INTERVAL = 30         # Seconds between database heartbeats
STALE_PROCESSING_SECONDS = 1200 # 20 Minutes before orphaned 'processing' jobs reset to 'pending'
WORKERS_PER_SESSION = 2         # 2 concurrent transfer slots per userbot session
URL_WORKERS = 2                 # Concurrent Bot-API / Web/X URL download slots
STRIP_CAPTION_SENTINEL = "__STRIP_CAPTION__"


def sanitize_db_url(url: str) -> str:
    """Ensures connection string has proper postgresql:// prefix and sslmode."""
    clean = url.strip().strip('"\'')
    if clean.startswith("postgres://"):
        clean = clean.replace("postgres://", "postgresql://", 1)
    if "sslmode=" not in clean:
        delimiter = "&" if "?" in clean else "?"
        clean = f"{clean}{delimiter}sslmode=require"
    return clean


def _chat_id_candidates(chat_id: int) -> list:
    """Returns canonical (-100...), negative, and bare positive variants for DB mapping lookups."""
    try:
        cid = int(chat_id)
    except Exception:
        return [chat_id]
    s = str(cid)
    cands = [cid]
    if s.startswith("-100"):
        bare = s[4:]
        if bare.isdigit():
            cands.extend([int(bare), -int(bare)])
    elif cid < 0:
        bare = s[1:]
        if bare.isdigit():
            cands.extend([int(f"-100{bare}"), int(bare)])
    else:
        cands.extend([int(f"-100{s}"), -int(s)])
    seen = []
    for x in cands:
        if x not in seen:
            seen.append(x)
    return seen


def _extract_reply_src_id_from_msg(msg) -> int:
    """Extracts source reply target message ID while ignoring bare Forum Topic root headers."""
    if not msg:
        return 0
    r_hdr = getattr(msg, "reply_to", None)
    r_id = getattr(msg, "reply_to_msg_id", None) or (getattr(r_hdr, "reply_to_msg_id", None) if r_hdr else None)
    if not r_id:
        return 0
    if r_hdr and getattr(r_hdr, "forum_topic", False):
        top_id = getattr(r_hdr, "reply_to_top_id", None)
        if not top_id or int(r_id) == int(top_id):
            return 0
    try:
        return int(r_id)
    except Exception:
        return 0


async def _probe_video_ffprobe(local_path: str) -> tuple:
    """Uses ffprobe (if installed) to recover missing duration, width, and height."""
    if not shutil.which("ffprobe"):
        return 0.0, 0, 0
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration:stream_side_data=rotation:format=duration",
            "-of", "json", local_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=12.0)
        data = json.loads(stdout.decode("utf-8", errors="ignore") or "{}")
        streams = data.get("streams") or []
        fmt = data.get("format") or {}
        w = h = 0
        dur = 0.0
        if streams:
            st = streams[0]
            w = int(st.get("width") or 0)
            h = int(st.get("height") or 0)
            dur = float(st.get("duration") or 0.0)
            for sd in st.get("side_data_list") or []:
                rot = abs(int(float(sd.get("rotation") or 0)))
                if rot in (90, 270) and w and h:
                    w, h = h, w
                    break
        if dur <= 0.0 and fmt.get("duration"):
            dur = float(fmt.get("duration") or 0.0)
        return dur, w, h
    except Exception:
        return 0.0, 0, 0


async def _extract_ffmpeg_thumb(local_path: str, thumb_out: str, dur: float = 0.0) -> str:
    """Extracts a Telegram-compliant (<=320px, <=200KB JPEG) thumbnail frame using ffmpeg if available."""
    if not shutil.which("ffmpeg"):
        return None
    seek_at = "00:00:01" if (dur <= 0 or dur >= 1.5) else "00:00:00"
    for seek in (seek_at, "00:00:00"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", seek, "-i", local_path,
                "-vframes", "1",
                "-vf", "scale=w=320:h=320:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-q:v", "4", thumb_out,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=15.0)
            if os.path.exists(thumb_out) and 0 < os.path.getsize(thumb_out) <= 204800:
                return thumb_out
        except Exception:
            pass
    return None


async def prepare_media_upload_meta(client, msg, local_path: str, work_dir: str) -> dict:
    """
    Preserves original Telegram video/audio/document attributes and thumbnails so
    uploaded videos show real thumbnails, duration, aspect ratio, and streaming support
    instead of blank 'Video, 0:00' placeholders.
    """
    if not msg or not local_path or not os.path.exists(local_path):
        return {}

    media = getattr(msg, "media", None)
    if types is not None and isinstance(media, types.MessageMediaPhoto):
        return {}
    if tg_utils is not None:
        try:
            if tg_utils.is_image(local_path):
                return {}
        except Exception:
            pass

    doc = getattr(media, "document", None) or getattr(msg, "document", None)
    ext = os.path.splitext(str(local_path))[1].lower()
    if doc is None and ext not in (".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v", ".3gp", ".flv"):
        return {}

    orig_mime = str(getattr(doc, "mime_type", "") or "").lower() if doc is not None else ""
    raw_attrs = list(getattr(doc, "attributes", None) or []) if doc is not None else []

    orig_filename = None
    is_animated = False
    is_sticker = False
    video_attr = None
    audio_attr = None
    img_size_attr = None

    if types is not None:
        for a in raw_attrs:
            if isinstance(a, types.DocumentAttributeSticker):
                is_sticker = True
            elif isinstance(a, types.DocumentAttributeAnimated):
                is_animated = True
            elif isinstance(a, types.DocumentAttributeVideo):
                video_attr = a
            elif isinstance(a, types.DocumentAttributeAudio):
                audio_attr = a
            elif isinstance(a, types.DocumentAttributeFilename):
                orig_filename = getattr(a, "file_name", None)
            elif isinstance(a, types.DocumentAttributeImageSize):
                img_size_attr = a

    if is_sticker:
        return {}

    is_video = (
        (video_attr is not None)
        or orig_mime.startswith("video/")
        or ext in (".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v", ".3gp", ".flv")
    )

    meta = {}
    if orig_mime:
        meta["mime_type"] = orig_mime
    elif is_video:
        meta["mime_type"] = "video/mp4"

    thumb_dir = os.path.join(work_dir, "_thumbs")
    os.makedirs(thumb_dir, exist_ok=True)
    thumb_out = os.path.join(thumb_dir, f"thumb_{getattr(msg, 'id', '0')}.jpg")
    thumb_path = None

    # 1. Pick static photo thumbnails from source document (never VideoSize / PhotoPathSize)
    photo_thumbs = []
    stripped_thumbs = []
    if doc is not None and types is not None:
        for t in (getattr(doc, "thumbs", None) or []):
            if isinstance(t, types.PhotoSize):
                photo_thumbs.append((int(getattr(t, "size", 0) or 0), t))
            elif isinstance(t, types.PhotoCachedSize):
                photo_thumbs.append((len(getattr(t, "bytes", b"") or b""), t))
            elif isinstance(t, types.PhotoStrippedSize):
                stripped_thumbs.append((len(getattr(t, "bytes", b"") or b""), t))

    async def _try_dl_thumb(thumb_obj) -> str:
        try:
            res_p = await asyncio.wait_for(
                client.download_media(msg, file=thumb_out, thumb=thumb_obj),
                timeout=20.0,
            )
            if res_p and os.path.exists(res_p) and 0 < os.path.getsize(res_p) <= 204800:
                return res_p
        except Exception:
            pass
        return None

    if photo_thumbs:
        photo_thumbs.sort(key=lambda x: x[0], reverse=True)
        thumb_path = await _try_dl_thumb(photo_thumbs[0][1])

    upload_attrs = []
    if types is not None:
        fname = orig_filename or os.path.basename(str(local_path))
        if fname:
            upload_attrs.append(types.DocumentAttributeFilename(fname))
        if is_animated:
            upload_attrs.append(types.DocumentAttributeAnimated())

        if is_video:
            dur = float(getattr(video_attr, "duration", 0) or 0) if video_attr else 0.0
            w = int(getattr(video_attr, "w", 0) or 0) if video_attr else 0
            h = int(getattr(video_attr, "h", 0) or 0) if video_attr else 0
            round_msg = bool(getattr(video_attr, "round_message", False)) if video_attr else False
            nosound = getattr(video_attr, "nosound", None) if video_attr else None

            if dur <= 0.0 or w <= 1 or h <= 1:
                p_dur, p_w, p_h = await _probe_video_ffprobe(local_path)
                if dur <= 0.0 and p_dur > 0.0:
                    dur = p_dur
                if w <= 1 and p_w > 1:
                    w = p_w
                if h <= 1 and p_h > 1:
                    h = p_h

            if not thumb_path and not is_animated:
                thumb_path = await _extract_ffmpeg_thumb(local_path, thumb_out, dur=dur)
            if not thumb_path and stripped_thumbs:
                stripped_thumbs.sort(key=lambda x: x[0], reverse=True)
                thumb_path = await _try_dl_thumb(stripped_thumbs[0][1])

            if video_attr is not None or dur > 0.0 or (w > 1 and h > 1):
                upload_attrs.append(
                    types.DocumentAttributeVideo(
                        duration=float(max(0.0, dur)),
                        w=int(max(1, w)),
                        h=int(max(1, h)),
                        round_message=round_msg,
                        supports_streaming=(not is_animated),
                        nosound=nosound,
                    )
                )

            if not is_animated:
                meta["supports_streaming"] = True
                meta["nosound_video"] = True
                if round_msg:
                    meta["video_note"] = True
        else:
            if not thumb_path and stripped_thumbs:
                stripped_thumbs.sort(key=lambda x: x[0], reverse=True)
                thumb_path = await _try_dl_thumb(stripped_thumbs[0][1])
            if audio_attr is not None:
                upload_attrs.append(
                    types.DocumentAttributeAudio(
                        duration=int(getattr(audio_attr, "duration", 0) or 0),
                        voice=bool(getattr(audio_attr, "voice", False)),
                        title=getattr(audio_attr, "title", None),
                        performer=getattr(audio_attr, "performer", None),
                        waveform=getattr(audio_attr, "waveform", None),
                    )
                )
            elif img_size_attr is not None and not is_animated:
                upload_attrs.append(
                    types.DocumentAttributeImageSize(
                        w=int(getattr(img_size_attr, "w", 1) or 1),
                        h=int(getattr(img_size_attr, "h", 1) or 1),
                    )
                )

    if upload_attrs:
        meta["attributes"] = upload_attrs
    if thumb_path:
        meta["thumb"] = thumb_path
    return meta


async def main():
    start_time = time.time()
    last_active_time = time.time()
    last_heartbeat_time = 0.0
    last_stale_reclaim_time = 0.0
    stop_requested = asyncio.Event()

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🚀 Matrix Cloud Relay Parallel Worker Starting ({WORKERS_PER_SESSION} slots/userbot)...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Validate Environment Variables (Zero Session Strings in Env!)
    api_id_raw = (os.getenv("TG_API_ID") or os.getenv("API_ID") or "").strip()
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("API_HASH") or "").strip()
    raw_db_url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()

    missing = []
    if not api_id_raw:
        missing.append("TG_API_ID (or API_ID)")
    if not api_hash:
        missing.append("TG_API_HASH (or API_HASH)")
    if not raw_db_url:
        missing.append("DATABASE_URL (or DB_URL)")

    if missing:
        print(f"❌ CRITICAL: The following GitHub Secrets were empty/missing: {', '.join(missing)}")
        print("💡 Check whether they were added under Secrets (not Variables) or if the repository has access.")
        sys.exit(1)

    try:
        api_id = int(api_id_raw)
    except ValueError:
        print("❌ Invalid TG_API_ID format.")
        sys.exit(1)

    db_url = sanitize_db_url(raw_db_url)

    # 1. Connect to PostgreSQL
    try:
        print("🔗 Connecting to PostgreSQL Queue over SSL...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        c = conn.cursor()
        print("✅ Database connection established.")
    except Exception as dbe:
        print(f"❌ Failed to connect to database: {type(dbe).__name__}")
        sys.exit(1)

    db_lock = asyncio.Lock()

    async def ensure_schema_columns():
        try:
            async with db_lock:
                try:
                    c.execute("ALTER TABLE cloud_relay_queue ADD COLUMN IF NOT EXISTS reply_to_msg_id BIGINT DEFAULT NULL;")
                except Exception:
                    pass
                try:
                    c.execute(
                        """
                        CREATE TABLE IF NOT EXISTS message_mappings (
                            id SERIAL PRIMARY KEY,
                            source_chat_id BIGINT,
                            source_msg_id BIGINT,
                            target_chat_id BIGINT,
                            target_msg_id BIGINT,
                            UNIQUE(source_chat_id, source_msg_id, target_chat_id)
                        )
                        """
                    )
                except Exception:
                    pass
                try:
                    c.execute(
                        """
                        CREATE TABLE IF NOT EXISTS cloud_url_jobs (
                            id SERIAL PRIMARY KEY,
                            source_plugin TEXT NOT NULL DEFAULT 'web_scraper',
                            media_type TEXT DEFAULT 'video',
                            media_url TEXT NOT NULL,
                            page_url TEXT DEFAULT '',
                            thumb_url TEXT DEFAULT '',
                            title TEXT DEFAULT '',
                            caption TEXT DEFAULT '',
                            headers_json TEXT DEFAULT '',
                            target_chat_id BIGINT NOT NULL,
                            target_topic_id BIGINT DEFAULT 0,
                            notify_chat_id BIGINT DEFAULT 0,
                            batch_tag TEXT DEFAULT '',
                            status TEXT DEFAULT 'pending',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                except Exception:
                    pass
        except Exception:
            pass

    await ensure_schema_columns()

    # Recover any orphaned 'processing' jobs left behind if an earlier shift was force-killed
    async def reclaim_stale_processing_jobs(stale_secs: int = STALE_PROCESSING_SECONDS):
        try:
            async with db_lock:
                c.execute(
                    """
                    UPDATE cloud_relay_queue
                    SET status = 'pending', updated_at = CURRENT_TIMESTAMP
                    WHERE status = 'processing'
                      AND updated_at < (CURRENT_TIMESTAMP - (%s * INTERVAL '1 second'))
                    """,
                    (int(stale_secs),),
                )
                reclaimed = c.rowcount if getattr(c, "rowcount", 0) and c.rowcount > 0 else 0
                try:
                    c.execute(
                        """
                        UPDATE cloud_url_jobs
                        SET status = 'pending', updated_at = CURRENT_TIMESTAMP
                        WHERE status = 'processing'
                          AND updated_at < (CURRENT_TIMESTAMP - (%s * INTERVAL '1 second'))
                        """,
                        (int(stale_secs),),
                    )
                    u_rec = c.rowcount if getattr(c, "rowcount", 0) and c.rowcount > 0 else 0
                    reclaimed += u_rec
                except Exception:
                    pass
                if reclaimed:
                    print(f"🔄 Reclaimed {reclaimed} interrupted job(s) back to 'pending'.")
        except Exception:
            pass

    await reclaim_stale_processing_jobs(stale_secs=90)

    bot_token = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    bot_client_holder = {"client": None, "lock": asyncio.Lock(), "missing_warned": False}
    active_url_workers = {}     # {slot_idx: asyncio.Task}
    in_flight_url_ids = set()   # {url_job_id}
    url_batch_status_msgs = {}  # {batch_key: (chat_id, msg_id)}
    url_batch_locks = {}        # {batch_key: asyncio.Lock}
    url_transient_retries = {}  # {url_job_id: retry_count}

    # 2. In-Memory Userbot Client Pool
    client_pool = {}          # {session_hash: TelegramClient}
    client_locks = {}         # {session_hash: asyncio.Lock}
    revoked_keys = set()      # {session_hash} for revoked or permanently invalid sessions
    flood_wait_until = {}     # {session_hash: epoch_ts}
    active_workers = {}       # {(session_hash, slot_idx): asyncio.Task}
    in_flight_ids = set()     # {task_id} claimed by this runner
    full_dialogs_warmed = {}  # {session_hash: epoch_ts}
    transient_retries = {}    # {task_id: retry_count}

    async def warm_client_dialogs(cl, s_key: int, full_scan: bool = False):
        if not cl or not hasattr(cl, "get_dialogs"):
            return
        try:
            lim = None if full_scan else 250
            await asyncio.wait_for(cl.get_dialogs(limit=lim), timeout=60.0 if full_scan else 45.0)
            if full_scan:
                full_dialogs_warmed[s_key] = time.time()
        except Exception:
            pass

    async def lookup_access_hash_from_db(chat_id: int) -> int:
        try:
            async with db_lock:
                c.execute(
                    """
                    SELECT source_access_hash FROM ironroute_routes
                    WHERE source_peer_id = %s AND source_access_hash IS NOT NULL AND source_access_hash != 0
                    LIMIT 1
                    """,
                    (int(chat_id),),
                )
                row = c.fetchone()
                if row and row[0]:
                    return int(row[0])
                c.execute(
                    """
                    SELECT target_access_hash FROM ironroute_routes
                    WHERE target_peer_id = %s AND target_access_hash IS NOT NULL AND target_access_hash != 0
                    LIMIT 1
                    """,
                    (int(chat_id),),
                )
                row2 = c.fetchone()
                if row2 and row2[0]:
                    return int(row2[0])
        except Exception:
            pass
        return 0

    async def resolve_chat_entity(cl, s_key: int, chat_id: int):
        if not cl or not hasattr(cl, "get_input_entity"):
            return chat_id
        try:
            return await cl.get_input_entity(int(chat_id))
        except Exception:
            # Cold worker sessions start with an empty entity cache -> refresh dialogs before failing
            if time.time() - full_dialogs_warmed.get(s_key, 0.0) > 180.0:
                await warm_client_dialogs(cl, s_key, full_scan=True)
                try:
                    return await cl.get_input_entity(int(chat_id))
                except Exception:
                    pass

            ah = await lookup_access_hash_from_db(int(chat_id))
            if ah and types and hasattr(types, "InputPeerChannel"):
                s_cid = str(chat_id)
                if s_cid.startswith("-100"):
                    bare_id = int(s_cid[4:])
                    return types.InputPeerChannel(channel_id=bare_id, access_hash=ah)
            raise

    async def lookup_mapped_reply_id(src_chat: int, src_reply_msg_id: int, tgt_chat: int):
        if not src_reply_msg_id or int(src_reply_msg_id) <= 0:
            return None
        s_cands = _chat_id_candidates(src_chat)
        t_cands = _chat_id_candidates(tgt_chat)
        try:
            async with db_lock:
                c.execute(
                    """
                    SELECT target_msg_id FROM message_mappings
                    WHERE source_chat_id = ANY(%s)
                      AND source_msg_id = %s
                      AND target_chat_id = ANY(%s)
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (s_cands, int(src_reply_msg_id), t_cands),
                )
                row = c.fetchone()
                if row and row[0] and int(row[0]) > 0:
                    return int(row[0])
        except Exception:
            pass
        return None

    async def save_mapped_reply_id(src_chat: int, src_msg_id: int, tgt_chat: int, tgt_msg_id: int):
        if not src_msg_id or not tgt_msg_id or int(tgt_msg_id) <= 0:
            return
        try:
            async with db_lock:
                c.execute(
                    """
                    INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_chat_id, source_msg_id, target_chat_id)
                    DO UPDATE SET target_msg_id = EXCLUDED.target_msg_id
                    """,
                    (int(src_chat), int(src_msg_id), int(tgt_chat), int(tgt_msg_id)),
                )
        except Exception:
            pass

    async def get_or_connect_client(session_str: str):
        """Retrieves or starts a connected Telethon client in memory with clean socket lifecycle."""
        if not session_str or not isinstance(session_str, str) or len(session_str.strip()) < 20:
            return None

        clean_sess = session_str.strip()
        s_key = hash(clean_sess)
        if s_key in revoked_keys:
            return None

        c_lock = client_locks.setdefault(s_key, asyncio.Lock())
        async with c_lock:
            if s_key in revoked_keys:
                return None

            if s_key in client_pool:
                cl = client_pool[s_key]
                if cl and cl.is_connected():
                    return cl
                client_pool.pop(s_key, None)

            cl = None
            try:
                # receive_updates=False ensures the worker acts as an on-demand RPC client
                # without subscribing to event streams, eliminating MTProto seq_no collision.
                cl = TelegramClient(
                    StringSession(clean_sess),
                    api_id,
                    api_hash,
                    device_model="Cloud Runner 64bit",
                    system_version="Linux Ubuntu",
                    app_version="4.16.30",
                    sequential_updates=False,
                    receive_updates=False,
                )
                await cl.connect()
                if not await cl.is_user_authorized():
                    print("  ↳ ⚠️ Userbot session is unauthorized/logged out.")
                    revoked_keys.add(s_key)
                    await cl.disconnect()
                    return None

                user_obj = None
                try:
                    user_obj = await cl.get_me()
                except Exception:
                    pass
                cl._cached_uid = str(user_obj.id) if user_obj else "unknown"
                cl._cached_phone = f"+{str(user_obj.phone).lstrip('+')}" if (user_obj and user_obj.phone) else "unknown"
                cl._cached_name = user_obj.first_name if user_obj else "Userbot"

                # Populate fresh worker StringSession entity cache so -100 channel IDs resolve immediately
                await warm_client_dialogs(cl, s_key, full_scan=False)

                client_pool[s_key] = cl
                print(f"  ↳ 🔑 Authenticated Userbot UID {cl._cached_uid} ({cl._cached_phone}) securely in memory.")
                print(f"  ↳ 👥 Connected Userbots in RAM Pool: {len(client_pool)}")
                return cl

            except errors.AuthKeyDuplicatedError:
                print("  ↳ ❌ CRITICAL: AuthKeyDuplicatedError! Session key was revoked by Telegram.")
                print("  ↳ 💡 Root Cause: This exact session was used from two IPs simultaneously (Render + GitHub).")
                print("  ↳ 💡 Solution: Generate a separate worker session for this userbot to avoid IP collision.")
                revoked_keys.add(s_key)
                if cl:
                    try:
                        await cl.disconnect()
                    except Exception:
                        pass
                return None

            except (errors.SessionRevokedError, errors.UserDeactivatedError, errors.UserDeactivatedBanError) as sess_err:
                print(f"  ↳ ❌ Session invalid or revoked ({type(sess_err).__name__}).")
                revoked_keys.add(s_key)
                if cl:
                    try:
                        await cl.disconnect()
                    except Exception:
                        pass
                return None

            except Exception as conn_err:
                print(f"  ↳ ⚠️ Userbot client authorization error: {type(conn_err).__name__}")
                if cl:
                    try:
                        await cl.disconnect()
                    except Exception:
                        pass
                return None

    async def claim_next_job_for_session(session_str: str):
        async with db_lock:
            c.execute(
                """
                UPDATE cloud_relay_queue
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id
                    FROM cloud_relay_queue
                    WHERE status = 'pending' AND session_string = %s
                    ORDER BY id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, source_chat_id, source_msg_id, target_chat_id, target_topic_id, caption, COALESCE(reply_to_msg_id, 0);
                """,
                (session_str,),
            )
            row = c.fetchone()
            if row:
                in_flight_ids.add(row[0])
            return row

    async def mark_job_deleted(task_id: int):
        async with db_lock:
            c.execute("DELETE FROM cloud_relay_queue WHERE id = %s", (task_id,))
            in_flight_ids.discard(task_id)

    async def mark_job_pending(task_id: int):
        async with db_lock:
            c.execute(
                "UPDATE cloud_relay_queue SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = %s AND status = 'processing'",
                (task_id,),
            )
            in_flight_ids.discard(task_id)

    async def mark_job_failed(task_id: int):
        async with db_lock:
            c.execute(
                "UPDATE cloud_relay_queue SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (task_id,),
            )
            in_flight_ids.discard(task_id)

    async def fail_all_for_session(session_str: str):
        async with db_lock:
            c.execute(
                "UPDATE cloud_relay_queue SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE session_string = %s AND status IN ('pending', 'processing')",
                (session_str,),
            )

    async def send_with_caption_and_reply(client, tgt_entity, temp_path, cap_to_send, fmt_entities, target_reply_to, topic_reply_to, media_meta=None):
        """Sends media preserving attributes/thumbnail, formatting entities, and mapped reply target."""
        kwargs = {
            "entity": tgt_entity,
            "file": temp_path,
            "caption": cap_to_send,
            "reply_to": target_reply_to,
        }
        if fmt_entities and cap_to_send:
            kwargs["formatting_entities"] = fmt_entities
        if media_meta:
            for k in ("thumb", "attributes", "mime_type", "supports_streaming", "nosound_video", "video_note"):
                if media_meta.get(k) is not None:
                    kwargs[k] = media_meta[k]

        for _ in range(3):
            try:
                return await asyncio.wait_for(client.send_file(**kwargs), timeout=1800.0)
            except Exception as send_err:
                err_s = str(send_err).lower()
                retried = False
                if "formatting_entities" in kwargs and ("entit" in err_s or "custom_emoji" in err_s or "offset" in err_s):
                    kwargs.pop("formatting_entities", None)
                    retried = True
                if target_reply_to != topic_reply_to and kwargs.get("reply_to") != topic_reply_to and any(
                    k in err_s for k in ("reply", "message_id_invalid", "msg_id_invalid", "topic_deleted")
                ):
                    kwargs["reply_to"] = topic_reply_to
                    retried = True
                if "thumb" in kwargs and any(k in err_s for k in ("thumb", "photo_invalid", "image_process")):
                    kwargs.pop("thumb", None)
                    retried = True
                if any(k in kwargs for k in ("attributes", "nosound_video", "video_note")) and any(
                    k in err_s for k in ("attribute", "video", "media_invalid", "file_parts")
                ):
                    kwargs.pop("attributes", None)
                    kwargs.pop("nosound_video", None)
                    kwargs.pop("video_note", None)
                    retried = True
                if not retried:
                    raise
        return await asyncio.wait_for(client.send_file(**kwargs), timeout=1800.0)

    async def run_userbot_worker(session_str: str, slot_idx: int = 0):
        """Dedicated concurrent worker slot for one userbot session."""
        nonlocal last_active_time
        clean_sess = session_str.strip()
        s_key = hash(clean_sess)

        while not stop_requested.is_set():
            if s_key in revoked_keys:
                await fail_all_for_session(session_str)
                break

            fw_until = flood_wait_until.get(s_key, 0.0)
            if time.time() < fw_until:
                break

            try:
                job = await claim_next_job_for_session(session_str)
            except Exception as qe:
                print(f"⚠️ Database claim error: {type(qe).__name__}")
                await asyncio.sleep(3)
                continue

            if not job:
                break

            task_id, src_chat, msg_id, tgt_chat, tgt_topic, caption, db_reply_src_id = job
            last_active_time = time.time()

            client = await get_or_connect_client(clean_sess)
            if not client:
                if s_key in revoked_keys:
                    await mark_job_failed(task_id)
                    await fail_all_for_session(session_str)
                    print(f"  ↳ 🚫 [Job #{task_id}] Session revoked/invalid; marked pending jobs for this account failed.")
                else:
                    # Transient connect issue -> release back to pending rather than failing forever
                    await mark_job_pending(task_id)
                break

            userbot_uid = getattr(client, "_cached_uid", "unknown")
            userbot_phone = getattr(client, "_cached_phone", "")
            ub_label = f"UID {userbot_uid} ({userbot_phone})" if userbot_phone != "unknown" else f"UID {userbot_uid}"

            print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"📥 [Job #{task_id}] RESTRICTED MEDIA RELAY TRANSFER [Slot {slot_idx + 1}/{WORKERS_PER_SESSION}]")
            print(f"  • Userbot:        {ub_label} [Active Workers: {len(active_workers)} | Pool: {len(client_pool)}]")
            print(f"  • Source Chat ID: {src_chat} (Message #{msg_id})")
            print(f"  • Target Chat ID: {tgt_chat} (Topic #{tgt_topic or 0})")
            print(f"  • Route:          Source [{src_chat}] ➔ Target [{tgt_chat}]")

            job_tmp_dir = tempfile.mkdtemp(prefix=f"relay_job_{task_id}_")
            try:
                src_entity = await resolve_chat_entity(client, s_key, src_chat)
                msg = await asyncio.wait_for(client.get_messages(src_entity, ids=msg_id), timeout=60.0)
                if not msg or not msg.media:
                    print(f"  ↳ Notice: Message #{msg_id} has no media in Source [{src_chat}]. Marking completed.")
                    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                    await mark_job_deleted(task_id)
                    continue

                print(f"  ↳ ⬇️ [UID {userbot_uid}:S{slot_idx + 1}] Downloading media from Source [{src_chat}]...")
                temp_path = await asyncio.wait_for(client.download_media(msg, file=job_tmp_dir), timeout=1800.0)

                if not temp_path or not os.path.exists(temp_path):
                    raise RuntimeError("Downloaded media file path not found on disk.")

                file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
                print(f"  ↳ 📦 [UID {userbot_uid}:S{slot_idx + 1}] Payload Size: {file_size_mb:.2f} MB")

                # Resolve Caption & Formatting Entities honoring Target-Pair / Auto-Pair caption setting
                orig_msg_cap = getattr(msg, "message", "") or ""
                if caption == STRIP_CAPTION_SENTINEL:
                    cap_to_send = ""
                    fmt_entities = None
                elif caption and caption != orig_msg_cap:
                    cap_to_send = caption
                    fmt_entities = None
                else:
                    cap_to_send = orig_msg_cap if orig_msg_cap else (caption or "")
                    fmt_entities = getattr(msg, "entities", None) if cap_to_send == orig_msg_cap else None

                # Resolve Reply-To Target ID via message_mappings (using queued or live msg reply header)
                topic_reply_to = int(tgt_topic) if tgt_topic and int(tgt_topic) > 0 else None
                eff_reply_src_id = int(db_reply_src_id or 0) or _extract_reply_src_id_from_msg(msg)
                mapped_reply_id = await lookup_mapped_reply_id(src_chat, eff_reply_src_id, tgt_chat) if eff_reply_src_id else None
                target_reply_to = mapped_reply_id if mapped_reply_id else topic_reply_to

                print(
                    f"  ↳ ⬆️ [UID {userbot_uid}:S{slot_idx + 1}] Streaming upload to Target [{tgt_chat}]"
                    + (f" (Replying to #{mapped_reply_id})" if mapped_reply_id else "")
                    + "..."
                )

                tgt_entity = await resolve_chat_entity(client, s_key, tgt_chat)
                media_meta = await prepare_media_upload_meta(client, msg, temp_path, job_tmp_dir)
                sent_msg = await send_with_caption_and_reply(
                    client,
                    tgt_entity,
                    temp_path,
                    cap_to_send,
                    fmt_entities,
                    target_reply_to,
                    topic_reply_to,
                    media_meta=media_meta,
                )

                sent_id = getattr(sent_msg, "id", None) if sent_msg else None
                if sent_id:
                    await save_mapped_reply_id(src_chat, msg_id, tgt_chat, sent_id)

                await mark_job_deleted(task_id)
                transient_retries.pop(task_id, None)
                last_active_time = time.time()
                print(f"  ↳ ✅ [UID {userbot_uid}:S{slot_idx + 1}] Successfully transferred {file_size_mb:.2f} MB to Target [{tgt_chat}].")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                await asyncio.sleep(1.0)

            except errors.FloodWaitError as fwe:
                wait_s = int(getattr(fwe, "seconds", 15))
                print(f"  ↳ ⏳ [UID {userbot_uid}] FloodWait: Telegram requested {wait_s}s wait on Job #{task_id}.")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                if wait_s <= 30:
                    await mark_job_pending(task_id)
                    await asyncio.sleep(wait_s + 2)
                else:
                    # Isolate FloodWait strictly to THIS userbot; never stop other parallel userbots!
                    flood_wait_until[s_key] = time.time() + wait_s + 2
                    await mark_job_pending(task_id)
                    print(f"  ↳ ⏸️ Pausing UID {userbot_uid} for {wait_s}s while other userbots continue transferring.")
                    break

            except errors.AuthKeyDuplicatedError:
                print(f"  ↳ ❌ [UID {userbot_uid}] AuthKeyDuplicatedError on Job #{task_id}! Session invalidated by Telegram.")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                revoked_keys.add(s_key)
                async with client_locks.setdefault(s_key, asyncio.Lock()):
                    cl_old = client_pool.pop(s_key, None)
                    if cl_old:
                        try:
                            await cl_old.disconnect()
                        except Exception:
                            pass
                await mark_job_failed(task_id)
                await fail_all_for_session(session_str)
                print(f"  ↳ 🚫 Marked remaining jobs for UID {userbot_uid} failed; other userbots unaffected.")
                break

            except (errors.SessionRevokedError, errors.UserDeactivatedError, errors.UserDeactivatedBanError) as se:
                print(f"  ↳ ❌ [UID {userbot_uid}] Session revoked on Job #{task_id}: {type(se).__name__}")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                revoked_keys.add(s_key)
                async with client_locks.setdefault(s_key, asyncio.Lock()):
                    cl_old = client_pool.pop(s_key, None)
                    if cl_old:
                        try:
                            await cl_old.disconnect()
                        except Exception:
                            pass
                await mark_job_failed(task_id)
                await fail_all_for_session(session_str)
                break

            except asyncio.CancelledError:
                await mark_job_pending(task_id)
                raise

            except Exception as proc_err:
                err_txt = str(proc_err)
                is_transient = isinstance(proc_err, (asyncio.TimeoutError, TimeoutError, ConnectionError)) or any(
                    k in err_txt for k in ("Request was unsuccessful", "Server closed the connection", "Timed out")
                )
                cur_tries = transient_retries.get(task_id, 0)
                if is_transient and cur_tries < 2:
                    transient_retries[task_id] = cur_tries + 1
                    print(
                        f"  ↳ 🔁 Transient timeout on Job #{task_id} ({type(proc_err).__name__}); "
                        f"re-queueing (retry {cur_tries + 1}/2)..."
                    )
                    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                    await mark_job_pending(task_id)
                    await asyncio.sleep(5)
                else:
                    transient_retries.pop(task_id, None)
                    print(
                        f"  ↳ ❌ Error processing Job #{task_id} (Source [{src_chat}] ➔ Target [{tgt_chat}] "
                        f"via Userbot {userbot_uid}): {type(proc_err).__name__} - {proc_err}"
                    )
                    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                    await mark_job_failed(task_id)
                    await asyncio.sleep(2)

            finally:
                if os.path.exists(job_tmp_dir):
                    try:
                        shutil.rmtree(job_tmp_dir, ignore_errors=True)
                    except Exception:
                        pass

    # -------------------------------------------------------------------------
    # Bot-API / URL Media Worker Pool (Web Scraper, Site Collector, Explorer, X)
    # -------------------------------------------------------------------------
    async def _bot_api_call(method: str, data: dict = None, files=None, timeout: float = 45.0):
        if not bot_token or not requests:
            return None
        api_url = f"https://api.telegram.org/bot{bot_token}/{method}"

        def _do():
            if files is not None:
                return requests.post(api_url, data=data or {}, files=files, timeout=timeout)
            return requests.post(api_url, json=data or {}, timeout=timeout)

        try:
            resp = await asyncio.to_thread(_do)
            return resp.json() if resp is not None else None
        except Exception:
            return None

    async def get_or_connect_bot_client():
        if not bot_token:
            return None
        async with bot_client_holder["lock"]:
            cl = bot_client_holder["client"]
            if cl and cl.is_connected():
                return cl
            try:
                cl = TelegramClient(StringSession(), api_id, api_hash, sequential_updates=False)
                await cl.start(bot_token=bot_token)
                bot_client_holder["client"] = cl
                return cl
            except Exception as be:
                print(f"  ↳ ⚠️ Could not start Telethon Bot client for >50MB upload: {type(be).__name__} - {be}")
                return None

    async def claim_next_url_job():
        async with db_lock:
            c.execute(
                """
                SELECT id, source_plugin, media_type, media_url, page_url, thumb_url, title,
                       caption, headers_json, target_chat_id, target_topic_id, notify_chat_id, batch_tag
                FROM cloud_url_jobs
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 1
                """
            )
            row = c.fetchone()
            if not row:
                return None
            job_id = row[0]
            c.execute(
                "UPDATE cloud_url_jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = %s AND status = 'pending'",
                (job_id,),
            )
            if getattr(c, "rowcount", 1) == 0:
                return None
            in_flight_url_ids.add(job_id)
            return row

    async def is_url_job_cancelled(job_id: int) -> bool:
        try:
            async with db_lock:
                c.execute("SELECT status FROM cloud_url_jobs WHERE id = %s", (job_id,))
                row = c.fetchone()
                return bool(row and row[0] == "cancelled")
        except Exception:
            return False

    async def mark_url_job_status(job_id: int, new_status: str):
        in_flight_url_ids.discard(job_id)
        try:
            async with db_lock:
                if new_status == "deleted":
                    c.execute("DELETE FROM cloud_url_jobs WHERE id = %s", (job_id,))
                else:
                    c.execute(
                        "UPDATE cloud_url_jobs SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (new_status, job_id),
                    )
        except Exception:
            pass

    def _download_http_file_sync(url: str, dest_path: str, headers: dict, max_wait: int = 240) -> tuple:
        if not requests:
            return False, "requests missing"
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(15, max_wait), allow_redirects=True) as r:
                if r.status_code not in (200, 206):
                    return False, f"HTTP {r.status_code}"
                ctype = (r.headers.get("content-type") or "").lower()
                if "text/html" in ctype or "application/xhtml" in ctype or "mpegurl" in ctype:
                    return False, f"non-media content-type ({ctype})"
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=131072):
                        if chunk:
                            f.write(chunk)
            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 4096:
                return True, ""
            return False, "empty file"
        except Exception as e:
            return False, str(e)

    def _download_ytdlp_sync(urls_to_try: list, out_dir: str, headers: dict, audio_only: bool = False) -> tuple:
        try:
            import yt_dlp
        except ImportError:
            return None, "yt_dlp not installed"
        out_tmpl = os.path.join(out_dir, "dl_%(autonumber)s.%(ext)s")
        fmt = "bestaudio/best" if audio_only else "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
        ydl_opts = {
            "format": fmt,
            "outtmpl": out_tmpl,
            "quiet": True,
            "no_warnings": True,
            "overwrites": True,
            "concurrent_fragment_downloads": 6,
            "socket_timeout": 30,
            "http_headers": headers,
            "merge_output_format": "mp4" if not audio_only else None,
        }
        last_err = "no file"
        for target_u in urls_to_try:
            if not target_u:
                continue
            try:
                with yt_dlp.YoutubeDL({k: v for k, v in ydl_opts.items() if v is not None}) as ydl:
                    ydl.download([target_u])
                cands = [
                    os.path.join(out_dir, fn)
                    for fn in os.listdir(out_dir)
                    if not fn.endswith(".part") and os.path.getsize(os.path.join(out_dir, fn)) > 8192
                ]
                if cands:
                    cands.sort(key=lambda p: os.path.getsize(p), reverse=True)
                    return cands[0], ""
            except Exception as e:
                last_err = str(e)
        return None, last_err

    async def _send_bot_status_connected(job_id: int, batch_key: str, ui_chat: int, ui_topic: int, label_text: str):
        lock = url_batch_locks.setdefault(batch_key, asyncio.Lock())
        async with lock:
            existing = url_batch_status_msgs.get(batch_key)
            text = (
                f"⚡ <b>GitHub Actions 7GB Runner Connected!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 <b>Target:</b> <code>{html.escape(label_text[:70])}</code>\n"
                f"🚀 <i>Downloading and streaming via 10Gbps Azure pipeline...</i>"
            )
            if existing:
                e_chat, e_msg = existing
                await _bot_api_call("editMessageText", {"chat_id": e_chat, "message_id": e_msg, "text": text, "parse_mode": "HTML"})
                return e_chat, e_msg
            payload = {"chat_id": int(ui_chat), "text": text, "parse_mode": "HTML"}
            if ui_topic and int(ui_topic) > 0:
                payload["message_thread_id"] = int(ui_topic)
            res = await _bot_api_call("sendMessage", payload)
            if res and res.get("ok") and res.get("result"):
                msg_id_sent = res["result"].get("message_id")
                url_batch_status_msgs[batch_key] = (int(ui_chat), int(msg_id_sent))
                return int(ui_chat), int(msg_id_sent)
        return None, None

    async def _finish_url_status_if_batch_done(batch_key: str, batch_tag: str, st_chat: int, st_msg_id: int):
        if not (st_chat and st_msg_id):
            return
        should_clear = not batch_tag
        if batch_tag:
            try:
                async with db_lock:
                    c.execute(
                        "SELECT COUNT(*) FROM cloud_url_jobs WHERE batch_tag = %s AND status IN ('pending', 'processing')",
                        (batch_tag,),
                    )
                    rem_row = c.fetchone()
                    rem = int(rem_row[0]) if rem_row else 0
                should_clear = (rem == 0)
            except Exception:
                pass
        if should_clear:
            await _bot_api_call("deleteMessage", {"chat_id": st_chat, "message_id": st_msg_id})
            url_batch_status_msgs.pop(batch_key, None)

    async def run_url_media_worker(slot_idx: int = 0):
        nonlocal last_active_time
        while not stop_requested.is_set():
            if not bot_token:
                if not bot_client_holder["missing_warned"]:
                    bot_client_holder["missing_warned"] = True
                    print("⚠️ BOT_TOKEN is empty in GitHub Secrets; URL media jobs (web/collector/explorer/X) paused.")
                break

            try:
                row = await claim_next_url_job()
            except Exception as qe:
                print(f"⚠️ URL Queue claim error: {type(qe).__name__}")
                await asyncio.sleep(3)
                continue

            if not row:
                break

            (
                u_job_id, source_plugin, media_type, media_url, page_url, thumb_url,
                title, caption, headers_json, tgt_chat, tgt_topic, notify_chat, batch_tag
            ) = row
            last_active_time = time.time()

            if await is_url_job_cancelled(u_job_id):
                await mark_url_job_status(u_job_id, "cancelled")
                continue

            ui_chat = int(notify_chat) if notify_chat and int(notify_chat) != 0 else int(tgt_chat)
            ui_topic = int(tgt_topic) if (ui_chat == int(tgt_chat) and tgt_topic and int(tgt_topic) > 0) else 0
            batch_key = batch_tag or f"url_{u_job_id}"
            disp_label = title or page_url or media_url

            print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"🌐 [URL Job #{u_job_id}] {source_plugin.upper()} ({media_type.upper()}) [URL Slot {slot_idx + 1}/{URL_WORKERS}]")
            print(f"  • Target Chat ID: {tgt_chat} (Topic #{tgt_topic or 0})")

            job_tmp_dir = tempfile.mkdtemp(prefix=f"relay_url_{u_job_id}_")
            try:
                hdrs = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Referer": page_url or (media_url if not media_url.startswith("[") else ""),
                }
                if headers_json:
                    try:
                        parsed_h = json.loads(headers_json)
                        if isinstance(parsed_h, dict):
                            hdrs.update({str(k): str(v) for k, v in parsed_h.items() if v is not None})
                    except Exception:
                        pass

                st_chat, st_msg_id = await _send_bot_status_connected(u_job_id, batch_key, ui_chat, ui_topic, disp_label)

                if media_type == "album":
                    try:
                        img_urls = json.loads(media_url) if media_url.strip().startswith("[") else [media_url]
                    except Exception:
                        img_urls = [media_url]
                    img_files = []
                    for idx_i, iu in enumerate(img_urls[:10]):
                        if await is_url_job_cancelled(u_job_id):
                            raise asyncio.CancelledError()
                        ip = os.path.join(job_tmp_dir, f"img_{idx_i}.jpg")
                        ok_i, _ = await asyncio.to_thread(_download_http_file_sync, iu, ip, hdrs, 30)
                        if ok_i:
                            img_files.append(ip)
                    if not img_files:
                        raise RuntimeError("No album photos could be downloaded.")
                    if len(img_files) > 1:
                        media_payload = []
                        open_handles = {}
                        try:
                            for idx_f, fp in enumerate(img_files):
                                attach_key = f"photo_{idx_f}"
                                open_handles[attach_key] = open(fp, "rb")
                                item_d = {"type": "photo", "media": f"attach://{attach_key}"}
                                if idx_f == 0 and caption:
                                    item_d["caption"] = caption[:1020]
                                    item_d["parse_mode"] = "HTML"
                                media_payload.append(item_d)
                            form_d = {"chat_id": int(tgt_chat), "media": json.dumps(media_payload)}
                            if tgt_topic and int(tgt_topic) > 0:
                                form_d["message_thread_id"] = int(tgt_topic)
                            res_grp = await _bot_api_call("sendMediaGroup", form_d, files=open_handles, timeout=120.0)
                        finally:
                            for fh in open_handles.values():
                                try:
                                    fh.close()
                                except Exception:
                                    pass
                        if not (res_grp and res_grp.get("ok")):
                            for idx_f, fp in enumerate(img_files):
                                with open(fp, "rb") as pf:
                                    fd = {"chat_id": int(tgt_chat), "caption": caption[:1020] if idx_f == 0 else "", "parse_mode": "HTML"}
                                    if tgt_topic and int(tgt_topic) > 0:
                                        fd["message_thread_id"] = int(tgt_topic)
                                    await _bot_api_call("sendPhoto", fd, files={"photo": pf}, timeout=60.0)
                    else:
                        with open(img_files[0], "rb") as pf:
                            fd = {"chat_id": int(tgt_chat), "caption": caption[:1020], "parse_mode": "HTML"}
                            if tgt_topic and int(tgt_topic) > 0:
                                fd["message_thread_id"] = int(tgt_topic)
                            await _bot_api_call("sendPhoto", fd, files={"photo": pf}, timeout=60.0)
                    await mark_url_job_status(u_job_id, "deleted")
                    print(f"  ↳ ✅ [URL Job #{u_job_id}] Delivered album ({len(img_files)} photos).")
                    await _finish_url_status_if_batch_done(batch_key, batch_tag, st_chat, st_msg_id)
                    continue

                if media_type == "photo":
                    photo_path = os.path.join(job_tmp_dir, "photo.jpg")
                    ok_p, err_p = await asyncio.to_thread(_download_http_file_sync, media_url, photo_path, hdrs, 45)
                    if not ok_p:
                        raise RuntimeError(f"Photo download failed: {err_p}")
                    p_mb = os.path.getsize(photo_path) / (1024 * 1024)
                    if p_mb > 9.5:
                        with open(photo_path, "rb") as df:
                            fd = {"chat_id": int(tgt_chat), "caption": caption[:1020], "parse_mode": "HTML"}
                            if tgt_topic and int(tgt_topic) > 0:
                                fd["message_thread_id"] = int(tgt_topic)
                            await _bot_api_call("sendDocument", fd, files={"document": df}, timeout=90.0)
                    else:
                        with open(photo_path, "rb") as pf:
                            fd = {"chat_id": int(tgt_chat), "caption": caption[:1020], "parse_mode": "HTML"}
                            if tgt_topic and int(tgt_topic) > 0:
                                fd["message_thread_id"] = int(tgt_topic)
                            res_ph = await _bot_api_call("sendPhoto", fd, files={"photo": pf}, timeout=90.0)
                        if not (res_ph and res_ph.get("ok")):
                            with open(photo_path, "rb") as df:
                                await _bot_api_call("sendDocument", fd, files={"document": df}, timeout=90.0)
                    await mark_url_job_status(u_job_id, "deleted")
                    print(f"  ↳ ✅ [URL Job #{u_job_id}] Delivered photo ({p_mb:.2f} MB).")
                    await _finish_url_status_if_batch_done(batch_key, batch_tag, st_chat, st_msg_id)
                    continue

                # Video / Audio flow
                is_audio = (media_type == "audio")
                dl_path = None
                dl_err = ""
                u_low = media_url.lower()
                is_page_or_hls = any(k in u_low for k in (".m3u8", ".mpd", "/watch", "/view_video", "x.com/", "twitter.com/", "reddit.com/", "redd.it/"))

                if not is_page_or_hls:
                    ext_g = ".mp3" if is_audio else ".mp4"
                    cand_p = os.path.join(job_tmp_dir, f"direct{ext_g}")
                    ok_d, err_d = await asyncio.to_thread(_download_http_file_sync, media_url, cand_p, hdrs, 300)
                    if ok_d:
                        dl_path = cand_p
                    else:
                        dl_err = err_d

                if not dl_path:
                    cands_yt = [media_url]
                    if page_url and page_url != media_url:
                        cands_yt.append(page_url)
                    dl_path, y_err = await asyncio.to_thread(_download_ytdlp_sync, cands_yt, job_tmp_dir, hdrs, is_audio)
                    if not dl_path:
                        dl_err = y_err or dl_err

                if not dl_path or not os.path.exists(dl_path):
                    raise RuntimeError(f"Download failed: {dl_err}")

                if await is_url_job_cancelled(u_job_id):
                    await mark_url_job_status(u_job_id, "cancelled")
                    continue

                f_size = os.path.getsize(dl_path)
                f_mb = f_size / (1024 * 1024)
                print(f"  ↳ 📦 [URL Job #{u_job_id}] Downloaded {f_mb:.2f} MB. Uploading to Target [{tgt_chat}]...")

                if st_chat and st_msg_id:
                    await _bot_api_call(
                        "editMessageText",
                        {
                            "chat_id": st_chat,
                            "message_id": st_msg_id,
                            "text": (
                                f"⚡ <b>GitHub Actions 7GB Runner Connected!</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"🎯 <b>Target:</b> <code>{html.escape(disp_label[:70])}</code>\n"
                                f"⬆️ <b>Uploading ({f_mb:.1f} MB) via 10Gbps pipeline...</b>"
                            ),
                            "parse_mode": "HTML",
                        },
                    )

                thumb_p = None
                if thumb_url and thumb_url.startswith("http"):
                    tp = os.path.join(job_tmp_dir, "thumb.jpg")
                    ok_th, _ = await asyncio.to_thread(_download_http_file_sync, thumb_url, tp, hdrs, 15)
                    if ok_th and os.path.getsize(tp) <= 204800:
                        thumb_p = tp

                dur, w, h = (0.0, 0, 0)
                if not is_audio:
                    dur, w, h = await _probe_video_ffprobe(dl_path)
                    if not thumb_p:
                        thumb_p = await _extract_ffmpeg_thumb(dl_path, os.path.join(job_tmp_dir, "ff_thumb.jpg"), dur=dur)

                uploaded = False
                if f_mb <= 48.0:
                    method = "sendAudio" if is_audio else "sendVideo"
                    field_k = "audio" if is_audio else "video"
                    fd = {"chat_id": int(tgt_chat), "caption": caption[:1020], "parse_mode": "HTML"}
                    if not is_audio:
                        fd["supports_streaming"] = "true"
                    if dur > 0:
                        fd["duration"] = int(round(dur))
                    if not is_audio and w > 0 and h > 0:
                        fd["width"] = int(w)
                        fd["height"] = int(h)
                    if tgt_topic and int(tgt_topic) > 0:
                        fd["message_thread_id"] = int(tgt_topic)
                    with open(dl_path, "rb") as mf:
                        files_up = {field_k: mf}
                        tf_h = open(thumb_p, "rb") if (thumb_p and os.path.exists(thumb_p)) else None
                        if tf_h:
                            files_up["thumbnail"] = tf_h
                        try:
                            res_up = await _bot_api_call(method, fd, files=files_up, timeout=240.0)
                            uploaded = bool(res_up and res_up.get("ok"))
                        finally:
                            if tf_h:
                                tf_h.close()

                if not uploaded:
                    # Telethon Bot MTProto supports up to 2.0 GB!
                    bot_cl = await get_or_connect_bot_client()
                    if not bot_cl:
                        raise RuntimeError(f"Upload failed (file={f_mb:.1f} MB; Bot API failed & Telethon bot unavailable)")
                    up_kw = {
                        "caption": caption[:1020],
                        "parse_mode": "html",
                        "supports_streaming": (not is_audio),
                    }
                    if thumb_p and os.path.exists(thumb_p):
                        up_kw["thumb"] = thumb_p
                    if tgt_topic and int(tgt_topic) > 0:
                        up_kw["reply_to"] = int(tgt_topic)
                    if types is not None and not is_audio and (dur > 0 or (w > 0 and h > 0)):
                        up_kw["attributes"] = [
                            types.DocumentAttributeVideo(
                                duration=float(max(0.0, dur)),
                                w=int(max(1, w or 1280)),
                                h=int(max(1, h or 720)),
                                supports_streaming=True,
                            )
                        ]
                    await asyncio.wait_for(bot_cl.send_file(int(tgt_chat), dl_path, **up_kw), timeout=1800.0)
                    uploaded = True

                await mark_url_job_status(u_job_id, "deleted")
                url_transient_retries.pop(u_job_id, None)
                last_active_time = time.time()
                print(f"  ↳ ✅ [URL Job #{u_job_id}] Successfully transferred {f_mb:.2f} MB to Target [{tgt_chat}].")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

                await _finish_url_status_if_batch_done(batch_key, batch_tag, st_chat, st_msg_id)

            except asyncio.CancelledError:
                await mark_url_job_status(u_job_id, "pending")
                raise
            except Exception as uerr:
                uerr_s = str(uerr)
                is_tr = any(k in uerr_s.lower() for k in ("timeout", "connection", "temporarily", "reset"))
                cur_t = url_transient_retries.get(u_job_id, 0)
                if is_tr and cur_t < 2:
                    url_transient_retries[u_job_id] = cur_t + 1
                    print(f"  ↳ 🔁 Transient error on URL Job #{u_job_id}: {uerr_s[:100]}; retrying ({cur_t + 1}/2)...")
                    await mark_url_job_status(u_job_id, "pending")
                    await asyncio.sleep(4)
                else:
                    print(f"  ↳ ❌ Error processing URL Job #{u_job_id}: {type(uerr).__name__} - {uerr_s[:180]}")
                    await mark_url_job_status(u_job_id, "failed")
                    if ui_chat:
                        await _bot_api_call(
                            "sendMessage",
                            {
                                "chat_id": ui_chat,
                                "text": f"⚠️ <b>Cloud Download Note:</b> <code>{html.escape(disp_label[:50])}</code>\n<code>{html.escape(uerr_s[:160])}</code>",
                                "parse_mode": "HTML",
                            },
                        )
            finally:
                if os.path.exists(job_tmp_dir):
                    try:
                        shutil.rmtree(job_tmp_dir, ignore_errors=True)
                    except Exception:
                        pass

    try:
        async with db_lock:
            c.execute("SELECT count(*) FROM cloud_relay_queue WHERE status = 'pending'")
            p_count = c.fetchone()[0]
            c.execute("SELECT count(DISTINCT session_string) FROM cloud_relay_queue WHERE status = 'pending' AND session_string IS NOT NULL AND session_string != ''")
            s_count = c.fetchone()[0]
            try:
                c.execute("SELECT count(*) FROM cloud_url_jobs WHERE status = 'pending'")
                u_count = c.fetchone()[0]
            except Exception:
                u_count = 0
        print("📊 Initial Queue Telemetry:")
        print(f"  • Pending Media Tasks:               {p_count} items")
        print(f"  • Pending Web/URL Media Tasks:       {u_count} items ({URL_WORKERS} slots)")
        print(f"  • Unique Userbot Sessions in Queue:  {s_count} accounts ({WORKERS_PER_SESSION} slots each)")
    except Exception:
        pass

    print("🟢 Parallel Multi-Userbot Worker active. Spawning concurrent workers per userbot...\n")

    idle_notice_last_min = -1
    try:
        while True:
            now = time.time()
            elapsed = now - start_time

            # 1. Shift Limit Guard (5 Hours)
            if elapsed >= MAX_SHIFT_SECONDS:
                print(f"\n⏰ Shift duration limit reached ({int(elapsed/3600)}h). Ending shift cleanly...")
                stop_requested.set()
                break

            # 2. Heartbeat Ping (Every 30s)
            if now - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                try:
                    async with db_lock:
                        c.execute("UPDATE cloud_relay_state SET last_heartbeat = %s WHERE id = 1", (now,))
                    last_heartbeat_time = now
                except Exception:
                    pass

            # 3. Periodic check for any long-stale processing rows (every 5m)
            if now - last_stale_reclaim_time >= 300:
                last_stale_reclaim_time = now
                await reclaim_stale_processing_jobs(STALE_PROCESSING_SECONDS)

            # 4. Reap finished worker tasks
            for k in list(active_workers.keys()):
                t = active_workers[k]
                if t.done():
                    try:
                        exc = t.exception()
                        if exc and not isinstance(exc, asyncio.CancelledError):
                            print(f"⚠️ Worker task notice: {type(exc).__name__} - {exc}")
                    except Exception:
                        pass
                    active_workers.pop(k, None)

            for k in list(active_url_workers.keys()):
                t = active_url_workers[k]
                if t.done():
                    try:
                        exc = t.exception()
                        if exc and not isinstance(exc, asyncio.CancelledError):
                            print(f"⚠️ URL Worker task notice: {type(exc).__name__} - {exc}")
                    except Exception:
                        pass
                    active_url_workers.pop(k, None)

            # 5. Discover pending task counts per session & spawn up to WORKERS_PER_SESSION slots each
            session_pending_counts = []
            url_pending_cnt = 0
            try:
                async with db_lock:
                    c.execute(
                        """
                        SELECT session_string, COUNT(*)
                        FROM cloud_relay_queue
                        WHERE status = 'pending'
                          AND session_string IS NOT NULL
                          AND LENGTH(TRIM(session_string)) > 20
                        GROUP BY session_string
                        """
                    )
                    session_pending_counts = [(r[0].strip(), int(r[1])) for r in c.fetchall() if r and r[0]]
                    try:
                        c.execute("SELECT COUNT(*) FROM cloud_url_jobs WHERE status = 'pending'")
                        url_row = c.fetchone()
                        url_pending_cnt = int(url_row[0]) if url_row else 0
                    except Exception:
                        url_pending_cnt = 0
            except Exception as qe:
                print(f"⚠️ Database session poll error: {type(qe).__name__}")
                await asyncio.sleep(5)
                continue

            for sess_str, pending_cnt in session_pending_counts:
                s_key = hash(sess_str)
                if s_key in revoked_keys:
                    continue
                if time.time() < flood_wait_until.get(s_key, 0.0):
                    continue
                slots_wanted = min(WORKERS_PER_SESSION, pending_cnt)
                for slot_idx in range(slots_wanted):
                    w_key = (s_key, slot_idx)
                    if w_key not in active_workers:
                        active_workers[w_key] = asyncio.create_task(run_userbot_worker(sess_str, slot_idx=slot_idx))
                        last_active_time = time.time()

            if url_pending_cnt > 0 and bot_token:
                url_slots_wanted = min(URL_WORKERS, url_pending_cnt)
                for u_slot in range(url_slots_wanted):
                    if u_slot not in active_url_workers:
                        active_url_workers[u_slot] = asyncio.create_task(run_url_media_worker(slot_idx=u_slot))
                        last_active_time = time.time()

            # 6. Idle & FloodWait Shutdown Guard
            if active_workers or active_url_workers:
                last_active_time = time.time()
            else:
                idle_duration = now - last_active_time
                if idle_duration >= IDLE_TIMEOUT_SECONDS:
                    print(f"💤 Queue has had no runnable tasks for {int(idle_duration/60)} minutes. Shutting down runner to save compute.")
                    break
                cur_min = int(idle_duration // 60)
                if cur_min > 0 and cur_min != idle_notice_last_min:
                    idle_notice_last_min = cur_min
                    print(f"⏳ Standby: 0 runnable tasks (idle: {int(idle_duration)}s)...")

            await asyncio.sleep(3)
    finally:
        stop_requested.set()
        all_tasks = list(active_workers.values()) + list(active_url_workers.values())
        if all_tasks:
            print("⏳ Waiting up to 20s for active transfers to release cleanly...")
            done, pending_tasks = await asyncio.wait(all_tasks, timeout=20.0)
            for pt in pending_tasks:
                pt.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)

        # Return any still-in-flight jobs belonging to this runner back to pending cleanly
        if in_flight_ids:
            try:
                async with db_lock:
                    for fid in list(in_flight_ids):
                        c.execute(
                            "UPDATE cloud_relay_queue SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = %s AND status = 'processing'",
                            (fid,),
                        )
                    in_flight_ids.clear()
            except Exception:
                pass
        if in_flight_url_ids:
            try:
                async with db_lock:
                    for ufid in list(in_flight_url_ids):
                        c.execute(
                            "UPDATE cloud_url_jobs SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = %s AND status = 'processing'",
                            (ufid,),
                        )
                    in_flight_url_ids.clear()
            except Exception:
                pass

        print("\n👋 Disconnecting active userbot clients cleanly...")
        for cl in list(client_pool.values()):
            if cl:
                try:
                    await cl.disconnect()
                except Exception:
                    pass
        client_pool.clear()
        if bot_client_holder.get("client"):
            try:
                await bot_client_holder["client"].disconnect()
            except Exception:
                pass
            bot_client_holder["client"] = None

        try:
            conn.close()
        except Exception:
            pass

        print("🏁 Runner Shift Completed Successfully.")


if __name__ == "__main__":
    asyncio.run(main())