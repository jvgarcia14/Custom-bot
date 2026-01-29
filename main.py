# main.py
# ------------------------------------------------------------
# CUSTOMS BOT (Railway + Postgres)
#
# ✅ NO Drive create
# ✅ NO Drive delete
# ✅ Only CONNECTS existing folders to a Telegram Topic
#
# Folder structure:
#   CLIENTS_ROOT_ID/
#       <Client Name>/
#           OnlyFans ✅ (name contains "onlyfans")
#               Customs ✅ (name contains "customs")
#                   ...uploads + subfolders...
#
# Telegram:
# - Add bot to Telegram group
# - Inside the TOPIC you want notifications in:
#     /register autumn
#
# Fixes:
# - ✅ Do NOT reject webhook by channel/resource (that makes it "work once")
# - ✅ Persist page_token in Postgres (survive restarts)
# - ✅ DB reconnect guard
# ------------------------------------------------------------

import os
import json
import re
import uuid
import asyncio
import time
from typing import Optional, Dict, Any, List, Tuple

import httpx
import psycopg2
from psycopg2 import OperationalError
from fastapi import FastAPI, Request, Response, HTTPException

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# ---------------- ENV ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")  # NO trailing slash
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "change-me-telegram-secret")

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
WATCH_SECRET = os.getenv("WATCH_SECRET", "change-me-drive-secret")

DATABASE_URL = os.getenv("DATABASE_URL")
CLIENTS_ROOT_ID = os.getenv("CLIENTS_ROOT_ID")

ONLYFANS_TOKEN = os.getenv("ONLYFANS_TOKEN", "onlyfans")
CUSTOMS_TOKEN = os.getenv("CUSTOMS_TOKEN", "customs")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing env var: TELEGRAM_BOT_TOKEN")
if not BASE_URL:
    raise RuntimeError("Missing env var: BASE_URL")
if BASE_URL.endswith("/"):
    raise RuntimeError("BASE_URL must NOT end with '/'")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Missing env var: GOOGLE_SERVICE_ACCOUNT_JSON")
if not DATABASE_URL:
    raise RuntimeError("Missing env var: DATABASE_URL")
if not CLIENTS_ROOT_ID:
    raise RuntimeError("Missing env var: CLIENTS_ROOT_ID")

# ---------------- DB CONNECTION (reconnect-safe) ----------------
_db = None

def get_db():
    global _db
    try:
        if _db is None or getattr(_db, "closed", 1) != 0:
            _db = psycopg2.connect(DATABASE_URL, sslmode="require")
            _db.autocommit = True
    except OperationalError:
        _db = psycopg2.connect(DATABASE_URL, sslmode="require")
        _db.autocommit = True
    return _db

# ---------------- GOOGLE DRIVE CLIENT ----------------
SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIME = "application/vnd.google-apps.folder"

creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

# ---------------- STATE ----------------
STATE: Dict[str, Any] = {
    "page_token": None,
    "channel_id": None,
    "resource_id": None,
    "expiration_ms": None,

    "customs_map": {},          # customs_root_id -> {chat_id, thread_id, client_name,...}
    "bindings_by_client": {},   # client_key -> binding details (for /list)

    "parents_cache": {},
    "name_cache": {},
    "route_cache": {},
}

RENEW_TASK: Optional[asyncio.Task] = None

# ---------------- NORMALIZATION ----------------
def canonical_name(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def normalize_spaces(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    return name

# ---------------- DB INIT + APP STATE ----------------
def init_db() -> None:
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bindings (
                client_key TEXT PRIMARY KEY,
                client_name TEXT NOT NULL,
                client_folder_id TEXT NOT NULL,
                onlyfans_folder_id TEXT NOT NULL,
                customs_folder_id TEXT NOT NULL,
                chat_id BIGINT NOT NULL,
                thread_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notified_files (
                file_id TEXT PRIMARY KEY,
                notified_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

def state_set(k: str, v: str) -> None:
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO app_state (k, v, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = NOW();
            """,
            (k, v),
        )

def state_get(k: str) -> Optional[str]:
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT v FROM app_state WHERE k=%s;", (k,))
        row = cur.fetchone()
        return row[0] if row else None

def save_binding(
    client_key: str,
    client_name: str,
    client_folder_id: str,
    onlyfans_folder_id: str,
    customs_folder_id: str,
    chat_id: int,
    thread_id: int,
) -> None:
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bindings (
                client_key, client_name,
                client_folder_id, onlyfans_folder_id, customs_folder_id,
                chat_id, thread_id, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (client_key) DO UPDATE SET
                client_name = EXCLUDED.client_name,
                client_folder_id = EXCLUDED.client_folder_id,
                onlyfans_folder_id = EXCLUDED.onlyfans_folder_id,
                customs_folder_id = EXCLUDED.customs_folder_id,
                chat_id = EXCLUDED.chat_id,
                thread_id = EXCLUDED.thread_id,
                updated_at = NOW();
            """,
            (client_key, client_name, client_folder_id, onlyfans_folder_id, customs_folder_id, chat_id, thread_id),
        )

def delete_binding(client_key: str) -> bool:
    db = get_db()
    with db.cursor() as cur:
        cur.execute("DELETE FROM bindings WHERE client_key=%s;", (client_key,))
        return cur.rowcount > 0

def load_bindings_into_state() -> None:
    STATE["customs_map"] = {}
    STATE["bindings_by_client"] = {}
    STATE["route_cache"] = {}

    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT client_key, client_name, client_folder_id, onlyfans_folder_id, customs_folder_id, chat_id, thread_id
            FROM bindings;
            """
        )
        rows = cur.fetchall()

    for (client_key, client_name, client_folder_id, onlyfans_folder_id, customs_folder_id, chat_id, thread_id) in rows:
        STATE["bindings_by_client"][client_key] = {
            "client_key": client_key,
            "client_name": client_name,
            "client_folder_id": client_folder_id,
            "onlyfans_folder_id": onlyfans_folder_id,
            "customs_folder_id": customs_folder_id,
            "chat_id": int(chat_id),
            "thread_id": int(thread_id),
        }
        STATE["customs_map"][customs_folder_id] = {
            "chat_id": int(chat_id),
            "thread_id": int(thread_id),
            "client_key": client_key,
            "client_name": client_name,
            "customs_folder_id": customs_folder_id,
        }

def was_file_notified(file_id: str) -> bool:
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM notified_files WHERE file_id=%s;", (file_id,))
        return cur.fetchone() is not None

def mark_file_notified(file_id: str) -> None:
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO notified_files (file_id) VALUES (%s)
            ON CONFLICT (file_id) DO NOTHING;
            """,
            (file_id,),
        )

# ---------------- TELEGRAM HELPERS ----------------
async def tg_call(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram API error: {r.status_code} {r.text}")
        return r.json()

async def tg_send(chat_id: int, text: str, thread_id: Optional[int] = None) -> None:
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False}
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    await tg_call("sendMessage", payload)

async def ensure_telegram_webhook() -> None:
    hook_url = f"{BASE_URL}/telegram/webhook/{TELEGRAM_WEBHOOK_SECRET}"
    await tg_call("setWebhook", {"url": hook_url})

# ---------------- DRIVE LISTING (NO CREATE/DELETE) ----------------
def drive_files_list(q: str, page_size: int = 1000) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    page_token = None
    while True:
        res = drive.files().list(
            q=q,
            pageSize=page_size,
            fields="nextPageToken, files(id,name,mimeType,parents)",
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        out.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return out

def drive_list_child_folders(parent_id: str) -> List[Dict[str, str]]:
    q = f"'{parent_id}' in parents and mimeType = '{FOLDER_MIME}' and trashed = false"
    files = drive_files_list(q=q)
    return [{"id": f["id"], "name": f.get("name", "")} for f in files]

def drive_find_client_folder_exact_under_root(client_name: str) -> Optional[Dict[str, str]]:
    target = canonical_name(client_name)
    if not target:
        return None
    for f in drive_list_child_folders(CLIENTS_ROOT_ID):
        if canonical_name(f.get("name", "")) == target:
            return {"id": f["id"], "name": f.get("name", "")}
    return None

def drive_find_child_folder_contains(parent_id: str, token: str) -> Optional[Dict[str, str]]:
    t = canonical_name(token)
    if not t:
        return None
    for f in drive_list_child_folders(parent_id):
        n = canonical_name(f.get("name", ""))
        if t in n:
            return {"id": f["id"], "name": f.get("name", "")}
    return None

def find_client_onlyfans_customs(client_input: str) -> Tuple[str, str, str, str, str]:
    client = drive_find_client_folder_exact_under_root(client_input)
    if not client:
        raise ValueError("client_not_found")

    onlyfans = drive_find_child_folder_contains(client["id"], ONLYFANS_TOKEN)
    if not onlyfans:
        raise ValueError("onlyfans_not_found")

    customs = drive_find_child_folder_contains(onlyfans["id"], CUSTOMS_TOKEN)
    if not customs:
        raise ValueError("customs_not_found")

    return client["id"], client["name"], onlyfans["id"], customs["id"], customs["name"]

def drive_get_file_min(file_id: str) -> Dict[str, Any]:
    res = drive.files().get(
        fileId=file_id,
        fields="id,name,mimeType,parents,webViewLink",
        supportsAllDrives=True,
    ).execute()
    STATE["name_cache"][file_id] = res.get("name")
    STATE["parents_cache"][file_id] = res.get("parents") or []
    return res

def get_parents_cached(file_id: str) -> List[str]:
    if file_id in STATE["parents_cache"]:
        return STATE["parents_cache"][file_id] or []
    try:
        meta = drive_get_file_min(file_id)
        return meta.get("parents") or []
    except Exception:
        STATE["parents_cache"][file_id] = []
        return []

def get_name_cached(file_id: str) -> str:
    if file_id in STATE["name_cache"] and STATE["name_cache"][file_id]:
        return STATE["name_cache"][file_id]
    try:
        meta = drive_get_file_min(file_id)
        return meta.get("name") or "(no name)"
    except Exception:
        return "(no name)"

def make_drive_link(file_id: str, mime_type: Optional[str]) -> str:
    if mime_type == FOLDER_MIME:
        return f"https://drive.google.com/drive/folders/{file_id}"
    return f"https://drive.google.com/file/d/{file_id}/view"

def find_registered_customs_root(start_parent_id: str) -> Optional[str]:
    if start_parent_id in STATE["route_cache"]:
        return STATE["route_cache"][start_parent_id]

    visited = set()
    cur = start_parent_id

    while cur and cur not in visited:
        visited.add(cur)

        if cur in STATE["customs_map"]:
            for v in visited:
                STATE["route_cache"][v] = cur
            return cur

        parents = get_parents_cached(cur)
        if not parents:
            break
        cur = parents[0]

    for v in visited:
        STATE["route_cache"][v] = None
    return None

def build_path_from_customs(customs_root_id: str, item_parent_id: str, item_name: str, client_name: str) -> str:
    parts = []
    cur = item_parent_id
    visited = set()

    while cur and cur != customs_root_id and cur not in visited:
        visited.add(cur)
        parts.append(get_name_cached(cur))
        parents = get_parents_cached(cur)
        cur = parents[0] if parents else None

    customs_name = get_name_cached(customs_root_id)
    parts = list(reversed(parts))

    if parts:
        return f"{client_name} / {customs_name} / " + " / ".join(parts) + f" / {item_name}"
    return f"{client_name} / {customs_name} / {item_name}"

# ---------------- DRIVE WATCH & CHANGES ----------------
def get_start_page_token() -> str:
    res = drive.changes().getStartPageToken(supportsAllDrives=True).execute()
    return res["startPageToken"]

def list_changes(page_token: str) -> Tuple[List[dict], str]:
    res = drive.changes().list(
        pageToken=page_token,
        spaces="drive",
        includeRemoved=False,
        fields="newStartPageToken,nextPageToken,changes(fileId,file(id,name,mimeType,parents,webViewLink),removed)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    changes = res.get("changes", [])
    new_token = res.get("newStartPageToken") or res.get("nextPageToken") or page_token
    return changes, new_token

def start_watch() -> Dict[str, Any]:
    # load persisted page token if present
    if not STATE["page_token"]:
        saved = state_get("page_token")
        if saved:
            STATE["page_token"] = saved

    if not STATE["page_token"]:
        STATE["page_token"] = get_start_page_token()
        state_set("page_token", STATE["page_token"])

    channel_id = str(uuid.uuid4())
    webhook_url = f"{BASE_URL}/drive/webhook?secret={WATCH_SECRET}"

    body = {"id": channel_id, "type": "web_hook", "address": webhook_url}
    res = drive.changes().watch(
        pageToken=STATE["page_token"],
        body=body,
        supportsAllDrives=True,
    ).execute()

    STATE["channel_id"] = channel_id
    STATE["resource_id"] = res.get("resourceId")
    STATE["expiration_ms"] = int(res.get("expiration", "0")) if res.get("expiration") else None

    # optional store (debug)
    state_set("channel_id", STATE["channel_id"])
    state_set("resource_id", STATE["resource_id"] or "")
    state_set("expiration_ms", str(STATE["expiration_ms"] or ""))

    return res

def renew_watch_now() -> Dict[str, Any]:
    return start_watch()

# ---------------- AUTO-RENEW LOOP ----------------
async def watch_renewer_loop():
    RENEW_AHEAD_SECONDS = 6 * 60 * 60
    MIN_CHECK_SECONDS = 15 * 60
    MAX_CHECK_SECONDS = 6 * 60 * 60

    while True:
        try:
            exp_ms = STATE.get("expiration_ms")
            now_s = int(time.time())

            if not exp_ms:
                renew_watch_now()
                await asyncio.sleep(MIN_CHECK_SECONDS)
                continue

            exp_s = int(exp_ms / 1000)
            seconds_left = exp_s - now_s

            if seconds_left <= RENEW_AHEAD_SECONDS:
                renew_watch_now()
                await asyncio.sleep(MIN_CHECK_SECONDS)
                continue

            sleep_s = max(MIN_CHECK_SECONDS, seconds_left - RENEW_AHEAD_SECONDS)
            sleep_s = min(sleep_s, MAX_CHECK_SECONDS)
            await asyncio.sleep(sleep_s)

        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(MIN_CHECK_SECONDS)

# ---------------- STARTUP / SHUTDOWN ----------------
@app.on_event("startup")
async def on_startup():
    global RENEW_TASK
    init_db()
    load_bindings_into_state()
    await ensure_telegram_webhook()
    renew_watch_now()

    if RENEW_TASK is None or RENEW_TASK.done():
        RENEW_TASK = asyncio.create_task(watch_renewer_loop())

@app.on_event("shutdown")
async def on_shutdown():
    global RENEW_TASK
    if RENEW_TASK and not RENEW_TASK.done():
        RENEW_TASK.cancel()
        try:
            await RENEW_TASK
        except Exception:
            pass
    RENEW_TASK = None

# ---------------- TELEGRAM WEBHOOK ----------------
@app.post("/telegram/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != TELEGRAM_WEBHOOK_SECRET:
        return Response(status_code=403)

    update = await request.json()

    if "my_chat_member" in update:
        return {"ok": True}

    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return {"ok": True}

    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return {"ok": True}

    chat = msg["chat"]
    chat_id = chat["id"]
    chat_type = chat.get("type")
    thread_id = msg.get("message_thread_id")

    sender = msg.get("from") or {}
    user_id = sender.get("id")

    async def is_admin() -> bool:
        try:
            r = await tg_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
            status = r.get("result", {}).get("status")
            return status in ("administrator", "creator")
        except Exception:
            return False

    if text.lower().startswith("/register"):
        if chat_type not in ("group", "supergroup"):
            await tg_send(chat_id, "Use /register inside a Telegram group.")
            return {"ok": True}

        if thread_id is None:
            await tg_send(chat_id, "⚠️ Run /register inside the Topic where you want updates.")
            return {"ok": True}

        if not await is_admin():
            await tg_send(chat_id, "Only admins can run /register.", thread_id=thread_id)
            return {"ok": True}

        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await tg_send(
                chat_id,
                "✅ Connect THIS Topic:\n/register <client name>\n\nExample:\n/register autumn\n\n"
                "This bot does NOT create/delete anything in Drive.",
                thread_id=thread_id,
            )
            return {"ok": True}

        client_input = normalize_spaces(parts[1])
        client_key = canonical_name(client_input)

        try:
            client_folder_id, client_folder_name, onlyfans_id, customs_id, customs_name = find_client_onlyfans_customs(client_input)
        except Exception:
            await tg_send(
                chat_id,
                "❌ Folder does not exist.\n\nExpected:\n"
                f"{client_input} / (OnlyFans...) / (Customs...)\n\n"
                "✅ OnlyFans/Customs can include ✅ and still match.",
                thread_id=thread_id,
            )
            return {"ok": True}

        save_binding(
            client_key=client_key,
            client_name=client_folder_name,
            client_folder_id=client_folder_id,
            onlyfans_folder_id=onlyfans_id,
            customs_folder_id=customs_id,
            chat_id=int(chat_id),
            thread_id=int(thread_id),
        )
        load_bindings_into_state()

        await tg_send(
            chat_id,
            "✅ Connected successfully!\n"
            f"📁 Connected to: {client_folder_name}\n"
            f"🔗 Watching: {client_folder_name} / {get_name_cached(onlyfans_id)} / {customs_name}\n"
            "📌 Notifications will be sent in this topic.",
            thread_id=thread_id,
        )
        return {"ok": True}

    if text.lower().startswith("/unregister"):
        if chat_type not in ("group", "supergroup"):
            await tg_send(chat_id, "Use /unregister inside a Telegram group.")
            return {"ok": True}
        if thread_id is None:
            await tg_send(chat_id, "Run /unregister inside the Topic you want to unlink.")
            return {"ok": True}
        if not await is_admin():
            await tg_send(chat_id, "Only admins can run /unregister.", thread_id=thread_id)
            return {"ok": True}

        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await tg_send(chat_id, "Usage: /unregister <client name>", thread_id=thread_id)
            return {"ok": True}

        key = canonical_name(parts[1].strip())
        existed = delete_binding(key)
        load_bindings_into_state()

        await tg_send(
            chat_id,
            "🗑️ Unregistered (DB binding removed only)." if existed else "Nothing to delete for that name.",
            thread_id=thread_id,
        )
        return {"ok": True}

    if text.lower().startswith("/list"):
        lines = []
        for k, b in STATE["bindings_by_client"].items():
            lines.append(f"- {b['client_name']} | key={k} | customs_id={b['customs_folder_id']}")
        await tg_send(chat_id, "📌 Current bindings:\n" + ("\n".join(lines) if lines else "(none)"), thread_id=thread_id)
        return {"ok": True}

    return {"ok": True}

# ---------------- DRIVE WEBHOOK ----------------
@app.post("/drive/webhook")
async def drive_webhook(request: Request):
    # Security check
    secret = request.query_params.get("secret")
    if secret != WATCH_SECRET:
        return Response(status_code=403)

    # NOTE:
    # We intentionally do NOT reject by channel_id/resource_id.
    # Those change on renew/restart and will make the bot "work once".

    resource_state = request.headers.get("X-Goog-Resource-State")
    if resource_state == "sync":
        return Response(status_code=200)

    old_token = STATE.get("page_token")
    if not old_token:
        saved = state_get("page_token")
        if saved:
            old_token = saved
            STATE["page_token"] = saved

    if not old_token:
        STATE["page_token"] = get_start_page_token()
        state_set("page_token", STATE["page_token"])
        return {"ok": True, "notified": 0}

    changes, new_token = list_changes(old_token)
    STATE["page_token"] = new_token
    state_set("page_token", new_token)

    notified = 0

    for ch in changes:
        if ch.get("removed"):
            continue

        f = ch.get("file") or {}
        file_id = ch.get("fileId") or f.get("id")
        if not file_id:
            continue

        if was_file_notified(file_id):
            continue

        file_name = f.get("name") or "(no name)"
        mime_type = f.get("mimeType")
        parents = f.get("parents") or []

        STATE["name_cache"][file_id] = file_name
        STATE["parents_cache"][file_id] = parents

        if not parents:
            continue

        customs_root_id = None
        for p in parents:
            customs_root_id = find_registered_customs_root(p)
            if customs_root_id:
                break

        if not customs_root_id:
            continue

        dest = STATE["customs_map"].get(customs_root_id)
        if not dest:
            continue

        client_name = dest.get("client_name") or "Client"
        full_path = build_path_from_customs(
            customs_root_id=customs_root_id,
            item_parent_id=parents[0],
            item_name=file_name,
            client_name=client_name,
        )

        link = f.get("webViewLink") or make_drive_link(file_id, mime_type)

        if mime_type == FOLDER_MIME:
            text = f"📂 New folder\n🧾 {full_path}\n🔗 {link}"
        else:
            text = f"✅ New upload\n🧾 {full_path}\n🔗 {link}"

        try:
            await tg_send(dest["chat_id"], text, thread_id=dest.get("thread_id"))
            mark_file_notified(file_id)
            notified += 1
        except Exception:
            # don’t mark notified if TG failed
            continue

    return {"ok": True, "changes": len(changes), "notified": notified}

# ---------------- WATCH RENEW ----------------
@app.post("/watch/renew")
def renew_watch(secret: str):
    if secret != WATCH_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    res = renew_watch_now()
    return {
        "ok": True,
        "watch_channel_id": STATE["channel_id"],
        "watch_resource_id": STATE["resource_id"],
        "watch_expiration_ms": STATE["expiration_ms"],
        "raw": res,
    }

# ---------------- HEALTH ----------------
@app.get("/")
def health():
    return {
        "ok": True,
        "watch_channel_id": STATE["channel_id"],
        "watch_resource_id": STATE["resource_id"],
        "watch_expiration_ms": STATE["expiration_ms"],
        "page_token_set": bool(STATE.get("page_token")),
        "bindings_count": len(STATE["bindings_by_client"]),
        "auto_renew_running": bool(RENEW_TASK and not RENEW_TASK.done()),
        "note": "This bot never creates/deletes anything in Drive.",
    }
