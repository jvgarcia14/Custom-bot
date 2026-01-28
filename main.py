# main.py
# ------------------------------------------------------------
# CUSTOMS BOT (Production-ish)
# Google Drive -> Webhook -> Changes API -> Telegram Topic
#
# ✅ Add bot to Telegram group (make admin)
# ✅ In a Topic, type:
#    /register autumn
#    /register Autumn
#    /register miss lexa
#
# Behavior:
# - Case-insensitive, multi-word supported
# - Auto-creates Drive folder under: /Custom Orders/Completed/<client name> (if missing)
# - If folder exists (any case), reuses it (no duplicates)
# - Binds notifications to THAT GROUP + THAT TOPIC thread only
# - ✅ Persist bindings in Railway Postgres (so redeploys won't erase registrations)
#
# IMPORTANT:
# 1) Service account MUST have Editor access to the Completed folder to create subfolders.
# 2) Drive watches expire—call /watch/renew daily (cron/uptime ping).
# ------------------------------------------------------------

import os
import json
import re
import uuid
from typing import Optional, Dict, Any, List, Tuple

import httpx
import psycopg2
from fastapi import FastAPI, Request, Response, HTTPException

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------- APP ----------------
app = FastAPI()

# ---------------- ENV ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")  # e.g. https://your-app.up.railway.app  (NO trailing slash)
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "change-me-telegram-secret")

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
COMPLETED_FOLDER_ID = os.getenv("COMPLETED_FOLDER_ID")  # folder ID of /Custom Orders/Completed
WATCH_SECRET = os.getenv("WATCH_SECRET", "change-me-drive-secret")

DATABASE_URL = os.getenv("DATABASE_URL")  # Railway Postgres

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing env var: TELEGRAM_BOT_TOKEN")
if not BASE_URL:
    raise RuntimeError("Missing env var: BASE_URL")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Missing env var: GOOGLE_SERVICE_ACCOUNT_JSON")
if not COMPLETED_FOLDER_ID:
    raise RuntimeError("Missing env var: COMPLETED_FOLDER_ID")
if not DATABASE_URL:
    raise RuntimeError("Missing env var: DATABASE_URL (add Railway Postgres)")

# ---------------- DB (POSTGRES) ----------------
db = psycopg2.connect(DATABASE_URL, sslmode="require")
db.autocommit = True

def init_db() -> None:
    with db.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bindings (
            client_key TEXT PRIMARY KEY,
            folder_id TEXT NOT NULL,
            folder_name TEXT NOT NULL,
            chat_id BIGINT NOT NULL,
            thread_id BIGINT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)

def save_binding(client_key: str, folder_id: str, folder_name: str, chat_id: int, thread_id: int) -> None:
    with db.cursor() as cur:
        cur.execute("""
        INSERT INTO bindings (client_key, folder_id, folder_name, chat_id, thread_id, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (client_key) DO UPDATE SET
            folder_id = EXCLUDED.folder_id,
            folder_name = EXCLUDED.folder_name,
            chat_id = EXCLUDED.chat_id,
            thread_id = EXCLUDED.thread_id,
            updated_at = NOW();
        """, (client_key, folder_id, folder_name, chat_id, thread_id))

def delete_binding(client_key: str) -> bool:
    with db.cursor() as cur:
        cur.execute("DELETE FROM bindings WHERE client_key = %s;", (client_key,))
        return cur.rowcount > 0

def load_bindings_into_state(state: Dict[str, Any]) -> None:
    state["client_map"] = {}
    state["folder_index"] = {}
    with db.cursor() as cur:
        cur.execute("SELECT client_key, folder_id, folder_name, chat_id, thread_id FROM bindings;")
        rows = cur.fetchall()

    for client_key, folder_id, folder_name, chat_id, thread_id in rows:
        # folder index for /register case-insensitive matching
        state["folder_index"][client_key] = {"id": folder_id, "name": folder_name}
        # client map for routing Drive notifications
        state["client_map"][folder_id] = {
            "chat_id": int(chat_id),
            "thread_id": int(thread_id),
            "team_name": folder_name,
            "client_key": client_key,
        }
    state["folder_index_loaded"] = True

# ---------------- GOOGLE DRIVE CLIENT ----------------
# Needed for AUTO-CREATE folder:
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

    # folder_id -> destination
    "client_map": {},

    # canonical client key -> {"id": folder_id, "name": folder_name}
    "folder_index": {},

    "folder_index_loaded": False,
}

# ---------------- HELPERS: TELEGRAM ----------------
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

# ---------------- HELPERS: DRIVE FOLDER NAMING ----------------
def canonical_name(name: str) -> str:
    # "  Miss   Lexa  " -> "miss lexa"
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    return name.lower()

def display_name(name: str) -> str:
    # Keeps the user's casing but normalizes spaces.
    # If you prefer Title Case folders, use: return canonical then .title()
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    return name

def list_completed_folders() -> List[Dict[str, str]]:
    folders: List[Dict[str, str]] = []
    page_token = None
    q = (
        f"'{COMPLETED_FOLDER_ID}' in parents "
        f"and mimeType = '{FOLDER_MIME}' "
        f"and trashed = false"
    )
    while True:
        res = drive.files().list(
            q=q,
            pageSize=1000,
            fields="nextPageToken, files(id,name)",
            pageToken=page_token,
        ).execute()
        folders.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return folders

def refresh_folder_index_from_drive() -> None:
    """
    Rebuilds canonical folder index from Drive.
    This helps if someone manually adds folders in Drive.
    """
    idx: Dict[str, Dict[str, str]] = {}
    for f in list_completed_folders():
        key = canonical_name(f.get("name", ""))
        if not key:
            continue
        if key not in idx:
            idx[key] = {"id": f["id"], "name": f.get("name", "")}
    # Merge DB index + Drive index, but prefer DB (DB represents active bindings)
    # If DB has it, keep DB. Otherwise keep Drive.
    for k, v in idx.items():
        if k not in STATE["folder_index"]:
            STATE["folder_index"][k] = v
    STATE["folder_index_loaded"] = True

def create_client_folder(folder_name: str) -> Dict[str, str]:
    metadata = {
        "name": folder_name,
        "mimeType": FOLDER_MIME,
        "parents": [COMPLETED_FOLDER_ID],
    }
    created = drive.files().create(body=metadata, fields="id,name").execute()
    return {"id": created["id"], "name": created.get("name", folder_name)}

def find_or_create_client_folder(client_input: str) -> Tuple[str, str, bool]:
    """
    Returns: (folder_id, actual_folder_name, created_bool)
    Case-insensitive; supports multi-word names.
    Uses DB-loaded folder_index first; then Drive refresh; then creates.
    """
    key = canonical_name(client_input)
    if not key:
        raise ValueError("Empty client name")

    if not STATE["folder_index_loaded"]:
        # Load DB bindings already set folder_index; but just in case:
        refresh_folder_index_from_drive()

    existing = STATE["folder_index"].get(key)
    if existing:
        return existing["id"], existing["name"], False

    # Refresh from Drive before creating (prevents duplicates if someone created it manually)
    refresh_folder_index_from_drive()
    existing2 = STATE["folder_index"].get(key)
    if existing2:
        return existing2["id"], existing2["name"], False

    # Create new folder
    nice = display_name(client_input)
    created = create_client_folder(nice)

    # Update in-memory index (and DB will store binding when /register runs)
    STATE["folder_index"][key] = {"id": created["id"], "name": created["name"]}
    return created["id"], created["name"], True

# ---------------- HELPERS: DRIVE WATCH & CHANGES ----------------
def get_start_page_token() -> str:
    res = drive.changes().getStartPageToken().execute()
    return res["startPageToken"]

def list_changes(page_token: str) -> Tuple[List[dict], str]:
    res = drive.changes().list(
        pageToken=page_token,
        spaces="drive",
        includeRemoved=False,
        fields="newStartPageToken,nextPageToken,changes(fileId,file(name,mimeType,parents,webViewLink),removed)",
    ).execute()
    changes = res.get("changes", [])
    new_token = res.get("newStartPageToken") or res.get("nextPageToken") or page_token
    return changes, new_token

def start_watch() -> Dict[str, Any]:
    if not STATE["page_token"]:
        STATE["page_token"] = get_start_page_token()

    channel_id = str(uuid.uuid4())
    webhook_url = f"{BASE_URL}/drive/webhook?secret={WATCH_SECRET}"

    body = {"id": channel_id, "type": "web_hook", "address": webhook_url}
    res = drive.changes().watch(pageToken=STATE["page_token"], body=body).execute()

    STATE["channel_id"] = channel_id
    STATE["resource_id"] = res.get("resourceId")
    STATE["expiration_ms"] = int(res.get("expiration", "0")) if res.get("expiration") else None
    return res

# ---------------- STARTUP ----------------
@app.on_event("startup")
async def on_startup():
    init_db()
    load_bindings_into_state(STATE)  # ✅ restores /register bindings after deploys
    # Also merge in any folders that exist in Drive but not in DB (optional)
    refresh_folder_index_from_drive()

    start_watch()
    await ensure_telegram_webhook()

# ---------------- TELEGRAM WEBHOOK ----------------
@app.post("/telegram/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != TELEGRAM_WEBHOOK_SECRET:
        return Response(status_code=403)

    update = await request.json()

    # When bot is added/removed or permissions changed
    if "my_chat_member" in update:
        chat = update["my_chat_member"]["chat"]
        chat_id = chat["id"]
        await tg_send(
            chat_id,
            "👋 Hi! I’m Customs Notify Bot.\n\n"
            "✅ Go INSIDE the Topic where you want updates, then type:\n"
            "/register <client name>\n\n"
            "Examples:\n"
            "/register autumn\n"
            "/register Autumn\n"
            "/register miss lexa\n\n"
            "I’ll auto-create the folder under Completed (if missing) and send updates to THIS Topic only."
        )
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
    thread_id = msg.get("message_thread_id")  # present if command is run inside a topic

    sender = msg.get("from") or {}
    user_id = sender.get("id")

    async def is_admin() -> bool:
        try:
            r = await tg_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
            status = r.get("result", {}).get("status")
            return status in ("administrator", "creator")
        except:
            return False

    # ---------- /register ----------
    if text.lower().startswith("/register"):
        if chat_type not in ("group", "supergroup"):
            await tg_send(chat_id, "Use /register inside a Telegram group.")
            return {"ok": True}

        if thread_id is None:
            await tg_send(chat_id, "Run /register inside the Topic where you want updates posted.")
            return {"ok": True}

        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await tg_send(
                chat_id,
                "Usage: /register <client name>\nExamples:\n/register autumn\n/register miss lexa",
                thread_id=thread_id,
            )
            return {"ok": True}

        if not await is_admin():
            await tg_send(chat_id, "Only admins can run /register.", thread_id=thread_id)
            return {"ok": True}

        client_input = parts[1].strip()
        key = canonical_name(client_input)

        try:
            folder_id, folder_name, created = find_or_create_client_folder(client_input)
        except Exception as e:
            await tg_send(
                chat_id,
                "❌ I couldn’t create/find that Drive folder.\n"
                "Make sure the service account has *Editor* access to the Completed folder.\n"
                f"Error: {str(e)[:200]}",
                thread_id=thread_id,
            )
            return {"ok": True}

        # Bind: folder -> this group/topic
        STATE["client_map"][folder_id] = {
            "chat_id": int(chat_id),
            "thread_id": int(thread_id),
            "team_name": folder_name,
            "client_key": key,
        }
        STATE["folder_index"][key] = {"id": folder_id, "name": folder_name}
        STATE["folder_index_loaded"] = True

        # ✅ Persist in Postgres (survives redeploys)
        save_binding(
            client_key=key,
            folder_id=folder_id,
            folder_name=folder_name,
            chat_id=int(chat_id),
            thread_id=int(thread_id),
        )

        if created:
            reply = (
                f"✅ Registered & created!\n"
                f"📁 Drive folder: Completed/{folder_name}\n"
                f"🧷 Updates will post ONLY in this Topic."
            )
        else:
            reply = (
                f"✅ Registered!\n"
                f"📁 Folder already exists: Completed/{folder_name}\n"
                f"🧷 Updates will post ONLY in this Topic.\n"
                f"(No duplicate folder created.)"
            )

        await tg_send(chat_id, reply, thread_id=thread_id)
        return {"ok": True}

    # ---------- /unregister ----------
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
            await tg_send(chat_id, "Usage: /unregister <client name>\nExample: /unregister autumn", thread_id=thread_id)
            return {"ok": True}

        key = canonical_name(parts[1].strip())
        existed = delete_binding(key)

        # Reload state from DB to be consistent
        load_bindings_into_state(STATE)
        refresh_folder_index_from_drive()

        await tg_send(chat_id, "🗑️ Unregistered (deleted binding)." if existed else "Nothing to delete for that name.", thread_id=thread_id)
        return {"ok": True}

    # ---------- /list ----------
    if text.lower().startswith("/list"):
        lines = []
        for fid, dest in STATE["client_map"].items():
            lines.append(
                f"- {dest.get('team_name')} | folder_id={fid} | chat_id={dest.get('chat_id')} | thread_id={dest.get('thread_id')}"
            )
        await tg_send(chat_id, "📌 Current bindings:\n" + ("\n".join(lines) if lines else "(none yet)"), thread_id=thread_id)
        return {"ok": True}

    # ---------- /refreshfolders ----------
    if text.lower().startswith("/refreshfolders"):
        if not await is_admin():
            await tg_send(chat_id, "Only admins can run /refreshfolders.", thread_id=thread_id)
            return {"ok": True}
        refresh_folder_index_from_drive()
        await tg_send(chat_id, f"✅ Refreshed folder index. Known folders: {len(STATE['folder_index'])}.", thread_id=thread_id)
        return {"ok": True}

    return {"ok": True}

# ---------------- DRIVE WEBHOOK ----------------
@app.post("/drive/webhook")
async def drive_webhook(request: Request):
    secret = request.query_params.get("secret")
    if secret != WATCH_SECRET:
        return Response(status_code=403)

    channel_id = request.headers.get("X-Goog-Channel-Id")
    resource_id = request.headers.get("X-Goog-Resource-Id")
    resource_state = request.headers.get("X-Goog-Resource-State")

    if channel_id != STATE["channel_id"] or resource_id != STATE["resource_id"]:
        return Response(status_code=200)

    if resource_state == "sync":
        return Response(status_code=200)

    old_token = STATE["page_token"]
    if not old_token:
        STATE["page_token"] = get_start_page_token()
        return {"ok": True, "notified": 0}

    changes, new_token = list_changes(old_token)
    STATE["page_token"] = new_token

    notified = 0

    for ch in changes:
        if ch.get("removed"):
            continue

        f = ch.get("file")
        if not f:
            continue

        file_id = ch.get("fileId") or f.get("id")
        file_name = f.get("name", "(no name)")
        parents = f.get("parents") or []
        link = f.get("webViewLink") or (f"https://drive.google.com/file/d/{file_id}/view" if file_id else "")

        # Notify if direct parent folder is registered
        dest = None
        for p in parents:
            if p in STATE["client_map"]:
                dest = STATE["client_map"][p]
                break

        if not dest:
            continue

        text = (
            f"✅ Custom delivered: {dest.get('team_name')}\n"
            f"📁 {file_name}\n"
            f"🔗 {link}"
        )

        try:
            await tg_send(dest["chat_id"], text, thread_id=dest.get("thread_id"))
            notified += 1
        except Exception:
            continue

    return {"ok": True, "changes": len(changes), "notified": notified}

# ---------------- WATCH RENEW ----------------
@app.post("/watch/renew")
def renew_watch(secret: str):
    """
    Call daily using a cron/uptime ping:
    POST /watch/renew?secret=WATCH_SECRET
    """
    if secret != WATCH_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    res = start_watch()
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
        "client_map_size": len(STATE["client_map"]),
        "folder_index_size": len(STATE["folder_index"]),
    }
