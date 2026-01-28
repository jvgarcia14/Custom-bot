import os, json, uuid, time
from typing import Optional, Dict, Any, List

import httpx
from fastapi import FastAPI, Request, Response, HTTPException

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# ---------------- ENV ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
COMPLETED_FOLDER_ID = os.getenv("COMPLETED_FOLDER_ID")
BASE_URL = os.getenv("BASE_URL")  # https://xxxx.up.railway.app
WATCH_SECRET = os.getenv("WATCH_SECRET", "change-me")
SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

if not TELEGRAM_BOT_TOKEN or not COMPLETED_FOLDER_ID or not BASE_URL or not SA_JSON:
    raise RuntimeError("Missing env vars: TELEGRAM_BOT_TOKEN, COMPLETED_FOLDER_ID, BASE_URL, GOOGLE_SERVICE_ACCOUNT_JSON")

# ---------------- GOOGLE DRIVE CLIENT ----------------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

creds_info = json.loads(SA_JSON)
creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

# ---------------- SIMPLE IN-MEM STATE (swap to Postgres later) ----------------
STATE: Dict[str, Any] = {
    "page_token": None,
    "channel_id": None,
    "resource_id": None,
    "expiration": None,
    # Map client folder ID to where to post
    # Example: "1AbCClientFolderId": {"chat_id": -1001234567890, "thread_id": 12, "team_name": "Team Black"}
    "client_map": {}
}

# ---------------- HELPERS ----------------
def drive_file_link(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view"

async def telegram_send(chat_id: int, text: str, thread_id: Optional[int] = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False}
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram send failed: {r.status_code} {r.text}")

def get_start_page_token() -> str:
    res = drive.changes().getStartPageToken().execute()
    return res["startPageToken"]

def list_changes(page_token: str) -> (List[dict], str):
    # includeRemoved=False avoids spam from deletions
    res = drive.changes().list(
        pageToken=page_token,
        spaces="drive",
        fields="newStartPageToken,nextPageToken,changes(fileId,file(name,mimeType,parents,webViewLink),removed)"
    ).execute()

    changes = res.get("changes", [])
    new_token = res.get("newStartPageToken") or res.get("nextPageToken") or page_token
    return changes, new_token

def get_file(file_id: str) -> dict:
    return drive.files().get(
        fileId=file_id,
        fields="id,name,mimeType,parents,webViewLink"
    ).execute()

def is_under_completed(file_parents: List[str]) -> bool:
    # We’ll treat “directly inside Completed or inside any subfolder” as valid
    # But because parents only gives direct parent, we check:
    # - direct parent is Completed => OK
    # - direct parent is a known client folder => OK
    return (COMPLETED_FOLDER_ID in file_parents) or any(p in STATE["client_map"] for p in file_parents)

# ---------------- WATCH SETUP ----------------
def start_watch():
    # Ensure we have a page token
    if not STATE["page_token"]:
        STATE["page_token"] = get_start_page_token()

    channel_id = str(uuid.uuid4())
    webhook_url = f"{BASE_URL}/drive/webhook?secret={WATCH_SECRET}"

    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url
    }

    # Watch ALL changes that the service account can see (because folder is shared)
    res = drive.changes().watch(pageToken=STATE["page_token"], body=body).execute()

    STATE["channel_id"] = channel_id
    STATE["resource_id"] = res.get("resourceId")
    STATE["expiration"] = int(res.get("expiration", "0"))  # ms epoch
    return res

@app.on_event("startup")
def on_startup():
    # Start watch at boot
    start_watch()

# ---------------- API: Register client folder -> team ----------------
class RegisterClientBody(dict):
    pass

@app.post("/clients/register")
async def register_client(body: Dict[str, Any]):
    """
    body:
      {
        "client_folder_id": "...",
        "chat_id": -100123,
        "thread_id": 12,      # optional
        "team_name": "Team Black"
      }
    """
    client_folder_id = body.get("client_folder_id")
    chat_id = body.get("chat_id")
    if not client_folder_id or chat_id is None:
        raise HTTPException(status_code=400, detail="client_folder_id and chat_id required")

    STATE["client_map"][client_folder_id] = {
        "chat_id": int(chat_id),
        "thread_id": body.get("thread_id"),
        "team_name": body.get("team_name", "Team")
    }
    return {"ok": True, "client_map_size": len(STATE["client_map"])}

# ---------------- WEBHOOK: Drive push notifications ----------------
@app.post("/drive/webhook")
async def drive_webhook(request: Request):
    # Quick secret check (keeps random people from hitting it)
    secret = request.query_params.get("secret")
    if secret != WATCH_SECRET:
        return Response(status_code=403)

    # Validate channel/resource
    channel_id = request.headers.get("X-Goog-Channel-Id")
    resource_id = request.headers.get("X-Goog-Resource-Id")
    resource_state = request.headers.get("X-Goog-Resource-State")

    if channel_id != STATE["channel_id"] or resource_id != STATE["resource_id"]:
        # Not our watch channel
        return Response(status_code=200)

    # Google sometimes sends "sync" first
    if resource_state == "sync":
        return Response(status_code=200)

    # Pull changes since last page token
    old_token = STATE["page_token"]
    changes, new_token = list_changes(old_token)
    STATE["page_token"] = new_token

    # Process changes
    notified = 0
    for ch in changes:
        if ch.get("removed"):
            continue
        f = ch.get("file")
        if not f:
            # Sometimes file object missing; fetch
            file_id = ch.get("fileId")
            if not file_id:
                continue
            f = get_file(file_id)

        parents = f.get("parents") or []
        if not parents:
            continue

        if not is_under_completed(parents):
            continue

        # Determine team destination:
        # - if parent is a client folder we registered, use that
        # - if parent is Completed itself, you can send to a general chat (optional)
        dest = None
        for p in parents:
            if p in STATE["client_map"]:
                dest = STATE["client_map"][p]
                break

        if not dest:
            # If you want: send to an admin chat for “unmapped client folder”
            continue

        team_name = dest.get("team_name", "Team")
        link = f.get("webViewLink") or drive_file_link(f["id"])
        text = (
            f"✅ Custom delivered for *{team_name}*\n"
            f"📁 File: {f.get('name','(no name)')}\n"
            f"🔗 {link}"
        )
        # Telegram markdown needs parse_mode if you use *...*
        # If you want Markdown, add parse_mode="Markdown" in telegram_send payload above.
        # For simplicity, remove asterisks:
        text = text.replace("*", "")

        await telegram_send(dest["chat_id"], text, dest.get("thread_id"))
        notified += 1

    return {"ok": True, "changes": len(changes), "notified": notified}

# ---------------- HEALTH ----------------
@app.get("/")
def health():
    return {
        "ok": True,
        "watch_channel_id": STATE["channel_id"],
        "watch_resource_id": STATE["resource_id"],
        "watch_expiration_ms": STATE["expiration"],
        "client_map_size": len(STATE["client_map"])
    }
