import asyncio
import json
import time
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fetcher import get_streams
from scanner import scan_stream, watch_giveaway

DISCOVERY_CONCURRENCY = 4
DISCOVERED_FILE       = "discovered_categories.json"

from categories import CATEGORIES
from config import update_auth, AUTH


def _load_discovered_categories():
    """Merge previously-discovered categories from disk into CATEGORIES."""
    try:
        with open(DISCOVERED_FILE, encoding="utf-8") as f:
            extras = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    if not isinstance(extras, dict):
        return
    for label, cid in extras.items():
        if isinstance(label, str) and isinstance(cid, str) and label not in CATEGORIES:
            CATEGORIES[label] = cid
    if extras:
        print(f"[categories] loaded {len(extras)} discovered from {DISCOVERED_FILE}")


def _save_discovered_categories():
    """Persist everything not in the original seed list to disk."""
    extras = {label: cid for label, cid in CATEGORIES.items() if label not in SEED_LABELS}
    try:
        with open(DISCOVERED_FILE, "w", encoding="utf-8") as f:
            json.dump(extras, f, indent=2, sort_keys=True)
    except OSError as e:
        print(f"[categories] failed to save: {e}")


# Snapshot the seed labels before we merge anything from disk
SEED_LABELS = set(CATEGORIES.keys())
_load_discovered_categories()


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Shared state ──────────────────────────────────────────────────────────────
state = {
    "scanning":           False,
    "current_stream":     None,
    "giveaways":          [],   # detected giveaway entries
    "streams_scanned":    0,
    "total_streams":      0,
    "selected_categories": [],
    "error":              None,
    "auth_expired":       False,
    # Diagnostic counters — reset each scan run
    "joined":             0,
    "with_events":        0,
    "rejected":           0,
    "errors":             0,
    "skipped":            0,
    # Rolling tail of recent per-stream scan outcomes (max 50)
    "scan_log":           [],
}

_sse_clients: list[asyncio.Queue] = []
_stop_event   = asyncio.Event()

# ── Persistent watchers for active giveaways ────────────────────────────────
# stream_id → asyncio.Task. Each watcher holds a long-running WS connection
# to that stream's channel and removes the giveaway from state when it ends.
active_watchers: dict[str, asyncio.Task] = {}

# Sample of first-seen payload per event_name on watcher channels — used to
# discover what fields Whatnot sends (end-time, audience-restriction, etc.).
# Bounded so a long-lived process can't leak.
payload_samples: dict[str, dict] = {}
PAYLOAD_SAMPLES_MAX = 60


# ── SSE broadcast ─────────────────────────────────────────────────────────────
async def broadcast(event: str, data: dict):
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    for q in _sse_clients:
        await q.put(msg)


# ── Giveaway metadata extraction ─────────────────────────────────────────────
# Speculative — these field names are guesses based on common API patterns.
# We surface unknown fields on the card so we can refine the list once we
# see real Whatnot payloads.
_END_TIME_KEYS  = ("endsAt", "endsAtUtc", "endTime", "expiresAt", "closesAt", "endAt")
_END_DELTA_KEYS = ("endsIn", "secondsRemaining", "timeRemaining", "remainingSeconds")
_PRODUCT_NAME_KEYS = ("productName", "productTitle", "itemName", "name", "title", "description", "productDescription")
_PRODUCT_IMAGE_KEYS = ("productImage", "imageUrl", "image", "thumbnailUrl", "thumbnail")
_AUDIENCE_KEYS  = ("audience", "audienceType", "restrictedTo", "eligibility", "audienceFilter")
_KNOWN_KEYS = (
    {"entryCount", "productId"}
    | set(_END_TIME_KEYS) | set(_END_DELTA_KEYS)
    | set(_PRODUCT_NAME_KEYS) | set(_PRODUCT_IMAGE_KEYS)
    | set(_AUDIENCE_KEYS)
)


def _coerce_unix_seconds(v) -> int | None:
    """Best-effort: convert a candidate timestamp to a unix-seconds int."""
    if isinstance(v, (int, float)):
        if v <= 0:
            return None
        # Heuristic: > 1e11 is millis, < 1e10 is seconds
        return int(v / 1000) if v > 1e11 else int(v)
    if isinstance(v, str) and v:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def _first_str(d: dict, keys) -> str | None:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:240]
    return None


def _extract_giveaway_meta(payload: dict) -> dict:
    """Pull human-meaningful fields off a giveaway payload. Returns only keys
    we recognized, plus an `unknown_fields` dict containing every payload key
    we didn't map — surfaced on the dashboard so we can iterate."""
    if not isinstance(payload, dict):
        return {}
    meta: dict = {}

    # End time — absolute or relative
    for k in _END_TIME_KEYS:
        if k in payload:
            ts = _coerce_unix_seconds(payload[k])
            if ts:
                meta["ends_at"] = ts
                break
    if "ends_at" not in meta:
        for k in _END_DELTA_KEYS:
            v = payload.get(k)
            if isinstance(v, (int, float)) and v > 0:
                meta["ends_at"] = int(time.time() + (v / 1000 if v > 1e6 else v))
                break

    # Product name (top-level, then nested under `product`)
    name = _first_str(payload, _PRODUCT_NAME_KEYS)
    p = payload.get("product")
    if not name and isinstance(p, dict):
        name = _first_str(p, ("name", "title", "description"))
    if name:
        meta["product_name"] = name

    # Product image (top-level, then nested)
    img = _first_str(payload, _PRODUCT_IMAGE_KEYS)
    if not img and isinstance(p, dict):
        img = _first_str(p, ("image", "imageUrl", "thumbnail"))
    if img:
        meta["product_image"] = img

    # Audience / buyer restriction
    aud = _first_str(payload, _AUDIENCE_KEYS)
    if aud:
        meta["audience"] = aud
    else:
        for k in ("buyersOnly", "audienceMustBeBuyer", "restrictedToBuyers"):
            if payload.get(k) is True:
                meta["audience"] = "buyers_only"
                break

    # Unknown fields — preserve simple-typed values verbatim so we can spot
    # candidate field names from the dashboard.
    unknown = {}
    for k, v in payload.items():
        if k in _KNOWN_KEYS:
            continue
        if isinstance(v, (str, int, float, bool)):
            sv = str(v)
            unknown[k] = sv if len(sv) <= 80 else sv[:77] + "…"
    if unknown:
        meta["unknown_fields"] = unknown

    return meta


# ── Watcher callbacks ────────────────────────────────────────────────────────
async def on_watcher_update(stream: dict, payload: dict):
    """Entry count + metadata update from a long-running watcher connection."""
    sid = stream["id"]
    existing = next((g for g in state["giveaways"] if g["stream_id"] == sid), None)
    if not existing:
        return
    existing["entry_count"] = payload.get("entryCount", existing["entry_count"])
    # Re-extract every update so end-time/audience/etc. stay fresh
    meta = _extract_giveaway_meta(payload)
    for k, v in meta.items():
        existing[k] = v
    await broadcast("update", existing)


async def on_watcher_ended(stream: dict, reason: str):
    """Giveaway ended (winner announced, stream offline, etc.) — drop the card."""
    sid = stream["id"]
    state["giveaways"] = [g for g in state["giveaways"] if g["stream_id"] != sid]
    active_watchers.pop(sid, None)
    print(f"[watcher] {sid[:8]}… ended ({reason})")
    await broadcast("giveaway_ended", {"stream_id": sid, "reason": reason})


async def on_watcher_event_seen(stream: dict, event_name: str, payload: dict):
    """Capture first-seen payload per event_name for field discovery."""
    if event_name in payload_samples:
        return
    if len(payload_samples) >= PAYLOAD_SAMPLES_MAX:
        return
    payload_samples[event_name] = {
        "first_seen_at": int(time.time()),
        "stream_id":     stream["id"],
        "username":      stream.get("username", ""),
        "payload":       payload if isinstance(payload, (dict, list, str, int, float, bool, type(None))) else str(payload),
    }


def _start_watcher(stream: dict) -> None:
    """Spawn a persistent watcher task for a stream that just got a giveaway."""
    sid = stream["id"]
    if sid in active_watchers and not active_watchers[sid].done():
        return
    task = asyncio.create_task(
        watch_giveaway(
            stream,
            on_watcher_update,
            on_watcher_ended,
            on_event_seen=on_watcher_event_seen,
        ),
        name=f"watch:{sid[:8]}",
    )
    active_watchers[sid] = task


# ── Giveaway callback (called by discovery scanner) ──────────────────────────
async def on_giveaway(stream: dict, payload: dict):
    stream_id = stream["id"]

    # Update entry count if already flagged
    existing = next((g for g in state["giveaways"] if g["stream_id"] == stream_id), None)
    if existing:
        existing["entry_count"] = payload.get("entryCount", existing["entry_count"])
        await broadcast("update", existing)
        return

    entry = {
        "stream_id":   stream_id,
        "title":       stream.get("title", ""),
        "username":    stream.get("username", ""),
        "viewers":     stream.get("viewers", 0),
        "category":    stream.get("category", ""),
        "thumbnail":   stream.get("thumbnail", ""),
        "entry_count": payload.get("entryCount", 0),
        "product_id":  payload.get("productId", ""),
        "url":         stream.get("url", f"https://www.whatnot.com/live/{stream_id}"),
        "started_at":  int(time.time()),
    }
    entry.update(_extract_giveaway_meta(payload))
    state["giveaways"].append(entry)
    await broadcast("giveaway", entry)
    _start_watcher(stream)


# ── Per-stream scan-result callback (called by scanner) ──────────────────────
async def on_scan_result(result: dict):
    """Update counters + rolling log + broadcast every per-stream scan outcome."""
    outcome = result.get("outcome")
    if outcome == "joined_ok":
        state["joined"] += 1
        if result.get("giveaway_events", 0) > 0:
            state["with_events"] += 1
    elif outcome == "join_rejected":
        state["rejected"] += 1
    elif outcome in ("ws_403", "ws_error", "closed_before_join"):
        state["errors"] += 1
    elif outcome == "no_session_id":
        state["skipped"] += 1

    entry = {"at": int(time.time()), **result}
    state["scan_log"].append(entry)
    if len(state["scan_log"]) > 50:
        del state["scan_log"][:-50]

    await broadcast("scan_result", entry)


# ── Auth-expired callback (called by scanner on 403) ──────────────────────────
async def on_auth_expired():
    if state["auth_expired"]:
        return  # already broadcast for this run
    state["auth_expired"] = True
    state["error"] = "Session tokens expired"
    await broadcast("auth_expired", {
        "message": "Session tokens expired. Open whatnot.com → DevTools → WS → "
                   "copy fresh _csrf_token, sessionExtensionToken, and Cookie "
                   "into .env, then restart the server."
    })
    _stop_event.set()


# ── Main scanner loop ─────────────────────────────────────────────────────────
async def _cancel_all_watchers():
    """Cancel and clear every active watcher task. Used on start/stop."""
    for sid, task in list(active_watchers.items()):
        if not task.done():
            task.cancel()
    active_watchers.clear()


async def run_scanner():
    _stop_event.clear()
    await _cancel_all_watchers()
    state["streams_scanned"] = 0
    state["giveaways"]       = []
    state["error"]           = None
    state["auth_expired"]    = False
    state["joined"]          = 0
    state["with_events"]     = 0
    state["rejected"]        = 0
    state["errors"]          = 0
    state["skipped"]         = 0
    state["scan_log"]        = []

    try:
        while not _stop_event.is_set():
            for cat_name, explore_id in CATEGORIES.items():
                if cat_name not in state["selected_categories"]:
                    continue
                if _stop_event.is_set():
                    break

                await broadcast("status", {"message": f"Fetching streams: {cat_name}…"})
                result  = await get_streams(explore_id)
                streams = result["streams"]
                state["total_streams"] = result["totalCount"]

                if not streams:
                    await broadcast("status", {"message": f"No live streams found in {cat_name}"})
                    continue

                await broadcast("status", {
                    "message": f"Scanning {len(streams)} streams in {cat_name}…"
                })

                # Skip streams already covered by a persistent watcher.
                to_scan = [s for s in streams if s["id"] not in active_watchers]
                if len(to_scan) < len(streams):
                    state["streams_scanned"] += (len(streams) - len(to_scan))
                total_to_scan = len(to_scan)

                # Run discovery scans concurrently, capped by a semaphore.
                sem = asyncio.Semaphore(DISCOVERY_CONCURRENCY)

                async def scan_one(stream: dict):
                    if _stop_event.is_set():
                        return
                    async with sem:
                        if _stop_event.is_set():
                            return
                        state["current_stream"] = stream
                        state["streams_scanned"] += 1
                        await broadcast("scanning", {
                            "title":    stream["title"],
                            "username": stream["username"],
                            "viewers":  stream["viewers"],
                            "scanned":  state["streams_scanned"],
                            "total":    total_to_scan,
                        })
                        await scan_stream(stream, on_giveaway, on_auth_expired, on_scan_result)

                await asyncio.gather(*(scan_one(s) for s in to_scan))

            if not _stop_event.is_set():
                # Brief pause between full cycles
                await broadcast("status", {"message": "Cycle complete — restarting…"})
                await asyncio.sleep(5)

    except Exception as e:
        state["error"] = str(e)
        await broadcast("error", {"message": str(e)})
    finally:
        state["scanning"]       = False
        state["current_stream"] = None
        await broadcast("status", {"message": "Scanner stopped."})


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def index():
    with open("static/index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/state")
async def get_state():
    return state


@app.get("/categories")
async def get_categories():
    return sorted(CATEGORIES.keys())


_UUID_RE = __import__("re").compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


@app.post("/categories/discovered")
async def categories_discovered(request: Request):
    """Merge discovered (id, label) pairs from the extension into the runtime
    category registry. In-memory only — not persisted to categories.py."""
    body = await request.json()
    items = body.get("categories") or []
    added = []

    # Reverse lookup so we don't re-add an id under a different label
    known_ids = set(CATEGORIES.values())
    for item in items:
        if not isinstance(item, dict):
            continue
        cid   = (item.get("id")    or "").strip().lower()
        label = (item.get("label") or "").strip()
        if not _UUID_RE.match(cid) or not label or len(label) > 100:
            continue
        if cid in known_ids or label in CATEGORIES:
            continue
        CATEGORIES[label] = cid
        known_ids.add(cid)
        added.append({"label": label, "id": cid})

    if added:
        print(f"[categories] discovered {len(added)}: {[a['label'] for a in added]}")
        _save_discovered_categories()
        await broadcast("categories_updated", {
            "added":   added,
            "all":     sorted(CATEGORIES.keys()),
        })

    return {"ok": True, "added": added, "total": len(CATEGORIES)}


@app.get("/events")
async def sse(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    _sse_clients.append(queue)

    async def generator():
        # Send current state immediately on connect
        yield f"event: init\ndata: {json.dumps(state)}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=20)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"   # keep-alive
        finally:
            _sse_clients.remove(queue)

    return StreamingResponse(generator(), media_type="text/event-stream")


@app.post("/start")
async def start_scan(request: Request):
    body = await request.json()
    if state["scanning"]:
        return {"error": "Already scanning"}

    state["selected_categories"] = body.get("categories", list(CATEGORIES.keys()))
    state["scanning"] = True
    asyncio.create_task(run_scanner())
    return {"ok": True}


@app.post("/stop")
async def stop_scan():
    _stop_event.set()
    state["scanning"] = False
    await _cancel_all_watchers()
    return {"ok": True}


@app.get("/payload_samples")
async def get_payload_samples():
    """First-seen payload per event_name from watcher channels.
    Use this to discover what fields Whatnot actually sends (end-time,
    audience, productId metadata, etc.) so we can wire them into the UI."""
    return {
        "count": len(payload_samples),
        "samples": payload_samples,
    }


def _trunc(s: str, n: int = 12) -> str:
    return f"{s[:n]}…{s[-6:]}" if len(s) > n + 6 else s


@app.post("/auth/refresh")
async def refresh_auth(request: Request):
    """Receives fresh tokens from the Chrome extension and hot-swaps them."""
    body           = await request.json()
    cookie         = (body.get("cookie") or "").strip()
    csrf_token     = (body.get("csrf_token") or "").strip()
    session_token  = (body.get("session_token") or "").strip()
    client_version = (body.get("client_version") or "").strip()
    socket_kind    = (body.get("socket_kind") or "auction").strip()

    if not (cookie and csrf_token and session_token):
        print(f"[auth] refresh REJECTED — missing fields "
              f"(cookie={len(cookie)}, csrf={len(csrf_token)}, session={len(session_token)})")
        return {"ok": False, "error": "missing fields"}

    update_auth(cookie, csrf_token, session_token, client_version, socket_kind)
    state["auth_expired"] = False
    state["error"]        = None

    at = int(time.time())
    print(f"[auth] {socket_kind} refreshed at {time.strftime('%H:%M:%S')} → "
          f"cookie={len(cookie)}ch, csrf={_trunc(csrf_token)}, "
          f"session={_trunc(session_token)}, client_version={client_version or '(unchanged)'}")
    await broadcast("auth_refreshed", {"message": "Tokens refreshed", "at": at})
    return {"ok": True, "at": at}


@app.get("/auth/status")
async def auth_status():
    """Inspect what the server currently has in memory."""
    return {
        "cookie_len":         len(AUTH["cookie"]),
        "cookie_preview":     _trunc(AUTH["cookie"], 30),
        "csrf_token":         _trunc(AUTH["csrf_token"]),
        "csrf_token_full":    AUTH["csrf_token"],
        "session_token":      _trunc(AUTH["session_token"]),
        "session_token_full": AUTH["session_token"],
        "client_version":     AUTH["client_version"],
    }


@app.get("/auth/full")
async def auth_full():
    """Full AUTH dict — used by debug tooling to read live state.
    Localhost-only sensitive endpoint."""
    return dict(AUTH)


@app.get("/diagnostics")
async def diagnostics():
    """One-stop dashboard view: auth readiness, counters, recent scan log."""
    auction = AUTH.get("auction", {})
    live    = AUTH.get("live", {})

    def preview(s: str) -> str | None:
        return _trunc(s) if s else None

    return {
        "auth": {
            "cookie_len":             len(AUTH.get("cookie", "")),
            "auction_csrf":           preview(auction.get("csrf_token", "")),
            "auction_session":        preview(auction.get("session_token", "")),
            "live_csrf":              preview(live.get("csrf_token", "")),
            "live_session":           preview(live.get("session_token", "")),
            "livestream_session_id":  preview(AUTH.get("livestream_session_id", "")),
            "client_version":         AUTH.get("client_version", ""),
        },
        "counters": {
            "scanned":     state["streams_scanned"],
            "joined":      state["joined"],
            "with_events": state["with_events"],
            "rejected":    state["rejected"],
            "errors":      state["errors"],
            "skipped":     state["skipped"],
            "giveaways":   len(state["giveaways"]),
        },
        "scan_log": state["scan_log"],
    }


# ── Captured phx_join store (for diagnostics) ─────────────────────────────────
captured_joins: list[dict] = []


@app.post("/capture/join")
async def capture_join(request: Request):
    body    = await request.json()
    payload = body.get("payload") or {}
    captured_joins.append({
        "at":         int(time.time()),
        "socketKind": body.get("socketKind"),
        "topic":      body.get("topic"),
        "payload":    payload,
    })
    if len(captured_joins) > 100:
        del captured_joins[:-100]

    # Stash livestreamSessionId for the scanner to reuse across streams.
    if isinstance(payload, dict):
        sid = payload.get("livestreamSessionId")
        if sid and sid != AUTH.get("livestream_session_id"):
            AUTH["livestream_session_id"] = sid
            print(f"[capture] livestream_session_id captured: {_trunc(sid)}")
    return {"ok": True}


@app.get("/capture/joins")
async def list_joins():
    """Returns unique topics seen, with their socketKind and most-recent payload."""
    by_topic: dict[str, dict] = {}
    for j in captured_joins:
        by_topic[j["topic"]] = j
    return {
        "total_captured": len(captured_joins),
        "unique_topics":  sorted(by_topic.keys()),
        "details":        list(by_topic.values()),
    }


@app.post("/capture/clear")
async def clear_joins():
    captured_joins.clear()
    return {"ok": True}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000, reload=False)
