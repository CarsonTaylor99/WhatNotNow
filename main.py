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
# Field names confirmed from real Whatnot payloads. Giveaway state lives
# nested inside `activeGiveaway`; product info lives in `pinnedProduct`.
# We still iterate via candidate lists so future schema changes are easy.
_END_TIME_KEYS  = ("endsAt", "endsAtUtc", "expiresAt", "closesAt", "endAt", "endTime")
_END_DELTA_KEYS = ("endsIn", "secondsRemaining", "timeRemaining", "remainingSeconds")
_PRODUCT_NAME_KEYS  = ("title", "productName", "productTitle", "itemName", "name", "description", "productDescription")
_PRODUCT_IMAGE_KEYS = ("productImage", "imageUrl", "image", "thumbnailUrl", "thumbnail")
_AUDIENCE_KEYS      = ("audience", "audienceType", "restrictedTo", "eligibility", "audienceFilter")

# Top-level keys we recognize — anything else lands in unknown_fields so we
# can keep refining the parser.
_KNOWN_TOP_KEYS = (
    {"entryCount", "productId", "activeGiveaway", "pinnedProduct", "product",
     "requireQualifiedBuyer", "buyerQualifications"}
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


def _summarize(v, depth=0) -> str:
    """Compact stringy summary of any value — used to surface nested
    objects in unknown_fields so the dashboard shows what's actually
    inside activeGiveaway / pinnedProduct without dumping JSON walls."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        s = str(v)
        return s if len(s) <= 60 else s[:57] + "…"
    if isinstance(v, list):
        return f"[{len(v)} items]"
    if isinstance(v, dict):
        if depth >= 2:
            return "{…}"
        items = []
        for ik, iv in list(v.items())[:8]:
            items.append(f"{ik}={_summarize(iv, depth + 1)}")
        more = len(v) - len(items)
        return "{" + ", ".join(items) + (f", …+{more}" if more > 0 else "") + "}"
    return type(v).__name__


def _extract_giveaway_meta(payload: dict) -> dict:
    """Pull human-meaningful fields off a giveaway payload, looking inside
    nested `activeGiveaway` and `pinnedProduct` objects where Whatnot
    actually stores most of this state. Anything we don't recognize lands
    in unknown_fields so the dashboard keeps surfacing new fields."""
    if not isinstance(payload, dict):
        return {}
    meta: dict = {}

    active   = payload.get("activeGiveaway") if isinstance(payload.get("activeGiveaway"), dict) else None
    pinned   = payload.get("pinnedProduct")  if isinstance(payload.get("pinnedProduct"),  dict) else None
    product  = payload.get("product")        if isinstance(payload.get("product"),        dict) else None

    # Search order matters: nested giveaway state first, then top-level
    end_sources     = [s for s in (active, payload) if isinstance(s, dict)]
    product_sources = [s for s in (pinned, active, product, payload) if isinstance(s, dict)]
    audience_sources = [s for s in (active, payload) if isinstance(s, dict)]

    # End time
    for src in end_sources:
        for k in _END_TIME_KEYS:
            if k in src:
                ts = _coerce_unix_seconds(src[k])
                if ts:
                    meta["ends_at"] = ts
                    break
        if "ends_at" in meta:
            break
    if "ends_at" not in meta:
        for src in end_sources:
            for k in _END_DELTA_KEYS:
                v = src.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    meta["ends_at"] = int(time.time() + (v / 1000 if v > 1e6 else v))
                    break
            if "ends_at" in meta:
                break

    # Product name
    for src in product_sources:
        name = _first_str(src, _PRODUCT_NAME_KEYS)
        if name:
            meta["product_name"] = name
            break

    # Product image
    for src in product_sources:
        img = _first_str(src, _PRODUCT_IMAGE_KEYS)
        if img:
            meta["product_image"] = img
            break

    # Audience / buyer restriction
    aud = None
    for src in audience_sources:
        aud = _first_str(src, _AUDIENCE_KEYS)
        if aud:
            break
    if aud:
        meta["audience"] = aud
    elif payload.get("requireQualifiedBuyer") is True:
        meta["audience"] = "qualified_buyer"
    else:
        for k in ("buyersOnly", "audienceMustBeBuyer", "restrictedToBuyers"):
            if payload.get(k) is True:
                meta["audience"] = "buyers_only"
                break

    # Unknown fields — strings/numbers/bools verbatim; nested objects
    # get a key-summary so we can see *what's inside* without paging
    # through /payload_samples.
    unknown = {}
    for k, v in payload.items():
        if k in _KNOWN_TOP_KEYS:
            continue
        if isinstance(v, (str, int, float, bool)):
            sv = str(v)
            unknown[k] = sv if len(sv) <= 80 else sv[:77] + "…"
        elif isinstance(v, (dict, list)) and v:
            unknown[k] = _summarize(v)
    # Always surface activeGiveaway + pinnedProduct contents, even though
    # we extracted from them — helps spot fields we *should* be mapping.
    if active:
        unknown["activeGiveaway"] = _summarize(active)
    if pinned:
        unknown["pinnedProduct"] = _summarize(pinned)
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
    """Capture first-seen payload per event_name for field discovery, AND
    surface channel-event names + payload keys onto the giveaway card so
    real Whatnot field/event names become visible without spelunking
    /payload_samples manually."""
    # Buffered sample (first-seen per name)
    if event_name not in payload_samples and len(payload_samples) < PAYLOAD_SAMPLES_MAX:
        payload_samples[event_name] = {
            "first_seen_at": int(time.time()),
            "stream_id":     stream["id"],
            "username":      stream.get("username", ""),
            "payload":       payload if isinstance(payload, (dict, list, str, int, float, bool, type(None))) else str(payload),
        }

    # Live update on the card if it's still around
    sid = stream["id"]
    entry = next((g for g in state["giveaways"] if g["stream_id"] == sid), None)
    if not entry:
        return

    dirty = False
    events_set = set(entry.get("channel_events") or [])
    if event_name not in events_set:
        events_set.add(event_name)
        entry["channel_events"] = sorted(events_set)
        dirty = True

    if isinstance(payload, dict):
        keys_set = set(entry.get("payload_keys") or [])
        for k in payload.keys():
            if k not in keys_set:
                keys_set.add(k)
                dirty = True
        if dirty:
            entry["payload_keys"] = sorted(keys_set)

        # Metadata may arrive on non-entry-count events (e.g., a giveaway_started
        # event carrying endsAt + productName). Re-extract from every payload
        # and merge anything new.
        meta = _extract_giveaway_meta(payload)
        for k, v in meta.items():
            if k == "unknown_fields":
                # Merge unmapped keys cumulatively across events
                cur = dict(entry.get("unknown_fields") or {})
                cur.update(v)
                if cur != entry.get("unknown_fields"):
                    entry["unknown_fields"] = cur
                    dirty = True
            elif entry.get(k) != v:
                entry[k] = v
                dirty = True

    if dirty:
        await broadcast("update", entry)


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
        "message": "Whatnot returned 403 — session tokens look stale. Open any "
                   "Whatnot livestream once: the extension will push fresh "
                   "tokens automatically. No restart needed."
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
