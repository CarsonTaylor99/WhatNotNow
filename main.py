import asyncio
import json
import smtplib
import time
from email.message import EmailMessage
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fetcher import get_streams
from scanner import scan_stream, watch_giveaway, get_auth_failures

DISCOVERY_CONCURRENCY = 4
DISCOVERED_FILE       = "discovered_categories.json"

from categories import CATEGORIES
from config import (
    update_auth, AUTH,
    SMTP_USER, SMTP_APP_PASSWORD, RECIPIENT_EMAIL, SMTP_HOST, SMTP_PORT,
)


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

# Rolling history of /auth/refresh hits so the dashboard can show whether
# auto-refresh is actually minting fresh tokens or just recycling stale ones.
auth_history: list[dict] = []
AUTH_HISTORY_MAX = 200


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
    # If activeGiveaway has a nested product object, prefer that
    active_product = (
        active.get("product")
        if active and isinstance(active.get("product"), dict)
        else None
    )

    # Search order:
    # - End time / audience: only from giveaway-state sources
    # - Product info: ONLY from giveaway sources, never pinnedProduct
    #   (pinnedProduct is the active auction item, NOT the giveaway).
    end_sources      = [s for s in (active, payload) if isinstance(s, dict)]
    product_sources  = [s for s in (active_product, active, product) if isinstance(s, dict)]
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
def _clear_stale(entry: dict) -> bool:
    """Strip stale-state keys from a giveaway entry. Returns True if anything changed."""
    if entry.pop("stale", None):
        entry.pop("stale_reason", None)
        entry.pop("stale_since", None)
        entry.pop("stale_attempts", None)
        return True
    return False


_STALE_RESPAWN_MAX_ATTEMPTS = 4
_STALE_RESPAWN_BASE_DELAY   = 45  # seconds — backs off linearly from here


async def _delayed_respawn(stream: dict, attempt: int):
    """Sleep, then re-spawn the watcher if the card is still stale and the
    scanner is healthy. This auto-recovers cards whose individual WS got
    knocked over without waiting for the discovery loop to cycle back."""
    delay = _STALE_RESPAWN_BASE_DELAY * attempt  # 45, 90, 135, 180s
    await asyncio.sleep(delay)
    sid = stream["id"]
    existing = next((g for g in state["giveaways"] if g["stream_id"] == sid), None)
    if not existing or not existing.get("stale"):
        return                       # card removed or already recovered
    if not state.get("scanning"):
        return                       # user pressed Stop
    if _scanner_in_outage():
        return                       # don't fight a real outage; discovery will retry
    if sid in active_watchers and not active_watchers[sid].done():
        return                       # somehow already running
    print(f"[main] {sid[:8]}… respawning stale watcher (attempt {attempt})")
    _start_watcher(stream)


async def on_watcher_update(stream: dict, payload: dict):
    """Entry count + metadata update from a long-running watcher connection."""
    sid = stream["id"]
    existing = next((g for g in state["giveaways"] if g["stream_id"] == sid), None)
    if not existing:
        return
    existing["entry_count"] = payload.get("entryCount", existing["entry_count"])
    _clear_stale(existing)  # any fresh watcher data means we're connected again
    # Re-extract every update so end-time/audience/etc. stay fresh
    meta = _extract_giveaway_meta(payload)
    for k, v in meta.items():
        existing[k] = v
    await broadcast("update", existing)


# Reasons in `on_watcher_ended` that mean we lost visibility, NOT that the
# giveaway actually ended. On these we keep the card and mark it stale —
# the giveaway is almost certainly still live on Whatnot, we just can't see
# it. The card will auto-recover when discovery sees the same stream's
# giveaway again or the watcher reconnects.
_LOST_VISIBILITY_REASON_PREFIXES = (
    "ws_403",
    "reconnect_exhausted",
    "join_rejected",
    "no_session_id",
    "stopped",
)


def _is_lost_visibility(reason: str) -> bool:
    return any(reason.startswith(p) for p in _LOST_VISIBILITY_REASON_PREFIXES)


def _scanner_in_outage() -> bool:
    """True when failures are very likely our problem (auth gone, paused, offline)."""
    return (
        state.get("auth_expired", False)
        or _offline_started_at is not None
        or time.time() < _pause_scanning_until
    )


async def on_watcher_ended(stream: dict, reason: str):
    """Watcher exited. Decide whether the giveaway actually ended (drop the
    card) or we just lost visibility (keep card, mark stale)."""
    sid = stream["id"]
    active_watchers.pop(sid, None)

    if _is_lost_visibility(reason) or _scanner_in_outage():
        existing = next((g for g in state["giveaways"] if g["stream_id"] == sid), None)
        if existing:
            attempts = existing.get("stale_attempts", 0) + 1
            existing["stale"]          = True
            existing["stale_reason"]   = reason
            existing["stale_since"]    = int(time.time())
            existing["stale_attempts"] = attempts
            print(f"[watcher] {sid[:8]}… stale (attempt {attempts}, {reason})")
            await broadcast("update", existing)
            # Schedule a background respawn so the card self-heals without
            # waiting for the discovery loop. Capped to avoid thrashing.
            if attempts <= _STALE_RESPAWN_MAX_ATTEMPTS:
                asyncio.create_task(_delayed_respawn(stream, attempts))
        return

    # True end signal (explicit end event, idle_timeout on a connected
    # socket, max_watch_reached) — drop the card.
    state["giveaways"] = [g for g in state["giveaways"] if g["stream_id"] != sid]
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
        was_stale = _clear_stale(existing)
        if was_stale:
            print(f"[main] {stream_id[:8]}… reconnected (was stale) — respawning watcher")
        await broadcast("update", existing)
        # If watcher died (e.g., reconnect_exhausted during a cliff), bring it back
        if stream_id not in active_watchers or active_watchers[stream_id].done():
            _start_watcher(stream)
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


# When N consecutive closed_before_join outcomes happen, that's a transient
# cliff event — Whatnot just rotated session tokens and our in-flight WS
# connections all died together. Pause briefly so the cliff burst can clear,
# trigger a refresh request to nudge the page if it's gone silent, then
# keep scanning.
_consecutive_close_before_join = 0
_CLOSE_BEFORE_JOIN_BURST_THRESHOLD = 5
_BURST_PAUSE_SECONDS            = 8
_pause_scanning_until: float = 0.0
_pause_started_at:     float = 0.0  # for early-exit comparison vs fresh-push timestamp
_last_fresh_push_at:   float = 0.0  # updated on every successful tokens_changed push

# ── Offline-time tracker ──────────────────────────────────────────────────
# We mark the scanner "offline" when a burst pause begins, and "online"
# when the next successful join happens. Each completed offline period
# is recorded so the dashboard can show how often / for how long the
# scanner was unable to scan.
_offline_started_at: float | None = None
_offline_periods: list[dict] = []
_OFFLINE_PERIODS_MAX = 100


def _mark_offline(reason: str) -> None:
    global _offline_started_at
    if _offline_started_at is None:
        _offline_started_at = time.time()
        print(f"[offline] scanner went offline ({reason})")


def _mark_online() -> None:
    global _offline_started_at
    if _offline_started_at is None:
        return
    end = time.time()
    period = {
        "start":    int(_offline_started_at),
        "end":      int(end),
        "duration": int(end - _offline_started_at),
    }
    _offline_periods.append(period)
    if len(_offline_periods) > _OFFLINE_PERIODS_MAX:
        del _offline_periods[:-_OFFLINE_PERIODS_MAX]
    print(f"[offline] scanner back online (was offline {period['duration']}s)")
    _offline_started_at = None


# ── Per-stream scan-result callback (called by scanner) ──────────────────────
async def on_scan_result(result: dict):
    """Update counters + rolling log + broadcast every per-stream scan outcome."""
    global _consecutive_close_before_join, _pause_scanning_until, _pause_started_at
    outcome = result.get("outcome")
    if outcome == "joined_ok":
        state["joined"] += 1
        if result.get("giveaway_events", 0) > 0:
            state["with_events"] += 1
        _consecutive_close_before_join = 0
        _mark_online()
    elif outcome == "join_rejected":
        state["rejected"] += 1
        _consecutive_close_before_join = 0
        _mark_online()
    elif outcome == "closed_before_join":
        state["errors"] += 1
        _consecutive_close_before_join += 1
        if _consecutive_close_before_join >= _CLOSE_BEFORE_JOIN_BURST_THRESHOLD:
            _consecutive_close_before_join = 0
            _pause_started_at     = time.time()
            _pause_scanning_until = _pause_started_at + _BURST_PAUSE_SECONDS
            _mark_offline(f"{_CLOSE_BEFORE_JOIN_BURST_THRESHOLD}+ closed_before_join in a row")
            # Reactive refresh — kick the extension to mint fresh tokens NOW
            # rather than waiting for the dashboard's age-based trigger or
            # the page's organic reconnect to come around.
            await _request_refresh("burst")
            print(f"[main] cliff burst — pausing scanner for {_BURST_PAUSE_SECONDS}s")
            await broadcast("status", {
                "message": f"Auth cliff — pausing {_BURST_PAUSE_SECONDS}s, refresh requested…"
            })
    elif outcome in ("ws_403", "ws_error"):
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
        "message": "Scanner can't join any streams — session tokens look stale. "
                   "Open any Whatnot livestream once: the extension will push "
                   "fresh tokens automatically, then click Start again."
    })
    _stop_event.set()
    # Watchers run as independent tasks and would otherwise keep retrying
    # with the same stale tokens, spamming closed_before_join entries until
    # they exhaust their reconnect budget. Cancel them.
    await _cancel_all_watchers()


# ── Main scanner loop ─────────────────────────────────────────────────────────
async def _cancel_all_watchers():
    """Cancel and clear every active watcher task. Used on start/stop."""
    for sid, task in list(active_watchers.items()):
        if not task.done():
            task.cancel()
    active_watchers.clear()


async def run_scanner():
    global _consecutive_close_before_join, _pause_scanning_until
    global _pause_started_at, _offline_started_at
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
    _consecutive_close_before_join = 0
    _pause_scanning_until          = 0.0
    _pause_started_at              = 0.0
    _offline_started_at            = None
    _offline_periods.clear()

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
                    # If a cliff burst just hit, wait — but exit early if a
                    # fresh-token push lands during the pause. Most cliffs
                    # resolve in 2-3s once the page reconnects; the 8s here
                    # is just a fallback ceiling.
                    pause_ref = _pause_started_at
                    while _pause_scanning_until > time.time():
                        if _stop_event.is_set():
                            return
                        if _last_fresh_push_at > pause_ref:
                            # Fresh tokens arrived after this pause began —
                            # safe to resume now instead of waiting out the
                            # full timeout.
                            break
                        await asyncio.sleep(0.25)
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
def _build_giveaway_email(entry: dict) -> EmailMessage:
    """Compose the email body for a single giveaway. Plain text is enough —
    iOS Mail will auto-link the URL so it's tappable."""
    msg = EmailMessage()
    msg["Subject"] = f"🎁 Whatnot giveaway from @{entry.get('username', '?')}"
    msg["From"]    = SMTP_USER
    msg["To"]      = RECIPIENT_EMAIL

    lines = [
        f"@{entry.get('username','?')} is running a giveaway right now.",
        "",
        f"Title:   {entry.get('title','—')}",
        f"Entries: {entry.get('entry_count', 0)}",
    ]
    if entry.get("category"):
        lines.append(f"Category: {entry['category']}")
    if entry.get("product_name"):
        lines.append(f"Product:  {entry['product_name']}")
    if entry.get("audience"):
        lines.append(f"Audience: {entry['audience']}")
    if isinstance(entry.get("ends_at"), int):
        remaining = entry["ends_at"] - int(time.time())
        if remaining > 0:
            lines.append(f"Ends in:  {remaining // 60}m {remaining % 60}s")
    lines += ["", f"Join: {entry.get('url', '')}"]
    msg.set_content("\n".join(lines))
    return msg


def _send_email_blocking(entry: dict) -> None:
    """Sync SMTP call — run inside asyncio.to_thread so it doesn't block the loop."""
    if not (SMTP_USER and SMTP_APP_PASSWORD and RECIPIENT_EMAIL):
        raise RuntimeError(
            "Email not configured — set SMTP_USER, SMTP_APP_PASSWORD, "
            "and RECIPIENT_EMAIL in .env (see env.example for Gmail steps)."
        )
    msg = _build_giveaway_email(entry)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_APP_PASSWORD)
        smtp.send_message(msg)


@app.post("/email/{stream_id}")
async def email_giveaway(stream_id: str):
    """Send a single giveaway's join link to RECIPIENT_EMAIL via SMTP."""
    if not _UUID_RE.match(stream_id):
        return {"ok": False, "error": "invalid stream_id"}
    entry = next((g for g in state["giveaways"] if g["stream_id"] == stream_id), None)
    if not entry:
        return {"ok": False, "error": "giveaway not in state (may have ended)"}
    try:
        await asyncio.to_thread(_send_email_blocking, entry)
    except Exception as e:
        print(f"[email] failed for {stream_id[:8]}…: {e}")
        return {"ok": False, "error": str(e)}
    print(f"[email] sent giveaway @{entry.get('username','?')} → {RECIPIENT_EMAIL}")
    return {"ok": True}


@app.get("/email/config")
async def email_config():
    """Tells the frontend whether to render the email button."""
    return {"enabled": bool(SMTP_USER and SMTP_APP_PASSWORD and RECIPIENT_EMAIL)}


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


# ── Extension event channel ───────────────────────────────────────────────
# The extension subscribes on startup; events broadcast here go to all
# connected clients. Used to ask the extension to force_reconnect — fired
# from two places: the dashboard's age-based check (proactive) and the
# burst detector (reactive, when scanner notices it's stuck).
_extension_clients: list[asyncio.Queue] = []
_last_refresh_signaled_at: float = 0.0
_REFRESH_COOLDOWN_SEC = 15  # tightened — user wants more aggressive refresh


async def _broadcast_to_extension(event: str, data: dict) -> int:
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    for q in _extension_clients:
        await q.put(msg)
    return len(_extension_clients)


async def _request_refresh(reason: str) -> dict:
    """Internal: ask all connected extension clients to force_reconnect.
    Honors the cooldown so we don't spam Whatnot's session machinery."""
    global _last_refresh_signaled_at
    now = time.time()
    if (now - _last_refresh_signaled_at) < _REFRESH_COOLDOWN_SEC:
        return {"ok": False, "skipped": "cooldown"}
    _last_refresh_signaled_at = now
    delivered = await _broadcast_to_extension("refresh", {"at": int(now), "reason": reason})
    print(f"[refresh] reason={reason}, delivered to {delivered} extension client(s)")
    return {"ok": True, "delivered": delivered}


@app.get("/extension/keepalive")
async def extension_keepalive(request: Request):
    """Long-running event stream the extension subscribes to on startup.
    Two jobs: (1) holds a fetch open so the SW isn't evicted (Brave/Chrome
    MV3 throttle hard otherwise), (2) carries `refresh` events fired by
    the dashboard (age-based) or the burst detector (cliff-reactive)."""
    queue: asyncio.Queue = asyncio.Queue()
    _extension_clients.append(queue)

    async def gen():
        try:
            yield f": connected at {int(time.time())}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25)
                    yield msg
                except asyncio.TimeoutError:
                    yield f": ping {int(time.time())}\n\n"
        except asyncio.CancelledError:
            return
        finally:
            try:
                _extension_clients.remove(queue)
            except ValueError:
                pass

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/extension/request_refresh")
async def extension_request_refresh():
    """Dashboard pings this when token age crosses its threshold."""
    return await _request_refresh("dashboard:age")


@app.get("/diagnostics/offline")
async def offline_diagnostics():
    """How often, and for how long, the scanner has been offline this run.
    Offline = a cliff burst happened and no successful join has landed
    since. Periods reset when the user starts a new scan run."""
    now = time.time()
    completed = list(_offline_periods)
    durations = [p["duration"] for p in completed]
    current = None
    if _offline_started_at is not None:
        current = {
            "since":            int(_offline_started_at),
            "duration_so_far":  int(now - _offline_started_at),
        }
    return {
        "now":             int(now),
        "currently_offline": current is not None,
        "current":         current,
        "periods":         completed,
        "stats": {
            "count":        len(completed),
            "total_offline_seconds":  sum(durations),
            "longest_seconds":        max(durations) if durations else 0,
            "avg_seconds":            (sum(durations) // len(durations)) if durations else 0,
        },
    }


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


def _decode_jwt_exp(token: str) -> int | None:
    """Best-effort: pull the `exp` claim out of a JWT without verifying it.
    Used to expose true token lifetime to the dashboard so we can tell
    whether a 'fresh' token is actually fresh or just a different string
    on the same expiry clock."""
    if not token or token.count(".") != 2:
        return None
    import base64
    try:
        payload_b64 = token.split(".")[1]
        # JWT payloads are base64url, often unpadded — pad to a multiple of 4
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(padded)
        claims = json.loads(raw)
        exp = claims.get("exp")
        return int(exp) if isinstance(exp, (int, float)) else None
    except Exception:
        return None


@app.post("/auth/refresh")
async def refresh_auth(request: Request):
    """Receives fresh tokens from the Chrome extension and hot-swaps them."""
    body           = await request.json()
    cookie         = (body.get("cookie") or "").strip()
    csrf_token     = (body.get("csrf_token") or "").strip()
    session_token  = (body.get("session_token") or "").strip()
    client_version = (body.get("client_version") or "").strip()
    socket_kind    = (body.get("socket_kind") or "auction").strip()
    source         = (body.get("source") or "unknown").strip()

    if not (cookie and csrf_token and session_token):
        print(f"[auth] refresh REJECTED — missing fields "
              f"(cookie={len(cookie)}, csrf={len(csrf_token)}, session={len(session_token)})")
        auth_history.append({
            "at":              int(time.time()),
            "source":          source,
            "socket_kind":     socket_kind,
            "tokens_changed":  False,
            "cookie_changed":  False,
            "ok":              False,
            "csrf_preview":    _trunc(csrf_token) if csrf_token else "",
            "session_preview": _trunc(session_token) if session_token else "",
            "cookie_len":      len(cookie),
            "session_exp":     _decode_jwt_exp(session_token),
            "csrf_exp":        _decode_jwt_exp(csrf_token),
        })
        if len(auth_history) > AUTH_HISTORY_MAX:
            del auth_history[:-AUTH_HISTORY_MAX]
        return {"ok": False, "error": "missing fields"}

    # Detect whether the incoming tokens differ from what we already have
    prev = AUTH.get(socket_kind, {}) if socket_kind in ("auction", "live") else {}
    prev_csrf    = prev.get("csrf_token", "")
    prev_session = prev.get("session_token", "")
    tokens_changed = (csrf_token != prev_csrf) or (session_token != prev_session)
    cookie_changed = (cookie != AUTH.get("cookie", ""))

    update_auth(cookie, csrf_token, session_token, client_version, socket_kind)
    state["auth_expired"] = False
    state["error"]        = None

    at = int(time.time())
    flag = "NEW ✓" if tokens_changed else "same"
    cflag = "new" if cookie_changed else "same"
    print(f"[auth] {socket_kind} refresh @ {time.strftime('%H:%M:%S')} "
          f"src={source} — tokens {flag}, cookie {cflag} ({len(cookie)}ch)")
    session_exp = _decode_jwt_exp(session_token)
    csrf_exp    = _decode_jwt_exp(csrf_token)
    if tokens_changed:
        global _last_fresh_push_at
        _last_fresh_push_at = float(at)
    auth_history.append({
        "at":              at,
        "source":          source,
        "socket_kind":     socket_kind,
        "tokens_changed":  tokens_changed,
        "cookie_changed":  cookie_changed,
        "ok":              True,
        "csrf_preview":    _trunc(csrf_token),
        "session_preview": _trunc(session_token),
        "cookie_len":      len(cookie),
        "session_exp":     session_exp,
        "csrf_exp":        csrf_exp,
    })
    if session_exp:
        ttl = session_exp - at
        print(f"[auth]   session_token exp={time.strftime('%H:%M:%S', time.localtime(session_exp))} "
              f"(ttl {ttl}s)")
    if len(auth_history) > AUTH_HISTORY_MAX:
        del auth_history[:-AUTH_HISTORY_MAX]
    await broadcast("auth_refreshed", {
        "message": "Tokens refreshed",
        "at": at,
        "tokens_changed": tokens_changed,
        "cookie_changed": cookie_changed,
    })
    return {"ok": True, "at": at, "tokens_changed": tokens_changed}


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


def _token_first_seen_at(session_preview: str, socket_kind: str, before_ts: int) -> int | None:
    """Walk auth_history forward in time. Find when this session_preview was
    first introduced in its most-recent contiguous run before `before_ts`.
    That's the answer to 'how long had this token been alive when it failed?'

    Filters by socket_kind — auction and live tokens are independent, so an
    auction push doesn't reset the live token's run (and vice versa).

    A 'run' = consecutive successful pushes carrying the same session_preview.
    A push with a *different* preview ends the run; we then start tracking
    again when that preview reappears. We return the start of the run that
    ends at-or-before before_ts."""
    if not session_preview:
        return None
    run_start: int | None = None
    for e in auth_history:
        if e["at"] > before_ts:
            break
        if not e.get("ok"):
            continue
        if e.get("socket_kind") != socket_kind:
            continue
        e_prev = e.get("session_preview")
        if e_prev == session_preview:
            if run_start is None:
                run_start = e["at"]
        elif e_prev:
            # Different token observed — end any open run.
            run_start = None
    return run_start


def _enrich_failure(f: dict) -> dict:
    """Add token_age_at_failure and the matching push timestamp."""
    out = dict(f)
    first_at = _token_first_seen_at(
        f.get("session_preview", ""),
        f.get("socket_kind", "live"),
        f["at"],
    )
    out["token_first_seen_at"] = first_at
    out["token_age_seconds"]   = (f["at"] - first_at) if first_at else None
    return out


@app.get("/auth/failures")
async def auth_failures_view():
    """Recent 403s + transient closed_errors with token-age correlation.
    The key signal: do failures cluster at a consistent token age? If yes,
    Whatnot has a fixed token-lifetime cliff regardless of refresh timing."""
    raw = get_auth_failures()
    enriched = [_enrich_failure(f) for f in raw]
    ages = [e["token_age_seconds"] for e in enriched if e["token_age_seconds"] is not None]
    return {
        "events": list(reversed(enriched)),  # newest first
        "stats": {
            "total":         len(enriched),
            "ages_seconds":  ages,
            "min_age":       min(ages) if ages else None,
            "max_age":       max(ages) if ages else None,
            "avg_age":       (sum(ages) // len(ages)) if ages else None,
        },
    }


@app.get("/auth/history")
async def auth_history_view():
    """Recent /auth/refresh hits with summary stats.
    Lets the dashboard show whether auto-refresh is actually minting new
    tokens (source='page', tokens_changed=true) or just recycling stale ones
    via the cookie-fallback path (source='alarm', tokens_changed=false)."""
    now = int(time.time())
    last_hour = [e for e in auth_history if (now - e["at"]) <= 3600]

    by_source: dict[str, int] = {}
    by_source_changed: dict[str, int] = {}
    for e in last_hour:
        s = e.get("source", "?")
        by_source[s] = by_source.get(s, 0) + 1
        if e.get("tokens_changed"):
            by_source_changed[s] = by_source_changed.get(s, 0) + 1

    last_fresh_at = next(
        (e["at"] for e in reversed(auth_history) if e.get("tokens_changed")),
        None,
    )

    # Latest exp values seen per socket kind — lets the dashboard show
    # whether multiple "fresh" tokens are actually on the same expiry clock.
    latest_exp_by_kind: dict[str, dict] = {}
    for e in reversed(auth_history):
        kind = e.get("socket_kind", "?")
        if kind in latest_exp_by_kind or not e.get("ok"):
            continue
        if e.get("session_exp"):
            latest_exp_by_kind[kind] = {
                "session_exp":   e["session_exp"],
                "csrf_exp":      e.get("csrf_exp"),
                "captured_at":   e["at"],
            }
    # Correlate recent failures with auth_history so the dashboard can render
    # "token age at failure" inline.
    recent_failures = [_enrich_failure(f) for f in get_auth_failures()][-10:]
    failure_ages = [e["token_age_seconds"] for e in recent_failures
                    if e["token_age_seconds"] is not None]

    return {
        "events": list(reversed(auth_history)),  # newest first
        "stats": {
            "now":                          now,
            "total_last_hour":              len(last_hour),
            "tokens_changed_last_hour":     sum(1 for e in last_hour if e.get("tokens_changed")),
            "by_source_last_hour":          by_source,
            "by_source_changed_last_hour":  by_source_changed,
            "last_event_at":                auth_history[-1]["at"] if auth_history else None,
            "last_fresh_at":                last_fresh_at,
            "latest_exp_by_kind":           latest_exp_by_kind,
            "failures_recent":              list(reversed(recent_failures)),
            "failure_age_min":              min(failure_ages) if failure_ages else None,
            "failure_age_max":              max(failure_ages) if failure_ages else None,
            "failure_age_avg":              (sum(failure_ages) // len(failure_ages)) if failure_ages else None,
        },
    }


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
