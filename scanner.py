import asyncio
import json
from uuid import uuid4
from urllib.parse import urlencode

import websockets

from config import WS_LIVE_URL, AUTH, SCAN_DURATION


def _build_ws_url() -> str:
    """Build the LIVE-socket WS URL with this user's live-socket csrf+session."""
    live = AUTH.get("live", {})
    params = {
        "_csrf_token":           live.get("csrf_token", ""),
        "client_layer":          "nextjs",
        "client_type":           "web",
        "client_version":        AUTH["client_version"],
        "live_version":          "v2",
        "sessionExtensionToken": live.get("session_token", ""),
        "vsn":                   "2.0.0",
    }
    return f"{WS_LIVE_URL}?{urlencode(params)}"


def _join_payload() -> dict:
    return {
        "livestreamSessionId": AUTH.get("livestream_session_id", ""),
        "slo_stories": [{"story": "story_livestream_join", "key": str(uuid4())}],
    }


def _browser_headers() -> dict:
    return {
        "Pragma":          "no-cache",
        "Cache-Control":   "no-cache",
        "Origin":          "https://www.whatnot.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie":          AUTH["cookie"],
        "Sec-GPC":         "1",
    }


async def _heartbeat(ws, stop_event: asyncio.Event):
    ref = 100
    while not stop_event.is_set():
        await asyncio.sleep(30)
        if stop_event.is_set():
            break
        ref += 1
        try:
            await ws.send(json.dumps([None, str(ref), "phoenix", "heartbeat", {}]))
        except Exception:
            break


# Event names that signal a giveaway has finished. We don't have a confirmed
# list from Whatnot — these are the most likely candidates. on_event_seen
# captures samples so we can refine this list from real data (Phase 2).
GIVEAWAY_END_EVENTS = {
    "giveaway_ended",
    "giveaway_finished",
    "giveaway_winner",
    "giveaway_winner_announced",
    "giveaway_completed",
    "giveaway_closed",
}


async def scan_stream(stream: dict, on_giveaway, on_auth_expired=None, on_result=None) -> None:
    """
    Connect to the live socket, join auction:{stream_uuid}, and listen for
    giveaway_entry_count_updated for SCAN_DURATION seconds.

    Emits a single structured result via on_result(dict) when the scan ends:
        outcome ∈ {joined_ok, join_rejected, no_session_id, ws_403,
                   closed_before_join, ws_error}
    """
    stream_id = stream["id"]
    topic     = f"auction:{stream_id}"

    outcome: str | None = None
    extra: dict = {}

    if not AUTH.get("livestream_session_id"):
        print(f"[scanner] {stream_id[:8]}… skipped — no livestreamSessionId captured yet. "
              "Open any Whatnot stream in Chrome to seed it.")
        outcome = "no_session_id"
    else:
        url = _build_ws_url()
        ref = 0
        def next_ref():
            nonlocal ref
            ref += 1
            return str(ref)

        join_accepted   = False
        events_seen     = 0
        giveaway_events = 0
        join_error      = None

        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                open_timeout=8,
                close_timeout=5,
                additional_headers=_browser_headers(),
            ) as ws:
                join_ref = next_ref()
                await ws.send(json.dumps([join_ref, next_ref(), topic, "phx_join", _join_payload()]))

                stop_event = asyncio.Event()
                hb_task    = asyncio.create_task(_heartbeat(ws, stop_event))

                try:
                    deadline = asyncio.get_event_loop().time() + SCAN_DURATION
                    while asyncio.get_event_loop().time() < deadline:
                        remaining = deadline - asyncio.get_event_loop().time()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5))
                        except asyncio.TimeoutError:
                            continue

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(msg, list) or len(msg) < 5:
                            continue

                        event   = msg[3]
                        payload = msg[4]
                        events_seen += 1

                        if event == "phx_reply" and isinstance(payload, dict):
                            status = payload.get("status")
                            if status == "ok":
                                join_accepted = True
                            elif status == "error":
                                join_error = payload.get("response", {})
                                print(f"[scanner] {stream_id[:8]}… join rejected: {join_error}")
                                break

                        if event == "giveaway_entry_count_updated":
                            giveaway_events += 1
                            await on_giveaway(stream, payload)
                            # Hand off to the persistent watcher; don't keep
                            # this discovery WS competing for the same channel.
                            break

                finally:
                    stop_event.set()
                    hb_task.cancel()

            # Clean exit, no exception raised
            if join_error is not None:
                outcome = "join_rejected"
                extra   = {"reason": str(join_error)}
            elif join_accepted:
                outcome = "joined_ok"
                extra   = {"events_seen": events_seen, "giveaway_events": giveaway_events}
            else:
                outcome = "closed_before_join"

        except websockets.exceptions.InvalidStatus as e:
            if "403" in str(e):
                print("[scanner] ⚠  403 on WebSocket — session tokens may have expired. "
                      "Have the extension re-push, or re-paste tokens.")
                outcome = "ws_403"
                if on_auth_expired:
                    await on_auth_expired()
            else:
                print(f"[scanner] WS error on {stream_id}: {e}")
                outcome = "ws_error"
                extra   = {"reason": str(e)}
        except websockets.exceptions.ConnectionClosedOK:
            if join_accepted:
                outcome = "joined_ok"
                extra   = {"events_seen": events_seen, "giveaway_events": giveaway_events}
                print(f"[scanner] {stream_id[:8]}… closed cleanly (likely ended)")
            else:
                print(f"[scanner] {stream_id[:8]}… closed before join — likely auth/freshness issue")
                outcome = "closed_before_join"
                if on_auth_expired:
                    await on_auth_expired()
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[scanner] {stream_id[:8]}… closed with error: {e}")
            outcome = "ws_error"
            extra   = {"reason": str(e)}
        except Exception as e:
            print(f"[scanner] Error scanning {stream_id}: {e}")
            outcome = "ws_error"
            extra   = {"reason": str(e)}

    if on_result and outcome:
        await on_result({
            "stream_id": stream_id,
            "title":     stream.get("title", ""),
            "username":  stream.get("username", ""),
            "outcome":   outcome,
            **extra,
        })


async def watch_giveaway(
    stream: dict,
    on_update,           # async (stream, payload) — entry-count updates
    on_ended,            # async (stream, reason)  — giveaway done / stream offline
    on_event_seen=None,  # async (stream, event_name, payload) — for sample capture
    stop_event: asyncio.Event | None = None,
    max_seconds: int = 30 * 60,
) -> None:
    """Hold a long-running connection to a stream that has an active giveaway.

    Forwards entry-count updates and detects end-of-giveaway via known event
    names, ws close, or max_seconds timeout. on_event_seen, if provided, fires
    for *every* channel event — used by main.py to capture payload samples
    so we can learn which fields Whatnot actually sends.
    """
    stream_id = stream["id"]
    topic     = f"auction:{stream_id}"

    if not AUTH.get("livestream_session_id"):
        await on_ended(stream, "no_session_id")
        return

    url = _build_ws_url()
    ref = 0
    def next_ref():
        nonlocal ref
        ref += 1
        return str(ref)

    join_accepted = False

    try:
        async with websockets.connect(
            url,
            ping_interval=None,
            open_timeout=8,
            close_timeout=5,
            additional_headers=_browser_headers(),
        ) as ws:
            await ws.send(json.dumps([next_ref(), next_ref(), topic, "phx_join", _join_payload()]))

            hb_stop = asyncio.Event()
            hb_task = asyncio.create_task(_heartbeat(ws, hb_stop))

            try:
                deadline = asyncio.get_event_loop().time() + max_seconds
                while asyncio.get_event_loop().time() < deadline:
                    if stop_event and stop_event.is_set():
                        await on_ended(stream, "stopped")
                        return

                    remaining = deadline - asyncio.get_event_loop().time()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 20))
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(msg, list) or len(msg) < 5:
                        continue

                    event   = msg[3]
                    payload = msg[4]

                    if event == "phx_reply" and isinstance(payload, dict):
                        if payload.get("status") == "ok":
                            join_accepted = True
                        elif payload.get("status") == "error":
                            await on_ended(stream, f"join_rejected: {payload.get('response')}")
                            return

                    if on_event_seen:
                        try:
                            await on_event_seen(stream, event, payload)
                        except Exception:
                            pass

                    if event == "giveaway_entry_count_updated":
                        await on_update(stream, payload)
                    elif event in GIVEAWAY_END_EVENTS:
                        await on_ended(stream, event)
                        return

                # Hit max_seconds without an explicit end event
                await on_ended(stream, "max_watch_reached")

            finally:
                hb_stop.set()
                hb_task.cancel()

    except websockets.exceptions.ConnectionClosedOK:
        await on_ended(stream, "stream_offline" if join_accepted else "closed_before_join")
    except websockets.exceptions.ConnectionClosedError as e:
        await on_ended(stream, f"ws_closed_error: {e}")
    except websockets.exceptions.InvalidStatus as e:
        await on_ended(stream, "ws_403" if "403" in str(e) else f"ws_error: {e}")
    except Exception as e:
        await on_ended(stream, f"error: {e}")
