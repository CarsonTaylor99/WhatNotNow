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


async def scan_stream(stream: dict, on_giveaway, on_auth_expired=None) -> None:
    """
    Connect to the live socket, join auction:{stream_uuid}, and listen for
    giveaway_entry_count_updated for SCAN_DURATION seconds.
    """
    stream_id = stream["id"]
    topic     = f"auction:{stream_id}"

    if not AUTH.get("livestream_session_id"):
        print(f"[scanner] {stream_id[:8]}… skipped — no livestreamSessionId captured yet. "
              "Open any Whatnot stream in Chrome to seed it.")
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

                    if event == "phx_reply" and isinstance(payload, dict):
                        status = payload.get("status")
                        if status == "ok":
                            join_accepted = True
                        elif status == "error":
                            err = payload.get("response", {})
                            print(f"[scanner] {stream_id[:8]}… join rejected: {err}")
                            break

                    if event == "giveaway_entry_count_updated":
                        await on_giveaway(stream, payload)

            finally:
                stop_event.set()
                hb_task.cancel()

    except websockets.exceptions.InvalidStatus as e:
        if "403" in str(e):
            print("[scanner] ⚠  403 on WebSocket — session tokens may have expired. "
                  "Have the extension re-push, or re-paste tokens.")
            if on_auth_expired:
                await on_auth_expired()
        else:
            print(f"[scanner] WS error on {stream_id}: {e}")
    except websockets.exceptions.ConnectionClosedOK:
        if not join_accepted:
            print(f"[scanner] {stream_id[:8]}… closed before join — likely auth/freshness issue")
            if on_auth_expired:
                await on_auth_expired()
        else:
            print(f"[scanner] {stream_id[:8]}… closed cleanly (likely ended)")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"[scanner] {stream_id[:8]}… closed with error: {e}")
    except Exception as e:
        print(f"[scanner] Error scanning {stream_id}: {e}")
