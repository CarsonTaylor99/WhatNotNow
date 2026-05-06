import asyncio
import json
import websockets
from urllib.parse import urlencode
from config import WS_BASE, CSRF_TOKEN, SESSION_TOKEN, CLIENT_VERSION, SCAN_DURATION


def _build_ws_url() -> str:
    params = {
        "_csrf_token":          CSRF_TOKEN,
        "client_layer":         "nextjs",
        "client_type":          "web",
        "client_version":       CLIENT_VERSION,
        "live_version":         "v2",
        "sessionExtensionToken": SESSION_TOKEN,
        "vsn":                  "2.0.0",
    }
    return f"{WS_BASE}?{urlencode(params)}"


async def _heartbeat(ws, stop_event: asyncio.Event):
    """Send Phoenix heartbeat every 30 s to keep connection alive."""
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


async def scan_stream(stream: dict, on_giveaway) -> None:
    """
    Connect to a stream's auction Phoenix channel and listen for
    giveaway_entry_count_updated events for SCAN_DURATION seconds.
    Calls on_giveaway(stream, payload) when a giveaway is detected.
    """
    stream_id = stream["id"]
    topic     = f"auction:{stream_id}"
    url       = _build_ws_url()
    ref       = 0

    def next_ref():
        nonlocal ref
        ref += 1
        return str(ref)

    try:
        async with websockets.connect(
            url,
            ping_interval=None,          # we handle heartbeat manually
            open_timeout=8,
            close_timeout=5,
            additional_headers={
                "Origin": "https://www.whatnot.com",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/147.0.0.0 Safari/537.36"
                ),
            },
        ) as ws:
            # ── Join the auction channel ──────────────────────────────────
            join_ref = next_ref()
            await ws.send(json.dumps([join_ref, next_ref(), topic, "phx_join", {}]))

            stop_event = asyncio.Event()
            hb_task    = asyncio.create_task(_heartbeat(ws, stop_event))

            try:
                deadline = asyncio.get_event_loop().time() + SCAN_DURATION
                while asyncio.get_event_loop().time() < deadline:
                    remaining = deadline - asyncio.get_event_loop().time()
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=min(remaining, 5)
                        )
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Phoenix message: [join_ref, ref, topic, event, payload]
                    if len(msg) < 5:
                        continue

                    event   = msg[3]
                    payload = msg[4]

                    if event == "giveaway_entry_count_updated":
                        await on_giveaway(stream, payload)

            finally:
                stop_event.set()
                hb_task.cancel()

    except websockets.exceptions.InvalidStatus as e:
        # 403 = tokens expired; surface clearly
        if "403" in str(e):
            print("[scanner] ⚠  403 on WebSocket — session tokens may have expired. "
                  "Re-copy them from DevTools and update your .env file.")
        else:
            print(f"[scanner] WS error on {stream_id}: {e}")
    except Exception as e:
        print(f"[scanner] Error scanning {stream_id}: {e}")
