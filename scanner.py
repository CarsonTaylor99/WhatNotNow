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
                # Whatnot routinely closes idle/transient connections; this is
                # only an auth signal in combination with a 403 elsewhere.
                # Don't fire auth_expired from here.
                print(f"[scanner] {stream_id[:8]}… closed before join (transient)")
                outcome = "closed_before_join"
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


IDLE_TIMEOUT = 90  # sec without an entry-count update → giveaway considered over


async def _watch_attempt(
    stream: dict,
    on_update,
    on_event_seen,
    stop_event: asyncio.Event | None,
    deadline: float,
    idle_timeout: int = IDLE_TIMEOUT,
) -> tuple[str, str | None]:
    """One connect+listen attempt. Returns (status, reason).
    status:
      "explicit_end"  — saw a known end event OR idle-timed-out (terminal)
      "max_watch"     — deadline reached (terminal)
      "stopped"       — stop_event set (terminal)
      "join_rejected" — Phoenix rejected the join (terminal)
      "auth_403"      — WS upgrade 403 (terminal)
      "transient"     — WS closed without a definitive signal (retryable)
      "fatal"         — non-recoverable exception (terminal)
    """
    stream_id = stream["id"]
    topic     = f"auction:{stream_id}"
    url       = _build_ws_url()
    ref       = 0
    def next_ref():
        nonlocal ref
        ref += 1
        return str(ref)

    join_accepted = False
    last_update_at = asyncio.get_event_loop().time()  # idle timer reference

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
                while asyncio.get_event_loop().time() < deadline:
                    if stop_event and stop_event.is_set():
                        return ("stopped", None)

                    # Idle-timeout: if no entry update in N sec, giveaway is
                    # almost certainly over — Whatnot tends to just stop firing
                    # entry events rather than send an explicit ended event.
                    if asyncio.get_event_loop().time() - last_update_at > idle_timeout:
                        return ("explicit_end", "idle_timeout")

                    remaining = deadline - asyncio.get_event_loop().time()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 10))
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
                            return ("join_rejected", str(payload.get("response")))

                    if on_event_seen:
                        try:
                            await on_event_seen(stream, event, payload)
                        except Exception:
                            pass

                    if event == "giveaway_entry_count_updated":
                        last_update_at = asyncio.get_event_loop().time()
                        await on_update(stream, payload)
                    elif event in GIVEAWAY_END_EVENTS:
                        return ("explicit_end", event)

                return ("max_watch", None)

            finally:
                hb_stop.set()
                hb_task.cancel()

    except websockets.exceptions.ConnectionClosedOK:
        # Could be the stream truly ending OR Whatnot rebalancing/idle-pruning
        # our connection. Without an explicit end event we treat as transient
        # and let the outer loop decide whether to retry.
        return ("transient", "closed_ok")
    except websockets.exceptions.ConnectionClosedError as e:
        return ("transient", f"closed_error: {e}")
    except websockets.exceptions.InvalidStatus as e:
        if "403" in str(e):
            return ("auth_403", str(e))
        return ("transient", f"ws_error: {e}")
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except Exception as e:
        return ("fatal", f"unexpected: {e}")
    finally:
        # If we never got the join ack and an exception was bubbling, the
        # outer loop sees status="transient" and gets to decide on retry.
        _ = join_accepted  # noqa: F841 — kept for readability/debugging


async def watch_giveaway(
    stream: dict,
    on_update,           # async (stream, payload) — entry-count updates
    on_ended,            # async (stream, reason)  — giveaway done / stream offline
    on_event_seen=None,  # async (stream, event_name, payload) — for sample capture
    stop_event: asyncio.Event | None = None,
    max_seconds: int = 30 * 60,
    max_reconnects: int = 4,
    backoff_base: float = 2.0,
) -> None:
    """Hold a long-running connection to a stream that has an active giveaway.

    Auto-reconnects on transient WS closes — Whatnot frequently drops idle
    or rebalanced connections, so a single ConnectionClosedOK doesn't mean
    the stream actually ended. Only emits on_ended for terminal conditions:
    an explicit end event, max_seconds, stop_event, hard auth failure, or
    max_reconnects exhausted without a successful follow-up join.
    """
    stream_id = stream["id"]

    if not AUTH.get("livestream_session_id"):
        await on_ended(stream, "no_session_id")
        return

    deadline       = asyncio.get_event_loop().time() + max_seconds
    consec_fails   = 0  # transient closes in a row without a productive run

    while True:
        if stop_event and stop_event.is_set():
            await on_ended(stream, "stopped")
            return
        if asyncio.get_event_loop().time() >= deadline:
            await on_ended(stream, "max_watch_reached")
            return

        status, reason = await _watch_attempt(
            stream, on_update, on_event_seen, stop_event, deadline,
        )

        if status == "explicit_end":
            await on_ended(stream, reason or "ended")
            return
        if status == "max_watch":
            await on_ended(stream, "max_watch_reached")
            return
        if status == "stopped":
            await on_ended(stream, "stopped")
            return
        if status == "join_rejected":
            await on_ended(stream, f"join_rejected: {reason}")
            return
        if status == "auth_403":
            await on_ended(stream, "ws_403")
            return
        if status == "fatal":
            await on_ended(stream, reason or "fatal")
            return

        # transient — try to reconnect with backoff
        consec_fails += 1
        if consec_fails > max_reconnects:
            await on_ended(stream, f"reconnect_exhausted ({reason})")
            return

        sleep_for = min(backoff_base ** consec_fails, 30)
        print(f"[watcher] {stream_id[:8]}… transient close ({reason}); "
              f"reconnect {consec_fails}/{max_reconnects} in {sleep_for:.1f}s")
        try:
            await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            await on_ended(stream, "stopped")
            return
