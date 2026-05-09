import asyncio
import json
import time
from uuid import uuid4
from urllib.parse import urlencode

import websockets

from config import WS_LIVE_URL, AUTH, SCAN_DURATION


# Module-level ring buffer of recent auth failures (403s). main.py reads this
# via get_auth_failures() to correlate failures with auth_history pushes.
_auth_failures: list[dict] = []
_AUTH_FAILURES_MAX = 100


def _trunc(s: str, n: int = 12) -> str:
    """Mirror of main._trunc — must produce the same preview format so
    auth_history.session_preview can be matched against the token captured
    here at connect-time."""
    if not s:
        return ""
    return f"{s[:n]}…{s[-6:]}" if len(s) > n + 6 else s


def get_auth_failures() -> list[dict]:
    """Snapshot of recent 403 events. Newest last."""
    return list(_auth_failures)


def _record_auth_failure(
    socket_kind: str,
    used_session_token: str,
    used_csrf_token: str,
    context: str,
    error: str,
) -> None:
    _auth_failures.append({
        "at":              int(time.time()),
        "socket_kind":     socket_kind,
        "context":         context,
        "session_preview": _trunc(used_session_token),
        "csrf_preview":    _trunc(used_csrf_token),
        "error":           str(error)[:200],
    })
    if len(_auth_failures) > _AUTH_FAILURES_MAX:
        del _auth_failures[:-_AUTH_FAILURES_MAX]


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

        # Capture exactly which tokens this connection is using, so if a 403
        # surfaces later we can attribute it to the right token even if AUTH
        # has since been hot-swapped by an extension push.
        live_at_connect = AUTH.get("live", {})
        used_session    = live_at_connect.get("session_token", "")
        used_csrf       = live_at_connect.get("csrf_token", "")

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
                                _record_auth_failure(
                                    socket_kind="live",
                                    used_session_token=used_session,
                                    used_csrf_token=used_csrf,
                                    context="scan_stream:join_rejected",
                                    error=f"phoenix: {json.dumps(join_error)[:160]}",
                                )
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
                _record_auth_failure(
                    socket_kind="live",
                    used_session_token=used_session,
                    used_csrf_token=used_csrf,
                    context="scan_stream",
                    error=str(e),
                )
                print(f"[scanner] ⚠  403 on WebSocket — session tokens may have expired. "
                      f"failed_token={_trunc(used_session)} — see /auth/failures for age.")
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
                # When auth goes stale, Whatnot's Phoenix layer doesn't 403
                # the WS upgrade — it accepts the connection and then closes
                # cleanly before ack'ing phx_join. So a cluster of these IS
                # the auth-expired signal. Record so we can correlate token
                # age at failure.
                _record_auth_failure(
                    socket_kind="live",
                    used_session_token=used_session,
                    used_csrf_token=used_csrf,
                    context="scan_stream:closed_before_join",
                    error="ws closed cleanly before phoenix join ack",
                )
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
    idle_state: dict,
    idle_timeout: int = IDLE_TIMEOUT,
) -> tuple[str, str | None]:
    """One connect+listen attempt. Returns (status, reason).

    idle_state["last_entry_at"] is the wall-clock time of the most recent
    giveaway_entry_count_updated event seen by ANY attempt for this watcher.
    Persisting it across reconnects is what makes idle-timeout actually
    work — otherwise a flapping WS would reset the timer on every connect.

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

    # Tokens-in-use snapshot — see comment in scan_stream for rationale.
    live_at_connect = AUTH.get("live", {})
    used_session    = live_at_connect.get("session_token", "")
    used_csrf       = live_at_connect.get("csrf_token", "")

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

                    # Idle-timeout: if no entry update in N sec (across all
                    # reconnect attempts), giveaway is almost certainly over —
                    # Whatnot tends to just stop firing entry events rather
                    # than send an explicit ended event.
                    if asyncio.get_event_loop().time() - idle_state["last_entry_at"] > idle_timeout:
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
                            _record_auth_failure(
                                socket_kind="live",
                                used_session_token=used_session,
                                used_csrf_token=used_csrf,
                                context="watch_attempt:join_rejected",
                                error=f"phoenix: {json.dumps(payload.get('response'))[:160]}",
                            )
                            return ("join_rejected", str(payload.get("response")))

                    if on_event_seen:
                        try:
                            await on_event_seen(stream, event, payload)
                        except Exception:
                            pass

                    if event == "giveaway_entry_count_updated":
                        idle_state["last_entry_at"] = asyncio.get_event_loop().time()
                        await on_update(stream, payload)
                    elif event in GIVEAWAY_END_EVENTS:
                        return ("explicit_end", event)

                return ("max_watch", None)

            finally:
                hb_stop.set()
                hb_task.cancel()

    except websockets.exceptions.ConnectionClosedOK:
        # If we never got the join ack, this is the auth-expired pattern
        # (Phoenix closes cleanly instead of returning 403 / phx_reply error).
        # Only record in that case — a post-join clean close is a legitimate
        # stream-end signal, not auth.
        if not join_accepted:
            _record_auth_failure(
                socket_kind="live",
                used_session_token=used_session,
                used_csrf_token=used_csrf,
                context="watch_attempt:closed_before_join",
                error="ws closed cleanly before phoenix join ack",
            )
        return ("transient", "closed_ok")
    except websockets.exceptions.ConnectionClosedError as e:
        # Treat closed-with-error like a failure for diagnostic purposes —
        # if these cluster around the same token age as 403s, the server is
        # silently killing stale-token connections instead of returning 403.
        _record_auth_failure(
            socket_kind="live",
            used_session_token=used_session,
            used_csrf_token=used_csrf,
            context="watch_attempt:closed_error",
            error=str(e),
        )
        return ("transient", f"closed_error: {e}")
    except websockets.exceptions.InvalidStatus as e:
        if "403" in str(e):
            _record_auth_failure(
                socket_kind="live",
                used_session_token=used_session,
                used_csrf_token=used_csrf,
                context="watch_attempt",
                error=str(e),
            )
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
    max_reconnects: int = 12,
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

    deadline     = asyncio.get_event_loop().time() + max_seconds
    consec_fails = 0  # transient closes in a row without a productive run

    # Persisted across reconnects so flapping doesn't reset the idle timer.
    idle_state = {"last_entry_at": asyncio.get_event_loop().time()}

    while True:
        if stop_event and stop_event.is_set():
            await on_ended(stream, "stopped")
            return
        if asyncio.get_event_loop().time() >= deadline:
            await on_ended(stream, "max_watch_reached")
            return
        # Idle check at outer-loop boundary too — covers the case where every
        # _watch_attempt is closing fast (so the inner check never fires)
        # but no entry events are arriving in between.
        if asyncio.get_event_loop().time() - idle_state["last_entry_at"] > IDLE_TIMEOUT:
            await on_ended(stream, "idle_timeout")
            return

        last_entry_before = idle_state["last_entry_at"]
        status, reason = await _watch_attempt(
            stream, on_update, on_event_seen, stop_event, deadline, idle_state,
        )
        # If this attempt actually delivered entry events, the connection
        # was working — reset the consecutive-failure counter so a long-
        # lived watcher doesn't accumulate a death sentence over hours of
        # otherwise-healthy reconnects.
        if idle_state["last_entry_at"] > last_entry_before:
            consec_fails = 0

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
