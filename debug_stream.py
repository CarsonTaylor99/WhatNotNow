"""
Connect to a Whatnot Phoenix socket and print every message received.

Default: uses the auction socket built from current AUTH, joins auction:{uuid}.

Override with --url (full WS URL from DevTools) and/or --topic to test other
sockets — e.g., the live socket where giveaways may actually flow.

Usage:
    .venv/bin/python debug_stream.py <stream_uuid> [seconds]
    .venv/bin/python debug_stream.py --url 'wss://...' --topic 'live:UUID' <uuid> 60
"""
import argparse
import asyncio
import json
from urllib.parse import urlencode

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from config import WS_AUCTION_URL, WS_LIVE_URL, AUTH as LOCAL_AUTH

# Live state — populated from the running server before we connect.
# Falls back to LOCAL_AUTH (read from .env) if the server isn't running.
AUTH = dict(LOCAL_AUTH)


def fetch_live_auth() -> bool:
    """Pull current AUTH from the running server. Returns True on success."""
    try:
        r = httpx.get("http://localhost:5000/auth/full", timeout=2)
        r.raise_for_status()
        AUTH.update(r.json())
        return True
    except Exception as e:
        print(f"⚠  Could not fetch live AUTH from server ({e}). Using .env values.")
        return False


# Browser-realistic headers. Whatnot's edge appears to fingerprint clients —
# missing Sec-Fetch-* headers in particular tend to get cleanly closed.
def browser_headers() -> dict:
    # Match exactly what Chrome sends for a WebSocket upgrade — NO Sec-Fetch-*
    # or Sec-Ch-Ua-* headers (browsers omit those on WS upgrades by spec).
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


def build_url(socket_kind: str = "auction") -> str:
    sock = AUTH.get(socket_kind, {}) if isinstance(AUTH.get(socket_kind), dict) else {}
    params = {
        "_csrf_token":           sock.get("csrf_token") or AUTH["csrf_token"],
        "client_layer":          "nextjs",
        "client_type":           "web",
        "client_version":        AUTH["client_version"],
        "live_version":          "v2",
        "sessionExtensionToken": sock.get("session_token") or AUTH["session_token"],
        "vsn":                   "2.0.0",
    }
    base = WS_AUCTION_URL if socket_kind == "auction" else WS_LIVE_URL
    return f"{base}?{urlencode(params)}"


def fetch_capture(topic: str) -> dict | None:
    """Look up a captured phx_join payload for this topic from the running server."""
    try:
        r = httpx.get("http://localhost:5000/capture/joins", timeout=2)
        r.raise_for_status()
        for d in r.json().get("details", []):
            if d.get("topic") == topic:
                return d
    except Exception:
        pass
    return None


async def listen(stream_id: str, duration: int, url: str | None, topic: str | None,
                 payload_json: str | None = None, socket_kind: str = "auction") -> None:
    fetched = fetch_live_auth()
    topic = topic or f"auction:{stream_id}"
    url   = url   or build_url(socket_kind)

    # Auto-pull payload from /capture/joins if not provided
    if not payload_json:
        cap = fetch_capture(topic)
        if cap and cap.get("payload"):
            payload_json = json.dumps(cap["payload"])
            print(f"   (auto-loaded join payload from /capture/joins for topic {topic})")

    sock = AUTH.get(socket_kind, {}) if isinstance(AUTH.get(socket_kind), dict) else {}
    csrf = sock.get("csrf_token") or AUTH["csrf_token"]
    sess = sock.get("session_token") or AUTH["session_token"]

    print(f"\n→ AUTH source: {'live server' if fetched else '.env (server not running)'}")
    print(f"   socket:        {socket_kind}")
    print(f"   cookie:        {len(AUTH['cookie'])} chars")
    print(f"   csrf_token:    {csrf[:16]}…  (len {len(csrf)})")
    print(f"   session_token: {sess[:16]}…")
    print(f"   client_ver:    {AUTH['client_version']}")
    print(f"→ connecting to {url[:120]}…")
    print(f"→ topic: {topic}\n")

    event_counts: dict[str, int] = {}

    try:
        async with websockets.connect(
            url,
            ping_interval=None,
            open_timeout=10,
            close_timeout=5,
            additional_headers=browser_headers(),
        ) as ws:
            print("✓ WS upgraded successfully\n")

            # Wait briefly to see if server sends anything before we speak
            try:
                pre = await asyncio.wait_for(ws.recv(), timeout=1.0)
                print(f"[server-initiated message] {pre[:300]}")
            except asyncio.TimeoutError:
                pass
            except ConnectionClosed as e:
                print(f"✗ Server closed BEFORE we sent anything.")
                print(f"   close code:   {e.code}")
                print(f"   close reason: {e.reason!r}")
                print(f"   → This is edge/WAF/bot-detection rejecting the connection.")
                return

            # phx_join
            join_payload = json.loads(payload_json) if payload_json else {}
            await ws.send(json.dumps(["1", "1", topic, "phx_join", join_payload]))
            print(f"→ sent phx_join for {topic} payload={json.dumps(join_payload)[:120]}\n")

            end = asyncio.get_event_loop().time() + duration
            while asyncio.get_event_loop().time() < end:
                remaining = end - asyncio.get_event_loop().time()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5))
                except asyncio.TimeoutError:
                    continue

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[non-json] {raw[:200]}")
                    continue

                if not isinstance(msg, list) or len(msg) < 5:
                    print(f"[odd shape] {msg}")
                    continue

                _, _, topic_in, event, payload = msg
                event_counts[event] = event_counts.get(event, 0) + 1

                is_giveaway = "giveaway" in event.lower() or (
                    isinstance(payload, dict) and any(
                        "giveaway" in str(k).lower() or "entry" in str(k).lower()
                        for k in payload.keys()
                    )
                )
                tag = "🎁" if is_giveaway else "  "
                payload_short = json.dumps(payload)[:300]
                print(f"{tag} [{topic_in}] {event}  {payload_short}")

    except ConnectionClosed as e:
        print(f"\n✗ connection closed: code={e.code}, reason={e.reason!r}")
    except Exception as e:
        print(f"\n✗ {type(e).__name__}: {e}")

    print("\n— event counts —")
    if not event_counts:
        print("  (none)")
    for ev, n in sorted(event_counts.items(), key=lambda x: -x[1]):
        print(f"  {n:>4}  {ev}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stream_uuid")
    parser.add_argument("seconds", nargs="?", type=int, default=60)
    parser.add_argument("--url",   help="Full WS URL (overrides config-built URL)")
    parser.add_argument("--topic", help="Phoenix channel topic to join (default auction:{uuid})")
    parser.add_argument("--payload", help="JSON string for phx_join payload (default {})")
    parser.add_argument("--payload-file", help="Path to file containing JSON payload")
    parser.add_argument("--socket", choices=["auction", "live"], default="auction",
                        help="Which Whatnot socket to connect to (default auction)")
    args = parser.parse_args()
    payload = args.payload
    if args.payload_file:
        with open(args.payload_file) as f:
            payload = f.read().strip()
    asyncio.run(listen(args.stream_uuid, args.seconds, args.url, args.topic, payload, args.socket))
