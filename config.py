import os
from dotenv import load_dotenv

load_dotenv()

# Mutable auth state — fetcher and scanner read from this every request,
# so /auth/refresh can hot-swap tokens without a restart.
AUTH = {
    "cookie":         os.getenv("WHATNOT_COOKIE", ""),
    "client_version": os.getenv("WHATNOT_CLIENT_VERSION", "20260506-0505"),
    # Per-socket csrf+session tokens (each socket gets its own pair from Whatnot)
    "auction": {
        "csrf_token":    os.getenv("WHATNOT_CSRF_TOKEN", ""),
        "session_token": os.getenv("WHATNOT_SESSION_TOKEN", ""),
    },
    "live": {
        "csrf_token":    "",
        "session_token": "",
    },
    # Per-user session ID required in phx_join payloads. Extracted from any
    # captured phx_join — same value works across all streams for this user.
    "livestream_session_id": "",
    # Back-compat top-level mirrors the auction socket so old code paths keep working
    "csrf_token":    os.getenv("WHATNOT_CSRF_TOKEN", ""),
    "session_token": os.getenv("WHATNOT_SESSION_TOKEN", ""),
}

def update_auth(
    cookie: str,
    csrf_token: str,
    session_token: str,
    client_version: str = "",
    socket_kind: str = "auction",
) -> None:
    AUTH["cookie"] = cookie
    if client_version:
        AUTH["client_version"] = client_version
    if socket_kind in ("auction", "live"):
        AUTH[socket_kind]["csrf_token"]    = csrf_token
        AUTH[socket_kind]["session_token"] = session_token
    # Mirror auction tokens at top level for back-compat
    if socket_kind == "auction":
        AUTH["csrf_token"]    = csrf_token
        AUTH["session_token"] = session_token

SCAN_DURATION  = int(os.getenv("SCAN_DURATION", "15"))

GRAPHQL_URL    = "https://www.whatnot.com/services/graphql/"
WS_AUCTION_URL = "wss://www.whatnot.com/services/auction/socket/websocket"
WS_LIVE_URL    = "wss://www.whatnot.com/services/live/socket/websocket"
WS_BASE        = WS_AUCTION_URL  # back-compat
