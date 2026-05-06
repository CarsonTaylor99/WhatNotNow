import os
from dotenv import load_dotenv

load_dotenv()

COOKIE         = os.getenv("WHATNOT_COOKIE", "")
CSRF_TOKEN     = os.getenv("WHATNOT_CSRF_TOKEN", "")
SESSION_TOKEN  = os.getenv("WHATNOT_SESSION_TOKEN", "")
SCAN_DURATION  = int(os.getenv("SCAN_DURATION", "15"))

GRAPHQL_URL    = "https://www.whatnot.com/services/graphql/"
WS_BASE        = "wss://www.whatnot.com/services/auction/socket/websocket"
CLIENT_VERSION = "20260506-0505"
