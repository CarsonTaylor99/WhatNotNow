"""Phase 2 — WebSocket listener for a single Whatnot livestream.

Message schema is reverse-engineered from browser traffic (Chrome DevTools → Network → WS).
Fill in WS_URL_TEMPLATE once the schema is confirmed.
"""

import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

# Placeholder — replace with the real WebSocket endpoint pattern once sniffed.
WS_URL_TEMPLATE = "wss://api.whatnot.com/livestreams/{stream_id}/ws"


class GiveawayEvent:
    START = "giveaway_start"
    ENTRY = "giveaway_entry"
    WIN = "giveaway_win"


def _parse_event(raw: str) -> dict | None:
    """Return a normalised event dict or None if the message is not actionable."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None

    event_type = msg.get("type") or msg.get("event")
    if not event_type:
        return None

    return {"type": event_type, "payload": msg.get("data") or msg.get("payload") or {}}


async def listen(stream_id: str, on_event) -> None:
    """Connect to the WebSocket for *stream_id* and call *on_event(event)* for each message."""
    url = WS_URL_TEMPLATE.format(stream_id=stream_id)
    logger.info("Connecting to %s", url)

    async with websockets.connect(url) as ws:
        async for raw in ws:
            event = _parse_event(raw)
            if event:
                await on_event(event)
