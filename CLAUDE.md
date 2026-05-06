# Whatnot Giveaway Scanner — Claude Code Brief

## What this project is
A lightweight Python tool that monitors Whatnot.com live streams for active giveaways and surfaces them in a local web dashboard. The user selects categories, the scanner iterates through live streams one at a time, connects to each stream's WebSocket, and flags any stream where Whatnot fires a native `giveaway_entry_count_updated` event.

No Playwright. No browser automation. Pure HTTP + WebSocket.

---

## Stack
| Layer | Tech |
|---|---|
| Backend / server | FastAPI + uvicorn |
| HTTP requests | httpx (async) |
| WebSocket | websockets library (Phoenix Channels protocol) |
| Real-time UI updates | Server-Sent Events (SSE) |
| Frontend | Vanilla HTML/CSS/JS served from `/static` |
| Config | python-dotenv via `.env` file |

---

## File map
```
whatnot-scanner/
├── main.py          # FastAPI server, SSE, scan orchestrator, route handlers
├── scanner.py       # Phoenix WebSocket channel connector, per-stream scan logic
├── fetcher.py       # GraphQL stream list fetcher (LiveStreamExplore query)
├── categories.py    # Category name → Whatnot explore feed UUID mapping
├── config.py        # Loads .env vars (cookie, tokens, scan duration, endpoints)
├── requirements.txt
├── .env.example     # Template — user copies to .env and fills in credentials
└── static/
    └── index.html   # Dashboard UI (category toggles, live giveaway feed, stats)
```

---

## How the scanner works

### 1. Stream discovery (fetcher.py)
Calls Whatnot's internal GraphQL endpoint:
- **URL:** `https://www.whatnot.com/services/graphql/`
- **Operation:** `LiveStreamExplore`
- **Variable:** `id` — a category-level explore feed UUID (see `categories.py`)
- Returns up to 24 live streams per fetch with: `id`, `title`, `username`, `activeViewers`, `thumbnail`, `livestreamCategories`
- Auth: user's browser cookie passed in `Cookie` header

### 2. Per-stream WebSocket scan (scanner.py)
For each stream, connects to Whatnot's auction Phoenix Channel:
- **URL:** `wss://www.whatnot.com/services/auction/socket/websocket`
- **Query params required:** `_csrf_token`, `sessionExtensionToken`, `client_version`, `vsn=2.0.0`
- **Phoenix protocol:** messages are JSON arrays `[join_ref, ref, topic, event, payload]`
- **Joins topic:** `auction:{stream_uuid}`
- **Listens for:** `giveaway_entry_count_updated` → payload: `{entryCount: int, productId: string}`
- Sends heartbeat every 30s (`phoenix` / `heartbeat`)
- Moves to next stream after `SCAN_DURATION` seconds (default 15, configurable in `.env`)

### 3. Orchestration (main.py → run_scanner)
- Iterates selected categories → fetches stream list → scans each stream sequentially
- Calls `on_giveaway(stream, payload)` when event fires
- Broadcasts SSE events to all connected dashboard clients
- Loops continuously until stopped

### 4. Dashboard (static/index.html)
- Connects to `/events` (SSE stream)
- SSE event types: `init`, `status`, `scanning`, `giveaway`, `update`, `error`
- Category toggles → POST `/start` with `{categories: [...]}` → POST `/stop`
- Giveaway cards show: stream title, seller username, entry count, thumbnail, Join button
- Entry count updates live as `giveaway_entry_count_updated` fires repeatedly

---

## Auth — important
Three credentials required in `.env`:

| Variable | Where to get it |
|---|---|
| `WHATNOT_COOKIE` | DevTools → any whatnot.com request → Request Headers → `Cookie` |
| `WHATNOT_CSRF_TOKEN` | DevTools → WS tab → auction socket URL → `_csrf_token` param |
| `WHATNOT_SESSION_TOKEN` | Same WS URL → `sessionExtensionToken` param |

**These expire with the user's browser session.** When the scanner logs a 403 on WebSocket connect, the user needs to re-copy tokens from DevTools and update `.env`. This is a known limitation — token auto-refresh is a planned improvement.

---

## Categories
`categories.py` maps display names to Whatnot explore feed UUIDs. Only one category is seeded (Crystals & Gems). To add more:
1. Navigate to `whatnot.com/live` → click a category
2. DevTools → Network → filter `graphql` → find `LiveStreamExplore`
3. Payload → `variables.id` → paste into `categories.py`

---

## Known limitations / planned work
- **Token expiry:** No auto-refresh. User must manually re-paste tokens from DevTools.
- **Single page of streams:** `LiveStreamExplore` returns ~24 streams. Pagination not yet implemented.
- **Product lookup:** `giveaway_entry_count_updated` returns a `productId` but we don't yet fetch the product name/image. This is the next feature to add — needs a `GetListing` or `GetProduct` GraphQL query.
- **Category IDs:** Only Crystals & Gems is seeded. Others need to be discovered manually.
- **No persistence:** Giveaway history is in-memory only, lost on server restart.

---

## Running locally
```bash
pip install -r requirements.txt
cp .env.example .env
# fill in .env with your credentials
python main.py
# open http://localhost:5000
```

---

## Conventions
- All async — use `asyncio` and `await` throughout, never blocking calls
- `broadcast(event, data)` is the single function for pushing SSE to all clients
- `on_giveaway(stream, payload)` is the callback interface between scanner and orchestrator — keep it clean, scanner should not know about SSE
- Error handling: catch exceptions per-stream in `scanner.py`, log and continue — one bad stream should never crash the loop
- Do not hardcode credentials anywhere; always read from `config.py` which reads from `.env`
