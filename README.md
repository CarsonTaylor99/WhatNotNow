# WhatNotNow

A local dashboard that watches Whatnot.com livestreams in real time and surfaces every active giveaway as it appears — no chat scraping, no polling, no manual stream-hopping. Click a category, hit Start, and giveaways pop in as they fire.

## What it does

- **Discovers live streams** in the categories you've selected via Whatnot's internal GraphQL endpoint.
- **Listens for `giveaway_entry_count_updated` events** on each stream's Phoenix WebSocket channel — Whatnot's own native signal for "someone just entered the giveaway."
- **Holds a connection per detected giveaway** so it can track entry-count updates live and detect when the giveaway ends (no entry events for 90s, stream goes offline, etc.). Cards disappear automatically when giveaways end.
- **Auto-discovers categories** as you browse `whatnot.com/live` — the Chrome extension captures category UUIDs from real GraphQL traffic; no DevTools digging needed.
- **Auto-refreshes auth tokens** every 4 minutes by triggering Whatnot's own JS to reconnect its WebSocket. Fully autonomous — just leave one Whatnot tab open.

## How it works

```
Chrome (whatnot.com tab)            your machine
─────────────────────────────────   ──────────────────────────────
 Whatnot site JS                     FastAPI server (main.py)
   │                                   │
   │ Phoenix WebSocket                 │ ┌─ discovery scanner ────┐
   │  + GraphQL fetches                │ │  short WS scan to find │
   │                                   │ │  giveaways per stream  │
 Chrome extension (extension/)         │ └────────────────────────┘
   ├── inject.js                       │ ┌─ persistent watchers ──┐
   │   patches WebSocket + fetch in    │ │  one long WS per       │
   │   the page's main world           │ │  active giveaway       │
   ├── content.js                      │ └────────────────────────┘
   │   bridges page ↔ service worker   │
   └── background.js (service worker)  │ Dashboard
       - pushes captured tokens to     │   served at localhost:5000
         /auth/refresh                 │   live updates via SSE
       - pushes captured categories    ▲
         to /categories/discovered     │
       - on a 4-min alarm: tells the   │ POSTs to local server
         page to drop+reconnect its WS │ (cookie + per-socket csrf
         so Whatnot mints fresh tokens │  + sessionExtensionToken)
```

### The Chrome extension

Whatnot's WebSocket auth uses three things: the session **cookie**, a per-socket `_csrf_token`, and a per-socket `sessionExtensionToken`. The CSRF + session tokens are stamped into the WebSocket URL by Whatnot's own JS when the page opens a socket — and they expire fast (~5 min). Rather than asking you to copy them out of DevTools every few minutes, the extension does this:

1. **Patches `WebSocket`** in the page's main world. When Whatnot's JS opens a socket, the extension records the URL (which contains both tokens) and intercepts the outgoing `phx_join` payload (which contains the `livestreamSessionId` needed for joins).
2. **Patches `fetch`** the same way — captures `LiveStreamExplore` GraphQL responses to discover category UUIDs from real traffic.
3. **Pushes captured data** to the local server's API: `/auth/refresh` for tokens, `/categories/discovered` for categories, `/capture/join` for the session ID.
4. **Refreshes autonomously**: every 4 minutes, the service worker tells the open Whatnot tab to drop its WebSocket. Whatnot's client auto-reconnects with a fresh URL → fresh tokens → captured by the patch → pushed to the server. No clicking required.

Closing the Whatnot tab stops auto-refresh. Tokens then drift stale within ~5 minutes; reopen a stream to recover.

### The WebSocket layer

Whatnot's chat / giveaway / bid events run over **Phoenix Channels** (Elixir's WebSocket framework). There are two sockets in play: an *auction socket* (`/services/auction/socket/websocket`) for bid/sale events, and a *live socket* (`/services/live/socket/websocket`) for everything else. Giveaway entry events fire on the **live socket** under the topic `auction:{stream_uuid}` — confusingly named but accurate.

The scanner has two roles:

- **Discovery scanner** opens a quick WebSocket per stream, joins the topic, listens up to `SCAN_DURATION` seconds. If a `giveaway_entry_count_updated` event fires, it hands off to a watcher and moves on. Runs 4 streams in parallel via `asyncio.Semaphore`.
- **Watcher** keeps a long-lived WebSocket per active giveaway. Forwards every entry-count update, parses metadata (item title from `activeGiveaway.product`, end time from `activeGiveaway.endsAt`, audience from `activeGiveaway.audience` / `requireQualifiedBuyer`), and detects end via: explicit end event, 90s with no entry updates ("idle timeout"), or stream going offline. Auto-reconnects through transient WebSocket closes (Whatnot prunes idle connections often).

## Install

### Windows — one-click

1. **Install Python 3.10+** from https://www.python.org/downloads/ — make sure to check **"Add Python to PATH"** during install.
2. Download `install.bat` from the repo:
   https://raw.githubusercontent.com/CarsonTaylor99/WhatNotNow/master/install.bat
   (right-click the page → Save As → `install.bat`)
3. Double-click `install.bat`. It asks for an install folder, downloads the project from GitHub, creates a Python virtualenv, and installs all dependencies.
4. Optional: drop your `.env` from a previous install into the new folder.
5. In Chrome → `chrome://extensions` → toggle **Developer mode** on → **Load unpacked** → select the `extension\` folder inside the install folder.
6. Double-click `start.bat`. Dashboard opens at http://localhost:5000.

To update later, just re-run `install.bat` — it pulls the latest version from GitHub.

### Manual install (Mac, Linux, anyone who wants control)

```bash
git clone https://github.com/CarsonTaylor99/WhatNotNow.git
cd WhatNotNow
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Then load `extension/` in Chrome as an unpacked extension.

### Optional: seed `.env`

You don't *need* `.env` to start — the extension feeds tokens to the server as soon as you open a Whatnot stream. But if you want an initial fallback (e.g., to scan immediately after `start.bat` without first triggering the extension), copy `env.example` to `.env` and fill in:

| Variable | Where to find it |
|---|---|
| `WHATNOT_COOKIE` | DevTools → Network → any whatnot.com request → Request Headers → `Cookie` |
| `WHATNOT_CSRF_TOKEN` | DevTools → Network → WS filter → auction socket URL → `_csrf_token` query param |
| `WHATNOT_SESSION_TOKEN` | Same URL → `sessionExtensionToken` query param |

These will be hot-swapped by the extension on its next push, so they only matter for the first ~5 minutes after launch.

## Usage

1. Open one Whatnot livestream in Chrome and leave it open. (This is what keeps tokens fresh.)
2. Open the dashboard at http://localhost:5000.
3. The sidebar shows category toggles. To add more, just navigate around `whatnot.com/live` — the extension auto-discovers any category you click into. Discovered categories are persisted to `discovered_categories.json` so they survive restarts.
4. Hit **Start**. The status bar shows progress; the activity log tracks each stream's outcome. When a giveaway is detected, a card appears with:
   - Entry count (live)
   - Elapsed time since detection
   - Item description (when Whatnot exposes it)
   - Audience restriction (if any — e.g., "qualified_buyer")
   - **Join ↗** link straight to the stream
5. Cards disappear automatically when a giveaway ends.

The sidebar also shows the auth panel (cookie / auction tok / live tok / session id) — green when present, orange when missing — so you can spot at a glance whether the extension is feeding tokens.

## Project structure

```
.
├── main.py                  # FastAPI server, SSE, scan orchestrator
├── scanner.py               # Phoenix WebSocket: discovery + persistent watcher
├── fetcher.py               # GraphQL stream-list fetcher (LiveStreamExplore)
├── categories.py            # Seed list of (label → category UUID)
├── config.py                # .env loader + mutable AUTH state
├── static/index.html        # Dashboard UI (vanilla JS)
├── extension/               # Chrome extension (load as unpacked)
│   ├── manifest.json
│   ├── inject.js            #   patches WebSocket+fetch in page main world
│   ├── content.js           #   bridges page ↔ service worker
│   ├── background.js        #   service worker — token push, alarms, force-reconnect
│   ├── popup.html / popup.js
├── install.bat              # One-click installer (Windows)
├── setup.bat                # Creates venv + installs deps
├── start.bat                # Launches the server
├── requirements.txt
└── env.example              # Template for .env
```

## Notes & limitations

- **Personal-use tool.** Whatnot's API and WebSocket protocol are undocumented and can change without warning. Each giveaway card has a small **debug** toggle that exposes unmapped payload fields and seen channel events — useful for spotting protocol changes and updating the parser.
- **One Chrome profile** at a time. The extension reads cookies from whatever Chrome session it's loaded in.
- **Closing your Whatnot tab** stops auto-token-refresh. Tokens go stale within ~5 min; reopening any stream re-seeds them.
- **Scan duration** per stream defaults to 15 seconds; tunable via `SCAN_DURATION` in `.env`.
- **Discovered categories persist** to `discovered_categories.json` (gitignored). Delete that file to reset to just the seed list.
- The server is **localhost only** (binds to `127.0.0.1:5000`). Don't expose it to the internet — it has no auth and exposes raw cookies via `/auth/full` for debugging.
