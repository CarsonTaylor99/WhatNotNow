# WhatNotNow

A local dashboard that watches Whatnot.com livestreams in real time and surfaces every active giveaway as it appears — no chat scraping, no polling, no manual stream-hopping. Click a category, hit **Start**, and giveaways pop in as they fire.

Built as a personal tool, then hardened: autonomous auth-token refresh via a bundled Chrome extension, two-tier WebSocket scanner, automatic giveaway-end detection, one-click Windows installer.

## Features

- **Real-time giveaway feed** — listens to Whatnot's native `giveaway_entry_count_updated` Phoenix WebSocket events. Cards appear the moment a giveaway opens and update live as the entry count climbs.
- **Auto-discovery of categories** — the bundled Chrome extension captures category UUIDs from real GraphQL traffic as you browse `whatnot.com/live`, so the sidebar grows itself.
- **Self-refreshing auth** — every 4 minutes the extension forces Whatnot's WebSocket to reconnect, harvests the freshly-minted CSRF + session tokens, and pushes them to the local server. No DevTools, no manual token paste, runs indefinitely as long as one Whatnot tab is open.
- **Automatic end detection** — watcher infers giveaway end via three signals: explicit end event, 90 seconds of entry-update silence, or the stream going offline. Cards drop themselves.
- **Per-card metadata** — item title, audience restriction (e.g. `qualified_buyer`), elapsed time, and a one-click join link straight to the stream.
- **Optional email notifications** — per-card "Send to phone" button using SMTP (Gmail app password).
- **One-click Windows install** — `install.bat` installs Python if missing (via winget), downloads the project, builds the venv, and installs dependencies. No WSL, no manual steps. Updates by re-running.

## Tech Stack

| Layer | Tech |
|---|---|
| Language | Python 3.10+ |
| Web server | FastAPI + uvicorn |
| Async runtime | asyncio (everything I/O-bound runs concurrently) |
| HTTP client | httpx (async) |
| WebSocket client | `websockets` — Phoenix Channels v2.0.0 wire protocol |
| Real-time UI updates | Server-Sent Events (SSE) |
| Frontend | Vanilla HTML / CSS / JS — no framework, no build step |
| Browser integration | Chrome MV3 extension (service worker + content script + main-world inject) |
| Config | python-dotenv |
| Notifications (optional) | SMTP via stdlib `smtplib` |
| Packaging | Windows `.bat` installer (zero-dependency bootstrap) |

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

Whatnot's WebSocket auth uses three things: the session **cookie**, a per-socket `_csrf_token`, and a per-socket `sessionExtensionToken`. The CSRF + session tokens are stamped into the WebSocket URL by Whatnot's own JS when the page opens a socket — and they expire in roughly 5 minutes. Rather than asking you to copy them out of DevTools every few minutes, the extension does this:

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

## Engineering highlights

The harder problems and how they're solved — Whatnot's API is undocumented and session-protected, so most of the work was reverse engineering and building durable workarounds.

- **Phoenix Channels reverse-engineering.** Whatnot uses Elixir's Phoenix Channels over WebSocket with the v2.0.0 wire format (`[join_ref, ref, topic, event, payload]` arrays). Two distinct sockets, with the giveaway events firing on the *live* socket under a topic prefixed `auction:` — figuring this out required diffing real network traffic against Phoenix source.
- **Token refresh without screen-scraping.** Per-socket `_csrf_token` and `sessionExtensionToken` rotate every ~5 minutes. The bundled MV3 extension solves this by patching `WebSocket` and `fetch` in the page's *main world* (not the isolated content-script world), capturing the URL Whatnot's own JS generates with the fresh tokens, and forwarding it to the server. A 4-minute `chrome.alarms` cycle forces a reconnect to keep the rotation moving.
- **Two-tier scan architecture.** Discovery scanners run N short-lived WebSocket scans bounded by `asyncio.Semaphore` to find streams with active giveaways. Hits hand off to long-lived watchers that hold a single connection per giveaway and stream updates to the dashboard over SSE. Cleanly separates "find" from "follow."
- **Robust end-of-giveaway detection.** Phoenix occasionally prunes idle connections, so a closed socket isn't proof a giveaway ended. The watcher distinguishes three end signals (explicit `giveaway_ended` event, 90s of entry-update silence, stream going offline) and auto-reconnects through transient closes.
- **Browser → server bridge.** The extension service worker, content script, and page-injected script communicate through `window.postMessage` and `chrome.runtime.sendMessage` channels and POST to `localhost:5000` — three different security contexts wired together with no user interaction required.

## Install

### Windows — one-click

1. Download `install.bat`:
   https://raw.githubusercontent.com/CarsonTaylor99/WhatNotNow/master/install.bat
   (right-click the page → Save As → `install.bat`)
2. Double-click `install.bat`. It prompts for an install folder, **installs Python for you if it isn't already there** (via `winget` — falls back to a download link otherwise), downloads the project from GitHub, creates a Python virtualenv, and installs all dependencies. At the end it offers to launch the scanner.
3. Optional: drop a `.env` from a previous install into the new folder.
4. Load the bundled extension: open your browser's extensions page (`chrome://extensions`, `edge://extensions`, or `brave://extensions`) → toggle **Developer mode** on → **Load unpacked** → select the `extension\` folder inside the install folder.
5. Double-click `start.bat` (or let the installer do it). The dashboard opens automatically at http://localhost:5000.

No WSL or Linux involved — everything runs on native Windows Python. To update later, re-run `install.bat`; it pulls the latest version from GitHub and leaves your `.env` in place.

> Already have the repo cloned? Skip `install.bat` and just run `setup.bat` (builds the venv) then `start.bat`.

### Manual (Mac / Linux / anyone who wants control)

```bash
git clone https://github.com/CarsonTaylor99/WhatNotNow.git
cd WhatNotNow
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Then load `extension/` in Chrome as an unpacked extension (see step 5 above).

### Optional: seed `.env`

You don't *need* `.env` to start — the extension feeds tokens to the server as soon as you open a Whatnot stream. But if you want an initial fallback (e.g., to scan immediately after launch without first triggering the extension), copy `env.example` to `.env` and fill in:

| Variable | Where to find it |
|---|---|
| `WHATNOT_COOKIE` | DevTools → Network → any whatnot.com request → Request Headers → `Cookie` |
| `WHATNOT_CSRF_TOKEN` | DevTools → Network → WS filter → auction socket URL → `_csrf_token` query param |
| `WHATNOT_SESSION_TOKEN` | Same URL → `sessionExtensionToken` query param |

These are hot-swapped by the extension on its next push, so they only matter for the first ~5 minutes after launch.

### Optional: email notifications

To enable the "Send to phone" button on each giveaway card, add Gmail SMTP credentials to `.env`:

```
SMTP_USER=you@gmail.com
SMTP_APP_PASSWORD=xxxxxxxxxxxxxxxx   # 16-char Gmail app password
RECIPIENT_EMAIL=you@gmail.com         # or your phone's MMS gateway
```

Generate the app password at https://myaccount.google.com/apppasswords (requires 2FA enabled). Leave blank to disable the button.

## Usage

1. Open one Whatnot livestream in Chrome and leave it open. (This is what keeps tokens fresh.)
2. Open the dashboard at http://localhost:5000.
3. The sidebar shows category toggles. To add more, just navigate around `whatnot.com/live` — the extension auto-discovers any category you click into. Discovered categories persist to `discovered_categories.json` so they survive restarts.
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
├── extension/               # Chrome MV3 extension (load as unpacked)
│   ├── manifest.json
│   ├── inject.js            #   patches WebSocket+fetch in page main world
│   ├── content.js           #   bridges page ↔ service worker
│   ├── background.js        #   service worker — token push, alarms, force-reconnect
│   └── popup.html / popup.js
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
