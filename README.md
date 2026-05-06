# WhatNotNow

A lightweight tool that monitors Whatnot.com live streams for active giveaways and surfaces them in a local dashboard — so you never miss one.

## How it works

- Fetches live streams by category via Whatnot's internal GraphQL API
- Connects to each stream's auction WebSocket (Phoenix Channels) sequentially
- Detects native `giveaway_entry_count_updated` events — no chat scraping, no keyword guessing
- Displays flagged streams in real time with entry count and a direct join link

## Stack

- **Python** — FastAPI, httpx, websockets
- **Frontend** — Vanilla HTML/CSS/JS, served locally
- **Real-time updates** — Server-Sent Events (SSE)

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Configure credentials**
```bash
cp .env.example .env
```

Open `.env` and fill in three values from your browser's DevTools:

| Variable | Where to find it |
|---|---|
| `WHATNOT_COOKIE` | DevTools → any whatnot.com request → Request Headers → `Cookie` |
| `WHATNOT_CSRF_TOKEN` | DevTools → WS tab → auction socket URL → `_csrf_token` param |
| `WHATNOT_SESSION_TOKEN` | Same WS URL → `sessionExtensionToken` param |

> These tokens expire with your browser session. When the scanner logs a 403, re-copy them and update `.env`.

**3. Add categories**

Open `categories.py` and add the categories you want to scan. To find a category's explore ID:
1. Navigate to `whatnot.com/live` → click a category
2. DevTools → Network → filter `graphql` → find `LiveStreamExplore`
3. Payload → `variables.id` → paste into `categories.py`

**4. Run**
```bash
python main.py
```

Open `http://localhost:5000` in your browser.

## Usage

1. Select which categories to scan using the toggles
2. Hit **Start** — the scanner works through streams one by one
3. Any stream with an active giveaway appears in the feed instantly
4. Click **Join** to open the stream directly in your browser

## Project structure

```
├── main.py          # FastAPI server, SSE, scan orchestrator
├── scanner.py       # Phoenix WebSocket connector, per-stream logic
├── fetcher.py       # GraphQL stream list fetcher
├── categories.py    # Category name → explore feed UUID mapping
├── config.py        # Loads .env config
├── static/
│   └── index.html   # Dashboard UI
├── .env.example     # Credential template
└── CLAUDE.md        # Context file for Claude Code
```

## Notes

- `.env` is gitignored — never commit your credentials
- Scan duration per stream defaults to 15 seconds, configurable via `SCAN_DURATION` in `.env`
- Product name lookup for giveaway items is not yet implemented — coming next
