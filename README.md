# WhatNotNow

Real-time Whatnot livestream tracker and giveaway monitor.

## Features

- **Stream browser** — fetches all active Whatnot livestreams via the GraphQL API with full pagination
- **WebSocket listener** — connects to a stream's real-time feed to capture giveaway events (start / entry / win)
- **Live dashboard** — Rich terminal UI that auto-refreshes the stream list

## Install

```bash
pip install -e ".[dev]"
```

## Usage

```bash
# Print all active streams to stdout
whatnotnow list

# Launch the live dashboard (default 30s refresh)
whatnotnow dash
whatnotnow dash --interval 10
```

## Project layout

```
whatnotnow/
  streams.py    # Phase 1 — GraphQL stream fetcher
  listener.py   # Phase 2 — WebSocket event listener
  dashboard.py  # Phase 3 — Rich terminal dashboard
  cli.py        # CLI entry point
```

## Roadmap

- [ ] Confirm GraphQL query shape against live API
- [ ] Sniff WebSocket message schema via Chrome DevTools
- [ ] Wire giveaway events into the dashboard as a live sidebar
- [ ] Optional: export stream/giveaway data to a spreadsheet
