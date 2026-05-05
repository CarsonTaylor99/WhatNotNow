# WhatNotNow — Claude context

## Project
Python tool that scrapes Whatnot livestream data and displays it in a real-time terminal dashboard.

## Phases
| Phase | Module | Status |
|-------|--------|--------|
| 1 — Stream Browser | `whatnotnow/streams.py` | scaffold done |
| 2 — WebSocket Listener | `whatnotnow/listener.py` | scaffold done; WS URL/schema TBD |
| 3 — Live Dashboard | `whatnotnow/dashboard.py` | scaffold done |

## Install
```
pip install -e ".[dev]"
```

## Run
```
whatnotnow list          # print active streams
whatnotnow dash          # live Rich dashboard (refreshes every 30s)
whatnotnow dash --interval 10
```

## Key decisions
- GraphQL endpoint: `https://api.whatnot.com/graphql` — internal, undocumented, may break.
- WebSocket endpoint template in `listener.py` is a placeholder until schema is sniffed via Chrome DevTools Network → WS tab.
- UI layer: Rich terminal dashboard (phase 3 is open to revision).

## Stack
- `requests` — GraphQL HTTP calls
- `websockets` — async WebSocket listener
- `rich` — terminal UI
