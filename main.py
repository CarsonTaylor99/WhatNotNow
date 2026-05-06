import asyncio
import json
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fetcher import get_streams
from scanner import scan_stream
from categories import CATEGORIES

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Shared state ──────────────────────────────────────────────────────────────
state = {
    "scanning":           False,
    "current_stream":     None,
    "giveaways":          [],   # detected giveaway entries
    "streams_scanned":    0,
    "total_streams":      0,
    "selected_categories": [],
    "error":              None,
}

_sse_clients: list[asyncio.Queue] = []
_stop_event   = asyncio.Event()


# ── SSE broadcast ─────────────────────────────────────────────────────────────
async def broadcast(event: str, data: dict):
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    for q in _sse_clients:
        await q.put(msg)


# ── Giveaway callback (called by scanner) ─────────────────────────────────────
async def on_giveaway(stream: dict, payload: dict):
    stream_id = stream["id"]

    # Update entry count if already flagged
    existing = next((g for g in state["giveaways"] if g["stream_id"] == stream_id), None)
    if existing:
        existing["entry_count"] = payload.get("entryCount", existing["entry_count"])
        await broadcast("update", existing)
        return

    entry = {
        "stream_id":   stream_id,
        "title":       stream.get("title", ""),
        "username":    stream.get("username", ""),
        "viewers":     stream.get("viewers", 0),
        "category":    stream.get("category", ""),
        "thumbnail":   stream.get("thumbnail", ""),
        "entry_count": payload.get("entryCount", 0),
        "product_id":  payload.get("productId", ""),
        "url":         stream.get("url", f"https://www.whatnot.com/live/{stream_id}"),
    }
    state["giveaways"].append(entry)
    await broadcast("giveaway", entry)


# ── Main scanner loop ─────────────────────────────────────────────────────────
async def run_scanner():
    _stop_event.clear()
    state["streams_scanned"] = 0
    state["giveaways"]       = []
    state["error"]           = None

    try:
        while not _stop_event.is_set():
            for cat_name, explore_id in CATEGORIES.items():
                if cat_name not in state["selected_categories"]:
                    continue
                if _stop_event.is_set():
                    break

                await broadcast("status", {"message": f"Fetching streams: {cat_name}…"})
                result  = await get_streams(explore_id)
                streams = result["streams"]
                state["total_streams"] = result["totalCount"]

                if not streams:
                    await broadcast("status", {"message": f"No live streams found in {cat_name}"})
                    continue

                await broadcast("status", {
                    "message": f"Scanning {len(streams)} streams in {cat_name}…"
                })

                for stream in streams:
                    if _stop_event.is_set():
                        break

                    state["current_stream"] = stream
                    state["streams_scanned"] += 1

                    await broadcast("scanning", {
                        "title":    stream["title"],
                        "username": stream["username"],
                        "viewers":  stream["viewers"],
                        "scanned":  state["streams_scanned"],
                        "total":    len(streams),
                    })

                    await scan_stream(stream, on_giveaway)

            if not _stop_event.is_set():
                # Brief pause between full cycles
                await broadcast("status", {"message": "Cycle complete — restarting…"})
                await asyncio.sleep(5)

    except Exception as e:
        state["error"] = str(e)
        await broadcast("error", {"message": str(e)})
    finally:
        state["scanning"]       = False
        state["current_stream"] = None
        await broadcast("status", {"message": "Scanner stopped."})


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def index():
    with open("static/index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/state")
async def get_state():
    return state


@app.get("/categories")
async def get_categories():
    return list(CATEGORIES.keys())


@app.get("/events")
async def sse(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    _sse_clients.append(queue)

    async def generator():
        # Send current state immediately on connect
        yield f"event: init\ndata: {json.dumps(state)}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=20)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"   # keep-alive
        finally:
            _sse_clients.remove(queue)

    return StreamingResponse(generator(), media_type="text/event-stream")


@app.post("/start")
async def start_scan(request: Request):
    body = await request.json()
    if state["scanning"]:
        return {"error": "Already scanning"}

    state["selected_categories"] = body.get("categories", list(CATEGORIES.keys()))
    state["scanning"] = True
    asyncio.create_task(run_scanner())
    return {"ok": True}


@app.post("/stop")
async def stop_scan():
    _stop_event.set()
    state["scanning"] = False
    return {"ok": True}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000, reload=False)
