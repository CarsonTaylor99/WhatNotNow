"""Phase 3 — live-updating Rich terminal dashboard."""

import asyncio
from datetime import datetime

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.table import Table

from .streams import fetch_streams

console = Console()


def _build_streams_table(streams: list[dict]) -> Table:
    table = Table(title="Active Whatnot Streams", expand=True, highlight=True)
    table.add_column("Seller", style="bold cyan", no_wrap=True)
    table.add_column("Title", ratio=3)
    table.add_column("Category", style="magenta")
    table.add_column("Viewers", justify="right", style="green")
    table.add_column("URL", style="blue")

    for s in sorted(streams, key=lambda x: x["viewers"], reverse=True):
        table.add_row(
            s["seller"],
            s["title"],
            s["category"],
            str(s["viewers"]),
            s["url"],
        )
    return table


async def run(refresh_interval: int = 30) -> None:
    """Poll for streams every *refresh_interval* seconds and redraw the table."""
    streams: list[dict] = []
    last_updated = "never"

    layout = Layout()
    layout.split_column(Layout(name="table"), Layout(name="footer", size=1))

    with Live(layout, console=console, screen=True, refresh_per_second=4) as live:
        while True:
            streams = fetch_streams()
            last_updated = datetime.now().strftime("%H:%M:%S")

            layout["table"].update(_build_streams_table(streams))
            layout["footer"].update(
                f"[dim]Last updated {last_updated} — {len(streams)} streams — refreshing every {refresh_interval}s[/dim]"
            )
            live.refresh()

            await asyncio.sleep(refresh_interval)
