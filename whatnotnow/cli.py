"""Entry point — `whatnotnow` or `python -m whatnotnow`."""

import argparse
import asyncio

from .dashboard import run as run_dashboard
from .streams import fetch_streams


def main() -> None:
    parser = argparse.ArgumentParser(description="WhatNotNow — Whatnot stream tracker")
    sub = parser.add_subparsers(dest="cmd")

    dash = sub.add_parser("dash", help="Launch live dashboard")
    dash.add_argument("--interval", type=int, default=30, help="Refresh interval in seconds")

    sub.add_parser("list", help="Print active streams and exit")

    args = parser.parse_args()

    if args.cmd == "dash":
        asyncio.run(run_dashboard(refresh_interval=args.interval))
    elif args.cmd == "list":
        streams = fetch_streams()
        for s in streams:
            print(f"{s['viewers']:>6}  {s['seller']:<20}  {s['title']}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
