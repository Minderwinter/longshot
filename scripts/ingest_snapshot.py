"""CLI: ingest a single snapshot from Kalshi into S3.

Usage:
    uv run python scripts/ingest_snapshot.py --snapshot-ts 1735768800
"""

from __future__ import annotations

import argparse
import logging

from longshot.ingestion.snapshot import run_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest a Kalshi markets+trades snapshot into S3."
    )
    parser.add_argument(
        "--snapshot-ts",
        type=int,
        required=True,
        help="Unix timestamp for the snapshot (e.g. 1735768800 = 2025-01-01T20:00:00Z)",
    )
    parser.add_argument(
        "--skip-trades",
        action="store_true",
        help="Skip trades ingestion (markets only)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    summary = run_snapshot(args.snapshot_ts, skip_trades=args.skip_trades)

    print("\n=== Snapshot Ingestion Complete ===")
    print(f"  Snapshot TS       : {summary['snapshot_ts']}")
    print(f"  All markets       : {summary['all_market_count']}")
    print(f"  Snapshot markets  : {summary['snapshot_market_count']}")
    print(f"  Trades            : {summary['trade_count']}")
    print(f"  All markets path  : {summary['all_markets_path']}")
    print(f"  Snapshot path     : {summary['snapshot_markets_path']}")
    print(f"  Trades path       : {summary['trades_path']}")


if __name__ == "__main__":
    main()
