#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests"
# ]
# ///
"""
update_orderbook.py

Small utility that refreshes order book data for a single market and writes the
result into the SQLite database (and optionally into a JSON snapshot). The
script leans on the datastore helpers so it behaves exactly like the existing
order book ingestion flow while keeping the code easy to understand.
"""

import argparse
import json
from pathlib import Path
from typing import Iterable, List

import requests

from datastore import get_market, save_orderbook_to_db

CLOB_URL = "https://clob.polymarket.com"


def parse_args() -> argparse.Namespace:
    """
    Build the CLI interface and parse user-provided arguments.

    We keep the options minimal: the market ID is required, while the token,
    depth, and output path remain optional so this tool stays flexible.
    """
    parser = argparse.ArgumentParser(
        description="Fetch and store order book data for a specific market."
    )
    parser.add_argument(
        "--market-id",
        "-m",
        required=True,
        help="Market identifier stored in the local markets table.",
    )
    parser.add_argument(
        "--token-id",
        "-t",
        help=(
            "Specific token id to refresh. If omitted, the script updates every "
            "token attached to the market."
        ),
    )
    parser.add_argument(
        "--depth",
        "-d",
        type=int,
        default=50,
        help="Depth parameter forwarded to the CLOB API (defaults to 50).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help=(
            "Optional path to write a JSON snapshot for each refreshed token. "
            "Files are named <token_id>.json inside this directory."
        ),
    )
    return parser.parse_args()


def extract_token_ids(market_row: dict) -> List[str]:
    """
    Decode the stored clobTokenIds column and return a clean list of token ids.

    The column stores JSON text, so we handle empty strings gracefully and give
    the caller a descriptive error if parsing fails.
    """
    raw_value = market_row.get("clobTokenIds")
    if not raw_value:
        raise ValueError("Market does not expose any clobTokenIds.")

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError("Could not parse clobTokenIds JSON.") from error

    if not isinstance(parsed, Iterable):
        raise ValueError("clobTokenIds JSON is not iterable.")

    cleaned = [token for token in parsed if token]
    if not cleaned:
        raise ValueError("No usable token ids found in clobTokenIds.")
    return cleaned


def fetch_orderbook(token_id: str, depth: int) -> dict:
    """
    Request the order book from the CLOB API.

    We return the decoded JSON payload so the caller can decide how to persist
    it. Any HTTP or decoding issues bubble up as a RuntimeError with context.
    """
    url = f"{CLOB_URL}/book"
    params = {"token_id": token_id, "depth": depth}

    try:
        response = requests.get(url, params=params, timeout=10)
    except requests.RequestException as error:
        raise RuntimeError(f"Network error while fetching {token_id}") from error

    if response.status_code != 200:
        raise RuntimeError(
            f"CLOB API returned {response.status_code} for token {token_id}"
        )

    try:
        payload = response.json()
    except ValueError as error:
        raise RuntimeError(f"Invalid JSON returned for token {token_id}") from error

    return payload


def write_snapshot(directory: Path, token_id: str, payload: dict) -> Path:
    """
    Persist the fetched order book into a JSON file for offline inspection.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{token_id}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def update_market_orderbooks(
    market_id: str, tokens: Iterable[str], depth: int, snapshot_dir: Path | None
) -> None:
    """
    Iterate through the provided tokens, refresh their order books, and persist
    the data to the database (and snapshots when requested).
    """
    successes = 0
    failures = 0

    for token_id in tokens:
        print(f"\nFetching order book for token {token_id}...")
        try:
            book = fetch_orderbook(token_id, depth)
        except RuntimeError as error:
            print(f"Error: {error}")
            failures += 1
            continue

        bids = len(book.get("bids", []))
        asks = len(book.get("asks", []))
        print(f"  Received {bids} bids and {asks} asks.")

        if snapshot_dir is not None:
            snapshot_path = write_snapshot(snapshot_dir, token_id, book)
            print(f"  Snapshot written to {snapshot_path}")

        if save_orderbook_to_db(market_id, token_id, book):
            print("  Order book stored in database.")
            successes += 1
        else:
            print("  Failed to write order book into the database.")
            failures += 1

    print("\nSummary:")
    print(f"  Successful updates: {successes}")
    print(f"  Failed updates: {failures}")


def main() -> int:
    """
    Load the market from the local database and refresh its order books.
    """
    args = parse_args()

    market = get_market(args.market_id)
    if not market:
        print(f"Error: market {args.market_id} not found in the local database.")
        return 1

    print(f"Market: {market.get('question', 'Unknown market question')}")

    try:
        token_ids = extract_token_ids(market)
    except ValueError as error:
        print(f"Error: {error}")
        return 1

    if args.token_id:
        if args.token_id not in token_ids:
            print(
                "Warning: provided token id is not listed on the market. "
                "Proceeding anyway in case the schema changed."
            )
        tokens_to_refresh = [args.token_id]
    else:
        tokens_to_refresh = token_ids

    update_market_orderbooks(
        market_id=args.market_id,
        tokens=tokens_to_refresh,
        depth=args.depth,
        snapshot_dir=args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

