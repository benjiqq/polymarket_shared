#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "tabulate"
# ]
# ///
"""
showorderbook.py

Display stored order book data for a specific market. The script reads the
latest rows saved in the local SQLite database and prints the bid / ask books
in an easy to scan tabular format.
"""

import argparse
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

from tabulate import tabulate

DB_PATH = Path("data/markets.db")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments and validate basic input.
    """
    parser = argparse.ArgumentParser(
        description="Show the stored order book for a market."
    )
    parser.add_argument(
        "--market-id",
        "-m",
        required=True,
        help="Market identifier stored in the markets table.",
    )
    parser.add_argument(
        "--token-id",
        "-t",
        help=(
            "Optional token id. When omitted, the script shows the most recent "
            "order book entry for each token linked to the market."
        ),
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=10,
        help="Maximum number of bid / ask rows to display (default 10).",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=Path,
        default=DB_PATH,
        help="Path to the SQLite database (defaults to data/markets.db).",
    )
    return parser.parse_args()


def ensure_database(db_path: Path) -> None:
    """
    Confirm that the database exists before we try connecting to it.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")
    if not db_path.is_file():
        raise FileNotFoundError(f"Path is not a file: {db_path}")


def open_connection(db_path: Path) -> sqlite3.Connection:
    """
    Open a sqlite3 connection using Row factory for convenient column access.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_market(conn: sqlite3.Connection, market_id: str) -> sqlite3.Row | None:
    """
    Retrieve the market metadata to display context up front.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id, question FROM markets WHERE id = ?", (market_id,))
    return cursor.fetchone()


def fetch_tokens(
    conn: sqlite3.Connection, market_id: str, token_id: str | None
) -> List[Tuple[int, str, str | None, str | None]]:
    """
    Return the most recent order book row id plus metadata for each token.

    We rely on created_at when timestamp is missing because some legacy writes
    did not populate the timestamp field. The function always returns the newest
    row per token to keep the output concise.
    """
    cursor = conn.cursor()

    if token_id:
        candidate_tokens: Sequence[str] = [token_id]
    else:
        cursor.execute(
            "SELECT DISTINCT token_id FROM orderbooks WHERE market_id = ?;",
            (market_id,),
        )
        candidate_tokens = [row["token_id"] for row in cursor.fetchall()]

    results: List[Tuple[int, str, str | None, str | None]] = []

    for token in candidate_tokens:
        cursor.execute(
            "SELECT id, token_id, timestamp, created_at "
            "FROM orderbooks "
            "WHERE market_id = ? AND token_id = ? "
            "ORDER BY COALESCE(timestamp, created_at) DESC, id DESC "
            "LIMIT 1;",
            (market_id, token),
        )
        row = cursor.fetchone()
        if row:
            results.append(
                (
                    row["id"],
                    row["token_id"],
                    row["timestamp"],
                    row["created_at"],
                )
            )

    return results


def fetch_orderbook_rows(
    conn: sqlite3.Connection, row_id: int
) -> Tuple[List[List[float]], List[List[float]]]:
    """
    Pull the bids and asks JSON from the orderbooks table and convert them into
    simple lists for display.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT bids, asks FROM orderbooks WHERE id = ?;",
        (row_id,),
    )
    row = cursor.fetchone()
    if not row:
        return [], []

    import json

    bids = json.loads(row["bids"]) if row["bids"] else []
    asks = json.loads(row["asks"]) if row["asks"] else []
    return bids, asks


def format_book_side(
    side: List[List[float]],
    limit: int,
    descending: bool,
) -> List[List[float]]:
    """
    Trim and normalize one side of the order book for human-friendly printing.
    """
    formatted = []
    for entry in side:
        if isinstance(entry, list) and len(entry) >= 2:
            price, size = entry[0], entry[1]
        elif isinstance(entry, dict):
            price = entry.get("price")
            size = entry.get("size")
        else:
            continue

        try:
            price_value = float(price)
        except (TypeError, ValueError):
            price_value = price

        try:
            size_value = float(size)
        except (TypeError, ValueError):
            size_value = size

        formatted.append([price_value, size_value])
    sorted_rows = sorted(formatted, key=lambda item: item[0], reverse=descending)
    return sorted_rows[:limit]


def _as_float(value) -> float | None:
    """
    Best-effort conversion helper so we can compute mid prices even when the
    stored JSON uses strings. Returns None when conversion fails.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def print_orderbook(
    market: sqlite3.Row,
    token_id: str,
    timestamp: str | None,
    created_at: str | None,
    bids: List[List[float]],
    asks: List[List[float]],
    limit: int,
) -> None:
    """
    Render the order book side by side using tabulate for nice text tables.
    """
    print("\n" + "=" * 80)
    print(f"Market: {market['question']} ({market['id']})")
    print(f"Token:  {token_id}")
    if timestamp:
        print(f"Time:   {timestamp}")
    if created_at:
        print(f"Saved:  {created_at}")
    print("=" * 80)

    bid_rows = format_book_side(bids, limit, descending=True)
    ask_rows = format_book_side(asks, limit, descending=False)

    if not bid_rows and not ask_rows:
        print("No order book data stored for this token.")
        return

    bid_table = tabulate(bid_rows, headers=["Bid Price", "Bid Size"], tablefmt="simple")
    ask_table = tabulate(ask_rows, headers=["Ask Price", "Ask Size"], tablefmt="simple")

    best_bid = _as_float(bid_rows[0][0]) if bid_rows else None
    best_ask = _as_float(ask_rows[0][0]) if ask_rows else None

    if best_bid is not None and best_ask is not None:
        mid_price = (best_bid + best_ask) / 2
        mid_message = (
            f"Mid price: {mid_price:.6f} "
            f"(best bid {best_bid:.6f}, best ask {best_ask:.6f})"
        )
    elif best_bid is None and best_ask is None:
        mid_message = "Mid price: unavailable (missing both bid and ask data)."
    elif best_bid is None:
        mid_message = "Mid price: unavailable (missing bids)."
    else:
        mid_message = "Mid price: unavailable (missing asks)."

    print("\nAsks:")
    print(ask_table if ask_rows else "(empty)")

    print("\nBids:")
    print(bid_table if bid_rows else "(empty)")

    print(f"\n{mid_message}")

    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
        spread_pct = (spread / best_bid * 100) if best_bid else None
        if spread_pct is not None:
            spread_message = f"Spread: {spread:.6f} ({spread_pct:.2f}%)"
        else:
            spread_message = f"Spread: {spread:.6f} (percentage unavailable)"
    else:
        spread_message = "Spread: unavailable"

    print(spread_message)


def main() -> int:
    """
    Program entry point.
    """
    args = parse_args()
    db_path: Path = args.database.expanduser().resolve()

    try:
        ensure_database(db_path)
    except FileNotFoundError as error:
        print(f"Error: {error}")
        return 1

    conn = open_connection(db_path)

    try:
        market = fetch_market(conn, args.market_id)
        if not market:
            print(f"Error: market {args.market_id} not found in database.")
            return 1

        tokens = fetch_tokens(conn, args.market_id, args.token_id)
        if not tokens:
            print("No stored order book rows for the requested parameters.")
            return 0

        for row_id, token_id, timestamp, created_at in tokens:
            bids, asks = fetch_orderbook_rows(conn, row_id)
            print_orderbook(
                market=market,
                token_id=token_id,
                timestamp=timestamp,
                created_at=created_at,
                bids=bids,
                asks=asks,
                limit=args.limit,
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

