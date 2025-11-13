# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests"
# ]
# ///

"""
Display orderbook for a given market ID.
Fetches market data from Polymarket Gamma API and orderbook data from CLOB API,
then displays it in a readable format.
"""

import sys
import argparse
import json
import requests
from datastore.orderbook import parse_token_ids, get_order_book

GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events"


def fetch_market_from_api(market_id):
    """
    Fetch market data from Polymarket Gamma API by market ID.
    
    Args:
        market_id (str): Market ID to fetch
    
    Returns:
        dict: Market object from API, or None if not found
    """
    try:
        # Try markets endpoint first
        response = requests.get(
            GAMMA_MARKET_URL,
            params={"ids": market_id},
            timeout=10
        )
        response.raise_for_status()
        payload = response.json()
        
        # Handle list response
        if isinstance(payload, list):
            for item in payload:
                if str(item.get("id")) == str(market_id):
                    return item
        # Handle dict response
        elif isinstance(payload, dict) and str(payload.get("id")) == str(market_id):
            return payload
        
        # If not found in markets endpoint, try events endpoint
        # (sometimes the ID refers to an event)
        event_response = requests.get(f"{GAMMA_EVENT_URL}/{market_id}", timeout=10)
        if event_response.status_code == 200:
            event_data = event_response.json()
            markets = event_data.get("markets") or []
            if markets:
                # If the ID was an event, return the first market
                # Otherwise, find the matching market
                for market in markets:
                    if str(market.get("id")) == str(market_id):
                        return market
                return markets[0]
        
        return None
    except requests.RequestException as e:
        print(f"Error fetching market from API: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching market: {e}")
        return None


def get_token_ids_from_market(market):
    """
    Get token IDs from market, handling API (string) format.
    
    Args:
        market (dict): Market object from API
    
    Returns:
        list: List of token IDs, or None if not found or parsing fails
    """
    clob_token_ids_str = market.get('clobTokenIds')
    if not clob_token_ids_str:
        return None
    
    # If already a list, return it
    if isinstance(clob_token_ids_str, list):
        return clob_token_ids_str
    
    # Otherwise use the parse_token_ids function from orderbook.py
    return parse_token_ids(market)


def display_orderbook(token_id, book_data, token_index=None, total_tokens=None):
    """
    Display orderbook in a readable format.
    
    Args:
        token_id (str): Token ID
        book_data (dict): Orderbook data from API
        token_index (int): Optional token index for multi-token markets
        total_tokens (int): Optional total token count
    """
    if not book_data:
        print(f"  No orderbook data available")
        return
    
    # Parse bids and asks
    bids = book_data.get('bids', [])
    asks = book_data.get('asks', [])
    
    # Header
    print(f"\n{'='*80}")
    if token_index is not None and total_tokens is not None:
        print(f"Token {token_index} of {total_tokens}")
    print(f"Token ID: {token_id}")
    print(f"{'='*80}")
    
    # Display asks (selling side) - lowest price first
    print(f"\nASKS (Selling - {len(asks)} levels):")
    print(f"{'Price':<20} {'Size':<20} {'Value':<20}")
    print("-" * 60)
    
    total_ask_value = 0
    sorted_asks = sorted(asks, key=lambda x: float(x[0]) if isinstance(x, list) else float(x.get('price', 0)))
    
    for ask in sorted_asks[:20]:  # Show top 20
        # Handle both array format [price, size] and dict format {'price': x, 'size': y}
        if isinstance(ask, dict):
            price = float(ask.get('price', 0))
            size = float(ask.get('size', 0))
        else:
            price = float(ask[0])
            size = float(ask[1])
        value = price * size
        total_ask_value += value
        print(f"{price:<20.6f} {size:<20.6f} ${value:<19.2f}")
    
    if len(sorted_asks) > 20:
        print(f"... and {len(sorted_asks) - 20} more ask levels")
    
    # Best ask (lowest ask price)
    best_ask = None
    best_ask_size = 0
    if sorted_asks:
        first_ask = sorted_asks[0]
        if isinstance(first_ask, dict):
            best_ask = float(first_ask.get('price', 0))
            best_ask_size = float(first_ask.get('size', 0))
        else:
            best_ask = float(first_ask[0])
            best_ask_size = float(first_ask[1])
        print(f"\nBest Ask (Lowest): {best_ask:.6f} @ {best_ask_size:.6f}")
    
    print(f"Total Ask Value (first 20): ${total_ask_value:.2f}")
    
    # Display bids (buying side) - highest price first
    print(f"\n{'='*80}")
    print(f"BIDS (Buying - {len(bids)} levels):")
    print(f"{'Price':<20} {'Size':<20} {'Value':<20}")
    print("-" * 60)
    
    total_bid_value = 0
    sorted_bids = sorted(bids, key=lambda x: float(x[0]) if isinstance(x, list) else float(x.get('price', 0)), reverse=True)
    
    for bid in sorted_bids[:20]:  # Show top 20
        # Handle both array format [price, size] and dict format {'price': x, 'size': y}
        if isinstance(bid, dict):
            price = float(bid.get('price', 0))
            size = float(bid.get('size', 0))
        else:
            price = float(bid[0])
            size = float(bid[1])
        value = price * size
        total_bid_value += value
        print(f"{price:<20.6f} {size:<20.6f} ${value:<19.2f}")
    
    if len(sorted_bids) > 20:
        print(f"... and {len(sorted_bids) - 20} more bid levels")
    
    # Best bid (highest bid price)
    best_bid = None
    best_bid_size = 0
    if sorted_bids:
        first_bid = sorted_bids[0]
        if isinstance(first_bid, dict):
            best_bid = float(first_bid.get('price', 0))
            best_bid_size = float(first_bid.get('size', 0))
        else:
            best_bid = float(first_bid[0])
            best_bid_size = float(first_bid[1])
        print(f"\nBest Bid (Highest): {best_bid:.6f} @ {best_bid_size:.6f}")
    
    print(f"Total Bid Value (first 20): ${total_bid_value:.2f}")
    
    # Spread calculation
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
        mid_price = (best_bid + best_ask) / 2
        spread_pct = (spread / mid_price) * 100 if mid_price > 0 else 0
        print(f"\n{'='*80}")
        print(f"Spread Analysis:")
        print(f"  Best Bid (Highest): {best_bid:.6f}")
        print(f"  Best Ask (Lowest): {best_ask:.6f}")
        print(f"  Spread: {spread:.6f} ({spread_pct:.2f}% of mid price)")
        print(f"  Mid Price: {mid_price:.6f}")
    
    # Other metadata
    print(f"\n{'='*80}")
    if 'timestamp' in book_data:
        print(f"Timestamp: {book_data['timestamp']}")
    if 'min_order_size' in book_data:
        print(f"Min Order Size: {book_data['min_order_size']}")
    if 'tick_size' in book_data:
        print(f"Tick Size: {book_data['tick_size']}")
    print(f"{'='*80}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Show live orderbook for a market ID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python showorderbook.py 538932
  python showorderbook.py --market-id 538932 --depth 100
        """
    )
    parser.add_argument('market_id', nargs='?', help='Market ID to fetch orderbook for')
    parser.add_argument('--market-id', dest='market_id_arg', help='Market ID (alternative format)')
    parser.add_argument('--depth', type=int, default=50, help='Number of price levels to fetch (default: 50)')
    
    args = parser.parse_args()
    
    # Get market_id from either positional or named argument
    market_id = args.market_id or args.market_id_arg
    
    if not market_id:
        parser.print_help()
        print("\nError: Market ID is required")
        sys.exit(1)
    
    # Get market info from Polymarket API
    print(f"Fetching market {market_id} from Polymarket API...")
    market = fetch_market_from_api(market_id)
    
    if not market:
        print(f"\nError: Market {market_id} not found on Polymarket")
        print("Please verify the market ID is correct")
        sys.exit(1)
    
    print(f"\nMarket: {market.get('question', 'Unknown')}")
    print(f"Market ID: {market_id}")
    print(f"Active: {market.get('active', False)}")
    
    # Parse token IDs - handle both database and API formats
    token_ids = get_token_ids_from_market(market)
    if not token_ids:
        print("\nError: No token IDs found for this market")
        sys.exit(1)
    
    print(f"\nFound {len(token_ids)} token(s) for this market")
    
    # Fetch and display orderbook for each token
    successful_fetches = 0
    for i, token_id in enumerate(token_ids, 1):
        if not token_id:
            continue
        
        print(f"\n{'#'*80}")
        print(f"Fetching orderbook for token {i}/{len(token_ids)}: {token_id[:40]}...")
        print(f"{'#'*80}")
        
        book_data = get_order_book(token_id, depth=args.depth)
        
        if book_data:
            display_orderbook(token_id, book_data, token_index=i, total_tokens=len(token_ids))
            successful_fetches += 1
        else:
            print(f"  Failed to fetch orderbook for token {i}")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total tokens: {len(token_ids)}")
    print(f"Successful fetches: {successful_fetches}")
    print(f"Failed fetches: {len(token_ids) - successful_fetches}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

