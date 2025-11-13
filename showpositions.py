"""
Show user positions from Polymarket Data API

Fetches and displays current positions for a user address using the
Polymarket Data API endpoint: GET /positions

This provides more comprehensive position data including PnL, average prices,
and other metrics compared to querying balances directly.
"""

import argparse
import sys
import toml
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional

from trading import get_address_from_key

# Load configuration
SETTINGS_FILE = Path("settings.toml")

if not SETTINGS_FILE.exists():
    print(f"ERROR: {SETTINGS_FILE} not found!")
    sys.exit(1)

try:
    config = toml.load(SETTINGS_FILE)
except Exception as e:
    print(f"ERROR: Could not load {SETTINGS_FILE}: {e}")
    sys.exit(1)

# Configuration
key = config.get("account", {}).get("private_key", "")
proxy_address = config.get("account", {}).get("proxy_address", "")

# Data API base URL
DATA_API_URL = "https://data-api.polymarket.com"


def get_user_address() -> str:
    """
    Get the user address from settings (proxy if set, otherwise EOA).
    
    Returns:
        User address string (0x-prefixed)
    """
    if not key:
        print("ERROR: private_key not found in settings.toml")
        sys.exit(1)
    
    eoa_address = get_address_from_key(key)
    
    # Use proxy address if set and different from EOA, otherwise use EOA
    if proxy_address and proxy_address != eoa_address:
        return proxy_address
    return eoa_address


def fetch_positions(
    user: str,
    market: Optional[List[str]] = None,
    event_id: Optional[List[int]] = None,
    size_threshold: float = 1.0,
    redeemable: bool = False,
    mergeable: bool = False,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "TOKENS",
    sort_direction: str = "DESC",
    title: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch positions from Polymarket Data API.
    
    Args:
        user: User address (required)
        market: Comma-separated list of condition IDs (mutually exclusive with event_id)
        event_id: Comma-separated list of event IDs (mutually exclusive with market)
        size_threshold: Minimum position size (default: 1.0)
        redeemable: Filter for redeemable positions only (default: False)
        mergeable: Filter for mergeable positions only (default: False)
        limit: Maximum number of positions to return (default: 100, max: 500)
        offset: Pagination offset (default: 0, max: 10000)
        sort_by: Field to sort by - CURRENT, INITIAL, TOKENS, CASHPNL, PERCENTPNL,
                 TITLE, RESOLVING, PRICE, AVGPRICE (default: TOKENS)
        sort_direction: Sort direction - ASC or DESC (default: DESC)
        title: Filter by market title (max 100 chars)
    
    Returns:
        List of position dictionaries
    """
    url = f"{DATA_API_URL}/positions"
    
    params = {
        "user": user,
        "sizeThreshold": size_threshold,
        "redeemable": redeemable,
        "mergeable": mergeable,
        "limit": min(limit, 500),  # Enforce max limit
        "offset": min(offset, 10000),  # Enforce max offset
        "sortBy": sort_by,
        "sortDirection": sort_direction
    }
    
    # Add optional filters
    if market:
        # API expects comma-separated list
        params["market"] = ",".join(market)
    
    if event_id:
        # API expects comma-separated list
        params["eventId"] = ",".join(str(eid) for eid in event_id)
    
    if title:
        params["title"] = title[:100]  # Enforce max length
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        positions = response.json()
        return positions if isinstance(positions, list) else []
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch positions: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"Error details: {error_data}")
            except Exception:
                print(f"HTTP {e.response.status_code}: {e.response.text}")
        return []


def format_price(price: Any) -> str:
    """Format price value for display."""
    try:
        return f"${float(price):.4f}"
    except (ValueError, TypeError):
        return "N/A"


def format_size(size: Any) -> str:
    """Format position size for display."""
    try:
        return f"{float(size):.6f}"
    except (ValueError, TypeError):
        return "N/A"


def format_pnl(pnl: Any) -> str:
    """Format PnL value with color indication."""
    try:
        pnl_val = float(pnl)
        sign = "+" if pnl_val >= 0 else ""
        return f"{sign}${pnl_val:.2f}"
    except (ValueError, TypeError):
        return "N/A"


def format_percent_pnl(percent: Any) -> str:
    """Format percentage PnL with sign."""
    try:
        percent_val = float(percent)
        sign = "+" if percent_val >= 0 else ""
        return f"{sign}{percent_val:.2f}%"
    except (ValueError, TypeError):
        return "N/A"


def display_positions(positions: List[Dict[str, Any]], detailed: bool = False):
    """
    Display positions in a formatted table.
    
    Args:
        positions: List of position dictionaries
        detailed: If True, show detailed information for each position
    """
    if not positions:
        print("\nNo positions found.")
        return
    
    print(f"\nFound {len(positions)} position(s):")
    print("=" * 120)
    
    if detailed:
        # Detailed view - one position per section
        for idx, pos in enumerate(positions, 1):
            print(f"\n[Position {idx}/{len(positions)}]")
            print("-" * 120)
            print(f"Title: {pos.get('title', 'N/A')}")
            print(f"Slug: {pos.get('slug', 'N/A')}")
            print(f"Outcome: {pos.get('outcome', 'N/A')}")
            print(f"Condition ID: {pos.get('conditionId', 'N/A')}")
            print(f"Asset: {pos.get('asset', 'N/A')}")
            print(f"\nPosition Details:")
            print(f"  Size: {format_size(pos.get('size', 0))} shares")
            print(f"  Average Price: {format_price(pos.get('avgPrice', 0))}")
            print(f"  Current Price: {format_price(pos.get('curPrice', 0))}")
            print(f"\nValue:")
            print(f"  Initial Value: {format_price(pos.get('initialValue', 0))}")
            print(f"  Current Value: {format_price(pos.get('currentValue', 0))}")
            print(f"\nP&L:")
            print(f"  Cash P&L: {format_pnl(pos.get('cashPnl', 0))}")
            print(f"  Percent P&L: {format_percent_pnl(pos.get('percentPnl', 0))}")
            print(f"  Realized P&L: {format_pnl(pos.get('realizedPnl', 0))}")
            print(f"  Percent Realized P&L: {format_percent_pnl(pos.get('percentRealizedPnl', 0))}")
            print(f"  Total Bought: {format_price(pos.get('totalBought', 0))}")
            print(f"\nOther:")
            print(f"  Redeemable: {pos.get('redeemable', False)}")
            print(f"  Mergeable: {pos.get('mergeable', False)}")
            print(f"  Negative Risk: {pos.get('negativeRisk', False)}")
            if pos.get('endDate'):
                print(f"  End Date: {pos.get('endDate')}")
    else:
        # Compact table view
        print(f"\n{'Title':<40} {'Outcome':<20} {'Size':<12} {'Avg $':<10} {'Cur $':<10} {'P&L $':<12} {'P&L %':<10}")
        print("-" * 120)
        
        for pos in positions:
            title = (pos.get('title') or 'N/A')[:38]
            outcome = (pos.get('outcome') or 'N/A')[:18]
            size = format_size(pos.get('size', 0))
            avg_price = format_price(pos.get('avgPrice', 0))
            cur_price = format_price(pos.get('curPrice', 0))
            cash_pnl = format_pnl(pos.get('cashPnl', 0))
            percent_pnl = format_percent_pnl(pos.get('percentPnl', 0))
            
            print(f"{title:<40} {outcome:<20} {size:<12} {avg_price:<10} {cur_price:<10} {cash_pnl:<12} {percent_pnl:<10}")
        
        # Summary statistics
        total_initial = sum(float(p.get('initialValue', 0)) for p in positions)
        total_current = sum(float(p.get('currentValue', 0)) for p in positions)
        total_pnl = sum(float(p.get('cashPnl', 0)) for p in positions)
        total_pnl_percent = ((total_current - total_initial) / total_initial * 100) if total_initial > 0 else 0
        
        print("-" * 120)
        print(f"{'TOTAL':<40} {'':<20} {'':<12} {'':<10} {format_price(total_current):<10} {format_pnl(total_pnl):<12} {format_percent_pnl(total_pnl_percent):<10}")


def main():
    parser = argparse.ArgumentParser(
        description="Show user positions from Polymarket Data API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show all positions for default user
  python showpositions.py

  # Show positions for specific address
  python showpositions.py --user 0x1234...

  # Filter by market condition ID
  python showpositions.py --market 0xabcd...

  # Show only redeemable positions
  python showpositions.py --redeemable

  # Show detailed view
  python showpositions.py --detailed

  # Sort by cash P&L descending
  python showpositions.py --sort-by CASHPNL --sort-direction DESC
        """
    )
    
    parser.add_argument(
        '--user',
        type=str,
        help='User address (default: from settings.toml - proxy if set, else EOA)'
    )
    parser.add_argument(
        '--market',
        type=str,
        nargs='+',
        help='Filter by condition ID(s) (comma-separated or space-separated)'
    )
    parser.add_argument(
        '--event-id',
        type=int,
        nargs='+',
        help='Filter by event ID(s) (comma-separated or space-separated)'
    )
    parser.add_argument(
        '--size-threshold',
        type=float,
        default=1.0,
        help='Minimum position size (default: 1.0)'
    )
    parser.add_argument(
        '--redeemable',
        action='store_true',
        help='Show only redeemable positions'
    )
    parser.add_argument(
        '--mergeable',
        action='store_true',
        help='Show only mergeable positions'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of positions to return (default: 100, max: 500)'
    )
    parser.add_argument(
        '--offset',
        type=int,
        default=0,
        help='Pagination offset (default: 0, max: 10000)'
    )
    parser.add_argument(
        '--sort-by',
        type=str,
        choices=['CURRENT', 'INITIAL', 'TOKENS', 'CASHPNL', 'PERCENTPNL', 'TITLE', 'RESOLVING', 'PRICE', 'AVGPRICE'],
        default='TOKENS',
        help='Field to sort by (default: TOKENS)'
    )
    parser.add_argument(
        '--sort-direction',
        type=str,
        choices=['ASC', 'DESC'],
        default='DESC',
        help='Sort direction (default: DESC)'
    )
    parser.add_argument(
        '--title',
        type=str,
        help='Filter by market title (max 100 chars)'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed information for each position'
    )
    
    args = parser.parse_args()
    
    # Get user address
    user_address = args.user if args.user else get_user_address()
    
    if not user_address:
        print("ERROR: No user address specified and could not get from settings")
        sys.exit(1)
    
    print(f"\nFetching positions for: {user_address}")
    print(f"Data API: {DATA_API_URL}")
    
    # Fetch positions
    positions = fetch_positions(
        user=user_address,
        market=args.market,
        event_id=args.event_id,
        size_threshold=args.size_threshold,
        redeemable=args.redeemable,
        mergeable=args.mergeable,
        limit=args.limit,
        offset=args.offset,
        sort_by=args.sort_by,
        sort_direction=args.sort_direction,
        title=args.title
    )
    
    # Display positions
    display_positions(positions, detailed=args.detailed)


if __name__ == "__main__":
    main()

