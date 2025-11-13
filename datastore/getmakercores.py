"""
Fetch liquidity maker scores for a Polymarket market.

This script attempts to fetch maker scores from the stats-api.polymarket.com endpoint.
Note: If you encounter connection errors, the endpoint may require specific network
access or the domain may have changed. Check Polymarket documentation for current
endpoints.

The endpoint is documented as:
  https://stats-api.polymarket.com/liquidity/maker-scores?market_id=<MARKET_ID>

Usage:
    python getmakercores.py <market_id>

Example:
    python getmakercores.py 636929
"""

import argparse
import json
import sys
from typing import Any, Dict, Optional

import requests


def fetch_maker_scores(market_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch maker scores from Polymarket stats API.
    
    Args:
        market_id: The market ID to query
        
    Returns:
        Parsed JSON response or None on error
    """
    url = f"https://stats-api.polymarket.com/liquidity/maker-scores"
    params = {"market_id": market_id}
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        # Check if it's a DNS resolution error
        if "Failed to resolve" in error_msg or "nodename nor servname" in error_msg:
            print(f"Error: Cannot connect to stats-api.polymarket.com", file=sys.stderr)
            print(f"The stats API endpoint may be down or deprecated.", file=sys.stderr)
            print(f"Check Polymarket documentation for the current endpoint.", file=sys.stderr)
        else:
            print(f"Error fetching maker scores: {e}", file=sys.stderr)
        return None


def format_output(data: Dict[str, Any]) -> str:
    """
    Format the maker scores data for display.
    
    Args:
        data: The maker scores JSON response
        
    Returns:
        Formatted string
    """
    output = []
    
    output.append(f"Market ID: {data.get('market_id')}")
    output.append(f"Total Rewards: {data.get('rewards')}")
    output.append("")
    
    makers = data.get('makers', [])
    output.append(f"Total Makers: {len(makers)}")
    output.append("")
    
    if makers:
        output.append(f"{'Address':<45} {'Share':<10} {'Reward':<12} {'Score':<10}")
        output.append("-" * 80)
        
        for maker in makers:
            address = maker.get('address', 'N/A')
            share = maker.get('share', 0)
            reward = maker.get('reward', 0)
            score = maker.get('epoch_score', 0)
            
            share_pct = f"{share * 100:.2f}%"
            reward_str = f"${reward:.2f}"
            score_str = f"{score:.2f}"
            
            output.append(f"{address:<45} {share_pct:<10} {reward_str:<12} {score_str:<10}")
    
    return "\n".join(output)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch liquidity maker scores for a Polymarket market")
    parser.add_argument("market_id", help="Market ID to query")
    parser.add_argument("--json", action="store_true", help="Output raw JSON instead of formatted text")
    
    args = parser.parse_args()
    
    data = fetch_maker_scores(args.market_id)
    if data is None:
        sys.exit(1)
    
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        print(format_output(data))


if __name__ == "__main__":
    main()

