"""
Get trade data for a specific Polymarket trader
Fetches transaction history for a given trader address using the CLOB API
and optionally saves it to a JSON file
"""

import sys
import json
import toml
from pathlib import Path
from datetime import datetime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import TradeParams


# Load configuration
SETTINGS_FILE = Path("settings.toml")

if not SETTINGS_FILE.exists():
    print(f"ERROR: {SETTINGS_FILE} not found!")
    print("You need settings.toml with your API credentials to query the CLOB API")
    sys.exit(1)

try:
    config = toml.load(SETTINGS_FILE)
except Exception as e:
    print(f"ERROR: Could not load {SETTINGS_FILE}: {e}")
    sys.exit(1)

# Configuration from settings
key = config.get("account", {}).get("private_key", "")
proxy_address = config.get("account", {}).get("proxy_address", "")
chain_id = config.get("trading", {}).get("chain_id", 137)
host = config.get("trading", {}).get("clob_endpoint", "https://clob.polymarket.com")

if not key:
    print("ERROR: private_key not found in settings.toml")
    sys.exit(1)


def format_timestamp(ts):
    """Convert unix timestamp to readable date"""
    try:
        return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ts


def initialize_client():
    """Initialize a ClobClient for API access"""
    if proxy_address:
        client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=proxy_address)
    else:
        client = ClobClient(host, key=key, chain_id=chain_id)
    
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_trader_trades(client: ClobClient, address: str, market: str = None, 
                      after: int = None, before: int = None) -> list:
    """
    Get trade history for a specific trader address (both maker and taker)
    
    Args:
        client: Initialized ClobClient
        address: Ethereum address of the trader
        market: Optional market condition ID filter
        after: Optional unix timestamp - only trades after this time
        before: Optional unix timestamp - only trades before this time
    
    Returns:
        List of trades
    """
    try:
        trades = []
        
        # Get trades where this address is the maker (provided liquidity)
        print("  Fetching maker trades...")
        maker_params = TradeParams(
            maker_address=address.lower(),
            market=market,
            after=after,
            before=before
        )
        maker_trades = client.get_trades(params=maker_params)
        if maker_trades:
            # Add trader_side field to identify these as maker trades
            for trade in maker_trades:
                trade['trader_side'] = 'MAKER'
            trades.extend(maker_trades)
        
        # Get trades where this address is the taker (took liquidity)
        print("  Fetching taker trades...")
        taker_params = TradeParams(
            taker_address=address.lower(),
            market=market,
            after=after,
            before=before
        )
        taker_trades = client.get_trades(params=taker_params)
        if taker_trades:
            # Add trader_side field to identify these as taker trades
            for trade in taker_trades:
                trade['trader_side'] = 'TAKER'
            trades.extend(taker_trades)
        
        # Remove duplicates based on trade ID
        unique_trades = {}
        for trade in trades:
            trade_id = trade.get('id')
            if trade_id and trade_id not in unique_trades:
                unique_trades[trade_id] = trade
        
        return list(unique_trades.values())
            
    except Exception as e:
        print(f"Error fetching trades: {e}")
        return []


def display_trades(trades: list, address: str):
    """Display trades in a readable format"""
    print("\n" + "="*80)
    print(f"TRADE HISTORY FOR TRADER")
    print("="*80)
    print(f"Address: {address}")
    
    if trades:
        print(f"\nFound {len(trades)} trade(s):\n")
        
        total_volume = 0
        
        for i, trade in enumerate(trades, 1):
            print(f"Trade #{i}")
            print(f"  ID: {trade.get('id')}")
            print(f"  Side: {trade.get('side')}")
            print(f"  Type: {trade.get('type', 'N/A')}")
            
            # Market and asset info
            print(f"  Market: {trade.get('market', 'N/A')}")
            print(f"  Asset ID: {trade.get('asset_id', 'N/A')}")
            outcome = trade.get('outcome')
            if outcome:
                print(f"  Outcome: {outcome}")
            
            # Trade details
            price = float(trade.get('price', 0))
            size = float(trade.get('size', 0))
            trade_value = price * size
            
            print(f"  Price: ${price:.4f}")
            print(f"  Size: {size:,.2f} shares")
            print(f"  Total Value: ${trade_value:.2f}")
            
            total_volume += trade_value
            
            # Status
            print(f"  Status: {trade.get('status')}")
            print(f"  Fee Rate: {trade.get('fee_rate_bps', 0)} bps")
            
            # Timing
            match_time = trade.get('match_time') or trade.get('timestamp')
            if match_time:
                print(f"  Matched: {format_timestamp(match_time)}")
            
            last_update = trade.get('last_update')
            if last_update:
                print(f"  Last Update: {format_timestamp(last_update)}")
            
            # Transaction info
            tx_hash = trade.get('transaction_hash')
            if tx_hash:
                print(f"  Transaction: {tx_hash}")
                print(f"  View: https://polygonscan.com/tx/{tx_hash}")
            
            # Maker orders info
            maker_orders = trade.get('maker_orders', [])
            if maker_orders:
                print(f"  Matched against {len(maker_orders)} maker order(s)")
            
            print()
        
        # Summary statistics
        print("-"*80)
        print("SUMMARY")
        print(f"  Total Trades: {len(trades)}")
        print(f"  Total Volume: ${total_volume:,.2f}")
        print("-"*80)
        
    else:
        print("\nNo trades found for this trader")
        print("\nNote: This trader may not have any recorded trades, or the trades")
        print("may have been executed on a different account.")
    
    print("="*80)
    print()


def save_trades_to_json(trades: list, address: str, filename: str):
    """
    Save trades to a JSON file with metadata
    
    Args:
        trades: List of trade objects
        address: Trader address
        filename: Output filename
    """
    # Calculate statistics
    total_volume = sum(
        float(trade.get('size', 0)) * float(trade.get('price', 0)) 
        for trade in trades
    )
    
    # Prepare output data structure
    output_data = {
        "trader_address": address,
        "fetched_at": datetime.now().isoformat(),
        "total_trades": len(trades),
        "total_volume_usd": round(total_volume, 2),
        "trades": trades
    }
    
    # Save to file
    try:
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        print(f"\nSaved {len(trades)} trades to: {filename}")
    except Exception as e:
        print(f"\nError saving to JSON: {e}")


def check_trader(address: str, market: str = None, 
                 after: int = None, before: int = None, 
                 output_file: str = None, quiet: bool = False):
    """
    Check trades for a specific trader address
    
    Args:
        address: Ethereum address to check
        market: Optional market ID to filter trades
        after: Optional unix timestamp to filter trades after this time
        before: Optional unix timestamp to filter trades before this time
        output_file: Optional JSON file to save results to
        quiet: If True, only show output filename
    """
    if not quiet:
        print("\n" + "="*80)
        print("POLYMARKET TRADER CHECK")
        print("="*80)
        print(f"\nChecking trades for: {address}")
        
        if market:
            print(f"Filtering by market: {market}")
        if after:
            print(f"Trades after: {format_timestamp(after)}")
        if before:
            print(f"Trades before: {format_timestamp(before)}")
        
        # Initialize client
        print("\nInitializing CLOB client...")
        print("Fetching trades...")
    
    # Initialize client
    client = initialize_client()
    
    # Get trades
    if not quiet:
        trades = get_trader_trades(client, address, market=market, after=after, before=before)
    else:
        # Suppress output when quiet
        import io
        from contextlib import redirect_stdout
        
        f = io.StringIO()
        with redirect_stdout(f):
            trades = get_trader_trades(client, address, market=market, after=after, before=before)
    
    # Display results only if not quiet
    if not quiet:
        display_trades(trades, address)
    
    # Save to JSON file if requested
    if output_file and trades:
        save_trades_to_json(trades, address, output_file)
    
    return trades


def main():
    """Main function"""
    # Default trader address from your request
    TRADER_ADDRESS = "0xca85f4b9e472b542e1df039594eeaebb6d466bf2"
    
    # Parse optional filters first
    market_id = None
    after_ts = None
    before_ts = None
    output_file = None
    quiet = True  # Default to quiet mode
    
    if '--market' in sys.argv:
        market_index = sys.argv.index('--market')
        if market_index + 1 < len(sys.argv):
            market_id = sys.argv[market_index + 1]
    
    if '--after' in sys.argv:
        after_index = sys.argv.index('--after')
        if after_index + 1 < len(sys.argv):
            try:
                after_ts = int(sys.argv[after_index + 1])
            except ValueError:
                print("ERROR: --after must be a unix timestamp")
                sys.exit(1)
    
    if '--before' in sys.argv:
        before_index = sys.argv.index('--before')
        if before_index + 1 < len(sys.argv):
            try:
                before_ts = int(sys.argv[before_index + 1])
            except ValueError:
                print("ERROR: --before must be a unix timestamp")
                sys.exit(1)
    
    if '--output' in sys.argv:
        output_index = sys.argv.index('--output')
        if output_index + 1 < len(sys.argv):
            output_file = sys.argv[output_index + 1]
    
    if '--verbose' in sys.argv or '-v' in sys.argv:
        quiet = False
    
    # Allow override from command line (first positional argument after script name)
    # Skip script name and process args
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            # Skip flag and its value
            i += 2
        else:
            # First non-flag argument is the address
            TRADER_ADDRESS = sys.argv[i]
            break
    
    # Validate address format
    if not TRADER_ADDRESS.startswith('0x') or len(TRADER_ADDRESS) != 42:
        print("ERROR: Invalid Ethereum address format")
        print("Address should start with 0x and be 42 characters long")
        sys.exit(1)
    
    # Auto-generate filename based on trader address if no output specified
    if not output_file:
        output_file = f"trades_{TRADER_ADDRESS.lower()}.json"
    
    # Check the trader
    check_trader(TRADER_ADDRESS, market=market_id, after=after_ts, before=before_ts, output_file=output_file, quiet=quiet)


if __name__ == "__main__":
    main()

