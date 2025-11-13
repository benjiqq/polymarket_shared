"""
Polymarket SQLite Database Storage Functions

This module provides functions to store and manage market data in a SQLite database.
Market data fetching functions have been moved to marketdata.py.

CLI Commands:
- python datastore.py create-tables          Create database tables
- python datastore.py fetch --limit 100      Fetch markets from API
- python datastore.py list --active          List markets
- python datastore.py get --id 12345         Get specific market
- python datastore.py fetch-orderbook --id 12345  Fetch orderbook for a market
- python datastore.py stats                  Show database statistics
- python datastore.py delete --id 12345      Delete a market
- python datastore.py clear                  Clear all data
"""

import sqlite3
import argparse
import json
from datetime import datetime
from pathlib import Path
# Support both package-style imports (when scripts use `from datastore import ...`)
# and direct execution (e.g. `python datastore/datastore.py`).
try:
    from .marketdata import (
        get_markets_gamma,
        get_events,
        get_markets_batch,
        get_markets_by_category,
    )
except ImportError:  # pragma: no cover - fallback for direct execution context
    from marketdata import (  # type: ignore
        get_markets_gamma,
        get_events,
        get_markets_batch,
        get_markets_by_category,
    )


# Alias functions from marketdata for backward compatibility
def get_markets(active=True, limit=100, offset=0, closed=False, ascending=False, tag_id=None):
    """Alias for get_markets_gamma from marketdata.py"""
    return get_markets_gamma(active, limit, offset, closed, ascending, tag_id)


def getmarkets(filename="markets.json", total_markets=100, use_events=False):
    """Alias for get_markets_batch from marketdata.py"""
    return get_markets_batch(filename, total_markets, use_events)


def load_markets_from_file(filename="markets.json"):
    """
    Load previously saved markets from a JSON file.
    
    Args:
        filename (str): Name of the file to load markets from
    
    Returns:
        dict: Dictionary containing timestamp, count, and markets list
    """
    with open(filename, 'r') as f:
        data = json.load(f)
    
    print(f"Loaded {data['count']} markets from {filename}")
    print(f"Data timestamp: {data.get('timestamp', 'N/A')}")
    
    return data


# ============================================================================
# SQLite Database Functions
# ============================================================================

DB_PATH = "data/markets.db"

def get_db_connection():
    """Get a connection to the SQLite database."""
    Path("data").mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    """Create database tables for markets, events, and order books."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Markets table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            id TEXT PRIMARY KEY,
            question TEXT,
            slug TEXT UNIQUE,
            conditionId TEXT,
            description TEXT,
            outcomes TEXT,
            active BOOLEAN,
            closed BOOLEAN,
            archived BOOLEAN,
            restricted BOOLEAN,
            featured BOOLEAN,
            startDate TEXT,
            endDate TEXT,
            createdAt TEXT,
            updatedAt TEXT,
            volume REAL,
            liquidity REAL,
            volume24hr REAL,
            volume1wk REAL,
            volume1mo REAL,
            volume1yr REAL,
            enableOrderBook BOOLEAN,
            orderPriceMinTickSize REAL,
            orderMinSize REAL,
            clobTokenIds TEXT,
            market_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Events table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            ticker TEXT,
            slug TEXT UNIQUE,
            title TEXT,
            description TEXT,
            active BOOLEAN,
            closed BOOLEAN,
            archived BOOLEAN,
            startDate TEXT,
            endDate TEXT,
            createdAt TEXT,
            updatedAt TEXT,
            event_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Order books table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orderbooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT,
            token_id TEXT,
            timestamp TEXT,
            bids TEXT,
            asks TEXT,
            min_order_size TEXT,
            tick_size TEXT,
            neg_risk BOOLEAN,
            orderbook_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (market_id) REFERENCES markets(id)
        )
    """)
    
    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_closed ON markets(closed)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_slug ON markets(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbooks_market ON orderbooks(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbooks_token ON orderbooks(token_id)")
    
    conn.commit()
    conn.close()
    print("Database tables created successfully!")


def save_market_to_db(market):
    """Save a single market to the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO markets (
                id, question, slug, conditionId, description, outcomes,
                active, closed, archived, restricted, featured,
                startDate, endDate, createdAt, updatedAt,
                volume, liquidity, volume24hr, volume1wk, volume1mo, volume1yr,
                enableOrderBook, orderPriceMinTickSize, orderMinSize, clobTokenIds, market_data,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            market.get('id'),
            market.get('question'),
            market.get('slug'),
            market.get('conditionId'),
            market.get('description'),
            market.get('outcomes'),
            market.get('active'),
            market.get('closed'),
            market.get('archived'),
            market.get('restricted'),
            market.get('featured'),
            market.get('startDate'),
            market.get('endDate'),
            market.get('createdAt'),
            market.get('updatedAt'),
            market.get('volume'),
            market.get('liquidity'),
            market.get('volume24hr'),
            market.get('volume1wk'),
            market.get('volume1mo'),
            market.get('volume1yr'),
            market.get('enableOrderBook'),
            market.get('orderPriceMinTickSize'),
            market.get('orderMinSize'),
            market.get('clobTokenIds'),
            json.dumps(market)
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error saving market {market.get('id')}: {e}")
        return False
    finally:
        conn.close()


def save_orderbook_to_db(market_id, token_id, orderbook_data):
    """Save order book data to the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO orderbooks (
                market_id, token_id, timestamp, bids, asks,
                min_order_size, tick_size, neg_risk, orderbook_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            market_id,
            token_id,
            orderbook_data.get('timestamp'),
            json.dumps(orderbook_data.get('bids', [])),
            json.dumps(orderbook_data.get('asks', [])),
            orderbook_data.get('min_order_size'),
            orderbook_data.get('tick_size'),
            orderbook_data.get('neg_risk'),
            json.dumps(orderbook_data)
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error saving orderbook: {e}")
        return False
    finally:
        conn.close()


def fetch_and_save_markets(limit=100, use_events=False):
    """Fetch markets from API and save to database."""
    print(f"Fetching {limit} markets from API...")
    
    all_markets = []
    batch_size = 100
    offset = 0
    
    while len(all_markets) < limit:
        remaining = limit - len(all_markets)
        current_batch = min(batch_size, remaining)
        
        if use_events:
            events = get_events(limit=current_batch, offset=offset, closed=False)
            if not events:
                break
            for event in events:
                if 'markets' in event and event['markets']:
                    all_markets.extend(event['markets'])
        else:
            markets = get_markets(active=True, limit=current_batch, offset=offset, closed=False)
            if not markets:
                break
            all_markets.extend(markets)
        
        offset += current_batch
        if len(all_markets) >= limit:
            break
    
    # Save to database
    print(f"Saving {len(all_markets)} markets to database...")
    saved = 0
    for market in all_markets:
        if save_market_to_db(market):
            saved += 1
    
    print(f"Successfully saved {saved} markets to database!")
    return saved


def list_markets(active_only=True, limit=50):
    """List markets from database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, question, slug, active, closed, volume, liquidity FROM markets"
    params = []
    
    if active_only:
        query += " WHERE active = 1 AND closed = 0"
    
    query += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    markets = cursor.fetchall()
    conn.close()
    
    print(f"\n{'ID':<10} {'Active':<8} {'Closed':<8} {'Volume':<12} {'Liquidity':<12} {'Question'}")
    print("=" * 100)
    
    for market in markets:
        print(f"{market['id']:<10} {str(market['active']):<8} {str(market['closed']):<8} "
              f"{market['volume'] or 0:<12.2f} {market['liquidity'] or 0:<12.2f} {market['question'][:50]}")
    
    print(f"\nTotal: {len(markets)} markets")
    return markets


def get_market(market_id):
    """Get a specific market by ID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM markets WHERE id = ?", (market_id,))
    market = cursor.fetchone()
    conn.close()
    
    if market:
        print(f"\nMarket: {market['question']}")
        print(f"ID: {market['id']}")
        print(f"Slug: {market['slug']}")
        print(f"Active: {market['active']}")
        print(f"Closed: {market['closed']}")
        print(f"Volume: {market['volume']}")
        print(f"Liquidity: {market['liquidity']}")
        print(f"Updated: {market['updated_at']}")
        return dict(market)
    else:
        print(f"Market {market_id} not found")
        return None


def delete_market(market_id):
    """Delete a market from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM markets WHERE id = ?", (market_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    
    if deleted:
        print(f"Market {market_id} deleted successfully")
    else:
        print(f"Market {market_id} not found")
    
    return deleted > 0


def clear_database():
    """Clear all data from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM orderbooks")
    cursor.execute("DELETE FROM markets")
    cursor.execute("DELETE FROM events")
    
    conn.commit()
    conn.close()
    print("Database cleared successfully!")


def save_orderbook_to_db(market_id, token_id, orderbook_data):
    """
    Save orderbook data to database.
    
    Args:
        market_id: Market ID
        token_id: Token ID
        orderbook_data: Orderbook data from API
    
    Returns:
        bool: True if successful
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Parse orderbook data
        bids = orderbook_data.get('bids', [])
        asks = orderbook_data.get('asks', [])
        
        # Insert or update orderbook
        cursor.execute("""
            INSERT OR REPLACE INTO orderbooks (
                market_id, token_id, bids, asks, orderbook_data, created_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            market_id,
            token_id,
            json.dumps(bids),
            json.dumps(asks),
            json.dumps(orderbook_data)
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        conn.close()
        print(f"Error saving orderbook: {e}")
        return False


def fetch_orderbook_for_market(market_id):
    """
    Fetch orderbook data from CLOB API for a specific market and save to database.
    
    Args:
        market_id (str): Market ID to fetch orderbook for
    
    Returns:
        bool: True if successful, False otherwise
    """
    import requests
    
    print(f"Fetching orderbook for market {market_id}...")
    
    # Get market details from database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM markets WHERE id = ?", (market_id,))
    market = cursor.fetchone()
    conn.close()
    
    if not market:
        print(f"Error: Market {market_id} not found in database")
        return False
    
    print(f"Market: {market['question']}")
    
    # Parse clobTokenIds
    try:
        token_ids = json.loads(market['clobTokenIds'])
    except:
        print("Error: Could not parse clobTokenIds")
        return False
    
    print(f"Found {len(token_ids)} token(s)")
    
    # Fetch orderbook for each token
    success_count = 0
    for i, token_id in enumerate(token_ids, 1):
        print(f"\nToken {i}/{len(token_ids)}: {token_id[:20]}...")
        
        # Fetch orderbook from CLOB API
        url = "https://clob.polymarket.com/book"
        params = {"token_id": token_id}
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                orderbook_data = response.json()
                
                # Count bids and asks
                bids_count = len(orderbook_data.get('bids', []))
                asks_count = len(orderbook_data.get('asks', []))
                
                print(f"  Success! Bids: {bids_count}, Asks: {asks_count}")
                
                # Save to database
                if save_orderbook_to_db(market_id, token_id, orderbook_data):
                    print(f"  Saved to database")
                    success_count += 1
                else:
                    print(f"  Failed to save to database")
            else:
                print(f"  No orderbook available (status: {response.status_code})")
        
        except Exception as e:
            print(f"  Error: {e}")
    
    print(f"\n{'='*60}")
    print(f"Summary: {success_count}/{len(token_ids)} orderbooks saved")
    print(f"{'='*60}")
    
    return success_count > 0


def get_stats():
    """Get database statistics."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Total markets
    cursor.execute("SELECT COUNT(*) as total FROM markets")
    total = cursor.fetchone()['total']
    
    # Active markets
    cursor.execute("SELECT COUNT(*) as active FROM markets WHERE active = 1 AND closed = 0")
    active = cursor.fetchone()['active']
    
    # Closed markets
    cursor.execute("SELECT COUNT(*) as closed FROM markets WHERE closed = 1")
    closed = cursor.fetchone()['closed']
    
    # Total order books
    cursor.execute("SELECT COUNT(*) as orderbooks FROM orderbooks")
    orderbooks = cursor.fetchone()['orderbooks']
    
    # Total volume
    cursor.execute("SELECT SUM(volume) as total_volume FROM markets")
    total_volume = cursor.fetchone()['total_volume'] or 0
    
    conn.close()
    
    print("\n=== Database Statistics ===")
    print(f"Total Markets: {total}")
    print(f"Active Markets: {active}")
    print(f"Closed Markets: {closed}")
    print(f"Total Order Books: {orderbooks}")
    print(f"Total Volume: ${total_volume:,.2f}")
    print("=" * 30)


# ============================================================================
# CLI Commands
# ============================================================================

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Polymarket Markets Database Manager")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create tables command
    subparsers.add_parser('create-tables', help='Create database tables')
    
    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch markets from API')
    fetch_parser.add_argument('--limit', type=int, default=100, help='Number of markets to fetch')
    fetch_parser.add_argument('--use-events', action='store_true', help='Use events endpoint')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List markets from database')
    list_parser.add_argument('--active', action='store_true', default=True, help='Show only active markets')
    list_parser.add_argument('--all', action='store_true', help='Show all markets')
    list_parser.add_argument('--limit', type=int, default=50, help='Number of markets to show')
    
    # Get command
    get_parser = subparsers.add_parser('get', help='Get specific market')
    get_parser.add_argument('--id', required=True, help='Market ID')
    
    # Fetch orderbook command
    fetch_ob_parser = subparsers.add_parser('fetch-orderbook', help='Fetch orderbook for a market')
    fetch_ob_parser.add_argument('--id', required=True, help='Market ID to fetch orderbook for')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete a market')
    delete_parser.add_argument('--id', required=True, help='Market ID to delete')
    
    # Stats command
    subparsers.add_parser('stats', help='Show database statistics')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear all data from database')
    clear_parser.add_argument('--confirm', action='store_true', help='Confirm deletion')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute commands
    if args.command == 'create-tables':
        create_tables()
    
    elif args.command == 'fetch':
        fetch_and_save_markets(limit=args.limit, use_events=args.use_events)
    
    elif args.command == 'list':
        active_only = not args.all
        list_markets(active_only=active_only, limit=args.limit)
    
    elif args.command == 'get':
        get_market(args.id)
    
    elif args.command == 'fetch-orderbook':
        fetch_orderbook_for_market(args.id)
    
    elif args.command == 'delete':
        delete_market(args.id)
    
    elif args.command == 'stats':
        get_stats()
    
    elif args.command == 'clear':
        if args.confirm:
            clear_database()
        else:
            print("Use --confirm flag to clear the database")


if __name__ == "__main__":
    main()

