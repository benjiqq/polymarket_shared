# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests"
# ]
# ///

import requests
import json
import os
from datastore import get_markets, save_orderbook_to_db

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
DATA_DIR = "data"


def fetch_markets_from_gamma(limit=100, offset=0):
    """
    Fetch markets from the Gamma API.
    
    Args:
        limit (int): Maximum number of markets to fetch
        offset (int): Number of markets to skip for pagination
    
    Returns:
        list: List of market objects from Gamma API, or None if error
    """
    print("Fetching markets from Gamma API...")
    
    try:
        gamma_markets = get_markets(active=True, limit=limit, offset=offset, closed=False, ascending=False)
        print(f"Fetched {len(gamma_markets)} markets from Gamma API")
        
        if gamma_markets:
            print(f"Sample market keys: {list(gamma_markets[0].keys())}")
        
        return gamma_markets
    except Exception as e:
        print(f"Error fetching markets from Gamma API: {e}")
        return None


def parse_token_ids(market):
    """
    Parse clobTokenIds from a market object.
    
    Args:
        market (dict): Market object from Gamma API
    
    Returns:
        list: List of token IDs, or None if not found or parsing fails
    """
    clob_token_ids_str = market.get('clobTokenIds')
    if not clob_token_ids_str:
        return None
    
    try:
        # Parse the stringified JSON array
        token_ids = json.loads(clob_token_ids_str)
        return token_ids
    except json.JSONDecodeError:
        print(f"  Failed to parse clobTokenIds: {clob_token_ids_str}")
        return None


def check_order_book_exists(token_id):
    """
    Check if a token has an active order book on the CLOB API.
    
    Args:
        token_id (str): Token ID to check
    
    Returns:
        bool: True if order book exists, False otherwise
    """
    book_url = f"{CLOB_URL}/book"
    params = {"token_id": token_id}
    
    try:
        response = requests.get(book_url, params=params)
        return response.status_code == 200
    except Exception as e:
        print(f"  Error checking order book: {e}")
        return False


def find_market_with_order_book(gamma_markets):
    """
    Find a market with an active order book.
    
    Args:
        gamma_markets (list): List of market objects from Gamma API
    
    Returns:
        tuple: (market, token_id) if found, (None, None) otherwise
    """
    print(f"Searching through {len(gamma_markets)} markets for ones with active order books...")
    
    markets_checked = 0
    
    for m in gamma_markets:
        # Skip if m is not a dict
        if not isinstance(m, dict):
            continue
        
        markets_checked += 1
        
        # Only check active markets
        if not m.get('active', False):
            continue
        
        print(f"Checking market {markets_checked}: {m.get('question', 'Unknown')}")
        
        # Get token IDs for this market
        token_ids = parse_token_ids(m)
        if not token_ids:
            continue
        
        # Try to find a token with an active order book
        for token_id in token_ids:
            if token_id and check_order_book_exists(token_id):
                print(f"Found working token ID: {token_id}")
                return (m, token_id)
    
    print(f"Checked {markets_checked} markets")
    return (None, None)


def get_order_book(token_id, depth=50):
    """
    Fetch order book data from the CLOB API.
    
    Args:
        token_id (str): Token ID to fetch order book for
        depth (int): Number of price levels to return (default 50)
    
    Returns:
        dict: Order book data, or None if error
    """
    book_url = f"{CLOB_URL}/book"
    params = {"token_id": token_id, "depth": depth}
    
    try:
        response = requests.get(book_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching order book: {response.status_code}")
            print(response.text)
            return None
    except Exception as e:
        print(f"Error fetching order book: {e}")
        return None


def save_order_book_to_file(market, token_id, book_data):
    """
    Save order book data to a JSON file in the data directory and to database.
    
    Args:
        market (dict): Market object
        token_id (str): Token ID
        book_data (dict): Order book data
    
    Returns:
        str: Filename where data was saved, or None if error
    """
    # Create data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    filename = f"orderbook_{market.get('slug', 'market')}.json"
    filepath = os.path.join(DATA_DIR, filename)
    
    try:
        data = {
            "market": market.get('question', 'Unknown'),
            "market_id": market.get('id'),
            "token_id": token_id,
            "timestamp": book_data.get('timestamp'),
            "orderbook": book_data
        }
        
        # Save to JSON file
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Save to database
        if save_orderbook_to_db(market.get('id'), token_id, book_data):
            print(f"Order book saved to {filepath} and database")
        else:
            print(f"Order book saved to {filepath} (database save failed)")
        
        return filepath
    except Exception as e:
        print(f"Error saving order book to file: {e}")
        return None


def find_fallback_market(gamma_markets):
    """
    Find any market with tokens as a fallback option.
    
    Args:
        gamma_markets (list): List of market objects from Gamma API
    
    Returns:
        dict: Market object, or None if not found
    """
    print("\nNo active markets with order books found")
    print("This could mean:")
    print("  - Markets don't have active order books yet")
    print("  - Order books are not available for these tokens")
    print("\nTrying fallback: searching for any market with tokens...")
    
    for m in gamma_markets:
        if not isinstance(m, dict):
            continue
        
        # Look for markets with clobTokenIds
        if m.get('clobTokenIds'):
            print(f"Found market with tokens: {m.get('question', 'Unknown')}")
            return m
    
    print("No markets with tokens found at all")
    return None


def main():
    """
    Main function to fetch markets and get order book data for the first 10 markets.
    """
    # Step 1: Fetch markets from Gamma API
    gamma_markets = fetch_markets_from_gamma(limit=100, offset=0)
    
    if not gamma_markets:
        print("No markets found in Gamma API")
        return
    
    # Step 2: Loop through first 10 markets and fetch order books
    print(f"\nProcessing first 10 markets...")
    successful_fetches = 0
    failed_fetches = 0
    
    for i, market in enumerate(gamma_markets[:10], 1):
        print(f"\n{'='*60}")
        print(f"Market {i}/10: {market.get('question', 'Unknown')}")
        print(f"{'='*60}")
        
        # Skip if not a dict
        if not isinstance(market, dict):
            print("  Skipping: not a dictionary")
            failed_fetches += 1
            continue
        
        # Skip if not active
        if not market.get('active', False):
            print("  Skipping: market is not active")
            failed_fetches += 1
            continue
        
        # Get token IDs for this market
        token_ids = parse_token_ids(market)
        if not token_ids:
            print("  Skipping: no token IDs found")
            failed_fetches += 1
            continue
        
        print(f"  Found {len(token_ids)} token(s)")
        
        # Try to get order book for each token
        found_order_book = False
        for token_id in token_ids:
            if not token_id:
                continue
            
            print(f"  Checking token: {token_id[:20]}...")
            
            # Get order book
            book_data = get_order_book(token_id)
            if book_data:
                num_bids = len(book_data.get('bids', []))
                num_asks = len(book_data.get('asks', []))
                print(f"  Successfully fetched order book!")
                print(f"  Bids: {num_bids}, Asks: {num_asks}, Total: {num_bids + num_asks}")
                
                # Save to file
                filename = save_order_book_to_file(market, token_id, book_data)
                if filename:
                    successful_fetches += 1
                    found_order_book = True
                    break
            else:
                print(f"  No order book available for this token")
        
        if not found_order_book:
            print(f"  Failed: no order book found for any token in this market")
            failed_fetches += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total markets processed: 10")
    print(f"Successful fetches: {successful_fetches}")
    print(f"Failed fetches: {failed_fetches}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()