"""
Market Data Functions for Polymarket
Functions for fetching and managing market data from the CLOB and Gamma APIs
"""

import logging
import requests
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def fetch_active_markets_from_events(limit):
    """
    Fetch active markets from events endpoint (newest markets first).
    This is the recommended approach for getting the latest active markets.
    
    Args:
        limit: Number of markets to fetch (default: 10)
    
    Returns:
        List of active market dictionaries
    """
    try:
        logger.info(f"Fetching {limit} active markets from events endpoint (newest first)...")
        
        # Fetch events (events contain their associated markets)
        events = get_events(limit=limit, offset=0, closed=False, ascending=False)
        
        # Extract markets from events
        all_markets = []
        print("events length: ", len(events))
        for event in events:
            #print(event)
            if 'markets' in event and event['markets']:
                all_markets.extend(event['markets'])
        
        logger.info(f"Extracted {len(all_markets)} markets from events")
        
        # Filter for markets with order books enabled and active
        active_markets = [
            m for m in all_markets
            if m.get('enableOrderBook') is True
            and m.get('active') is True
            and m.get('closed') is False
            and m.get('archived') is False
        ]
        
        # Get the first 'limit' markets (newest first)
        markets_to_monitor = active_markets[:limit]
        
        logger.info(f"Market filtering results:")
        logger.info(f"  Total markets from events: {len(all_markets)}")
        logger.info(f"  Active markets with order books: {len(active_markets)}")
        logger.info(f"  Selected for monitoring: {len(markets_to_monitor)}")
        logger.info(f"  Filtered out: {len(all_markets) - len(active_markets)} (inactive/closed/archived/no orderbook)")
        
        # Log summary of markets we will monitor
        if markets_to_monitor:
            sample_question = markets_to_monitor[0].get('question', 'Unknown')
            logger.info(f"  Sample market: {sample_question[:80]}...")
        
        return markets_to_monitor
        
    except Exception as e:
        logger.error(f"Error fetching markets from events: {e}")
        return []


def fetch_active_markets(limit=10, use_events=True):
    """
    Fetch active markets from Polymarket.
    
    Args:
        limit: Number of markets to fetch (default: 10)
        use_events: If True, use events endpoint (recommended for newest markets)
    
    Returns:
        List of active market dictionaries
    """
    try:
        if use_events:
            return fetch_active_markets_from_events(limit=limit)
        else:
            # Use CLOB API
            url = "https://clob.polymarket.com/markets"
            logger.info(f"Fetching markets from {url}...")
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            all_markets = data.get('data', [])
            
            logger.info(f"Total markets fetched: {len(all_markets)}")
            
            # Debug: Check first market structure
            if all_markets:
                first_market = all_markets[0]
                logger.info(f"Sample market fields: {list(first_market.keys())}")
                logger.info(f"Sample market - active: {first_market.get('active')}, "
                           f"accepting_orders: {first_market.get('accepting_orders')}, "
                           f"closed: {first_market.get('closed')}, "
                           f"archived: {first_market.get('archived')}")
            
            # Filter for truly active markets
            # A market is active if it's active, accepting orders, not closed, and not archived
            active_markets = [
                market for market in all_markets
                if market.get('active') is True
                and market.get('accepting_orders') is True
                and market.get('closed') is False
                and market.get('archived') is False
            ]
            
            # If no markets found with strict filter, try lenient filter
            if not active_markets:
                logger.info("No markets found with strict filter, trying lenient filter...")
                active_markets = [
                    market for market in all_markets
                    if market.get('active') is True
                    and market.get('closed') is False
                    and market.get('archived') is False
                ]
            
            logger.info(f"Found {len(active_markets)} active markets (after filtering)")
            
            # Get the first 'limit' markets
            markets_to_monitor = active_markets[:limit]
            
            logger.info(f"Selected {len(markets_to_monitor)} markets to monitor")
            
            # Log the markets we will monitor
            for i, market in enumerate(markets_to_monitor, 1):
                question = market.get('question', 'Unknown')
                logger.info(f"  {i}. {question[:80]}...")
            
            return markets_to_monitor
        
    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        return []


def fetch_all_markets():
    """
    Fetch all markets from the Polymarket CLOB API without filtering.
    
    Returns:
        List of all market dictionaries
    """
    try:
        url = "https://clob.polymarket.com/markets"
        logger.info(f"Fetching all markets from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        all_markets = data.get('data', [])
        
        logger.info(f"Fetched {len(all_markets)} markets")
        return all_markets
        
    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        return []


def get_market_by_slug(market_slug):
    """
    Get a specific market by its slug.
    
    Args:
        market_slug: The market slug identifier
    
    Returns:
        Market dictionary or None if not found
    """
    try:
        url = "https://clob.polymarket.com/markets"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        all_markets = data.get('data', [])
        
        # Find market by slug
        for market in all_markets:
            if market.get('market_slug') == market_slug:
                return market
        
        logger.warning(f"Market not found: {market_slug}")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching market: {e}")
        return None


def get_market_tokens(market):
    """
    Extract token IDs from a market object.
    
    Args:
        market: Market dictionary
    
    Returns:
        List of token dictionaries with token_id and outcome
    """
    return market.get('tokens', [])


def get_market_condition_id(market):
    """
    Get the condition ID from a market object.
    
    Args:
        market: Market dictionary
    
    Returns:
        Condition ID string or None
    """
    return market.get('condition_id')


# ============================================================================
# Gamma API Functions (from datastore.py)
# ============================================================================

def get_markets_gamma(active=True, limit=100, offset=0, closed=False, ascending=False, tag_id=None):
    """
    Fetch markets from the Gamma API /markets endpoint.
    
    This endpoint is best for:
    - Individual market lookups
    - Category browsing with tag filtering
    - Market-specific queries
    
    Args:
        active (bool): Filter for active markets only
        limit (int): Maximum number of markets to return per request (API limit: 100)
        offset (int): Number of markets to skip for pagination
        closed (bool): Include closed markets (default False for active only)
        ascending (bool): Sort order - False gets newest first
        tag_id (int): Optional tag ID to filter by category
    
    Returns:
        list: List of market objects from the Gamma API
    
    Example:
        # Get first 50 active markets
        markets = get_markets_gamma(active=True, limit=50, offset=0, closed=False)
        
        # Get next 50 markets (page 2)
        markets = get_markets_gamma(active=True, limit=50, offset=50, closed=False)
        
        # Get markets by category tag
        markets = get_markets_gamma(tag_id=100381, closed=False, limit=25, offset=0)
    """
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "active": str(active).lower(),
        "closed": str(closed).lower(),
        "ascending": str(ascending).lower(),
        "limit": limit,
        "offset": offset
    }
    
    # Add tag filtering if specified
    if tag_id:
        params["tag_id"] = tag_id
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_events(limit=100, offset=0, closed=False, ascending=False):
    """
    Fetch events from the Gamma API /events endpoint.
    
    This endpoint is best for:
    - Complete market discovery
    - Getting all active markets systematically
    - Working with events that contain multiple markets
    
    The events endpoint is more efficient for broad market discovery because
    events contain their associated markets, reducing the need for multiple API calls.
    
    Args:
        limit (int): Maximum number of events to return per request (API limit: 100)
        offset (int): Number of events to skip for pagination
        closed (bool): Include closed events (default False for active only)
        ascending (bool): Sort order - False gets newest first
    
    Returns:
        list: List of event objects from the Gamma API
    
    Example:
        # Get first 100 active events (newest first)
        events = get_events(limit=100, offset=0, closed=False, ascending=False)
        
        # Get next 100 events (page 2)
        events = get_events(limit=100, offset=100, closed=False, ascending=False)
    """
    url = "https://gamma-api.polymarket.com/events"
    params = {
        "order": "id",
        "ascending": str(ascending).lower(),
        "closed": str(closed).lower(),
        "limit": limit,
        "offset": offset
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_markets_batch(filename="markets.json", total_markets=100, use_events=False):
    """
    Fetch active markets and save them to a JSON file.
    
    Uses pagination to fetch more than 100 markets if needed.
    Can use either the /markets or /events endpoint.
    
    Args:
        filename (str): Name of the file to save markets to
        total_markets (int): Total number of markets to fetch
        use_events (bool): If True, use /events endpoint (better for complete discovery)
                          If False, use /markets endpoint (better for specific queries)
    
    Returns:
        list: List of market objects
    
    Example:
        # Fetch 500 markets using events endpoint (recommended for discovery)
        markets = get_markets_batch("markets.json", total_markets=500, use_events=True)
        
        # Fetch 200 markets using markets endpoint
        markets = get_markets_batch("markets.json", total_markets=200, use_events=False)
    """
    logger.info(f"Fetching {total_markets} active markets using {'events' if use_events else 'markets'} endpoint...")
    
    all_markets = []
    limit = 100  # API limit per request
    offset = 0
    
    # Fetch markets in batches of 100
    while len(all_markets) < total_markets:
        remaining = total_markets - len(all_markets)
        batch_size = min(limit, remaining)
        
        logger.info(f"  Fetching batch: offset={offset}, limit={batch_size}...")
        
        if use_events:
            # Use events endpoint for better market discovery
            events_batch = get_events(
                limit=batch_size,
                offset=offset,
                closed=False,
                ascending=False
            )
            
            if not events_batch:
                logger.info("  No more events available")
                break
            
            # Extract markets from events
            for event in events_batch:
                if 'markets' in event and event['markets']:
                    all_markets.extend(event['markets'])
        else:
            # Use markets endpoint for direct market queries
            markets_batch = get_markets_gamma(
                active=True,
                limit=batch_size,
                offset=offset,
                closed=False,
                ascending=False
            )
            
            if not markets_batch:
                logger.info("  No more markets available")
                break
            
            all_markets.extend(markets_batch)
        
        offset += batch_size
        
        # If we got fewer results than requested, we've reached the end
        batch_result_count = len(events_batch) if use_events else len(markets_batch)
        if batch_result_count < batch_size:
            break
    
    # Add timestamp to the data
    data = {
        "timestamp": datetime.now().isoformat(),
        "count": len(all_markets),
        "endpoint_used": "events" if use_events else "markets",
        "markets": all_markets
    }
    
    # Save to JSON file
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Saved {len(all_markets)} markets to {filename}")
    return all_markets


def get_markets_by_category(tag_id, filename=None, total_markets=100):
    """
    Fetch markets filtered by category tag.
    
    This is useful for browsing markets in a specific category.
    
    Args:
        tag_id (int): Category tag ID to filter by
        filename (str): Optional filename to save results
        total_markets (int): Total number of markets to fetch
    
    Returns:
        list: List of market objects in the specified category
    
    Example:
        # Get markets in a specific category
        markets = get_markets_by_category(tag_id=100381, total_markets=50)
    """
    logger.info(f"Fetching markets for category tag {tag_id}...")
    
    all_markets = []
    limit = 100
    offset = 0
    
    while len(all_markets) < total_markets:
        remaining = total_markets - len(all_markets)
        batch_size = min(limit, remaining)
        
        logger.info(f"  Fetching markets {offset} to {offset + batch_size}...")
        
        markets_batch = get_markets_gamma(
            active=True,
            limit=batch_size,
            offset=offset,
            closed=False,
            ascending=False,
            tag_id=tag_id
        )
        
        if not markets_batch:
            logger.info("  No more markets available")
            break
        
        all_markets.extend(markets_batch)
        offset += batch_size
        
        if len(markets_batch) < batch_size:
            break
    
    logger.info(f"Fetched {len(all_markets)} markets for category {tag_id}")
    
    # Save to file if specified
    if filename:
        data = {
            "timestamp": datetime.now().isoformat(),
            "count": len(all_markets),
            "tag_id": tag_id,
            "markets": all_markets
        }
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved markets to {filename}")
    
    return all_markets

