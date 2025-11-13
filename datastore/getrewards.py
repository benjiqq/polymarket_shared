"""
Fetch rewards data from Polymarket rewards page
Gets real-time liquidity rewards information
"""

import requests
import logging
import json
import re
from typing import List, Dict, Any
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Polymarket rewards URL base
REWARDS_URL_BASE = "https://polymarket.com/rewards?id=rate_per_day&desc=true&q="

# Polymarket rewards API endpoint
REWARDS_API_URL = "https://polymarket.com/api/rewards/markets"


def fetch_rewards_html(page: int = 1):
    """
    Fetch HTML content from Polymarket rewards page.
    Automatically handles gzip compressed responses.
    
    Args:
        page: Page number to fetch (default: 1)
    
    Returns:
        str: HTML content of the page, or None if error
    """
    try:
        # Build URL with page parameter
        # Base URL: https://polymarket.com/rewards?id=rate_per_day&desc=true&q=
        # Add page parameter if page > 1, otherwise use base URL as-is
        if page > 1:
            url = f"{REWARDS_URL_BASE}&page={page}"
        else:
            url = REWARDS_URL_BASE
        logger.info(f"Fetching rewards page {page} from: {url}")
        
        # Set headers to mimic a browser request
        # Note: requests library automatically handles gzip decompression
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Make request with timeout
        # requests automatically decompresses gzip/deflate/br if Accept-Encoding not set
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Ensure we got text content
        response.encoding = response.apparent_encoding or 'utf-8'
        html_content = response.text
        
        logger.info(f"Successfully fetched page {page} ({len(html_content)} characters)")
        
        return html_content
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching rewards page {page}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error on page {page}: {e}")
        return None


def save_rewards_html(html_content, filename='rewards.html'):
    """
    Save HTML content to a file.
    
    Args:
        html_content: HTML string to save
        filename: Output filename (default: rewards.html)
    
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"Saved rewards HTML to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving HTML: {e}")
        return False


def save_rewards_json(markets, filename=None):
    """
    Save rewards market data to a JSON file in the data folder with a date.
    
    Args:
        markets: List of market dictionaries with reward info
        filename: Output filename (default: auto-generated with date in data folder)
    
    Returns:
        str: Path to saved file, or None if error
    """
    try:
        import os
        from datetime import datetime, timezone
        
        # Create data directory if it doesn't exist
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        
        # Generate filename with date if not provided
        if filename is None:
            now = datetime.now(timezone.utc)
            date_str = now.strftime('%Y-%m-%d')
            filename = os.path.join(data_dir, f'rewards_{date_str}.json')
        
        # Add metadata
        output = {
            'fetched_at': datetime.now(timezone.utc).isoformat(),
            'total_markets': len(markets),
            'markets': markets
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(markets)} markets to {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error saving JSON: {e}")
        return None


def load_rewards_json(filename=None):
    """
    Load rewards market data from a JSON file.
    If no filename specified, loads the most recent rewards file from data folder.
    
    Args:
        filename: Input filename (default: most recent in data folder)
    
    Returns:
        dict: Dictionary with 'fetched_at', 'total_markets', and 'markets' keys
              Returns None if file doesn't exist or error occurs
    """
    try:
        import os
        import glob
        
        # If no filename specified, find the most recent rewards file
        if filename is None:
            data_dir = 'data'
            pattern = os.path.join(data_dir, 'rewards_*.json')
            reward_files = glob.glob(pattern)
            
            if not reward_files:
                logger.warning(f"No rewards files found in {data_dir}/ folder")
                logger.info("Run getrewards.py first to fetch rewards data")
                return None
            
            # Get the most recent file (sorted by filename, which includes date)
            filename = max(reward_files)
            logger.info(f"Using most recent rewards file: {filename}")
        
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {data.get('total_markets', 0)} markets from {filename}")
        logger.info(f"Data fetched at: {data.get('fetched_at', 'unknown')}")
        
        return data
        
    except FileNotFoundError:
        logger.warning(f"Rewards file not found: {filename}")
        return None
    except Exception as e:
        logger.error(f"Error loading JSON: {e}")
        return None


def parse_rewards_page(html_content):
    """
    Parse the rewards page HTML to extract market information.
    Extracts JSON data embedded in the Next.js page.
    
    Args:
        html_content: HTML string from rewards page
    
    Returns:
        List of market dictionaries with reward info
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Log page title to verify we got the right page
        title = soup.find('title')
        if title:
            logger.info(f"Page title: {title.get_text()}")
        
        # Find the Next.js data script tag
        script_tag = soup.find('script', id='__NEXT_DATA__')
        
        if not script_tag:
            logger.error("Could not find __NEXT_DATA__ script tag")
            return []
        
        # Parse JSON data
        try:
            next_data = json.loads(script_tag.string)
            logger.info("Successfully parsed Next.js data")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
            return []
        
        # Navigate through the data structure to find markets
        # The data is in props.pageProps.dehydratedState.queries
        try:
            queries = next_data['props']['pageProps']['dehydratedState']['queries']
            
            # Find the query that contains market data
            # Try multiple possible paths in the data structure
            markets_data = None
            
            for query in queries:
                query_key = query.get('queryKey', [])
                query_key_str = str(query_key) if query_key else ''
                
                # Look for rewards-related queries
                if '/api/rewards' in query_key_str or 'rewards' in query_key_str.lower():
                    state = query.get('state', {})
                    data = state.get('data', {})
                    
                    # Try multiple possible data paths
                    if isinstance(data, dict):
                        # Path 1: state.data.data (array)
                        if 'data' in data and isinstance(data['data'], list):
                            markets_data = data['data']
                            logger.info(f"Found markets in state.data.data (query: {query_key_str[:100]})")
                            break
                        # Path 2: state.data (array directly)
                        elif isinstance(data, list):
                            markets_data = data
                            logger.info(f"Found markets in state.data (query: {query_key_str[:100]})")
                            break
                    # Path 3: state.data is already a list
                    elif isinstance(data, list):
                        markets_data = data
                        logger.info(f"Found markets directly in state.data (query: {query_key_str[:100]})")
                        break
            
            # If still not found, try searching all queries more broadly
            if not markets_data:
                logger.warning("Could not find markets in rewards queries, searching all queries...")
                for query in queries:
                    state = query.get('state', {})
                    data = state.get('data', {})
                    
                    # Check if this looks like market data
                    if isinstance(data, dict) and 'data' in data:
                        potential_markets = data.get('data', [])
                        if isinstance(potential_markets, list) and len(potential_markets) > 0:
                            # Check if first item looks like a market object
                            first_item = potential_markets[0] if potential_markets else {}
                            if isinstance(first_item, dict) and ('question' in first_item or 'market_id' in first_item):
                                markets_data = potential_markets
                                logger.info(f"Found markets by searching all queries (query: {str(query.get('queryKey', []))[:100]})")
                                break
            
            if not markets_data:
                logger.error("Could not find markets data in JSON")
                logger.debug(f"Available query keys: {[str(q.get('queryKey', []))[:100] for q in queries[:5]]}")
                return []
            
            logger.info(f"Found {len(markets_data)} markets on this page")
            
            # Extract relevant market information
            # Include ALL markets that appear on the rewards page, even if reward_rate is 0
            markets = []
            for market in markets_data:
                # Skip if this doesn't look like a market object
                if not isinstance(market, dict):
                    continue
                
                # Extract reward information from rewards_config
                rewards_config = market.get('rewards_config', [])
                reward_rate_usd = 0
                reward_total = 0
                reward_asset = None
                
                if rewards_config and isinstance(rewards_config, list) and len(rewards_config) > 0:
                    # Get the first (active) rewards config
                    config = rewards_config[0]
                    reward_rate_usd = config.get('rate_per_day', 0) or 0
                    reward_total = config.get('total_rewards', 0) or 0
                    reward_asset = config.get('asset_address')
                
                # Also check for direct reward fields (some markets might have different structure)
                if reward_rate_usd == 0:
                    reward_rate_usd = market.get('rate_per_day', 0) or 0
                
                # Also check for reward_rate_usd directly on the market
                if reward_rate_usd == 0:
                    reward_rate_usd = market.get('reward_rate_usd', 0) or 0
                
                # Extract all relevant market information
                market_info = {
                    'market_id': market.get('market_id'),
                    'condition_id': market.get('condition_id'),
                    'question': market.get('question'),
                    'market_slug': market.get('market_slug'),
                    'volume_24hr': market.get('volume_24hr', 0),
                    'rewards_config': rewards_config,
                    'reward_rate_usd': reward_rate_usd,  # USD per day (can be 0)
                    'reward_total_usd': reward_total,  # Total rewards (if available)
                    'reward_asset': reward_asset,  # Asset address (usually USDC)
                    'rewards_max_spread': market.get('rewards_max_spread'),
                    'rewards_min_size': market.get('rewards_min_size'),
                    'spread': market.get('spread'),
                    'market_competitiveness': market.get('market_competitiveness'),
                    'tokens': market.get('tokens', [])
                }
                
                # Include market even if reward_rate is 0 (they still appear on rewards page)
                markets.append(market_info)
            
            return markets
            
        except (KeyError, TypeError) as e:
            logger.error(f"Error navigating JSON structure: {e}")
            return []
        
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return []


def fetch_rewards_api(limit: int = 100, cursor: str = None) -> Dict[str, Any]:
    """
    Fetch rewards data from the Polymarket API endpoint.
    
    Args:
        limit: Number of markets to fetch per request (default: 100, max: 100)
        cursor: Cursor for pagination (default: None for first page)
    
    Returns:
        Dictionary with 'data' (list of markets), 'next_cursor', 'count', 'total_count'
    """
    try:
        params = {
            'limit': min(limit, 100)  # API max is 100
        }
        if cursor:
            params['cursor'] = cursor
        
        logger.info(f"Fetching from API: {REWARDS_API_URL}")
        response = requests.get(REWARDS_API_URL, params=params, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"API returned {result.get('count', 0)} markets (total: {result.get('total_count', 0)})")
        
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching rewards API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                logger.error(f"API error: {error_data}")
            except Exception:
                logger.error(f"HTTP {e.response.status_code}: {e.response.text}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching API: {e}")
        return {}


def fetch_all_rewards_pages(method: str = 'api', max_pages: int = 27) -> List[Dict[str, Any]]:
    """
    Fetch all rewards pages and combine markets.
    
    Uses either API method (preferred) or HTML scraping (fallback).
    
    Args:
        method: 'api' to use API endpoint, 'html' to scrape HTML pages
        max_pages: Maximum number of pages to fetch (for HTML method only, default: 27)
    
    Returns:
        List of all market dictionaries combined from all pages
    """
    all_markets = []
    
    if method == 'api':
        # Use API with cursor-based pagination
        logger.info("Fetching all rewards via API...")
        cursor = None
        
        while True:
            result = fetch_rewards_api(limit=100, cursor=cursor)
            
            if not result or 'data' not in result:
                logger.warning("API returned invalid response")
                break
            
            markets = result.get('data', [])
            if markets:
                all_markets.extend(markets)
                logger.info(f"Fetched {len(markets)} markets (total so far: {len(all_markets)})")
            else:
                logger.info("No more markets, reached end")
                break
            
            # Check if there's a next page
            next_cursor = result.get('next_cursor')
            if not next_cursor:
                logger.info("No next cursor, reached end")
                break
            
            cursor = next_cursor
        
        logger.info(f"Total markets fetched via API: {len(all_markets)}")
        return all_markets
    
    else:
        # Use HTML scraping method (fallback)
        logger.info(f"Fetching up to {max_pages} pages of rewards data via HTML...")
        
        for page in range(1, max_pages + 1):
            html = fetch_rewards_html(page)
            
            if not html:
                logger.warning(f"Failed to fetch page {page}, stopping")
                break
            
            # Parse this page
            markets = parse_rewards_page(html)
            
            if markets:
                all_markets.extend(markets)
                logger.info(f"Page {page}: Found {len(markets)} markets (total so far: {len(all_markets)})")
            else:
                logger.warning(f"Page {page}: No markets found, may have reached end")
                # If we get a page with no markets, we've probably reached the end
                break
        
        return all_markets


def main():
    """
    Main function to fetch and display rewards page.
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch rewards data from Polymarket rewards page",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all pages (default: 27 pages)
  python getrewards.py

  # Fetch specific number of pages
  python getrewards.py --pages 10

  # Fetch just page 1
  python getrewards.py --pages 1
        """
    )
    
    parser.add_argument(
        '--pages',
        type=int,
        default=27,
        help='Number of pages to fetch for HTML method (default: 27)'
    )
    parser.add_argument(
        '--method',
        type=str,
        choices=['api', 'html'],
        default='api',
        help='Method to fetch: api (preferred) or html (fallback)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("POLYMARKET REWARDS FETCHER")
    logger.info("="*60)
    logger.info(f"Using method: {args.method.upper()}")
    
    # Fetch all pages
    markets = fetch_all_rewards_pages(method=args.method, max_pages=args.pages)
    
    if markets:
        logger.info(f"\nSuccessfully extracted {len(markets)} total markets with rewards\n")
        
        # Save to JSON file
        save_rewards_json(markets)
        
        # Display first 10 markets
        for i, market in enumerate(markets[:10], 1):
            question = market['question']
            slug = market['market_slug']
            volume = market['volume_24hr']
            spread = market['rewards_max_spread']
            min_size = market['rewards_min_size']
            reward_rate = market.get('reward_rate_usd', 0)
            reward_total = market.get('reward_total_usd', 0)
            
            logger.info(f"{i}. {question[:50]}...")
            logger.info(f"   Slug: {slug}")
            logger.info(f"   24h Volume: ${volume:.2f}")
            logger.info(f"   Max Spread: {spread}%")
            logger.info(f"   Min Size: ${min_size}")
            logger.info(f"   Reward Rate: ${reward_rate}/day (USD)")
            if reward_total > 0:
                logger.info(f"   Total Rewards: ${reward_total} (USD)")
            logger.info("")
        
        if len(markets) > 10:
            logger.info(f"... and {len(markets) - 10} more markets")
    else:
        logger.error("Failed to fetch rewards data")
    
    logger.info("\n" + "="*60)


if __name__ == "__main__":
    main()

