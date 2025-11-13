"""
Market Update Service
Continuously fetches and updates market data from Polymarket
"""

import logging
import time
import threading
import requests

# Import market data helpers with compatibility for both package and script usage.
try:
    from .marketdata import (
        fetch_active_markets,
        fetch_active_markets_from_events,
        get_markets_gamma,
        get_markets_by_category,
    )
except ImportError:  # pragma: no cover - fallback for direct execution context
    from marketdata import (  # type: ignore
        fetch_active_markets,
        fetch_active_markets_from_events,
        get_markets_gamma,
        get_markets_by_category,
    )

from datastore import save_orderbook_to_db, save_market_to_db, get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class MarketUpdateService:
    """
    Service that continuously fetches and updates market data.
    """
    
    def __init__(self, events_interval=60, orderbook_interval=30, update_orderbooks=True):
        """
        Initialize the market update service.
        
        Args:
            events_interval: Seconds between event updates (default: 60)
            orderbook_interval: Seconds between orderbook updates (default: 30)
            update_orderbooks: If True, also update orderbook data (default: True)
        """
        self.events_interval = events_interval
        self.orderbook_interval = orderbook_interval
        self.update_orderbooks = update_orderbooks
        self.running = False
        self.events_thread = None
        self.orderbook_thread = None
        self.markets = []
        self.lock = threading.Lock()
        
        logger.info(f"Market Update Service initialized")
        logger.info(f"  Events update interval: {events_interval}s")
        logger.info(f"  Orderbook update interval: {orderbook_interval}s")
        logger.info(f"  Update orderbooks: {update_orderbooks}")
    
    def start(self):
        """
        Start the market update service in background threads.
        """
        if self.running:
            logger.warning("Market update service is already running")
            return
        
        # Log existing market count at startup
        self._log_existing_market_count()
        
        self.running = True
        
        # Start events update thread
        self.events_thread = threading.Thread(target=self._events_loop, daemon=True)
        self.events_thread.start()
        logger.info("Events update thread started")
        
        # Start orderbook update thread if enabled
        if self.update_orderbooks:
            self.orderbook_thread = threading.Thread(target=self._orderbook_loop, daemon=True)
            self.orderbook_thread.start()
            logger.info("Orderbook update thread started")
        
        logger.info("Market update service started")
    
    def stop(self):
        """
        Stop the market update service.
        """
        if not self.running:
            logger.warning("Market update service is not running")
            return
        
        self.running = False
        
        # Wait for threads to finish
        if self.events_thread:
            self.events_thread.join(timeout=5)
        if self.orderbook_thread:
            self.orderbook_thread.join(timeout=5)
        
        logger.info("Market update service stopped")
    
    def _log_existing_market_count(self):
        """
        Log how many markets are already known in the database.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM markets")
            total_count = cursor.fetchone()[0]
            
            # Get breakdown by status
            cursor.execute("SELECT COUNT(*) FROM markets WHERE active = 1")
            active_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM markets WHERE active = 0")
            inactive_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM markets WHERE closed = 1")
            closed_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM markets WHERE archived = 1")
            archived_count = cursor.fetchone()[0]
            
            conn.close()
            
            logger.info(f"Database market breakdown:")
            logger.info(f"  Total markets: {total_count}")
            logger.info(f"  Active: {active_count}")
            logger.info(f"  Inactive: {inactive_count}")
            logger.info(f"  Closed: {closed_count}")
            logger.info(f"  Archived: {archived_count}")
            
        except Exception as e:
            logger.warning(f"Could not count existing markets: {e}")
            try:
                conn.close()
            except Exception:
                pass

    def _market_exists(self, market_id):
        """
        Check if a market already exists in the database.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM markets WHERE id = ?", (market_id,))
            row = cursor.fetchone()
            conn.close()
            return row is not None
        except Exception as e:
            logger.warning(f"Could not check existence for market {market_id}: {e}")
            try:
                conn.close()
            except Exception:
                pass
            return False

    def _events_loop(self):
        """
        Events update loop that runs in background thread.
        """
        logger.info("Events update loop started")
        
        while self.running:
            try:                
                # Fetch latest markets from events endpoint
                logger.info("Updating markets from events endpoint...")
                self.update_from_events(limit=10000)
                
                # Sleep until next update
                time.sleep(self.events_interval)
                
            except Exception as e:
                logger.error(f"Error in events update loop: {e}")
                # Continue running even if there's an error
                time.sleep(self.events_interval)
        
        logger.info("Events update loop stopped")
    
    def _orderbook_loop(self):
        """
        Orderbook update loop that runs in background thread.
        """
        logger.info("Orderbook update loop started")
        
        while self.running:
            try:
                # Get current markets and update orderbooks
                markets = self.get_markets()
                if markets:
                    logger.info("Updating orderbooks...")
                    self._update_orderbooks(markets)
                else:
                    logger.info("No markets available for orderbook update")
                
                # Sleep until next update
                time.sleep(self.orderbook_interval)
                
            except Exception as e:
                logger.error(f"Error in orderbook update loop: {e}")
                # Continue running even if there's an error
                time.sleep(self.orderbook_interval)
        
        logger.info("Orderbook update loop stopped")
    
    
    def _update_orderbooks(self, markets):
        """
        Fetch and update orderbook data for all markets.
        
        Args:
            markets: List of market dictionaries
        """
        try:
            logger.info("\n" + "="*60)
            logger.info("ORDERBOOK UPDATE - Starting orderbook update")
            logger.info("="*60)
            
            total_tokens = 0
            successful_updates = 0
            
            for i, market in enumerate(markets, 1):
                market_id = market.get('id', 'Unknown')
                question = market.get('question', 'Unknown')
                
                # Parse clobTokenIds from Gamma API
                import json
                clob_token_ids = market.get('clobTokenIds', '[]')
                try:
                    token_ids = json.loads(clob_token_ids) if isinstance(clob_token_ids, str) else clob_token_ids
                    outcomes = json.loads(market.get('outcomes', '[]')) if isinstance(market.get('outcomes'), str) else market.get('outcomes', [])
                except:
                    token_ids = []
                    outcomes = []
                
                # Log progress every 50 markets
                if i % 50 == 0 or i == len(markets):
                    logger.info(f"Updating orderbooks - market {i}/{len(markets)}: {question[:60]}...")
                
                # Fetch orderbook for each token
                for j, token_id in enumerate(token_ids):
                    outcome = outcomes[j] if j < len(outcomes) else f"Outcome {j+1}"
                    
                    if not token_id:
                        continue
                    
                    total_tokens += 1
                    
                    # Fetch orderbook from CLOB API
                    url = "https://clob.polymarket.com/book"
                    params = {"token_id": token_id}
                    
                    try:
                        response = requests.get(url, params=params, timeout=5)
                        
                        if response.status_code == 200:
                            orderbook_data = response.json()
                            
                            # Count bids and asks
                            bids = orderbook_data.get('bids', [])
                            asks = orderbook_data.get('asks', [])
                            
                            # Save to database
                            if save_orderbook_to_db(market_id, token_id, orderbook_data):
                                successful_updates += 1
                        else:
                            logger.warning(f"Market {i} token {j+1}: No orderbook (status: {response.status_code})")
                    
                    except requests.Timeout:
                        logger.warning(f"Market {i} token {j+1}: Timeout")
                    except requests.RequestException as e:
                        logger.warning(f"Market {i} token {j+1}: Request error: {e}")
                    except Exception as e:
                        logger.warning(f"Market {i} token {j+1}: Error: {e}")
            
            logger.info("\n" + "="*60)
            logger.info(f"ORDERBOOK UPDATE COMPLETE")
            logger.info(f"  Total tokens: {total_tokens}")
            logger.info(f"  Successful updates: {successful_updates}")
            logger.info(f"  Failed: {total_tokens - successful_updates}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Error updating orderbooks: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def get_markets(self):
        """
        Get current market data.
        
        Returns:
            List of market dictionaries
        """
        with self.lock:
            return self.markets.copy()
    
    def update_from_events(self, limit=10):
        """
        Update markets from events endpoint (newest markets first).
        
        Args:
            limit: Number of markets to fetch
        """
        try:
            logger.info(f"Updating markets from events endpoint (limit: {limit})...")
            new_markets = fetch_active_markets_from_events(limit=limit)
            
            # Save markets to database and count new vs existing
            saved_count = 0
            new_count = 0
            existing_count = 0
            for market in new_markets:
                market_id = market.get('id')
                known_before = self._market_exists(market_id) if market_id else False
                if save_market_to_db(market):
                    saved_count += 1
                    if known_before:
                        existing_count += 1
                    else:
                        new_count += 1
            
            logger.info(
                f"Saved {saved_count}/{len(new_markets)} markets to database (new: {new_count}, existing: {existing_count})"
            )
            
            with self.lock:
                self.markets = new_markets
            
            logger.info(f"Events markets updated: {len(new_markets)} markets")
            
        except Exception as e:
            logger.error(f"Error updating events markets: {e}")
    
    def update_by_category(self, tag_id, limit=10):
        """
        Update markets filtered by category.
        
        Args:
            tag_id: Category tag ID
            limit: Number of markets to fetch
        """
        try:
            logger.info(f"Updating markets for category {tag_id} (limit: {limit})...")
            new_markets = get_markets_by_category(tag_id=tag_id, total_markets=limit)
            
            with self.lock:
                self.markets = new_markets
            
            logger.info(f"Category markets updated: {len(new_markets)} markets")
            
        except Exception as e:
            logger.error(f"Error updating category markets: {e}")
    
    def is_running(self):
        """
        Check if service is running.
        
        Returns:
            True if service is running, False otherwise
        """
        return self.running


def main():
    """
    Standalone mode for testing the market update service.
    """
    logger.info("="*60)
    logger.info("MARKET UPDATE SERVICE - STANDALONE MODE")
    logger.info("="*60)
    
    # Create and start service
    service = MarketUpdateService(events_interval=30, orderbook_interval=15)
    service.start()
    
    try:
        # Run for a while to demonstrate
        logger.info("Service running. Press Ctrl+C to stop...")
        while True:
            markets = service.get_markets()
            logger.info(f"Current markets: {len(markets)}")
            
            if markets:
                logger.info(f"Sample market: {markets[0].get('question', 'Unknown')[:60]}...")
            
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("\nStopping service...")
        service.stop()
        logger.info("Service stopped")


if __name__ == "__main__":
    main()

