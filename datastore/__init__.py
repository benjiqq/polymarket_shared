"""
Datastore package initialization helpers.

We expose commonly used database utility functions here so that scripts can
import them with simple ``from datastore import ...`` statements. This keeps
imports short while still reusing the implementations that live in
``datastore.datastore``.
"""

from .datastore import (
    get_db_connection,
    create_tables,
    save_market_to_db,
    save_orderbook_to_db,
    fetch_and_save_markets,
    list_markets,
    get_market,
    get_markets,
    delete_market,
    clear_database,
    fetch_orderbook_for_market,
    get_stats,
)

# Explicit export list to make the module intent clear and help static analyzers.
__all__ = [
    "get_db_connection",
    "create_tables",
    "save_market_to_db",
    "save_orderbook_to_db",
    "fetch_and_save_markets",
    "list_markets",
    "get_market",
    "get_markets",
    "delete_market",
    "clear_database",
    "fetch_orderbook_for_market",
    "get_stats",
]

