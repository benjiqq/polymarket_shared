import argparse
import logging
from typing import List, Dict, Any

from trading import initialize_client, get_address_from_key, key, POLYMARKET_PROXY_ADDRESS


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def _format_price(p: Any) -> str:
    try:
        return f"{float(p):.4f}"
    except Exception:
        return str(p)


def _format_size(s: Any) -> str:
    try:
        return f"{float(s):.4f}"
    except Exception:
        return str(s)


def _format_usd(usd: float) -> str:
    """Format USD amount."""
    try:
        return f"${usd:,.2f}"
    except Exception:
        return "$0.00"


def _calculate_usd_amount(order: Dict[str, Any]) -> float:
    """Calculate USD amount for an order based on remaining size and price."""
    try:
        original_size = float(order.get('original_size') or order.get('size') or 0)
        filled = float(order.get('filled') or 0)
        price = float(order.get('price') or 0)
        
        remaining_size = original_size - filled
        usd_amount = remaining_size * price
        return max(0.0, usd_amount)  # Ensure non-negative
    except (ValueError, TypeError):
        return 0.0


def filter_orders(orders: List[Dict[str, Any]], market_id: str = None, token_id: str = None) -> List[Dict[str, Any]]:
    if market_id:
        orders = [o for o in orders if str(o.get('market_id') or o.get('marketId') or '') == str(market_id)]
    if token_id:
        orders = [o for o in orders if str(o.get('token_id') or o.get('tokenId') or '') == str(token_id)]
    return orders


def main():
    parser = argparse.ArgumentParser(description="Show open orders from Polymarket")
    parser.add_argument('--market-id', help='Filter by market id', default=None)
    parser.add_argument('--token-id', help='Filter by token id', default=None)
    parser.add_argument('--limit', type=int, default=200, help='Max number of orders to show')
    args = parser.parse_args()

    # Decide signature type based on proxy configuration
    eoa_address = get_address_from_key(key)
    if POLYMARKET_PROXY_ADDRESS and POLYMARKET_PROXY_ADDRESS != eoa_address:
        client = initialize_client(signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)
        logger.info("Using proxy wallet (signature_type=2)")
    else:
        client = initialize_client(signature_type=0)
        logger.info("Using direct EOA (signature_type=0)")

    client.set_api_creds(client.create_or_derive_api_creds())

    try:
        orders = client.get_orders() or []
    except Exception as e:
        logger.error(f"Failed to fetch orders: {e}")
        return

    orders = filter_orders(orders, args.market_id, args.token_id)

    logger.info(f"Open Orders: {len(orders)}")
    if not orders:
        return

    # Calculate USD amounts for all orders (before limiting display)
    total_usd = 0.0
    for o in orders:
        usd_amount = _calculate_usd_amount(o)
        total_usd += usd_amount

    # Prepare orders for display (with USD amounts)
    orders_with_usd = []
    for o in orders[: args.limit]:
        usd_amount = _calculate_usd_amount(o)
        orders_with_usd.append((o, usd_amount))

    # Header
    print(f"\n{'ID':<24} {'Side':<6} {'Price':<10} {'Size':<10} {'Filled':<10} {'USD':<12} {'Status':<10} {'TokenId':<38} {'Market'}")
    print("-" * 160)

    for o, usd_amount in orders_with_usd:
        oid = str(o.get('id') or o.get('orderId') or '')
        side = str(o.get('side') or '')
        price = _format_price(o.get('price'))
        size = _format_size(o.get('original_size') or o.get('size'))
        filled = _format_size(o.get('filled') or 0)
        usd_str = _format_usd(usd_amount)
        status = str(o.get('status') or '')
        tok = str(o.get('token_id') or o.get('tokenId') or '')
        market = str(o.get('market_slug') or o.get('market') or o.get('market_slug') or '')
        print(f"{oid:<24} {side:<6} {price:<10} {size:<10} {filled:<10} {usd_str:<12} {status:<10} {tok[:36]:<38} {market[:60]}")

    # Print total
    print("-" * 160)
    if len(orders) > args.limit:
        print(f"{'Total Outstanding USD (all orders):':<82} {_format_usd(total_usd)}")
        print(f"Note: Showing {args.limit} of {len(orders)} orders")
    else:
        print(f"{'Total Outstanding USD:':<82} {_format_usd(total_usd)}")


if __name__ == '__main__':
    main()
