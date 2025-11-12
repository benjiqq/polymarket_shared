from typing import Dict, Iterable, List, Tuple

import requests


GAMMA_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events"


def _fetch_event_markets(event_id: str) -> List[Dict]:
    """
    Fetch full event details when the search payload only returned a lightweight stub.
    The expanded event includes the nested markets list that exposes real market ids.
    """
    try:
        response = requests.get(f"{GAMMA_EVENT_URL}/{event_id}", timeout=10)
    except requests.RequestException as exc:
        print(f"Warning: could not fetch event {event_id}: {exc}")
        return []

    if response.status_code != 200:
        print(f"Warning: event lookup for {event_id} returned {response.status_code}")
        return []

    event_payload = response.json()
    markets = event_payload.get("markets") or []
    if not markets:
        print(f"Warning: event {event_id} returned no markets.")
    return markets


def _resolve_markets(item: Dict) -> Tuple[bool, Iterable[Dict]]:
    """
    Decide whether the search item represents a full market or an event.
    Return a tuple (is_event, markets_iterable).
    """
    nested = item.get("markets") or []
    if nested:
        return True, nested

    # Plain markets expose ids directly and can be printed as-is.
    if item.get("marketId") or item.get("conditionId"):
        return False, [item]

    event_id = item.get("id")
    if event_id:
        return True, _fetch_event_markets(str(event_id))

    return False, []


def _print_market(market: Dict, prefix: str = "") -> None:
    """
    Print a single market line with its id and question/title.
    """
    market_id = market.get("id") or market.get("marketId")
    if not market_id:
        print(f"{prefix}Market <unknown id> {market.get('question') or market.get('title') or 'Untitled Market'}")
        return

    market_title = market.get("question") or market.get("title") or "Untitled Market"
    print(f"{prefix}Market {market_id} {market_title}")


def get_markets_for_query(query: str) -> List[Dict]:
    """
    Fetch all markets that match the query string.
    Returns a list of market dictionaries with the id and question/title fields.
    """
    params = {
        "q": query,
        "search_tags": "false",
        "search_profiles": "false",
        "limit_per_type": "50",
    }

    try:
        response = requests.get(GAMMA_SEARCH_URL, params=params, timeout=10)
    except requests.RequestException as exc:
        print(f"Search failed: {exc}")
        return []

    if response.status_code != 200:
        print(f"Search failed with status {response.status_code}: {response.text}")
        return []

    payload = response.json()
    items = payload.get("markets") or payload.get("events", [])
    if not items:
        return []

    flattened: List[Dict] = []
    for item in items:
        is_event, resolved_markets = _resolve_markets(item)
        if is_event:
            resolved_list = list(resolved_markets)
            if not resolved_list:
                continue
            flattened.extend(resolved_list)
        else:
            flattened.extend(resolved_markets)

    collected: List[Dict] = []
    for market in flattened:
        market_id = market.get("id") or market.get("marketId")
        if not market_id:
            continue
        title = market.get("question") or market.get("title") or "Untitled Market"
        collected.append({"id": str(market_id), "question": title})

    return collected


def sync_markets_to_sheet(markets: List[Dict], query: str) -> None:
    """
    Push the collected markets into the Google Sheet and refresh derived metrics.
    """
    # Write the raw list first so the sheet always shows the newest search results.
    write_market_list_to_sheet(markets, query)
    # Then enrich each market row with live orderbook metrics.
    #update_orderbook_metrics_in_sheet()


def main() -> None:
    """
    Query the Polymarket Gamma API for markets that match the hard-coded search term.
    The results are written to the shared Google Sheet so downstream tooling can work
    with the freshest search snapshot.
    """
    query = "XRP"

    markets = get_markets_for_query(query)

    if not markets:
        print("No markets found for the query.")
        return

    print(markets)

    # sync_markets_to_sheet(markets, query)
    # print(f"Wrote {len(markets)} markets to the sheet for query '{query}'.")


if __name__ == "__main__":
    main()

